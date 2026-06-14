package redisx

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/cachex/utils"
	"gopkg.in/yaml.v3"
)

// activeConfigCache is the per-path in-memory snapshot used by ParseFile so
// that hot-reload does not re-read the YAML from disk on every call.
var activeConfigCache sync.Map // path → *cachex.Config

// ParseFile loads a cachex Config from a YAML file. It returns the config and
// the configKey embedded after '#' (or "default" when omitted).
func ParseFile(path string) (*cachex.Config, string, error) {
	if val, ok := activeConfigCache.Load(path); ok {
		cfg := val.(*cachex.Config)
		_, configKey := splitPath(path)
		if configKey == "" {
			configKey = "default"
		}
		return cfg, configKey, nil
	}
	return parseFileFromDisk(path)
}

// parseFileFromDisk reads the YAML file and returns the matching config. When
// the file contains a top-level map of named configs, the '#configKey' suffix
// is used to pick one; otherwise the (single) config is returned under the
// "default" key.
//
// All string fields are passed through utils.ExpandEnvVars so that
// ${env:VAR} and ${env:VAR:-default} placeholders are resolved at load time.
func parseFileFromDisk(path string) (*cachex.Config, string, error) {
	filePath, configKey := splitPath(path)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, "", fmt.Errorf("redisx: read config file: %w", err)
	}
	// Expand env placeholders before YAML unmarshal so downstream fields see
	// the resolved string.
	expanded := utils.ExpandEnvVars(string(data))

	var all map[string]*cachex.Config
	if err := yaml.Unmarshal([]byte(expanded), &all); err != nil {
		var single cachex.Config
		if err2 := yaml.Unmarshal([]byte(expanded), &single); err2 != nil {
			return nil, "", fmt.Errorf("redisx: parse yaml: %w", err)
		}
		if configKey == "" {
			configKey = "default"
		}
		activeConfigCache.Store(path, &single)
		return &single, configKey, nil
	}

	if configKey == "" {
		// pick the first available key
		for k, v := range all {
			activeConfigCache.Store(path, v)
			return v, k, nil
		}
		return nil, "", fmt.Errorf("redisx: empty config file")
	}

	cfg, ok := all[configKey]
	if !ok {
		return nil, "", fmt.Errorf("redisx: config key %q not found", configKey)
	}
	activeConfigCache.Store(path, cfg)
	return cfg, configKey, nil
}

// splitPath returns (filePath, configKey) where configKey is the suffix after
// '#' (or empty when no '#' is present).
func splitPath(path string) (string, string) {
	if i := strings.LastIndex(path, "#"); i > 0 {
		return path[:i], path[i+1:]
	}
	return path, ""
}

// buildCacheKey returns the stable cache key used by PPS/PPC. The format is
// "producer:<mode>:<configKey>:<fingerprint>" so that distinct modes, keys,
// and config bodies all produce disjoint entries.
func buildCacheKey(mode, configKey string, cfg *cachex.Config) string {
	return fmt.Sprintf("producer:%s:%s:%s", mode, configKey, utils.ConfigFingerprint(cfg))
}

// Reload re-parses the YAML at path and atomically swaps the underlying pool.
// The old pool is closed on a background goroutine after a short grace period
// so that in-flight requests do not see an abrupt close.
func Reload(path string) error {
	log.Printf("[redisx] hot-reloading config from %s ...", path)

	cfg, key, err := parseFileFromDisk(path)
	if err != nil {
		return fmt.Errorf("reload parse error: %w", err)
	}

	// Determine old cache key (if any) so we can schedule its late close.
	oldCacheKey := ""
	if val, ok := activeConfigCache.Load(path); ok {
		// The old cfg was stored under the same path; rebuild its key under
		// the mode we are about to use. For single mode we use "single".
		oldCfg := val.(*cachex.Config)
		_, oldKey := splitPath(path)
		if oldKey == "" {
			oldKey = "default"
		}
		_ = oldCfg // fingerprint computed below
		oldCacheKey = buildCacheKey("single", oldKey, oldCfg)
	}

	newCacheKey := buildCacheKey("single", key, cfg)
	// create / fetch the new pool via the producer helper.
	if _, err := getOrCreateSingleProducer(newCacheKey, cfg); err != nil {
		return fmt.Errorf("reload build new pool error: %w", err)
	}

	if oldCacheKey != "" && oldCacheKey != newCacheKey {
		if val, ok := singleProducerCache.Load(oldCacheKey); ok {
			oldClient := val.(*redisClientHolder)
			go func() {
				time.Sleep(5 * time.Second)
				log.Printf("[redisx] hot-reload: delayed close on old pool [key=%s]", oldCacheKey)
				_ = oldClient.Close()
				singleProducerCache.Delete(oldCacheKey)
				producerLocks.Delete(oldCacheKey)
			}()
		}
	}

	log.Printf("[redisx] hot-reload success. New connections are ready.")
	return nil
}

// Shutdown gracefully closes every pooled client (single + cluster) in
// background goroutines. It returns after either all closes complete or the
// provided context deadline is hit.
func Shutdown(ctx context.Context) {
	var wg sync.WaitGroup

	singleProducerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, h *redisClientHolder) {
			defer wg.Done()
			log.Printf("[redisx] closing single producer [key=%s]", k)
			_ = h.Close()
			singleProducerCache.Delete(k)
			producerLocks.Delete(k)
		}(key.(string), value.(*redisClientHolder))
		return true
	})

	clusterProducerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, h *redisClusterHolder) {
			defer wg.Done()
			log.Printf("[redisx] closing cluster producer [key=%s]", k)
			_ = h.Close()
			clusterProducerCache.Delete(k)
			producerLocks.Delete(k)
		}(key.(string), value.(*redisClusterHolder))
		return true
	})

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Println("[redisx] all connections closed gracefully")
	case <-ctx.Done():
		log.Println("[redisx] shutdown timed out, some connections may not be fully closed")
	}
}

// HealthCheck returns a map of cacheKey → status string. Every pooled client
// is PINGed with a 2s timeout. Status is "healthy" or "unhealthy: <reason>".
func HealthCheck() map[string]string {
	result := make(map[string]string)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	singleProducerCache.Range(func(key, value any) bool {
		h := value.(*redisClientHolder)
		if err := h.Ping(ctx); err != nil {
			result[key.(string)] = "unhealthy: " + err.Error()
		} else {
			result[key.(string)] = "healthy"
		}
		return true
	})

	clusterProducerCache.Range(func(key, value any) bool {
		h := value.(*redisClusterHolder)
		if err := h.Ping(ctx); err != nil {
			result[key.(string)] = "unhealthy: " + err.Error()
		} else {
			result[key.(string)] = "healthy"
		}
		return true
	})

	return result
}
