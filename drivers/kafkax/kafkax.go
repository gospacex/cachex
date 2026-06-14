package kafkax

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gospacex/cachex/utils"
	"gopkg.in/yaml.v3"
)

// activeConfigCache is the per-path in-memory snapshot used by ParseFile so
// that hot-reload does not re-read the YAML from disk on every call.
var activeConfigCache sync.Map // path → *Config

// ParseFile loads a kafkax Config from a YAML file. It returns the config
// and the configKey embedded after '#' (or "default" when omitted).
func ParseFile(path string) (*Config, string, error) {
	if val, ok := activeConfigCache.Load(path); ok {
		cfg := val.(*Config)
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
func parseFileFromDisk(path string) (*Config, string, error) {
	filePath, configKey := splitPath(path)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, "", fmt.Errorf("kafkax: read config file: %w", err)
	}
	expanded := utils.ExpandEnvVars(string(data))

	var all map[string]*Config
	if err := yaml.Unmarshal([]byte(expanded), &all); err != nil {
		var single Config
		if err2 := yaml.Unmarshal([]byte(expanded), &single); err2 != nil {
			return nil, "", fmt.Errorf("kafkax: parse yaml: %w", err)
		}
		if configKey == "" {
			configKey = "default"
		}
		activeConfigCache.Store(path, &single)
		return &single, configKey, nil
	}

	if configKey == "" {
		for k, v := range all {
			activeConfigCache.Store(path, v)
			return v, k, nil
		}
		return nil, "", fmt.Errorf("kafkax: empty config file")
	}

	cfg, ok := all[configKey]
	if !ok {
		return nil, "", fmt.Errorf("kafkax: config key %q not found", configKey)
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
func buildCacheKey(mode, configKey string, cfg *Config) string {
	return fmt.Sprintf("producer:%s:%s:%s", mode, configKey, utils.ConfigFingerprint(cfg))
}

// Reload re-parses the YAML at path and atomically swaps the underlying
// producer pool. The old pool is flushed+closed on a background goroutine
// after a short grace period so that in-flight messages are not lost.
func Reload(path string) error {
	log.Printf("[kafkax] hot-reloading config from %s ...", path)

	cfg, key, err := parseFileFromDisk(path)
	if err != nil {
		return fmt.Errorf("reload parse error: %w", err)
	}

	oldCacheKey := ""
	if val, ok := activeConfigCache.Load(path); ok {
		oldCfg := val.(*Config)
		_, oldKey := splitPath(path)
		if oldKey == "" {
			oldKey = "default"
		}
		oldCacheKey = buildCacheKey("single", oldKey, oldCfg)
	}

	newCacheKey := buildCacheKey("single", key, cfg)
	if _, err := getOrCreateSyncProducer(newCacheKey, cfg); err != nil {
		return fmt.Errorf("reload build new producer error: %w", err)
	}

	if oldCacheKey != "" && oldCacheKey != newCacheKey {
		if val, ok := syncProducerCache.Load(oldCacheKey); ok {
			oldProd := val.(*syncProducerHolder)
			go func() {
				time.Sleep(5 * time.Second)
				log.Printf("[kafkax] hot-reload: flushing and closing old producer [key=%s]", oldCacheKey)
				_ = oldProd.Close(5000)
				syncProducerCache.Delete(oldCacheKey)
				producerLocks.Delete(oldCacheKey)
			}()
		}
	}

	log.Printf("[kafkax] hot-reload success. New connections are ready.")
	return nil
}

// Shutdown gracefully flushes+closes every pooled producer in background
// goroutines. It returns after either all closes complete or the provided
// context deadline is hit.
func Shutdown(ctx context.Context) {
	var wg sync.WaitGroup

	flushMs := int(flushTimeout(ctx).Milliseconds())

	syncProducerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, h *syncProducerHolder) {
			defer wg.Done()
			log.Printf("[kafkax] flushing and closing sync producer [key=%s]", k)
			_ = h.Close(flushMs)
			syncProducerCache.Delete(k)
			producerLocks.Delete(k)
		}(key.(string), value.(*syncProducerHolder))
		return true
	})

	asyncProducerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, h *asyncProducerHolder) {
			defer wg.Done()
			log.Printf("[kafkax] closing async producer [key=%s]", k)
			_ = h.Close()
			asyncProducerCache.Delete(k)
			producerLocks.Delete(k)
		}(key.(string), value.(*asyncProducerHolder))
		return true
	})

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Println("[kafkax] all connections closed gracefully")
	case <-ctx.Done():
		log.Println("[kafkax] shutdown timed out, some connections may not be fully closed")
	}
}

// HealthCheck returns a map of cacheKey → status string. Every pooled
// producer is metadata-probed with a 5s timeout. Status is "healthy" or
// "unhealthy: <reason>".
func HealthCheck() map[string]string {
	result := make(map[string]string)

	syncProducerCache.Range(func(key, value any) bool {
		h := value.(*syncProducerHolder)
		if err := h.ProbeMetadata(5 * time.Second); err != nil {
			result[key.(string)] = "unhealthy: " + err.Error()
		} else {
			result[key.(string)] = "healthy"
		}
		return true
	})

	asyncProducerCache.Range(func(key, value any) bool {
		h := value.(*asyncProducerHolder)
		if err := h.ProbeMetadata(5 * time.Second); err != nil {
			result[key.(string)] = "unhealthy: " + err.Error()
		} else {
			result[key.(string)] = "healthy"
		}
		return true
	})

	return result
}

// flushTimeout returns the flush budget derived from ctx. If ctx has no
// deadline, returns 15s. We use 80% of the remaining budget with a 1s floor.
func flushTimeout(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 15 * time.Second
	}
	remaining := time.Until(deadline)
	if remaining < time.Second {
		return time.Second
	}
	return time.Duration(float64(remaining) * 0.8)
}
