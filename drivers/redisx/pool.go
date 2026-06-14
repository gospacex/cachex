package redisx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/cachex/utils"
	"github.com/redis/go-redis/v9"
)

// cfgFingerprintPool is the per-fingerprint single-instance cache. Unlike
// singleProducerCache (which is keyed by path+configKey+fingerprint for the
// YAML-driven PPS/PPC API), this pool is keyed by the bare ConfigFingerprint
// so the backend layer can share a *redis.Client across creator.Create calls
// when the resolved *cachex.Config is structurally identical.
var cfgFingerprintPool sync.Map // fingerprint → *redis.Client

// PoolKey returns the stable pool key for cfg. The backend layer uses this
// directly to identify shared clients; the YAML-driven PPS/PPC API composes
// this with a mode + configKey to build its own cache key.
func PoolKey(cfg *cachex.Config) string {
	if cfg == nil {
		return utils.ConfigFingerprint(nil)
	}
	return utils.ConfigFingerprint(cfg)
}

// GetSingle returns a *redis.Client for cfg, sharing a single instance with
// every other caller that supplies a config with the same fingerprint. The
// first call dials the configured address; subsequent calls return the
// already-pooled client without re-dialling.
//
// If cfg is nil, an error is returned. If dialing fails, the partially-built
// client is NOT cached — the next call will retry the dial.
func GetSingle(cfg *cachex.Config) (*redis.Client, error) {
	if cfg == nil {
		return nil, errors.New("redisx: nil config")
	}
	key := PoolKey(cfg)
	if v, ok := cfgFingerprintPool.Load(key); ok {
		return v.(*redis.Client), nil
	}
	opts, err := buildSingleOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("redisx: build options: %w", err)
	}
	client := redis.NewClient(opts)
	actual, loaded := cfgFingerprintPool.LoadOrStore(key, client)
	stored := actual.(*redis.Client)
	if !loaded {
		// Best-effort health probe so we never cache a doomed client. If the
		// dial fails the caller can recover (and we evict our entry).
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pingErr := stored.Ping(pingCtx).Err()
		cancel()
		if pingErr != nil {
			cfgFingerprintPool.Delete(key)
			_ = stored.Close()
			return nil, fmt.Errorf("redisx: dial %v: %w", cfg.Addrs, pingErr)
		}
	}
	return stored, nil
}

// GetCluster returns a *redis.ClusterClient for cfg, shared across callers
// that supply a config with the same fingerprint. The first call dials the
// configured cluster; subsequent calls return the already-pooled client.
//
// If cfg is nil, an error is returned. If dialing fails, the partially-built
// client is NOT cached.
func GetCluster(cfg *cachex.Config) (*redis.ClusterClient, error) {
	if cfg == nil {
		return nil, errors.New("redisx: nil config")
	}
	key := PoolKey(cfg)
	if v, ok := cfgClusterPool.Load(key); ok {
		return v.(*redis.ClusterClient), nil
	}
	opts, err := buildClusterOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("redisx: build cluster options: %w", err)
	}
	client := redis.NewClusterClient(opts)
	actual, loaded := cfgClusterPool.LoadOrStore(key, client)
	stored := actual.(*redis.ClusterClient)
	if !loaded {
		// Cluster ping is heavier; keep the timeout short.
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pingErr := stored.Ping(pingCtx).Err()
		cancel()
		if pingErr != nil {
			cfgClusterPool.Delete(key)
			_ = stored.Close()
			return nil, fmt.Errorf("redisx: dial cluster %v: %w", cfg.Addrs, pingErr)
		}
	}
	return stored, nil
}

// cfgClusterPool is the per-fingerprint cluster-client pool.
var cfgClusterPool sync.Map // fingerprint → *redis.ClusterClient

// cfgFailoverPool is the per-fingerprint sentinel-failover client pool.
var cfgFailoverPool sync.Map // fingerprint → *redis.Client

// GetFailover returns a *redis.Client configured against the Sentinel set
// described by cfg.Addrs, using cfg.MasterName. Subsequent callers that
// supply a config with the same fingerprint share the same client.
//
// If cfg is nil, an error is returned. If the initial Sentinel handshake
// fails, the partially-built client is NOT cached.
func GetFailover(cfg *cachex.Config) (*redis.Client, error) {
	if cfg == nil {
		return nil, errors.New("redisx: nil config")
	}
	if cfg.MasterName == "" {
		return nil, errors.New("redisx: cfg.MasterName required for failover")
	}
	key := PoolKey(cfg)
	if v, ok := cfgFailoverPool.Load(key); ok {
		return v.(*redis.Client), nil
	}
	opts, err := buildFailoverOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("redisx: build failover options: %w", err)
	}
	client := redis.NewFailoverClient(opts)
	actual, loaded := cfgFailoverPool.LoadOrStore(key, client)
	stored := actual.(*redis.Client)
	if !loaded {
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pingErr := stored.Ping(pingCtx).Err()
		cancel()
		if pingErr != nil {
			cfgFailoverPool.Delete(key)
			_ = stored.Close()
			return nil, fmt.Errorf("redisx: dial failover %v: %w", cfg.Addrs, pingErr)
		}
	}
	return stored, nil
}

// buildFailoverOptions translates a cachex.Config with MasterName into
// redis.FailoverOptions.
func buildFailoverOptions(cfg *cachex.Config) (*redis.FailoverOptions, error) {
	if cfg.MasterName == "" {
		return nil, errors.New("redisx: empty master name")
	}
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("redisx: empty sentinel addrs")
	}
	tc, err := buildTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &redis.FailoverOptions{
		MasterName:       cfg.MasterName,
		SentinelAddrs:    cfg.Addrs,
		SentinelPassword: cfg.SentinelPassword,
		Password:         cfg.Password,
		DB:               cfg.DB,
		PoolSize:         cfg.PoolSize,
		MinIdleConns:     cfg.MinIdleConns,
		MaxRetries:       cfg.MaxRetries,
		DialTimeout:      time.Duration(cfg.DialTimeout) * time.Second,
		ReadTimeout:      time.Duration(cfg.ReadTimeout) * time.Second,
		WriteTimeout:     time.Duration(cfg.WriteTimeout) * time.Second,
		PoolTimeout:      time.Duration(cfg.PoolTimeout) * time.Second,
		ConnMaxIdleTime:  time.Duration(cfg.IdleTimeout) * time.Second,
		TLSConfig:        tc,
	}, nil
}

// resetPoolForTest clears all three fingerprint pools. Test-only.
func resetPoolForTest() {
	cfgFingerprintPool = sync.Map{}
	cfgClusterPool = sync.Map{}
	cfgFailoverPool = sync.Map{}
}

// storePooledForTest inserts a stub into the single-fingerprint pool under
// a synthetic key. Test-only: lets the concurrency test exercise the pool
// without dialing a real redis.
func storePooledForTest(key string, v any) {
	cfgFingerprintPool.Store(key, v)
}

// loadPooledForTest reads from the single-fingerprint pool. Test-only.
func loadPooledForTest(key string) (any, bool) {
	return cfgFingerprintPool.Load(key)
}
