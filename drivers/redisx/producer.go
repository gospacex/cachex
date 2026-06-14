package redisx

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gospacex/cachex"
	"github.com/redis/go-redis/v9"
)

// Package-level pools and per-key locks implementing double-checked locking.
//
// The caches are sync.Map so reads (the common path) are lock-free. A second
// caller for the same key blocks on the per-key mutex while the first builds
// the client; subsequent callers hit the fast-path Load.
var (
	singleProducerCache  sync.Map // cacheKey → *redisClientHolder
	clusterProducerCache sync.Map // cacheKey → *redisClusterHolder
	producerLocks        sync.Map // cacheKey → *sync.Mutex
)

// redisClientHolder wraps a single *redis.Client with its monitoring cancel.
type redisClientHolder struct {
	client *redis.Client
	cancel context.CancelFunc
}

func (h *redisClientHolder) Close() error {
	if h.cancel != nil {
		h.cancel()
	}
	if h.client != nil {
		return h.client.Close()
	}
	return nil
}

func (h *redisClientHolder) Ping(ctx context.Context) error {
	if h.client == nil {
		return fmt.Errorf("nil client")
	}
	return h.client.Ping(ctx).Err()
}

// redisClusterHolder wraps a *redis.ClusterClient with its monitoring cancel.
type redisClusterHolder struct {
	client *redis.ClusterClient
	cancel context.CancelFunc
}

func (h *redisClusterHolder) Close() error {
	if h.cancel != nil {
		h.cancel()
	}
	if h.client != nil {
		return h.client.Close()
	}
	return nil
}

func (h *redisClusterHolder) Ping(ctx context.Context) error {
	if h.client == nil {
		return fmt.Errorf("nil client")
	}
	return h.client.Ping(ctx).Err()
}

// PPS returns a shared *redis.Client for the single-instance topology
// described by path. Concurrent callers asking for the same configuration
// share the same underlying pool.
func PPS(path string) (*redis.Client, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("redisx.PPS: %w", err)
	}
	cacheKey := buildCacheKey("single", key, cfg)
	holder, err := getOrCreateSingleProducer(cacheKey, cfg)
	if err != nil {
		return nil, err
	}
	return holder.client, nil
}

// PPC returns a shared *redis.ClusterClient for the cluster topology
// described by path.
func PPC(path string) (*redis.ClusterClient, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("redisx.PPC: %w", err)
	}
	cacheKey := buildCacheKey("cluster", key, cfg)
	holder, err := getOrCreateClusterProducer(cacheKey, cfg)
	if err != nil {
		return nil, err
	}
	return holder.client, nil
}

// getOrCreateSingleProducer implements the double-checked locking idiom. The
// first Load is a fast-path read; the slow-path grabs a per-key mutex so
// concurrent first-time callers serialize on the same key without blocking
// other keys.
func getOrCreateSingleProducer(cacheKey string, cfg *cachex.Config) (*redisClientHolder, error) {
	if val, ok := singleProducerCache.Load(cacheKey); ok {
		return val.(*redisClientHolder), nil
	}

	if cfg.Backend != "" && cfg.Backend != "redis" &&
		cfg.Backend != "dragonfly" && cfg.Backend != "keydb" && cfg.Backend != "garnet" {
		return nil, fmt.Errorf("redisx: backend %q not supported by redisx driver", cfg.Backend)
	}

	lockVal, _ := producerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := singleProducerCache.Load(cacheKey); ok {
		return val.(*redisClientHolder), nil
	}

	if len(cfg.Addrs) == 0 {
		return nil, fmt.Errorf("redisx: no addresses provided")
	}

	opts, err := buildSingleOptions(cfg)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	pingErr := client.Ping(ctx).Err()
	cancel()
	if pingErr != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisx: ping failed: %w", pingErr)
	}

	monCtx, monCancel := context.WithCancel(context.Background())
	holder := &redisClientHolder{client: client, cancel: monCancel}
	startRedisPoolMonitor(monCtx, cacheKey, client)

	singleProducerCache.Store(cacheKey, holder)
	return holder, nil
}

// getOrCreateClusterProducer implements the cluster equivalent.
func getOrCreateClusterProducer(cacheKey string, cfg *cachex.Config) (*redisClusterHolder, error) {
	if val, ok := clusterProducerCache.Load(cacheKey); ok {
		return val.(*redisClusterHolder), nil
	}

	if cfg.Backend != "" && cfg.Backend != "redis" &&
		cfg.Backend != "dragonfly" && cfg.Backend != "keydb" && cfg.Backend != "garnet" {
		return nil, fmt.Errorf("redisx: backend %q not supported by redisx driver", cfg.Backend)
	}

	lockVal, _ := producerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := clusterProducerCache.Load(cacheKey); ok {
		return val.(*redisClusterHolder), nil
	}

	opts, err := buildClusterOptions(cfg)
	if err != nil {
		return nil, err
	}

	client := redis.NewClusterClient(opts)
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	pingErr := client.Ping(pingCtx).Err()
	pingCancel()
	if pingErr != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisx: cluster ping failed: %w", pingErr)
	}

	monCtx, monCancel := context.WithCancel(context.Background())
	holder := &redisClusterHolder{client: client, cancel: monCancel}
	startRedisPoolMonitor(monCtx, cacheKey+":cluster", client)

	clusterProducerCache.Store(cacheKey, holder)
	return holder, nil
}

// buildSingleOptions translates a cachex.Config into redis.Options.
func buildSingleOptions(cfg *cachex.Config) (*redis.Options, error) {
	if len(cfg.Addrs) == 0 {
		return nil, fmt.Errorf("redisx: empty addrs")
	}
	tc, err := buildTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &redis.Options{
		Addr:            cfg.Addrs[0],
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		MaxRetries:      cfg.MaxRetries,
		DialTimeout:     secondsToDuration(cfg.DialTimeout),
		ReadTimeout:     secondsToDuration(cfg.ReadTimeout),
		WriteTimeout:    secondsToDuration(cfg.WriteTimeout),
		PoolTimeout:     secondsToDuration(cfg.PoolTimeout),
		ConnMaxIdleTime: secondsToDuration(cfg.IdleTimeout),
		TLSConfig:       tc,
	}, nil
}

// buildClusterOptions translates a cachex.Config into redis.ClusterOptions.
func buildClusterOptions(cfg *cachex.Config) (*redis.ClusterOptions, error) {
	tc, err := buildTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &redis.ClusterOptions{
		Addrs:           cfg.Addrs,
		Password:        cfg.Password,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		MaxRetries:      cfg.MaxRetries,
		DialTimeout:     secondsToDuration(cfg.DialTimeout),
		ReadTimeout:     secondsToDuration(cfg.ReadTimeout),
		WriteTimeout:    secondsToDuration(cfg.WriteTimeout),
		PoolTimeout:     secondsToDuration(cfg.PoolTimeout),
		ConnMaxIdleTime: secondsToDuration(cfg.IdleTimeout),
		TLSConfig:       tc,
	}, nil
}

// buildTLSConfig returns a *tls.Config derived from cfg.TLS or nil when
// TLS is not enabled.
func buildTLSConfig(cfg *cachex.Config) (*tls.Config, error) {
	if !cfg.TLS.Enabled {
		return nil, nil
	}
	tc := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
	}
	if cfg.TLS.CAFile != "" {
		caCert, err := os.ReadFile(cfg.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("redisx: read CA file %s: %w", cfg.TLS.CAFile, err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("redisx: failed to append CA certificate from %s", cfg.TLS.CAFile)
		}
		tc.RootCAs = pool
	}
	if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("redisx: load client cert/key: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

func secondsToDuration(seconds int) time.Duration {
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}
