// Package redis provides a unified Redis-compatible backend for cachex.
//
// It supports all Redis protocol-compatible servers through a single codebase:
//   - Redis (native)
//   - Dragonfly (Redis-compatible, modern multi-core)
//   - KeyDB (Redis fork with multi-threading)
//   - Garnet (Redis-compatible with RocksDB storage)
//
// All four backends use go-redis/v9 under the hood, sharing identical wire
// protocol semantics. The Driver field in the config discriminates which
// specific server is in use (mostly for logging / documentation); the client
// code is identical.
package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/cachex/drivers/redisx"
	"github.com/redis/go-redis/v9"
)

// Driver identifies the concrete Redis-protocol server behind the backend.
type Driver string

const (
	DriverRedis     Driver = "redis"
	DriverDragonfly Driver = "dragonfly"
	DriverKeyDB     Driver = "keydb"
	DriverGarnet    Driver = "garnet"
)

// driverFromBackend maps a cachex backend name to a Driver.
func driverFromBackend(name string) Driver {
	switch name {
	case "dragonfly":
		return DriverDragonfly
	case "keydb":
		return DriverKeyDB
	case "garnet":
		return DriverGarnet
	default:
		return DriverRedis
	}
}

// creator produces cachex.Cache instances backed by a go-redis client.
type creator struct {
	driver Driver
}

func (c *creator) Create(cfg *cachex.Config) (cachex.Cache, error) {
	driver := c.driver
	if driver == "" {
		driver = driverFromBackend(cfg.Backend)
	}

	if len(cfg.Addrs) == 0 {
		return nil, cachex.ErrAddrsRequired
	}

	var client *redis.Client
	var clusterClient *redis.ClusterClient

	switch {
	case cfg.ClusterMode && len(cfg.Addrs) > 1:
		// Cluster topology: use the driver pool so multiple cachex.Cache
		// instances against the same fingerprint share a single
		// *redis.ClusterClient and the same broker connections.
		cc, err := redisx.GetCluster(cfg)
		if err != nil {
			return nil, err
		}
		clusterClient = cc
		return newRedisCache(nil, clusterClient, driver), nil

	case cfg.MasterName != "":
		// Sentinel-failover topology: pooled via redisx.GetFailover.
		fc, err := redisx.GetFailover(cfg)
		if err != nil {
			return nil, err
		}
		client = fc

	default:
		// Single-instance topology: pooled via redisx.GetSingle. This is
		// the key behaviour change of Task 3 — instead of building a fresh
		// *redis.Client per creator.Create call, the driver pool dedupes
		// clients by ConfigFingerprint.
		sc, err := redisx.GetSingle(cfg)
		if err != nil {
			return nil, err
		}
		client = sc
	}

	return newRedisCache(client, nil, driver), nil
}

// buildTLSConfig builds TLS configuration from cachex config.
func buildTLSConfig(tlsCfg cachex.TLSConfig) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: tlsCfg.InsecureSkipVerify,
	}

	if tlsCfg.CAFile != "" {
		caCert, err := os.ReadFile(tlsCfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("redis: read CA file %s: %w", tlsCfg.CAFile, err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("redis: failed to append CA certificate from %s", tlsCfg.CAFile)
		}
		cfg.RootCAs = pool
	}

	if tlsCfg.CertFile != "" && tlsCfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("redis: load client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

func cachexDuration(seconds int) time.Duration {
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

// redisCache implements cachex.Cache for all Redis-protocol backends.
type redisCache struct {
	client        *redis.Client
	clusterClient *redis.ClusterClient
	driver        Driver
	stats         *redisStats
}

func newRedisCache(client *redis.Client, clusterClient *redis.ClusterClient, driver Driver) *redisCache {
	return &redisCache{
		client:        client,
		clusterClient: clusterClient,
		driver:        driver,
		stats:         newRedisStats(),
	}
}

// redisStats implements cachex.Stats for Redis.
type redisStats struct {
	hits           int64
	misses         int64
	errors         int64
	totalLatencyNs int64
	opCount        int64
}

func newRedisStats() *redisStats {
	return &redisStats{}
}

func (s *redisStats) Hits() int64   { return atomic.LoadInt64(&s.hits) }
func (s *redisStats) Misses() int64 { return atomic.LoadInt64(&s.misses) }
func (s *redisStats) Errors() int64 { return atomic.LoadInt64(&s.errors) }

func (s *redisStats) Latency() int64 {
	count := atomic.LoadInt64(&s.opCount)
	if count == 0 {
		return 0
	}
	return atomic.LoadInt64(&s.totalLatencyNs) / count
}

func (s *redisStats) recordHit(latency time.Duration) {
	atomic.AddInt64(&s.hits, 1)
	atomic.AddInt64(&s.totalLatencyNs, latency.Nanoseconds())
	atomic.AddInt64(&s.opCount, 1)
}

func (s *redisStats) recordMiss(latency time.Duration) {
	atomic.AddInt64(&s.misses, 1)
	atomic.AddInt64(&s.totalLatencyNs, latency.Nanoseconds())
	atomic.AddInt64(&s.opCount, 1)
}

func (s *redisStats) recordError(latency time.Duration) {
	atomic.AddInt64(&s.errors, 1)
	atomic.AddInt64(&s.totalLatencyNs, latency.Nanoseconds())
	atomic.AddInt64(&s.opCount, 1)
}

// Driver returns the backend driver label.
func (c *redisCache) Driver() Driver { return c.driver }

// isCluster returns true if this is a cluster connection.
func (c *redisCache) isCluster() bool { return c.clusterClient != nil }

func (c *redisCache) recordOperation(err error, latency time.Duration) {
	if err != nil {
		if err == redis.Nil {
			c.stats.recordMiss(latency)
		} else {
			c.stats.recordError(latency)
		}
	} else {
		c.stats.recordHit(latency)
	}
}

func (c *redisCache) Get(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()
	var val string
	var err error

	if c.isCluster() {
		val, err = c.clusterClient.Get(ctx, key).Result()
	} else {
		val, err = c.client.Get(ctx, key).Result()
	}

	latency := time.Since(start)
	if err == redis.Nil {
		c.stats.recordMiss(latency)
		return nil, cachex.ErrKeyNotFound
	}

	c.recordOperation(err, latency)
	if err != nil {
		return nil, err
	}
	return []byte(val), nil
}

func (c *redisCache) Set(ctx context.Context, key string, value []byte) error {
	start := time.Now()
	var err error

	if c.isCluster() {
		err = c.clusterClient.Set(ctx, key, value, 0).Err()
	} else {
		err = c.client.Set(ctx, key, value, 0).Err()
	}

	c.recordOperation(err, time.Since(start))
	return err
}

func (c *redisCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	start := time.Now()
	d := time.Duration(ttlSeconds) * time.Second
	var err error

	if c.isCluster() {
		err = c.clusterClient.Set(ctx, key, value, d).Err()
	} else {
		err = c.client.Set(ctx, key, value, d).Err()
	}

	c.recordOperation(err, time.Since(start))
	return err
}

func (c *redisCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	start := time.Now()
	d := time.Duration(ttlSeconds) * time.Second
	var result bool
	var err error

	if c.isCluster() {
		result, err = c.clusterClient.SetNX(ctx, key, value, d).Result()
	} else {
		result, err = c.client.SetNX(ctx, key, value, d).Result()
	}

	c.recordOperation(err, time.Since(start))
	return result, err
}

func (c *redisCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	start := time.Now()
	var result int64
	var err error

	if c.isCluster() {
		result, err = c.clusterClient.Del(ctx, keys...).Result()
	} else {
		result, err = c.client.Del(ctx, keys...).Result()
	}

	c.recordOperation(err, time.Since(start))
	return result, err
}

func (c *redisCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	start := time.Now()
	var result int64
	var err error

	if c.isCluster() {
		result, err = c.clusterClient.Exists(ctx, keys...).Result()
	} else {
		result, err = c.client.Exists(ctx, keys...).Result()
	}

	c.recordOperation(err, time.Since(start))
	return result, err
}

func (c *redisCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	start := time.Now()
	d := time.Duration(ttlSeconds) * time.Second
	var result bool
	var err error

	if c.isCluster() {
		result, err = c.clusterClient.Expire(ctx, key, d).Result()
	} else {
		result, err = c.client.Expire(ctx, key, d).Result()
	}

	c.recordOperation(err, time.Since(start))
	return result, err
}

func (c *redisCache) TTL(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	var d time.Duration
	var err error

	if c.isCluster() {
		d, err = c.clusterClient.TTL(ctx, key).Result()
	} else {
		d, err = c.client.TTL(ctx, key).Result()
	}

	c.recordOperation(err, time.Since(start))
	if err != nil {
		return -2, err
	}
	if d < 0 {
		return int64(d), nil
	}
	return int64(d.Seconds()), nil
}

func (c *redisCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	start := time.Now()
	var values []interface{}
	var err error

	if c.isCluster() {
		values, err = c.clusterClient.MGet(ctx, keys...).Result()
	} else {
		values, err = c.client.MGet(ctx, keys...).Result()
	}

	c.recordOperation(err, time.Since(start))
	if err != nil {
		return nil, err
	}

	return stringsToBytes(values), nil
}

func (c *redisCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	start := time.Now()
	args := mapToArgs(kvs)
	var err error

	if c.isCluster() {
		err = c.clusterClient.MSet(ctx, args...).Err()
	} else {
		err = c.client.MSet(ctx, args...).Err()
	}

	c.recordOperation(err, time.Since(start))
	return err
}

func (c *redisCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	start := time.Now()
	var result []string
	var err error

	if c.isCluster() {
		result, err = c.clusterClient.Keys(ctx, pattern).Result()
	} else {
		result, err = c.client.Keys(ctx, pattern).Result()
	}

	c.recordOperation(err, time.Since(start))
	return result, err
}

func (c *redisCache) Incr(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	var result int64
	var err error

	if c.isCluster() {
		result, err = c.clusterClient.Incr(ctx, key).Result()
	} else {
		result, err = c.client.Incr(ctx, key).Result()
	}

	c.recordOperation(err, time.Since(start))
	return result, err
}

func (c *redisCache) Decr(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	var result int64
	var err error

	if c.isCluster() {
		result, err = c.clusterClient.Decr(ctx, key).Result()
	} else {
		result, err = c.client.Decr(ctx, key).Result()
	}

	c.recordOperation(err, time.Since(start))
	return result, err
}

func (c *redisCache) Ping(ctx context.Context) error {
	start := time.Now()
	var err error

	if c.isCluster() {
		err = c.clusterClient.Ping(ctx).Err()
	} else {
		err = c.client.Ping(ctx).Err()
	}

	c.recordOperation(err, time.Since(start))
	return err
}

func (c *redisCache) Close() error {
	if c.clusterClient != nil {
		return c.clusterClient.Close()
	}
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

func (c *redisCache) Stats() cachex.Stats {
	return c.stats
}

func stringsToBytes(values []interface{}) [][]byte {
	out := make([][]byte, len(values))
	for i, v := range values {
		if v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		out[i] = []byte(s)
	}
	return out
}

func mapToArgs(kvs map[string][]byte) []interface{} {
	args := make([]interface{}, 0, len(kvs)*2)
	for k, v := range kvs {
		args = append(args, k, string(v))
	}
	return args
}

// Client returns the underlying *redis.Client.
func (c *redisCache) Client() *redis.Client { return c.client }

// ClusterClient returns the underlying *redis.ClusterClient.
func (c *redisCache) ClusterClient() *redis.ClusterClient { return c.clusterClient }

// PoolStats returns connection pool statistics.
func (c *redisCache) PoolStats() *redis.PoolStats {
	if c.isCluster() {
		return nil // Cluster doesn't expose simple pool stats
	}
	if c.client != nil {
		return c.client.PoolStats()
	}
	return nil
}

// Scan implements cursor-based key iteration using Redis SCAN command.
// This is more memory-efficient than Keys() for large datasets.
// The returned channel yields keys in batches.
func (c *redisCache) Scan(ctx context.Context, cursor uint64, match string, count int64) (<-chan string, error) {
	ch := make(chan string, 1)

	go func() {
		defer close(ch)

		var keys []string
		var nextCursor uint64
		var err error
		currentCursor := cursor

		for {
			if c.isCluster() {
				keys, nextCursor, err = c.clusterClient.Scan(ctx, currentCursor, match, count).Result()
			} else {
				keys, nextCursor, err = c.client.Scan(ctx, currentCursor, match, count).Result()
			}

			if err != nil {
				return
			}

			for _, key := range keys {
				select {
				case ch <- key:
				case <-ctx.Done():
					return
				}
			}

			currentCursor = nextCursor
			if currentCursor == 0 {
				break
			}
		}
	}()

	return ch, nil
}

// Auto-registration
func init() {
	backends := []string{
		cachex.BackendRedis,
		cachex.BackendDragonfly,
		cachex.BackendKeyDB,
		cachex.BackendGarnet,
	}

	for _, name := range backends {
		cachex.DefaultFactory.Register(name, &creator{
			driver: driverFromBackend(name),
		})
	}
}

// Compile-time interface check.
var _ cachex.Cache = (*redisCache)(nil)
