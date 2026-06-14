// Package metrics provides observability metrics for cachex.
package metrics

import (
	"context"
	"time"

	"github.com/gospacex/cachex"
)

// MetricsCache wraps a cachex.Cache with transparent metrics collection.
type MetricsCache struct {
	cache     cachex.Cache
	collector MetricsCollector
}

// NewMetricsCache wraps a Cache with metrics collection.
func NewMetricsCache(cache cachex.Cache, collector MetricsCollector) *MetricsCache {
	return &MetricsCache{
		cache:     cache,
		collector: collector,
	}
}

// Get implements Cache interface with transparent metrics collection.
func (c *MetricsCache) Get(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()
	result, err := c.cache.Get(ctx, key)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "get", err)
		return result, err
	}

	hit := err == nil
	c.collector.RecordGet(ctx, hit, latency)
	return result, err
}

// Set implements Cache interface with transparent metrics collection.
func (c *MetricsCache) Set(ctx context.Context, key string, value []byte) error {
	start := time.Now()
	err := c.cache.Set(ctx, key, value)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "set", err)
		return err
	}

	c.collector.RecordSet(ctx, latency)
	return nil
}

// SetEX implements Cache interface with transparent metrics collection.
func (c *MetricsCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	start := time.Now()
	err := c.cache.SetEX(ctx, key, value, ttlSeconds)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "setex", err)
		return err
	}

	c.collector.RecordSet(ctx, latency)
	return nil
}

// SetNX implements Cache interface with transparent metrics collection.
func (c *MetricsCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	start := time.Now()
	result, err := c.cache.SetNX(ctx, key, value, ttlSeconds)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "setnx", err)
		return result, err
	}

	c.collector.RecordSet(ctx, latency)
	return result, nil
}

// Delete implements Cache interface with transparent metrics collection.
func (c *MetricsCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	start := time.Now()
	deleted, err := c.cache.Delete(ctx, keys...)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "delete", err)
		return deleted, err
	}

	c.collector.RecordDelete(ctx, deleted, latency)
	return deleted, nil
}

// Exists implements Cache interface with transparent metrics collection.
func (c *MetricsCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	start := time.Now()
	result, err := c.cache.Exists(ctx, keys...)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "exists", err)
		return result, err
	}

	c.collector.RecordSet(ctx, latency)
	return result, nil
}

// Expire implements Cache interface with transparent metrics collection.
func (c *MetricsCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	start := time.Now()
	result, err := c.cache.Expire(ctx, key, ttlSeconds)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "expire", err)
		return result, err
	}

	c.collector.RecordSet(ctx, latency)
	return result, nil
}

// TTL implements Cache interface with transparent metrics collection.
func (c *MetricsCache) TTL(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	result, err := c.cache.TTL(ctx, key)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "ttl", err)
		return result, err
	}

	c.collector.RecordSet(ctx, latency)
	return result, nil
}

// MGet implements Cache interface with transparent metrics collection.
func (c *MetricsCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	start := time.Now()
	result, err := c.cache.MGet(ctx, keys...)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "mget", err)
		return result, err
	}

	c.collector.RecordSet(ctx, latency)
	return result, nil
}

// MSet implements Cache interface with transparent metrics collection.
func (c *MetricsCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	start := time.Now()
	err := c.cache.MSet(ctx, kvs)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "mset", err)
		return err
	}

	c.collector.RecordSet(ctx, latency)
	return nil
}

// Keys implements Cache interface with transparent metrics collection.
func (c *MetricsCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	start := time.Now()
	result, err := c.cache.Keys(ctx, pattern)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "keys", err)
		return result, err
	}

	c.collector.RecordSet(ctx, latency)
	return result, nil
}

// Incr implements Cache interface with transparent metrics collection.
func (c *MetricsCache) Incr(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	result, err := c.cache.Incr(ctx, key)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "incr", err)
		return result, err
	}

	c.collector.RecordSet(ctx, latency)
	return result, nil
}

// Decr implements Cache interface with transparent metrics collection.
func (c *MetricsCache) Decr(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	result, err := c.cache.Decr(ctx, key)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "decr", err)
		return result, err
	}

	c.collector.RecordSet(ctx, latency)
	return result, nil
}

// Ping implements Cache interface with transparent metrics collection.
func (c *MetricsCache) Ping(ctx context.Context) error {
	start := time.Now()
	err := c.cache.Ping(ctx)
	latency := time.Since(start)

	if err != nil {
		c.collector.RecordError(ctx, "ping", err)
		return err
	}

	c.collector.RecordSet(ctx, latency)
	return nil
}

// Close implements Cache interface.
func (c *MetricsCache) Close() error {
	return c.cache.Close()
}

// Stats implements Cache interface.
func (c *MetricsCache) Stats() cachex.Stats {
	return c.cache.Stats()
}
