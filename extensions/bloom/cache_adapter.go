package bloom

import (
	"context"
	"sync"

	"github.com/gospacex/cachex"
)

// CacheBloomFilterAdapter wraps a Cache with Bloom filter pre-screening.
// Get/Exists operations first check the Bloom filter before hitting the cache.
type CacheBloomFilterAdapter struct {
	cache  cachex.Cache
	filter *CacheBloomFilter
	mu     sync.RWMutex
}

// NewCacheBloomFilterAdapter creates a cache with Bloom filter pre-screening.
// expectedItems: expected number of elements to store
// fpRate: false positive rate (0.0 to 1.0)
func NewCacheBloomFilterAdapter(cache cachex.Cache, expectedItems int, fpRate float64) *CacheBloomFilterAdapter {
	return &CacheBloomFilterAdapter{
		cache:  cache,
		filter: NewCacheBloomFilter(expectedItems, fpRate),
	}
}

// Get first checks Bloom filter, then conditionally fetches from cache.
func (c *CacheBloomFilterAdapter) Get(ctx context.Context, key string) ([]byte, error) {
	if !c.filter.MightContain([]byte(key)) {
		return nil, cachex.ErrKeyNotFound
	}
	return c.cache.Get(ctx, key)
}

// Set stores value and adds key to Bloom filter.
func (c *CacheBloomFilterAdapter) Set(ctx context.Context, key string, value []byte) error {
	c.filter.Add([]byte(key))
	return c.cache.Set(ctx, key, value)
}

// SetEX stores value with TTL and adds key to Bloom filter.
func (c *CacheBloomFilterAdapter) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	c.filter.Add([]byte(key))
	return c.cache.SetEX(ctx, key, value, ttlSeconds)
}

// SetNX sets value only if key doesn't exist, adds key to Bloom filter.
func (c *CacheBloomFilterAdapter) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	c.filter.Add([]byte(key))
	return c.cache.SetNX(ctx, key, value, ttlSeconds)
}

// Delete removes keys from cache. Note: Bloom filters can't delete, but we track it.
func (c *CacheBloomFilterAdapter) Delete(ctx context.Context, keys ...string) (int64, error) {
	return c.cache.Delete(ctx, keys...)
}

// Exists checks if keys exist, using Bloom filter for quick negative lookups.
func (c *CacheBloomFilterAdapter) Exists(ctx context.Context, keys ...string) (int64, error) {
	for _, key := range keys {
		if !c.filter.MightContain([]byte(key)) {
			return 0, nil
		}
	}
	return c.cache.Exists(ctx, keys...)
}

// Expire sets expiration on a key.
func (c *CacheBloomFilterAdapter) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	return c.cache.Expire(ctx, key, ttlSeconds)
}

// TTL returns time to live for a key.
func (c *CacheBloomFilterAdapter) TTL(ctx context.Context, key string) (int64, error) {
	return c.cache.TTL(ctx, key)
}

// MGet retrieves multiple keys.
func (c *CacheBloomFilterAdapter) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	result := make([][]byte, len(keys))
	bloomPositiveIndices := make([]int, 0)
	bloomPositiveKeys := make([]string, 0)

	// Check each key against Bloom filter
	for i, key := range keys {
		if c.filter.MightContain([]byte(key)) {
			bloomPositiveIndices = append(bloomPositiveIndices, i)
			bloomPositiveKeys = append(bloomPositiveKeys, key)
		}
	}

	if len(bloomPositiveKeys) == 0 {
		return result, nil
	}

	cacheResults, err := c.cache.MGet(ctx, bloomPositiveKeys...)
	if err != nil {
		return result, err
	}

	// Align results back to original key positions
	for i, idx := range bloomPositiveIndices {
		result[idx] = cacheResults[i]
	}

	return result, nil
}

// MSet sets multiple key-value pairs.
func (c *CacheBloomFilterAdapter) MSet(ctx context.Context, kvs map[string][]byte) error {
	for key := range kvs {
		c.filter.Add([]byte(key))
	}
	return c.cache.MSet(ctx, kvs)
}

// Keys returns all keys matching pattern.
func (c *CacheBloomFilterAdapter) Keys(ctx context.Context, pattern string) ([]string, error) {
	return c.cache.Keys(ctx, pattern)
}

// Incr increments a counter.
func (c *CacheBloomFilterAdapter) Incr(ctx context.Context, key string) (int64, error) {
	return c.cache.Incr(ctx, key)
}

// Decr decrements a counter.
func (c *CacheBloomFilterAdapter) Decr(ctx context.Context, key string) (int64, error) {
	return c.cache.Decr(ctx, key)
}

// Ping checks cache connectivity.
func (c *CacheBloomFilterAdapter) Ping(ctx context.Context) error {
	return c.cache.Ping(ctx)
}

// Close closes the cache.
func (c *CacheBloomFilterAdapter) Close() error {
	return c.cache.Close()
}

// Stats returns cache statistics.
func (c *CacheBloomFilterAdapter) Stats() cachex.Stats {
	return c.cache.Stats()
}

// FilterStats returns Bloom filter statistics.
func (c *CacheBloomFilterAdapter) FilterStats() map[string]interface{} {
	return c.filter.Stats()
}

var _ cachex.Cache = (*CacheBloomFilterAdapter)(nil)
