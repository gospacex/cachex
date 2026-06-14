// Package pubsub provides distributed cache invalidation via Pub/Sub.
package pubsub

import (
	"context"
	"fmt"
	"sync"

	"github.com/gospacex/cachex"
)

// CacheInvalidationPubSub wraps a cache with Pub/Sub-based distributed cache invalidation.
// When Delete is called on the wrapped cache, it publishes the key to the specified channel.
// Subscribers listening on that channel will invalidate their local cache.
type CacheInvalidationPubSub struct {
	cache   cachex.Cache
	pubsub  PubSub
	channel string
	mu      sync.Mutex
}

// NewCacheInvalidationPubSub wraps a cache with Pub/Sub-based invalidation.
func NewCacheInvalidationPubSub(cache cachex.Cache, pubsub PubSub, channel string) *CacheInvalidationPubSub {
	return &CacheInvalidationPubSub{
		cache:   cache,
		pubsub:  pubsub,
		channel: channel,
	}
}

// Delete removes keys from the cache and publishes invalidation events.
// The keys are published to the configured channel so subscribers can
// invalidate their local caches.
func (c *CacheInvalidationPubSub) Delete(ctx context.Context, keys ...string) (int64, error) {
	n, err := c.cache.Delete(ctx, keys...)
	if err != nil {
		return n, fmt.Errorf("failed to delete keys: %w", err)
	}

	// Publish invalidation for each deleted key
	for _, key := range keys {
		if err := c.pubsub.Publish(ctx, c.channel, key); err != nil {
			// Log but don't fail the operation
			// The local delete succeeded, remote invalidation is best-effort
			continue
		}
	}

	return n, nil
}

// InvalidateLocal invalidates a single key in the local cache.
// This is called by the subscriber handler when a remote invalidation is received.
func (c *CacheInvalidationPubSub) InvalidateLocal(ctx context.Context, key string) error {
	_, err := c.cache.Delete(ctx, key)
	return err
}

// SubscribeToInvalidations sets up a subscriber that listens for remote invalidations
// and invalidates the local cache accordingly.
func (c *CacheInvalidationPubSub) SubscribeToInvalidations(ctx context.Context) error {
	return c.pubsub.Subscribe(ctx, c.channel, func(key string) {
		if err := c.InvalidateLocal(ctx, key); err != nil {
			// Best-effort invalidation
			return
		}
	})
}

// Get implements Cache interface.
func (c *CacheInvalidationPubSub) Get(ctx context.Context, key string) ([]byte, error) {
	return c.cache.Get(ctx, key)
}

// Set implements Cache interface.
func (c *CacheInvalidationPubSub) Set(ctx context.Context, key string, value []byte) error {
	return c.cache.Set(ctx, key, value)
}

// SetEX implements Cache interface.
func (c *CacheInvalidationPubSub) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	return c.cache.SetEX(ctx, key, value, ttlSeconds)
}

// SetNX implements Cache interface.
func (c *CacheInvalidationPubSub) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	return c.cache.SetNX(ctx, key, value, ttlSeconds)
}

// Exists implements Cache interface.
func (c *CacheInvalidationPubSub) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.cache.Exists(ctx, keys...)
}

// Expire implements Cache interface.
func (c *CacheInvalidationPubSub) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	return c.cache.Expire(ctx, key, ttlSeconds)
}

// TTL implements Cache interface.
func (c *CacheInvalidationPubSub) TTL(ctx context.Context, key string) (int64, error) {
	return c.cache.TTL(ctx, key)
}

// MGet implements Cache interface.
func (c *CacheInvalidationPubSub) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	return c.cache.MGet(ctx, keys...)
}

// MSet implements Cache interface.
func (c *CacheInvalidationPubSub) MSet(ctx context.Context, kvs map[string][]byte) error {
	return c.cache.MSet(ctx, kvs)
}

// Keys implements Cache interface.
func (c *CacheInvalidationPubSub) Keys(ctx context.Context, pattern string) ([]string, error) {
	return c.cache.Keys(ctx, pattern)
}

// Incr implements Cache interface.
func (c *CacheInvalidationPubSub) Incr(ctx context.Context, key string) (int64, error) {
	return c.cache.Incr(ctx, key)
}

// Decr implements Cache interface.
func (c *CacheInvalidationPubSub) Decr(ctx context.Context, key string) (int64, error) {
	return c.cache.Decr(ctx, key)
}

// Ping implements Cache interface.
func (c *CacheInvalidationPubSub) Ping(ctx context.Context) error {
	return c.cache.Ping(ctx)
}

// Close implements Cache interface.
func (c *CacheInvalidationPubSub) Close() error {
	if err := c.pubsub.Close(); err != nil {
		return fmt.Errorf("failed to close pubsub: %w", err)
	}
	return c.cache.Close()
}

// Stats implements Cache interface.
func (c *CacheInvalidationPubSub) Stats() cachex.Stats {
	return c.cache.Stats()
}
