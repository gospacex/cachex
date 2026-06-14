// Package cachex provides a unified, production-ready cache client factory.
package cachex

import (
	"context"
	"time"
)

// =============================================================================
// Cache Interface - Unified cache operations
// =============================================================================

// Cache defines the interface that all cache implementations must satisfy.
type Cache interface {
	// Get retrieves a value by key.
	// Returns ErrKeyNotFound if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set stores a key-value pair with no expiration.
	Set(ctx context.Context, key string, value []byte) error

	// SetEX stores a key-value pair with expiration (in seconds).
	SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error

	// SetNX stores a key-value pair only if the key does not exist.
	// Returns true if the key was set, false otherwise.
	SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error)

	// Delete removes one or more keys.
	// Returns the number of keys that were removed.
	Delete(ctx context.Context, keys ...string) (int64, error)

	// Exists checks if one or more keys exist.
	// Returns the number of keys that exist.
	Exists(ctx context.Context, keys ...string) (int64, error)

	// Expire sets a key's time to live in seconds.
	// Returns true if the timeout was set, false if key does not exist.
	Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error)

	// TTL returns the remaining time to live of a key.
	// Returns -1 if the key has no expiration, -2 if key does not exist.
	TTL(ctx context.Context, key string) (int64, error)

	// MGet retrieves multiple values by keys.
	MGet(ctx context.Context, keys ...string) ([][]byte, error)

	// MSet stores multiple key-value pairs.
	MSet(ctx context.Context, kvs map[string][]byte) error

	// Keys returns all keys matching a pattern.
	// Pattern uses glob-style matching: * matches any number of characters.
	Keys(ctx context.Context, pattern string) ([]string, error)

	// Incr increments a key by 1.
	// Returns the new value after incrementation.
	Incr(ctx context.Context, key string) (int64, error)

	// Decr decrements a key by 1.
	// Returns the new value after decrementation.
	Decr(ctx context.Context, key string) (int64, error)

	// Ping checks the connection health.
	Ping(ctx context.Context) error

	// Close closes the cache connection.
	Close() error

	// Stats returns backend-specific statistics.
	Stats() Stats
}

// Stats defines common statistics that all backends should provide.
type Stats interface {
	// Hits returns the number of cache hits.
	Hits() int64
	// Misses returns the number of cache misses.
	Misses() int64
	// Errors returns the number of errors.
	Errors() int64
	// Latency returns average latency in microseconds.
	Latency() int64
}

// Creator is the interface for creating cache instances.
type Creator interface {
	Create(cfg *Config) (Cache, error)
}

// =============================================================================
// Health Check Support
// =============================================================================

// HealthChecker defines the interface for health checks.
type HealthChecker interface {
	Health(ctx context.Context) error
}

// ReadyChecker defines the interface for readiness checks.
type ReadyChecker interface {
	Ready(ctx context.Context) error
}

// HealthCheck returns a combined health check for a cache.
func HealthCheck(ctx context.Context, cache Cache) error {
	if hc, ok := cache.(HealthChecker); ok {
		return hc.Health(ctx)
	}
	return cache.Ping(ctx)
}

// =============================================================================
// Context Helpers
// =============================================================================

// WithTimeout creates a context with the specified timeout (in seconds).
func WithTimeout(parent context.Context, timeoutSeconds int) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, time.Duration(timeoutSeconds)*time.Second)
}

// WithDeadline creates a context with the specified deadline.
func WithDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	return context.WithDeadline(parent, deadline)
}
