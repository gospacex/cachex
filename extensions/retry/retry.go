// Package retry provides retry functionality for cachex.
package retry

import (
	"context"
	"time"

	"github.com/gospacex/cachex"
)

// Config holds retry configuration.
type Config struct {
	// MaxAttempts is the maximum number of retry attempts.
	MaxAttempts int

	// InitialBackoff is the initial backoff duration.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration.
	MaxBackoff time.Duration

	// Multiplier is the backoff multiplier.
	Multiplier float64

	// Jitter enables random jitter.
	Jitter bool
}

// DefaultConfig returns a default retry configuration.
func DefaultConfig() *Config {
	return &Config{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2.0,
		Jitter:         true,
	}
}

// Retry executes a function with retry logic.
func Retry(ctx context.Context, cfg *Config, fn func() error) error {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	var err error
	backoff := cfg.InitialBackoff

	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(calculateBackoff(backoff, cfg)):
				if backoff < cfg.MaxBackoff {
					backoff = time.Duration(float64(backoff) * cfg.Multiplier)
					if backoff > cfg.MaxBackoff {
						backoff = cfg.MaxBackoff
					}
				}
			}
		}

		err = fn()
		if err == nil {
			return nil
		}

		// Don't retry non-retryable errors
		if !cachex.IsRetryable(err) {
			return err
		}
	}

	return err
}

func calculateBackoff(backoff time.Duration, cfg *Config) time.Duration {
	if cfg.Jitter {
		// Add random jitter between 0% and 100% of the backoff
		jitter := time.Duration(float64(backoff) * (float64(time.Now().UnixNano()%100) / 100))
		return backoff + jitter
	}
	return backoff
}

// RetryableCache wraps a cache with retry logic.
type RetryableCache struct {
	cache cachex.Cache
	cfg   *Config
}

// NewRetryableCache wraps a cache with retry logic.
func NewRetryableCache(cache cachex.Cache, cfg *Config) *RetryableCache {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	return &RetryableCache{
		cache: cache,
		cfg:   cfg,
	}
}

// Get implements Cache interface with retry.
func (r *RetryableCache) Get(ctx context.Context, key string) ([]byte, error) {
	var result []byte
	err := Retry(ctx, r.cfg, func() error {
		var err error
		result, err = r.cache.Get(ctx, key)
		return err
	})
	return result, err
}

// Set implements Cache interface with retry.
func (r *RetryableCache) Set(ctx context.Context, key string, value []byte) error {
	return Retry(ctx, r.cfg, func() error {
		return r.cache.Set(ctx, key, value)
	})
}

// SetEX implements Cache interface with retry.
func (r *RetryableCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	return Retry(ctx, r.cfg, func() error {
		return r.cache.SetEX(ctx, key, value, ttlSeconds)
	})
}

// SetNX implements Cache interface with retry.
func (r *RetryableCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	var result bool
	err := Retry(ctx, r.cfg, func() error {
		var err error
		result, err = r.cache.SetNX(ctx, key, value, ttlSeconds)
		return err
	})
	return result, err
}

// Delete implements Cache interface with retry.
func (r *RetryableCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	var result int64
	err := Retry(ctx, r.cfg, func() error {
		var err error
		result, err = r.cache.Delete(ctx, keys...)
		return err
	})
	return result, err
}

// Exists implements Cache interface with retry.
func (r *RetryableCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	var result int64
	err := Retry(ctx, r.cfg, func() error {
		var err error
		result, err = r.cache.Exists(ctx, keys...)
		return err
	})
	return result, err
}

// Expire implements Cache interface with retry.
func (r *RetryableCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	var result bool
	err := Retry(ctx, r.cfg, func() error {
		var err error
		result, err = r.cache.Expire(ctx, key, ttlSeconds)
		return err
	})
	return result, err
}

// TTL implements Cache interface with retry.
func (r *RetryableCache) TTL(ctx context.Context, key string) (int64, error) {
	var result int64
	err := Retry(ctx, r.cfg, func() error {
		var err error
		result, err = r.cache.TTL(ctx, key)
		return err
	})
	return result, err
}

// MGet implements Cache interface with retry.
func (r *RetryableCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	var result [][]byte
	err := Retry(ctx, r.cfg, func() error {
		var err error
		result, err = r.cache.MGet(ctx, keys...)
		return err
	})
	return result, err
}

// MSet implements Cache interface with retry.
func (r *RetryableCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	return Retry(ctx, r.cfg, func() error {
		return r.cache.MSet(ctx, kvs)
	})
}

// Keys implements Cache interface with retry.
func (r *RetryableCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	var result []string
	err := Retry(ctx, r.cfg, func() error {
		var err error
		result, err = r.cache.Keys(ctx, pattern)
		return err
	})
	return result, err
}

// Incr implements Cache interface with retry.
func (r *RetryableCache) Incr(ctx context.Context, key string) (int64, error) {
	var result int64
	err := Retry(ctx, r.cfg, func() error {
		var err error
		result, err = r.cache.Incr(ctx, key)
		return err
	})
	return result, err
}

// Decr implements Cache interface with retry.
func (r *RetryableCache) Decr(ctx context.Context, key string) (int64, error) {
	var result int64
	err := Retry(ctx, r.cfg, func() error {
		var err error
		result, err = r.cache.Decr(ctx, key)
		return err
	})
	return result, err
}

// Ping implements Cache interface with retry.
func (r *RetryableCache) Ping(ctx context.Context) error {
	return Retry(ctx, r.cfg, func() error {
		return r.cache.Ping(ctx)
	})
}

// Close implements Cache interface.
func (r *RetryableCache) Close() error {
	return r.cache.Close()
}

// Stats implements Cache interface.
func (r *RetryableCache) Stats() cachex.Stats {
	return r.cache.Stats()
}
