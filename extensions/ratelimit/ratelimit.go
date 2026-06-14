// Package ratelimit provides rate limiting functionality for cachex.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/gospacex/cachex"
)

// TokenBucket implements a token bucket rate limiter.
type TokenBucket struct {
	mu         sync.Mutex
	capacity   int64
	tokens     int64
	fillRate   time.Duration // time to add one token
	lastRefill time.Time
}

// NewTokenBucket creates a new token bucket rate limiter.
// capacity: maximum number of tokens
// refillRate: tokens added per second
func NewTokenBucket(capacity int64, refillRate int64) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		tokens:     capacity,
		fillRate:   time.Second / time.Duration(refillRate),
		lastRefill: time.Now(),
	}
}

// Allow checks if a request is allowed.
func (t *TokenBucket) Allow() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.refill()

	if t.tokens > 0 {
		t.tokens--
		return true
	}
	return false
}

// Wait waits until a token is available.
func (t *TokenBucket) Wait(ctx context.Context) error {
	for {
		if t.Allow() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.fillRate):
			continue
		}
	}
}

// refill adds tokens based on elapsed time.
func (t *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(t.lastRefill)

	tokensToAdd := int64(elapsed / t.fillRate)
	if tokensToAdd > 0 {
		t.tokens = min(t.capacity, t.tokens+tokensToAdd)
		t.lastRefill = now
	}
}

// Available returns the number of available tokens.
func (t *TokenBucket) Available() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refill()
	return t.tokens
}

// Reset resets the token bucket to full capacity.
func (t *TokenBucket) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tokens = t.capacity
	t.lastRefill = time.Now()
}

// SlidingWindow implements a sliding window rate limiter.
type SlidingWindow struct {
	mu       sync.RWMutex
	capacity int64
	window   time.Duration
	requests []time.Time
}

// NewSlidingWindow creates a new sliding window rate limiter.
func NewSlidingWindow(capacity int64, window time.Duration) *SlidingWindow {
	return &SlidingWindow{
		capacity: capacity,
		window:   window,
		requests: make([]time.Time, 0),
	}
}

// Allow checks if a request is allowed.
func (s *SlidingWindow) Allow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanup()

	if int64(len(s.requests)) < s.capacity {
		s.requests = append(s.requests, time.Now())
		return true
	}
	return false
}

// Wait waits until a request is allowed.
func (s *SlidingWindow) Wait(ctx context.Context) error {
	for {
		if s.Allow() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
			continue
		}
	}
}

// cleanup removes expired requests.
func (s *SlidingWindow) cleanup() {
	cutoff := time.Now().Add(-s.window)
	i := 0
	for ; i < len(s.requests); i++ {
		if s.requests[i].After(cutoff) {
			break
		}
	}
	if i > 0 {
		s.requests = s.requests[i:]
	}
}

// Current returns the current number of requests in the window.
func (s *SlidingWindow) Current() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.cleanup()
	return int64(len(s.requests))
}

// RateLimitedCache wraps a cache with rate limiting.
type RateLimitedCache struct {
	cache   cachex.Cache
	limiter *TokenBucket
	mu      sync.Mutex
}

// NewRateLimitedCache wraps a cache with a token bucket rate limiter.
func NewRateLimitedCache(cache cachex.Cache, capacity, refillRate int64) *RateLimitedCache {
	return &RateLimitedCache{
		cache:   cache,
		limiter: NewTokenBucket(capacity, refillRate),
	}
}

// Allow checks if a request is allowed.
func (r *RateLimitedCache) Allow() bool {
	return r.limiter.Allow()
}

// Wait waits until a request is allowed.
func (r *RateLimitedCache) Wait(ctx context.Context) error {
	return r.limiter.Wait(ctx)
}

// Get implements Cache interface.
func (r *RateLimitedCache) Get(ctx context.Context, key string) ([]byte, error) {
	if err := r.Wait(ctx); err != nil {
		return nil, err
	}
	return r.cache.Get(ctx, key)
}

// Set implements Cache interface.
func (r *RateLimitedCache) Set(ctx context.Context, key string, value []byte) error {
	if err := r.Wait(ctx); err != nil {
		return err
	}
	return r.cache.Set(ctx, key, value)
}

// SetEX implements Cache interface.
func (r *RateLimitedCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	if err := r.Wait(ctx); err != nil {
		return err
	}
	return r.cache.SetEX(ctx, key, value, ttlSeconds)
}

// SetNX implements Cache interface.
func (r *RateLimitedCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	if err := r.Wait(ctx); err != nil {
		return false, err
	}
	return r.cache.SetNX(ctx, key, value, ttlSeconds)
}

// Delete implements Cache interface.
func (r *RateLimitedCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	if err := r.Wait(ctx); err != nil {
		return 0, err
	}
	return r.cache.Delete(ctx, keys...)
}

// Exists implements Cache interface.
func (r *RateLimitedCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	if err := r.Wait(ctx); err != nil {
		return 0, err
	}
	return r.cache.Exists(ctx, keys...)
}

// Expire implements Cache interface.
func (r *RateLimitedCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	if err := r.Wait(ctx); err != nil {
		return false, err
	}
	return r.cache.Expire(ctx, key, ttlSeconds)
}

// TTL implements Cache interface.
func (r *RateLimitedCache) TTL(ctx context.Context, key string) (int64, error) {
	if err := r.Wait(ctx); err != nil {
		return 0, err
	}
	return r.cache.TTL(ctx, key)
}

// MGet implements Cache interface.
func (r *RateLimitedCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	if err := r.Wait(ctx); err != nil {
		return nil, err
	}
	return r.cache.MGet(ctx, keys...)
}

// MSet implements Cache interface.
func (r *RateLimitedCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	if err := r.Wait(ctx); err != nil {
		return err
	}
	return r.cache.MSet(ctx, kvs)
}

// Keys implements Cache interface.
func (r *RateLimitedCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	if err := r.Wait(ctx); err != nil {
		return nil, err
	}
	return r.cache.Keys(ctx, pattern)
}

// Incr implements Cache interface.
func (r *RateLimitedCache) Incr(ctx context.Context, key string) (int64, error) {
	if err := r.Wait(ctx); err != nil {
		return 0, err
	}
	return r.cache.Incr(ctx, key)
}

// Decr implements Cache interface.
func (r *RateLimitedCache) Decr(ctx context.Context, key string) (int64, error) {
	if err := r.Wait(ctx); err != nil {
		return 0, err
	}
	return r.cache.Decr(ctx, key)
}

// Ping implements Cache interface.
func (r *RateLimitedCache) Ping(ctx context.Context) error {
	return r.cache.Ping(ctx)
}

// Close implements Cache interface.
func (r *RateLimitedCache) Close() error {
	return r.cache.Close()
}

// Stats implements Cache interface.
func (r *RateLimitedCache) Stats() cachex.Stats {
	return r.cache.Stats()
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
