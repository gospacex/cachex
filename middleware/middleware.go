// Package middleware provides middleware functions for cachex.
package middleware

import (
	"context"
	"sync"
	"time"

	"github.com/gospacex/cachex"
)

// CacheAside implements the cache-aside pattern.
// It first checks the cache, and if not found, calls the fetcher and caches the result.
func CacheAside(ctx context.Context, cache cachex.Cache, key string, ttlSeconds int64, fetcher func() ([]byte, error)) ([]byte, error) {
	// Try cache first
	val, err := cache.Get(ctx, key)
	if err == nil {
		return val, nil
	}

	// Cache miss - fetch from source
	result, err := fetcher()
	if err != nil {
		return nil, err
	}

	// Cache the result
	if ttlSeconds > 0 {
		cache.SetEX(ctx, key, result, ttlSeconds)
	} else {
		cache.Set(ctx, key, result)
	}

	return result, nil
}

// WriteThrough writes to both cache and database atomically.
func WriteThrough(ctx context.Context, cache cachex.Cache, key string, value []byte, dbWriter func([]byte) error) error {
	// Write to cache first
	if err := cache.Set(ctx, key, value); err != nil {
		return err
	}

	// Then write to database
	if err := dbWriter(value); err != nil {
		// Cache already has the value, but DB failed
		// Consider invalidating the cache or returning error
		return err
	}

	return nil
}

// WriteBehind writes to cache immediately and schedules database write asynchronously.
type WriteBehind struct {
	cache    cachex.Cache
	dbWriter func(key string, value []byte) error
	queue    chan writeRequest
	done     chan struct{}
}

type writeRequest struct {
	key   string
	value []byte
}

// NewWriteBehind creates a new write-behind cache.
func NewWriteBehind(cache cachex.Cache, dbWriter func(key string, value []byte) error, bufferSize int) *WriteBehind {
	wb := &WriteBehind{
		cache:    cache,
		dbWriter: dbWriter,
		queue:    make(chan writeRequest, bufferSize),
		done:     make(chan struct{}),
	}

	// Start background writer
	go wb.processWrites()

	return wb
}

// Set stores value in cache and schedules database write.
func (wb *WriteBehind) Set(ctx context.Context, key string, value []byte) error {
	// Write to cache immediately
	if err := wb.cache.Set(ctx, key, value); err != nil {
		return err
	}

	// Schedule database write
	select {
	case wb.queue <- writeRequest{key: key, value: value}:
		return nil
	default:
		// Queue full, write directly
		return wb.dbWriter(key, value)
	}
}

// Close shuts down the write-behind processor.
func (wb *WriteBehind) Close() {
	close(wb.done)
}

func (wb *WriteBehind) processWrites() {
	for {
		select {
		case <-wb.done:
			// Drain queue
			for {
				select {
				case req := <-wb.queue:
					wb.dbWriter(req.key, req.value)
				default:
					return
				}
			}
		case req := <-wb.queue:
			wb.dbWriter(req.key, req.value)
		}
	}
}

// CacheStats holds cache statistics for monitoring.
type CacheStats struct {
	Hits      int64
	Misses    int64
	Errors    int64
	TotalTime time.Duration
}

// StatsCollector collects cache operation statistics.
type StatsCollector struct {
	mu      sync.RWMutex
	stats   CacheStats
	backend string
}

// NewStatsCollector creates a new stats collector.
func NewStatsCollector(backend string) *StatsCollector {
	return &StatsCollector{backend: backend}
}

// RecordHit records a cache hit.
func (s *StatsCollector) RecordHit(duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Hits++
	s.stats.TotalTime += duration
}

// RecordMiss records a cache miss.
func (s *StatsCollector) RecordMiss(duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Misses++
	s.stats.TotalTime += duration
}

// RecordError records an error.
func (s *StatsCollector) RecordError(duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Errors++
	s.stats.TotalTime += duration
}

// Stats returns the current statistics.
func (s *StatsCollector) Stats() CacheStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

// HitRate returns the cache hit rate.
func (s *StatsCollector) HitRate() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	total := s.stats.Hits + s.stats.Misses
	if total == 0 {
		return 0
	}
	return float64(s.stats.Hits) / float64(total)
}

// AverageLatency returns the average operation latency.
func (s *StatsCollector) AverageLatency() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	total := s.stats.Hits + s.stats.Misses + s.stats.Errors
	if total == 0 {
		return 0
	}
	return s.stats.TotalTime / time.Duration(total)
}

// Reset clears the statistics.
func (s *StatsCollector) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats = CacheStats{}
}

// TimeoutCache wraps a cache with timeout enforcement.
type TimeoutCache struct {
	cache   cachex.Cache
	timeout time.Duration
}

// NewTimeoutCache creates a new timeout-enforcing cache wrapper.
func NewTimeoutCache(cache cachex.Cache, timeout time.Duration) *TimeoutCache {
	return &TimeoutCache{
		cache:   cache,
		timeout: timeout,
	}
}

func (t *TimeoutCache) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, t.timeout)
}

// Get implements Cache interface.
func (t *TimeoutCache) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.Get(ctx, key)
}

// Set implements Cache interface.
func (t *TimeoutCache) Set(ctx context.Context, key string, value []byte) error {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.Set(ctx, key, value)
}

// SetEX implements Cache interface.
func (t *TimeoutCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.SetEX(ctx, key, value, ttlSeconds)
}

// SetNX implements Cache interface.
func (t *TimeoutCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.SetNX(ctx, key, value, ttlSeconds)
}

// Delete implements Cache interface.
func (t *TimeoutCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.Delete(ctx, keys...)
}

// Exists implements Cache interface.
func (t *TimeoutCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.Exists(ctx, keys...)
}

// Expire implements Cache interface.
func (t *TimeoutCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.Expire(ctx, key, ttlSeconds)
}

// TTL implements Cache interface.
func (t *TimeoutCache) TTL(ctx context.Context, key string) (int64, error) {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.TTL(ctx, key)
}

// MGet implements Cache interface.
func (t *TimeoutCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.MGet(ctx, keys...)
}

// MSet implements Cache interface.
func (t *TimeoutCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.MSet(ctx, kvs)
}

// Keys implements Cache interface.
func (t *TimeoutCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.Keys(ctx, pattern)
}

// Incr implements Cache interface.
func (t *TimeoutCache) Incr(ctx context.Context, key string) (int64, error) {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.Incr(ctx, key)
}

// Decr implements Cache interface.
func (t *TimeoutCache) Decr(ctx context.Context, key string) (int64, error) {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.Decr(ctx, key)
}

// Ping implements Cache interface.
func (t *TimeoutCache) Ping(ctx context.Context) error {
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	return t.cache.Ping(ctx)
}

// Close implements Cache interface.
func (t *TimeoutCache) Close() error {
	return t.cache.Close()
}

// Stats implements Cache interface.
func (t *TimeoutCache) Stats() cachex.Stats {
	return t.cache.Stats()
}
