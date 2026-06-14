package cachex

import "sync/atomic"

// BaseStats provides common hit/miss/error counters for backends.
// Embed this struct or use its methods to implement cachex.Stats.
type BaseStats struct {
	hits   int64
	misses int64
	errors int64
}

// NewBaseStats creates a new BaseStats instance.
func NewBaseStats() *BaseStats {
	return &BaseStats{}
}

// Hits returns the number of cache hits.
func (s *BaseStats) Hits() int64 { return atomic.LoadInt64(&s.hits) }

// Misses returns the number of cache misses.
func (s *BaseStats) Misses() int64 { return atomic.LoadInt64(&s.misses) }

// Errors returns the number of errors.
func (s *BaseStats) Errors() int64 { return atomic.LoadInt64(&s.errors) }

// Latency returns average latency in microseconds (not tracked here).
func (s *BaseStats) Latency() int64 { return 0 }

// RecordHit records a cache hit.
func (s *BaseStats) RecordHit() { atomic.AddInt64(&s.hits, 1) }

// RecordMiss records a cache miss.
func (s *BaseStats) RecordMiss() { atomic.AddInt64(&s.misses, 1) }

// RecordError records an error.
func (s *BaseStats) RecordError() { atomic.AddInt64(&s.errors, 1) }
