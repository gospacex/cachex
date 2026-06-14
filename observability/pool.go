package observability

import (
	"context"
	"sync"
	"time"

	"github.com/gospacex/cachex"
)

// PoolStats represents connection pool statistics.
type PoolStats struct {
	// Active is the number of active connections.
	Active int64 `json:"active"`
	// Idle is the number of idle connections.
	Idle int64 `json:"idle"`
	// Total is the total number of connections.
	Total int64 `json:"total"`
	// Stale is the number of stale connections.
	Stale int64 `json:"stale"`
	// WaitCount is the number of times a caller had to wait for a connection.
	WaitCount int64 `json:"wait_count"`
	// WaitDuration is the total time waited for connections.
	WaitDuration time.Duration `json:"wait_duration"`
	// MaxIdleClosed is the number of connections closed due to max idle limit.
	MaxIdleClosed int64 `json:"max_idle_closed"`
	// MaxLifetimeClosed is the number of connections closed due to max lifetime limit.
	MaxLifetimeClosed int64 `json:"max_lifetime_closed"`
}

// PoolMonitor monitors and reports connection pool health.
type PoolMonitor struct {
	mu       sync.RWMutex
	samples  []PoolStats
	maxSize  int
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewPoolMonitor creates a new pool monitor.
func NewPoolMonitor(maxSize int, interval time.Duration) *PoolMonitor {
	return &PoolMonitor{
		samples:  make([]PoolStats, 0, 1000),
		maxSize:  maxSize,
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Record records pool statistics.
func (m *PoolMonitor) Record(stats PoolStats) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Keep only last 1000 samples
	if len(m.samples) >= 1000 {
		m.samples = m.samples[1:]
	}
	m.samples = append(m.samples, stats)
}

// Start begins monitoring a cache's pool.
func (m *PoolMonitor) Start(ctx context.Context, cache cachex.Cache) {
	go func() {
		defer close(m.doneCh)
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-m.stopCh:
				return
			case <-ticker.C:
				// Poll pool stats if cache provides them
				stats := m.collectStats(ctx, cache)
				m.Record(stats)
			}
		}
	}()
}

// Stop stops the pool monitor.
func (m *PoolMonitor) Stop() {
	close(m.stopCh)
	<-m.doneCh
}

func (m *PoolMonitor) collectStats(ctx context.Context, cache cachex.Cache) PoolStats {
	// Try to get stats from cache via stats interface
	// This is a simplified implementation
	return PoolStats{
		Active: 0,
		Idle:   0,
		Total:  0,
	}
}

// CurrentStats returns the most recent pool stats.
func (m *PoolMonitor) CurrentStats() PoolStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.samples) == 0 {
		return PoolStats{}
	}
	return m.samples[len(m.samples)-1]
}

// AverageStats returns average pool stats over all samples.
func (m *PoolMonitor) AverageStats() PoolStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.samples) == 0 {
		return PoolStats{}
	}

	var total PoolStats
	for _, s := range m.samples {
		total.Active += s.Active
		total.Idle += s.Idle
		total.Total += s.Total
		total.Stale += s.Stale
		total.WaitCount += s.WaitCount
		total.WaitDuration += s.WaitDuration
		total.MaxIdleClosed += s.MaxIdleClosed
		total.MaxLifetimeClosed += s.MaxLifetimeClosed
	}

	n := int64(len(m.samples))
	return PoolStats{
		Active:            total.Active / n,
		Idle:              total.Idle / n,
		Total:             total.Total / n,
		Stale:             total.Stale / n,
		WaitCount:         total.WaitCount / n,
		WaitDuration:      time.Duration(total.WaitDuration.Nanoseconds() / n),
		MaxIdleClosed:     total.MaxIdleClosed / n,
		MaxLifetimeClosed: total.MaxLifetimeClosed / n,
	}
}

// UtilizationRate returns the current pool utilization rate (0.0 to 1.0).
func (m *PoolMonitor) UtilizationRate() float64 {
	stats := m.CurrentStats()
	if stats.Total == 0 {
		return 0
	}
	return float64(stats.Active) / float64(stats.Total)
}

// HealthStatus returns the health status of the pool.
func (m *PoolMonitor) HealthStatus() string {
	util := m.UtilizationRate()

	if util > 0.9 {
		return "critical"
	} else if util > 0.7 {
		return "warning"
	}
	return "healthy"
}

// PoolHealthChecker wraps a cache with pool health monitoring.
type PoolHealthChecker struct {
	cache   cachex.Cache
	monitor *PoolMonitor
}

// NewPoolHealthChecker creates a new pool health checker.
func NewPoolHealthChecker(cache cachex.Cache) *PoolHealthChecker {
	return &PoolHealthChecker{
		cache:   cache,
		monitor: NewPoolMonitor(100, time.Second),
	}
}

// Health checks pool health.
func (p *PoolHealthChecker) Health(ctx context.Context) error {
	status := p.monitor.HealthStatus()
	if status == "critical" {
		return &PoolHealthError{
			Message: "pool utilization critically high",
			Status:  status,
		}
	}
	return p.cache.Ping(ctx)
}

// Monitor returns the pool monitor.
func (p *PoolHealthChecker) Monitor() *PoolMonitor {
	return p.monitor
}

// Close closes the health checker and its monitor.
func (p *PoolHealthChecker) Close() error {
	p.monitor.Stop()
	return p.cache.Close()
}

// PoolHealthError represents a pool health error.
type PoolHealthError struct {
	Message string
	Status  string
}

func (e *PoolHealthError) Error() string {
	return e.Message
}

// Get implements Cache interface.
func (p *PoolHealthChecker) Get(ctx context.Context, key string) ([]byte, error) {
	return p.cache.Get(ctx, key)
}

// Set implements Cache interface.
func (p *PoolHealthChecker) Set(ctx context.Context, key string, value []byte) error {
	return p.cache.Set(ctx, key, value)
}

// SetEX implements Cache interface.
func (p *PoolHealthChecker) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	return p.cache.SetEX(ctx, key, value, ttlSeconds)
}

// SetNX implements Cache interface.
func (p *PoolHealthChecker) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	return p.cache.SetNX(ctx, key, value, ttlSeconds)
}

// Delete implements Cache interface.
func (p *PoolHealthChecker) Delete(ctx context.Context, keys ...string) (int64, error) {
	return p.cache.Delete(ctx, keys...)
}

// Exists implements Cache interface.
func (p *PoolHealthChecker) Exists(ctx context.Context, keys ...string) (int64, error) {
	return p.cache.Exists(ctx, keys...)
}

// Expire implements Cache interface.
func (p *PoolHealthChecker) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	return p.cache.Expire(ctx, key, ttlSeconds)
}

// TTL implements Cache interface.
func (p *PoolHealthChecker) TTL(ctx context.Context, key string) (int64, error) {
	return p.cache.TTL(ctx, key)
}

// MGet implements Cache interface.
func (p *PoolHealthChecker) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	return p.cache.MGet(ctx, keys...)
}

// MSet implements Cache interface.
func (p *PoolHealthChecker) MSet(ctx context.Context, kvs map[string][]byte) error {
	return p.cache.MSet(ctx, kvs)
}

// Keys implements Cache interface.
func (p *PoolHealthChecker) Keys(ctx context.Context, pattern string) ([]string, error) {
	return p.cache.Keys(ctx, pattern)
}

// Incr implements Cache interface.
func (p *PoolHealthChecker) Incr(ctx context.Context, key string) (int64, error) {
	return p.cache.Incr(ctx, key)
}

// Decr implements Cache interface.
func (p *PoolHealthChecker) Decr(ctx context.Context, key string) (int64, error) {
	return p.cache.Decr(ctx, key)
}

// Ping implements Cache interface.
func (p *PoolHealthChecker) Ping(ctx context.Context) error {
	return p.cache.Ping(ctx)
}

// Stats implements Cache interface.
func (p *PoolHealthChecker) Stats() cachex.Stats {
	return p.cache.Stats()
}
