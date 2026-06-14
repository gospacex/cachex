// Package observability provides metrics, logging, and tracing support for cachex.
package observability

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// MetricsCollector collects Prometheus metrics for cache operations.
type MetricsCollector struct {
	mu sync.RWMutex

	// Operation counters
	operationsTotal *prometheus.CounterVec

	// Operation duration
	operationDuration *prometheus.HistogramVec

	// Error counter
	errorsTotal *prometheus.CounterVec

	// Cache hit/miss counters
	hitsTotal   *prometheus.CounterVec
	missesTotal *prometheus.CounterVec

	// Connection pool metrics
	connectionsActive *prometheus.GaugeVec
	connectionsIdle   *prometheus.GaugeVec

	// Backend-specific metrics
	backendMetrics map[string]*backendMetrics
}

// backendMetrics holds metrics for a specific backend.
type backendMetrics struct {
	operationsTotal   prometheus.Counter
	errorsTotal       prometheus.Counter
	operationDuration prometheus.Histogram
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector(namespace, subsystem string) *MetricsCollector {
	m := &MetricsCollector{
		operationsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "operations_total",
				Help:      "Total number of cache operations",
			},
			[]string{"backend", "operation", "status"},
		),
		operationDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "operation_duration_seconds",
				Help:      "Duration of cache operations in seconds",
				Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			},
			[]string{"backend", "operation"},
		),
		errorsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "errors_total",
				Help:      "Total number of cache errors",
			},
			[]string{"backend", "operation", "error_type"},
		),
		hitsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "hits_total",
				Help:      "Total number of cache hits",
			},
			[]string{"backend", "operation"},
		),
		missesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "misses_total",
				Help:      "Total number of cache misses",
			},
			[]string{"backend", "operation"},
		),
		connectionsActive: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "connections_active",
				Help:      "Number of active connections",
			},
			[]string{"backend"},
		),
		connectionsIdle: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "connections_idle",
				Help:      "Number of idle connections",
			},
			[]string{"backend"},
		),
		backendMetrics: make(map[string]*backendMetrics),
	}

	return m
}

// ObserveOperation records metrics for a cache operation.
func (m *MetricsCollector) ObserveOperation(ctx context.Context, op string, backend string, err error, duration time.Duration) {
	status := "success"
	if err != nil {
		status = "error"
		m.errorsTotal.WithLabelValues(backend, op, classifyError(err)).Inc()
	}

	m.operationsTotal.WithLabelValues(backend, op, status).Inc()
	m.operationDuration.WithLabelValues(backend, op).Observe(duration.Seconds())
}

// ObserveGet records metrics for a GET operation.
func (m *MetricsCollector) ObserveGet(ctx context.Context, backend string, hit bool, err error, duration time.Duration) {
	if err != nil {
		m.ObserveOperation(ctx, "get", backend, err, duration)
		return
	}

	if hit {
		m.hitsTotal.WithLabelValues(backend, "get").Inc()
	} else {
		m.missesTotal.WithLabelValues(backend, "get").Inc()
	}
	m.ObserveOperation(ctx, "get", backend, nil, duration)
}

// SetConnectionMetrics sets connection pool metrics.
func (m *MetricsCollector) SetConnectionMetrics(backend string, active, idle int64) {
	m.connectionsActive.WithLabelValues(backend).Set(float64(active))
	m.connectionsIdle.WithLabelValues(backend).Set(float64(idle))
}

// OnOperation implements the cachex.Observer interface.
func (m *MetricsCollector) OnOperation(ctx context.Context, op string, backend string, err error, duration time.Duration) {
	m.ObserveOperation(ctx, op, backend, err, duration)
}

// OnError implements the cachex.Observer interface.
func (m *MetricsCollector) OnError(ctx context.Context, op string, backend string, err error) {
	m.errorsTotal.WithLabelValues(backend, op, classifyError(err)).Inc()
}

// classifyError classifies an error into a category.
func classifyError(err error) string {
	if err == nil {
		return "none"
	}

	errStr := err.Error()
	switch {
	case contains(errStr, "not found"):
		return "not_found"
	case contains(errStr, "timeout"):
		return "timeout"
	case contains(errStr, "connection"):
		return "connection"
	case contains(errStr, "closed"):
		return "closed"
	default:
		return "other"
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// LatencyRecorder records operation latency.
type LatencyRecorder struct {
	mu     sync.RWMutex
	hits   int64
	misses int64
	total  int64
}

// RecordHit records a cache hit.
func (l *LatencyRecorder) RecordHit() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.hits++
	l.total++
}

// RecordMiss records a cache miss.
func (l *LatencyRecorder) RecordMiss() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.misses++
	l.total++
}

// HitRate returns the cache hit rate.
func (l *LatencyRecorder) HitRate() float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	total := l.total
	if total == 0 {
		return 0
	}
	return float64(l.hits) / float64(total)
}

// Stats returns the statistics.
func (l *LatencyRecorder) Stats() (hits, misses, total int64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.hits, l.misses, l.total
}

// MetricsMiddleware creates an observer that collects metrics.
func MetricsMiddleware(namespace, subsystem string) *MetricsCollector {
	return NewMetricsCollector(namespace, subsystem)
}
