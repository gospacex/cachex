// Package metrics provides Prometheus metrics collection for cachex.
package metrics

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gospacex/cachex"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collector collects Prometheus metrics for cache operations.
type Collector struct {
	mu sync.RWMutex

	operationsTotal   *prometheus.CounterVec
	operationDuration *prometheus.HistogramVec
	errorsTotal       *prometheus.CounterVec
	hitsTotal         *prometheus.CounterVec
	missesTotal       *prometheus.CounterVec
	connectionsActive *prometheus.GaugeVec
	connectionsIdle   *prometheus.GaugeVec
}

type backendMetrics struct {
	operationsTotal   prometheus.Counter
	errorsTotal       prometheus.Counter
	operationDuration prometheus.Histogram
}

// NewCollector creates a new metrics collector.
func NewCollector(namespace, subsystem string) *Collector {
	m := &Collector{
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
	}

	return m
}

// ObserveOperation records metrics for a cache operation.
func (m *Collector) ObserveOperation(ctx context.Context, op cachex.Operation, backend string, err error, duration time.Duration) {
	status := "success"
	if err != nil {
		status = "error"
		m.errorsTotal.WithLabelValues(backend, string(op), classifyError(err)).Inc()
	}

	m.operationsTotal.WithLabelValues(backend, string(op), status).Inc()
	m.operationDuration.WithLabelValues(backend, string(op)).Observe(duration.Seconds())
}

// ObserveGet records metrics for a GET operation.
func (m *Collector) ObserveGet(ctx context.Context, backend string, hit bool, err error, duration time.Duration) {
	if err != nil {
		m.ObserveOperation(ctx, cachex.OpGet, backend, err, duration)
		return
	}

	if hit {
		m.hitsTotal.WithLabelValues(backend, "get").Inc()
	} else {
		m.missesTotal.WithLabelValues(backend, "get").Inc()
	}
	m.ObserveOperation(ctx, cachex.OpGet, backend, nil, duration)
}

// SetConnectionMetrics sets connection pool metrics.
func (m *Collector) SetConnectionMetrics(backend string, active, idle int64) {
	m.connectionsActive.WithLabelValues(backend).Set(float64(active))
	m.connectionsIdle.WithLabelValues(backend).Set(float64(idle))
}

// OnOperation implements the cachex.Observer interface.
func (m *Collector) OnOperation(ctx context.Context, op cachex.Operation, backend string, err error, duration time.Duration) {
	m.ObserveOperation(ctx, op, backend, err, duration)
}

// OnError implements the cachex.Observer interface.
func (m *Collector) OnError(ctx context.Context, op cachex.Operation, backend string, err error) {
	m.errorsTotal.WithLabelValues(backend, string(op), classifyError(err)).Inc()
}

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
	return strings.Contains(s, substr)
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
	atomic.AddInt64(&l.hits, 1)
	atomic.AddInt64(&l.total, 1)
}

// RecordMiss records a cache miss.
func (l *LatencyRecorder) RecordMiss() {
	atomic.AddInt64(&l.misses, 1)
	atomic.AddInt64(&l.total, 1)
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
