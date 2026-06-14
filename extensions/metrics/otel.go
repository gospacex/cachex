// Package metrics provides observability metrics for cachex.
package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// OTeletryCollector implements MetricsCollector using OpenTelemetry.
type OTeletryCollector struct {
	meter metric.Meter

	// Counters
	requestsTotal metric.Int64Counter
	hitsTotal     metric.Int64Counter
	missesTotal   metric.Int64Counter
	errorsTotal   metric.Int64Counter

	// Histograms
	latencySeconds metric.Float64Histogram
}

// NewOTeletryCollector creates a new OpenTelemetry-based metrics collector.
func NewOTeletryCollector(meter metric.Meter) (*OTeletryCollector, error) {
	requestsTotal, err := meter.Int64Counter(
		"cachex_requests_total",
		metric.WithDescription("Total number of cache requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return nil, err
	}

	hitsTotal, err := meter.Int64Counter(
		"cachex_hits_total",
		metric.WithDescription("Total number of cache hits"),
		metric.WithUnit("{hit}"),
	)
	if err != nil {
		return nil, err
	}

	missesTotal, err := meter.Int64Counter(
		"cachex_misses_total",
		metric.WithDescription("Total number of cache misses"),
		metric.WithUnit("{miss}"),
	)
	if err != nil {
		return nil, err
	}

	errorsTotal, err := meter.Int64Counter(
		"cachex_errors_total",
		metric.WithDescription("Total number of cache errors"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	latencySeconds, err := meter.Float64Histogram(
		"cachex_latency_seconds",
		metric.WithDescription("Latency of cache operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	return &OTeletryCollector{
		meter:          meter,
		requestsTotal:  requestsTotal,
		hitsTotal:      hitsTotal,
		missesTotal:    missesTotal,
		errorsTotal:    errorsTotal,
		latencySeconds: latencySeconds,
	}, nil
}

// RecordGet implements MetricsCollector.
func (c *OTeletryCollector) RecordGet(ctx context.Context, hit bool, latency time.Duration) {
	attrs := []attribute.KeyValue{
		attribute.String("operation", "get"),
	}

	if hit {
		c.hitsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
		c.requestsTotal.Add(ctx, 1, metric.WithAttributes(append(attrs, attribute.String("status", "hit"))...))
	} else {
		c.missesTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
		c.requestsTotal.Add(ctx, 1, metric.WithAttributes(append(attrs, attribute.String("status", "miss"))...))
	}

	c.latencySeconds.Record(ctx, latency.Seconds(), metric.WithAttributes(attrs...))
}

// RecordSet implements MetricsCollector.
func (c *OTeletryCollector) RecordSet(ctx context.Context, latency time.Duration) {
	attrs := []attribute.KeyValue{
		attribute.String("operation", "set"),
		attribute.String("status", "success"),
	}

	c.requestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	c.latencySeconds.Record(ctx, latency.Seconds(), metric.WithAttributes(attrs...))
}

// RecordDelete implements MetricsCollector.
func (c *OTeletryCollector) RecordDelete(ctx context.Context, keysDeleted int64, latency time.Duration) {
	attrs := []attribute.KeyValue{
		attribute.String("operation", "delete"),
		attribute.String("status", "success"),
	}

	c.requestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	c.latencySeconds.Record(ctx, latency.Seconds(), metric.WithAttributes(attrs...))
}

// RecordError implements MetricsCollector.
func (c *OTeletryCollector) RecordError(ctx context.Context, operation string, err error) {
	attrs := []attribute.KeyValue{
		attribute.String("operation", operation),
		attribute.String("status", "error"),
		attribute.String("error_type", classifyError(err)),
	}

	c.errorsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	c.requestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// classifyError classifies an error into a category.
func classifyError(err error) string {
	if err == nil {
		return "none"
	}

	errStr := err.Error()
	switch {
	case contains(errStr, "not found") || contains(errStr, "KeyNotFound"):
		return "not_found"
	case contains(errStr, "timeout"):
		return "timeout"
	case contains(errStr, "closed"):
		return "closed"
	case contains(errStr, "connection"):
		return "connection"
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
