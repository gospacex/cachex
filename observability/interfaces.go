package observability

import (
	"context"
	"time"
)

// LoggerInterface defines the structured logging interface.
type LoggerInterface interface {
	Debug(ctx context.Context, msg string, fields ...map[string]interface{})
	Info(ctx context.Context, msg string, fields ...map[string]interface{})
	Warn(ctx context.Context, msg string, fields ...map[string]interface{})
	Error(ctx context.Context, msg string, fields ...map[string]interface{})
}

// Span is a running distributed-tracing span.
type Span interface {
	End(err error)
	SetAttribute(key string, value any)
}

// Tracer starts and manages distributed-tracing spans.
type Tracer interface {
	Start(ctx context.Context, spanName string, attrs ...any) (context.Context, Span)
}

// Labels is an ordered list of label key-value pairs attached to a metric.
type Labels = []string

// Metrics exposes the minimal recording surface cachex needs.
type Metrics interface {
	IncCounter(name string, labels Labels)
	AddCounter(name string, delta float64, labels Labels)
	SetGauge(name string, value float64, labels Labels)
	ObserveHistogram(name string, value float64, labels Labels)
}

// Provider bundles Logger, Metrics, and Tracer into a single value.
type Provider struct {
	Logger  LoggerInterface
	Metrics Metrics
	Tracer  Tracer
}

// DefaultProvider returns a Provider backed by no-op implementations.
func DefaultProvider() Provider {
	return Provider{
		Logger:  noopLogger{},
		Metrics: &noopMetrics{},
		Tracer:  &noopTracer{},
	}
}

// NoopLogger discards every log call.
type noopLogger struct{}

func (noopLogger) Debug(_ context.Context, _ string, _ ...map[string]interface{}) {}
func (noopLogger) Info(_ context.Context, _ string, _ ...map[string]interface{})  {}
func (noopLogger) Warn(_ context.Context, _ string, _ ...map[string]interface{})  {}
func (noopLogger) Error(_ context.Context, _ string, _ ...map[string]interface{}) {}

// noopMetrics discards every metric recording.
type noopMetrics struct{}

func (noopMetrics) IncCounter(_ string, _ Labels)                  {}
func (noopMetrics) AddCounter(_ string, _ float64, _ Labels)       {}
func (noopMetrics) SetGauge(_ string, _ float64, _ Labels)         {}
func (noopMetrics) ObserveHistogram(_ string, _ float64, _ Labels) {}

// noopTracer returns a no-op span on every Start call.
type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, _ string, _ ...any) (context.Context, Span) {
	return ctx, &noopSpan{}
}

type noopSpan struct{}

func (noopSpan) End(_ error)                  {}
func (noopSpan) SetAttribute(_ string, _ any) {}

// Since returns the number of seconds elapsed since t, as a float64.
func Since(t time.Time) float64 {
	return time.Since(t).Seconds()
}
