// Package observability tracing adapters.
//
// OtelTracer and TraceObserver both consume the OpenTelemetry global
// tracer provider installed by observability.InitTracing. They do not
// own or build their own SDK; callers wire the global via InitTracing
// (which dispatches to the jaeger / otlp / redis_stream / kafka_topic
// sub-packages) and any Tracer / Span obtained from these types will
// flow through that global. If InitTracing was never called (or
// Enabled was false), otel.Tracer() falls back to the package noop
// provider, so these types remain safe to use in uninitialised
// processes.

package observability

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// OtelTracer implements Tracer using OpenTelemetry.
type OtelTracer struct {
	tracer oteltrace.Tracer
}

// NewOtelTracer creates a new OpenTelemetry-based tracer.
func NewOtelTracer(instrumentationName string) *OtelTracer {
	return &OtelTracer{tracer: otel.Tracer(instrumentationName)}
}

// Start implements Tracer.
func (o *OtelTracer) Start(ctx context.Context, spanName string, attrs ...any) (context.Context, Span) {
	ctx, span := o.tracer.Start(ctx, spanName,
		oteltrace.WithSpanKind(oteltrace.SpanKindClient),
	)
	s := &otelSpan{span: span}
	for i := 0; i+1 < len(attrs); i += 2 {
		s.SetAttribute(fmt.Sprintf("%v", attrs[i]), attrs[i+1])
	}
	return ctx, s
}

// otelSpan wraps an oteltrace.Span to satisfy observability.Span.
type otelSpan struct {
	span oteltrace.Span
}

func (s *otelSpan) End(err error) {
	if err != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, err.Error())
	} else {
		s.span.SetStatus(codes.Ok, "")
	}
	s.span.End()
}

func (s *otelSpan) SetAttribute(key string, value any) {
	s.span.SetAttributes(anyToAttribute(key, value))
}

// anyToAttribute converts a Go value to an OTel attribute.KeyValue.
func anyToAttribute(key string, value any) attribute.KeyValue {
	switch v := value.(type) {
	case string:
		return attribute.String(key, v)
	case int:
		return attribute.Int(key, v)
	case int64:
		return attribute.Int64(key, v)
	case float64:
		return attribute.Float64(key, v)
	case bool:
		return attribute.Bool(key, v)
	default:
		return attribute.String(key, fmt.Sprintf("%v", v))
	}
}

// TraceObserver implements cachex.Observer with OpenTelemetry tracing.
type TraceObserver struct {
	tracer oteltrace.Tracer
}

// NewTraceObserver creates a new trace observer.
func NewTraceObserver(tracer oteltrace.Tracer) *TraceObserver {
	if tracer == nil {
		tracer = otel.Tracer("github.com/gospacex/cachex")
	}
	return &TraceObserver{tracer: tracer}
}

// OnOperation implements Observer interface with tracing.
// operation is passed as string to avoid import cycle with cachex package.
func (t *TraceObserver) OnOperation(ctx context.Context, operation string, backend string, err error, duration time.Duration) {
	span := oteltrace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.String("cache.operation", operation),
		attribute.String("cache.backend", backend),
		attribute.Int64("cache.duration_ms", duration.Milliseconds()),
	)
	if err != nil {
		span.SetAttributes(attribute.String("error", err.Error()))
		span.RecordError(err)
	}
}

// OnError implements Observer interface.
func (t *TraceObserver) OnError(ctx context.Context, operation string, backend string, err error) {
	span := oteltrace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.String("cache.operation", operation),
		attribute.String("cache.backend", backend),
		attribute.String("error", err.Error()),
	)
	span.RecordError(err)
}
