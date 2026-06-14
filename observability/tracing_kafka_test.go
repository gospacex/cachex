package observability

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// setupTracing installs a local TracerProvider + W3C propagator for the test,
// returning a cleanup func that restores the prior globals.
func setupTracing(t *testing.T) (oteltrace.Tracer, func()) {
	t.Helper()

	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	return tp.Tracer("test"), func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	}
}

func TestInjectKafkaTrace_EmptyContext_NoOp(t *testing.T) {
	headers := []sarama.RecordHeader{}
	InjectKafkaTrace(context.Background(), &headers)
	assert.Empty(t, headers, "no headers should be added when context has no active span")
}

func TestInjectKafkaTrace_WithSpan_AddsHeaders(t *testing.T) {
	tracer, cleanup := setupTracing(t)
	defer cleanup()

	ctx, span := tracer.Start(context.Background(), "test-op")
	defer span.End()

	headers := []sarama.RecordHeader{}
	InjectKafkaTrace(ctx, &headers)

	// Should now contain at least a traceparent header
	var found bool
	for _, h := range headers {
		if string(h.Key) == "traceparent" {
			found = true
			assert.NotEmpty(t, h.Value)
		}
	}
	assert.True(t, found, "expected traceparent header to be injected")
}

func TestExtractKafkaTrace_NoHeaders_ReturnsInput(t *testing.T) {
	tracer, cleanup := setupTracing(t)
	defer cleanup()

	ctx, span := tracer.Start(context.Background(), "test-op")
	defer span.End()

	extracted := ExtractKafkaTrace(ctx, nil)
	extracted2 := ExtractKafkaTrace(ctx, []sarama.RecordHeader{})

	// With no headers to extract, no new span context should be added.
	assert.Equal(t, span.SpanContext().TraceID(), oteltrace.SpanFromContext(extracted).SpanContext().TraceID())
	assert.Equal(t, span.SpanContext().TraceID(), oteltrace.SpanFromContext(extracted2).SpanContext().TraceID())
}

func TestExtractKafkaTrace_WithTraceparent_ReturnsContextWithSpan(t *testing.T) {
	_, cleanup := setupTracing(t)
	defer cleanup()

	// Manually craft a traceparent header
	traceID := oteltrace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := oteltrace.SpanID{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}

	// Build traceparent string: version-traceid-spanid-flags
	tp := "00-" + traceID.String() + "-" + spanID.String() + "-01"
	headers := []sarama.RecordHeader{
		{Key: []byte("traceparent"), Value: []byte(tp)},
	}

	extracted := ExtractKafkaTrace(context.Background(), headers)
	gotSC := oteltrace.SpanFromContext(extracted).SpanContext()
	assert.True(t, gotSC.IsValid(), "extracted SpanContext should be valid")
	assert.Equal(t, traceID, gotSC.TraceID(), "trace ID should round-trip via header")
}

func TestKafkaRoundTrip(t *testing.T) {
	tracer, cleanup := setupTracing(t)
	defer cleanup()

	ctx, span := tracer.Start(context.Background(), "producer-op")
	defer span.End()
	originalTraceID := span.SpanContext().TraceID()
	require.True(t, originalTraceID.IsValid())

	headers := []sarama.RecordHeader{}
	InjectKafkaTrace(ctx, &headers)

	// Simulate crossing a process boundary
	extracted := ExtractKafkaTrace(context.Background(), headers)
	gotSC := oteltrace.SpanFromContext(extracted).SpanContext()
	assert.True(t, gotSC.IsValid(), "extracted context should have a valid span")
	assert.Equal(t, originalTraceID, gotSC.TraceID(), "trace IDs should match across inject/extract")
}
