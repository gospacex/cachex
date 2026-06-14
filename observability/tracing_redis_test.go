package observability

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestInjectRedisTrace_EmptyContext_NoOp(t *testing.T) {
	values := map[string]interface{}{}
	InjectRedisTrace(context.Background(), values)
	_, ok := values["trace"]
	assert.False(t, ok, "no trace key should be added when context has no active span")
}

func TestInjectRedisTrace_WithSpan_AddsTraceKey(t *testing.T) {
	tracer, cleanup := setupRedisTracing(t)
	defer cleanup()

	ctx, span := tracer.Start(context.Background(), "test-op")
	defer span.End()

	values := map[string]interface{}{}
	InjectRedisTrace(ctx, values)

	raw, ok := values["trace"]
	require.True(t, ok, "values[trace] should be set after inject")
	carrier, ok := raw.(map[string]string)
	require.True(t, ok, "values[trace] should be map[string]string, got %T", raw)
	assert.Contains(t, carrier, "traceparent", "carrier should contain traceparent header")
}

func TestExtractRedisTrace_NoTraceKey_ReturnsInput(t *testing.T) {
	tracer, cleanup := setupRedisTracing(t)
	defer cleanup()

	ctx, span := tracer.Start(context.Background(), "test-op")
	defer span.End()

	values := map[string]interface{}{}
	extracted := ExtractRedisTrace(ctx, values)
	assert.Equal(t, span.SpanContext().TraceID(), oteltrace.SpanFromContext(extracted).SpanContext().TraceID())

	// Also when the "trace" key holds an unexpected type, we should fall back to ctx
	badValues := map[string]interface{}{"trace": "not-a-map"}
	extracted2 := ExtractRedisTrace(ctx, badValues)
	assert.Equal(t, span.SpanContext().TraceID(), oteltrace.SpanFromContext(extracted2).SpanContext().TraceID())
}

func TestExtractRedisTrace_WithTraceKey_ReturnsContextWithSpan(t *testing.T) {
	_, cleanup := setupRedisTracing(t)
	defer cleanup()

	traceID := oteltrace.TraceID{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30}
	spanID := oteltrace.SpanID{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38}
	tp := "00-" + traceID.String() + "-" + spanID.String() + "-01"

	values := map[string]interface{}{
		"trace": map[string]string{"traceparent": tp},
	}

	extracted := ExtractRedisTrace(context.Background(), values)
	gotSC := oteltrace.SpanFromContext(extracted).SpanContext()
	assert.True(t, gotSC.IsValid(), "extracted SpanContext should be valid")
	assert.Equal(t, traceID, gotSC.TraceID())
}

func TestRedisRoundTrip(t *testing.T) {
	tracer, cleanup := setupRedisTracing(t)
	defer cleanup()

	ctx, span := tracer.Start(context.Background(), "producer-op")
	defer span.End()
	originalTraceID := span.SpanContext().TraceID()
	require.True(t, originalTraceID.IsValid())

	values := map[string]interface{}{}
	InjectRedisTrace(ctx, values)

	extracted := ExtractRedisTrace(context.Background(), values)
	gotSC := oteltrace.SpanFromContext(extracted).SpanContext()
	assert.True(t, gotSC.IsValid())
	assert.Equal(t, originalTraceID, gotSC.TraceID(), "trace IDs should match across inject/extract")
}

// setupRedisTracing mirrors setupTracing from tracing_kafka_test.go but is
// duplicated here to keep each test file self-contained.
func setupRedisTracing(t *testing.T) (oteltrace.Tracer, func()) {
	t.Helper()

	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()

	tp := sdktrace.NewTracerProvider()
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
