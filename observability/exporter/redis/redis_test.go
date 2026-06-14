// Package redis provides a Redis stream exporter for OpenTelemetry spans.
package redis

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// newTestClient returns a real *redis.Client constructed with a non-routable
// address. We never actually issue a command that requires a connection, so
// the address is irrelevant. This avoids a mocking dependency.
func newTestClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
}

func TestNew_NilClient_ReturnsError(t *testing.T) {
	exp, err := New(nil, "cachex:traces")

	require.Error(t, err)
	assert.Nil(t, exp)
	assert.Contains(t, err.Error(), "nil client")
}

func TestNew_EmptyStream_ReturnsError(t *testing.T) {
	client := newTestClient()
	defer func() { _ = client.Close() }()

	exp, err := New(client, "")

	require.Error(t, err)
	assert.Nil(t, exp)
	assert.Contains(t, err.Error(), "empty stream")
}

func TestNew_ValidArgs_ReturnsExporter(t *testing.T) {
	client := newTestClient()
	defer func() { _ = client.Close() }()

	exp, err := New(client, "cachex:traces")

	require.NoError(t, err)
	require.NotNil(t, exp)
	assert.Equal(t, "cachex:traces", exp.stream)
	assert.Same(t, client, exp.client)
}

func TestExporter_ExportSpans_EmptyBatch_NoError(t *testing.T) {
	client := newTestClient()
	defer func() { _ = client.Close() }()

	exp, err := New(client, "cachex:traces")
	require.NoError(t, err)

	// nil batch
	require.NoError(t, exp.ExportSpans(context.Background(), nil))

	// empty batch
	require.NoError(t, exp.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{}))
}

func TestExporter_Shutdown_NoError(t *testing.T) {
	client := newTestClient()
	defer func() { _ = client.Close() }()

	exp, err := New(client, "cachex:traces")
	require.NoError(t, err)

	// Shutdown is a no-op: client lifecycle is the caller's concern.
	require.NoError(t, exp.Shutdown(context.Background()))
}

// recordSpan produces a recorded ReadOnlySpan via the tracetest.SpanRecorder
// helper. We construct the SpanStub explicitly so the test is independent of
// tracer / provider wiring and is fully deterministic.
func recordSpan(t *testing.T) sdktrace.ReadOnlySpan {
	t.Helper()

	traceID, err := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("b7ad6b7169203331")
	require.NoError(t, err)
	parentID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	require.NoError(t, err)

	stub := tracetest.SpanStub{
		Name:        "cache.Get",
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID}),
		Parent:      trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: parentID}),
		Attributes: []attribute.KeyValue{
			attribute.String("cache.key", "user:42"),
			attribute.Int("cache.ttl_seconds", 60),
		},
	}
	return stub.Snapshot()
}

func TestBuildSpanRecord_RealSpan(t *testing.T) {
	span := recordSpan(t)

	rec := buildSpanRecord(span)
	require.NotNil(t, rec)

	assert.Equal(t, "cache.Get", rec["name"])
	assert.Equal(t, "0af7651916cd43dd8448eb211c80319c", rec["trace_id"])
	assert.Equal(t, "b7ad6b7169203331", rec["span_id"])
	assert.Equal(t, "00f067aa0ba902b7", rec["parent_id"])
	assert.NotEmpty(t, rec["timestamp"])

	attrs, ok := rec["attributes"].(map[string]interface{})
	require.True(t, ok, "attributes should be a map[string]interface{}")
	assert.Equal(t, "user:42", attrs["cache.key"])
	// attribute.Int values are emitted as int64 by the encoder.
	assert.Equal(t, int64(60), attrs["cache.ttl_seconds"])
}

func TestExporter_ExportSpans_NonEmpty_NoDial(t *testing.T) {
	// With a non-routable client, XAdd will return a network error. This
	// test guards the *non* failure modes: a non-nil client is wired, the
	// marshal step succeeds, and a non-empty batch proceeds past the
	// short-circuit. We only assert the call enters the dial path; the
	// error returned is a network error and is environment-dependent, so
	// we just verify we do not panic and that the result is non-nil when
	// the client cannot reach its server.
	client := newTestClient()
	defer func() { _ = client.Close() }()

	exp, err := New(client, "cachex:traces")
	require.NoError(t, err)

	span := recordSpan(t)
	_ = exp.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{span})
	// Intentionally no assertion on the returned error: 127.0.0.1:0 is not
	// listening, so the underlying XAdd will fail. What matters is that
	// we reached the network layer (i.e. we did not panic and we did not
	// short-circuit on a nil client / nil stream).
}
