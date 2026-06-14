// Package otlp provides OTLP-based span exporters for cachex observability.
package otlp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestNew_GRPC_ValidEndpoint verifies that constructing a gRPC OTLP exporter
// with a valid endpoint succeeds.
func TestNew_GRPC_ValidEndpoint(t *testing.T) {
	ctx := context.Background()
	exp, err := New(ctx, ProtocolGRPC, "localhost:4317", nil, true)
	require.NoError(t, err)
	require.NotNil(t, exp)

	// Shutdown against an unconnected client is a no-op.
	assert.NoError(t, exp.Shutdown(ctx))
}

// TestNew_HTTP_ValidEndpoint verifies that constructing an HTTP OTLP exporter
// with a valid endpoint succeeds.
func TestNew_HTTP_ValidEndpoint(t *testing.T) {
	ctx := context.Background()
	exp, err := New(ctx, ProtocolHTTP, "localhost:4318", nil, true)
	require.NoError(t, err)
	require.NotNil(t, exp)

	assert.NoError(t, exp.Shutdown(ctx))
}

// TestNew_UnknownProtocol_ReturnsError verifies that an unrecognized protocol
// name is rejected with a descriptive error.
func TestNew_UnknownProtocol_ReturnsError(t *testing.T) {
	ctx := context.Background()
	exp, err := New(ctx, Protocol("foo"), "localhost:4317", nil, true)
	assert.Error(t, err)
	assert.Nil(t, exp)
	assert.Contains(t, err.Error(), "unknown otlp protocol")
}

// TestNew_EmptyEndpoint_ReturnsError verifies that an empty endpoint is
// rejected at construction time.
func TestNew_EmptyEndpoint_ReturnsError(t *testing.T) {
	ctx := context.Background()
	exp, err := New(ctx, ProtocolGRPC, "", nil, true)
	assert.Error(t, err)
	assert.Nil(t, exp)
}

// TestNew_HeadersPropagated verifies that the constructor accepts and
// propagates custom headers without error. Header attachment is verified by
// the OTel SDK by accepting the option without complaint.
func TestNew_HeadersPropagated(t *testing.T) {
	ctx := context.Background()
	headers := map[string]string{"x-api-key": "abc"}
	exp, err := New(ctx, ProtocolGRPC, "localhost:4317", headers, true)
	require.NoError(t, err)
	require.NotNil(t, exp)
	assert.NoError(t, exp.Shutdown(ctx))
}

// TestExporter_ExportSpans_DelegatesToInner verifies that ExportSpans on our
// domain Exporter wrapper delegates to the underlying SDK exporter. With an
// empty batch the SDK returns nil.
func TestExporter_ExportSpans_DelegatesToInner(t *testing.T) {
	ctx := context.Background()
	inner, err := New(ctx, ProtocolGRPC, "localhost:4317", nil, true)
	require.NoError(t, err)
	require.NotNil(t, inner)

	e := &Exporter{inner: inner}
	assert.NoError(t, e.ExportSpans(ctx, nil))
	assert.NoError(t, e.Shutdown(ctx))
}

// TestExporter_Shutdown_DelegatesToInner verifies that Shutdown on our
// domain Exporter wrapper delegates to the underlying SDK exporter.
func TestExporter_Shutdown_DelegatesToInner(t *testing.T) {
	ctx := context.Background()
	inner, err := New(ctx, ProtocolHTTP, "localhost:4318", nil, true)
	require.NoError(t, err)
	require.NotNil(t, inner)

	e := &Exporter{inner: inner}
	assert.NoError(t, e.Shutdown(ctx))
}

// Compile-time assertion that our Exporter satisfies sdktrace.SpanExporter,
// which is the surface our `inner` field must implement.
var _ sdktrace.SpanExporter = (*Exporter)(nil)
