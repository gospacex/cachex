package otlp

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Protocol enumerates the OTLP wire protocols we support.
type Protocol string

const (
	// ProtocolGRPC ships spans via OTLP/gRPC (typically port 4317).
	ProtocolGRPC Protocol = "grpc"
	// ProtocolHTTP ships spans via OTLP/HTTP (typically port 4318).
	ProtocolHTTP Protocol = "http"
)

// New constructs an OTel SpanExporter that ships spans via OTLP. The
// supplied protocol picks gRPC vs HTTP. endpoint is the collector target
// (e.g. "localhost:4317" for gRPC, "localhost:4318" for HTTP). insecure=true
// disables TLS. headers are added to every export request.
func New(ctx context.Context, protocol Protocol, endpoint string, headers map[string]string, insecure bool) (sdktrace.SpanExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("otlp: endpoint is required")
	}

	switch protocol {
	case ProtocolGRPC:
		opts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(endpoint),
			otlptracegrpc.WithHeaders(headers),
		}
		if insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		return otlptracegrpc.New(ctx, opts...)
	case ProtocolHTTP:
		opts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(endpoint),
			otlptracehttp.WithHeaders(headers),
		}
		if insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		return otlptracehttp.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("unknown otlp protocol %q", protocol)
	}
}

// Exporter wraps the OTel OTLP exporter to implement our domain-local
// ExportSpans surface (it just delegates to the SDK's serialized batch).
type Exporter struct {
	inner sdktrace.SpanExporter
}

// ExportSpans forwards spans to the underlying OTel SDK exporter.
func (e *Exporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return e.inner.ExportSpans(ctx, spans)
}

// Shutdown releases resources held by the underlying OTel SDK exporter.
func (e *Exporter) Shutdown(ctx context.Context) error {
	return e.inner.Shutdown(ctx)
}
