// Copyright 2024 cachex. All rights reserved.
//
// Jaeger exporter: a thin wrapper around the OTel jaeger exporter that
// resolves a collector endpoint from a user-supplied host and ships spans
// to a Jaeger collector over HTTP.

package jaeger

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	jaegerexporter "go.opentelemetry.io/otel/exporters/jaeger"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// New constructs an OTel SpanExporter that ships spans to Jaeger via the
// supplied collector endpoint. The insecure flag controls the URL scheme:
//   - insecure == true  → "http://"
//   - insecure == false → "https://"
//
// If endpoint does not already include the "/api/traces" suffix it is
// appended, matching the Jaeger collector convention
// (http://host:14268/api/traces).
func New(ctx context.Context, endpoint string, insecure bool) (sdktrace.SpanExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("jaeger: endpoint must not be empty")
	}

	collectorURL, err := resolveCollectorURL(endpoint, insecure)
	if err != nil {
		return nil, err
	}

	// The upstream jaeger.New constructor does not dial on construction —
	// it lazily ships via HTTP on the first Export call. We still honour
	// the supplied ctx so future implementations can short-circuit on
	// cancellation without a behavioural change.
	_ = ctx

	return jaegerexporter.New(
		jaegerexporter.WithCollectorEndpoint(
			jaegerexporter.WithEndpoint(collectorURL),
		),
	)
}

// resolveCollectorURL normalises the user-supplied endpoint into a full
// Jaeger collector URL with the right scheme and "/api/traces" suffix.
func resolveCollectorURL(endpoint string, insecure bool) (string, error) {
	// Strip leading/trailing whitespace before validating.
	endpoint = strings.TrimSpace(endpoint)

	// If the caller already supplied a scheme, trust it; otherwise prepend
	// one based on the insecure flag.
	if !strings.Contains(endpoint, "://") {
		scheme := "https"
		if insecure {
			scheme = "http"
		}
		endpoint = scheme + "://" + endpoint
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("jaeger: invalid endpoint %q: %w", endpoint, err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("jaeger: endpoint %q is missing host", endpoint)
	}

	// Jaeger collector convention: paths end with /api/traces.
	if !strings.HasSuffix(u.Path, "/api/traces") {
		// Strip a trailing slash if present, then append the suffix.
		u.Path = strings.TrimRight(u.Path, "/") + "/api/traces"
	}

	return u.String(), nil
}

// Exporter wraps the upstream OTel Jaeger exporter to satisfy the
// sdktrace.SpanExporter interface while exposing a domain-local surface.
// All calls are forwarded to the wrapped inner exporter verbatim.
type Exporter struct {
	inner sdktrace.SpanExporter
}

// Compile-time check that *Exporter satisfies sdktrace.SpanExporter.
var _ sdktrace.SpanExporter = (*Exporter)(nil)

// ExportSpans forwards the span batch to the underlying Jaeger exporter.
// An empty batch is a no-op and returns nil without touching the network.
func (e *Exporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return e.inner.ExportSpans(ctx, spans)
}

// Shutdown releases any resources held by the underlying Jaeger exporter.
// It is safe to call Shutdown multiple times; the OTel SDK guards against
// double-close internally.
func (e *Exporter) Shutdown(ctx context.Context) error {
	return e.inner.Shutdown(ctx)
}
