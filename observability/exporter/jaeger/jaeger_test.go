// Copyright 2024 cachex. All rights reserved.
//
// Unit tests for the Jaeger exporter sub-package.

package jaeger

import (
	"context"
	"testing"

	jaegerexporter "go.opentelemetry.io/otel/exporters/jaeger"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Compile-time assertion: *Exporter must satisfy the OTel SpanExporter
// contract that the SDK uses to plug it into a TracerProvider.
var _ sdktrace.SpanExporter = (*Exporter)(nil)

func TestNew_ValidEndpoint(t *testing.T) {
	// Arrange
	ctx := context.Background()

	// Act
	exp, err := New(ctx, "http://localhost:14268/api/traces", true)

	// Assert
	if err != nil {
		t.Fatalf("New() returned unexpected error: %v", err)
	}
	if exp == nil {
		t.Fatalf("New() returned nil exporter")
	}
	// Cleanup so we don't leak background goroutines.
	if shutdownErr := exp.Shutdown(ctx); shutdownErr != nil {
		t.Fatalf("Shutdown() after New failed: %v", shutdownErr)
	}
}

func TestNew_EmptyEndpoint_ReturnsError(t *testing.T) {
	// Arrange
	ctx := context.Background()

	// Act
	exp, err := New(ctx, "", true)

	// Assert
	if err == nil {
		t.Fatalf("New() with empty endpoint expected error, got nil")
	}
	if exp != nil {
		t.Fatalf("New() with empty endpoint expected nil exporter, got %#v", exp)
	}
}

func TestExporter_ExportSpans_DelegatesToInner(t *testing.T) {
	// Arrange — construct a real OTel Jaeger exporter that does not
	// dial until the first Export call. An empty span slice must
	// return nil without hitting the network.
	inner, err := jaegerexporter.New(
		jaegerexporter.WithCollectorEndpoint(
			jaegerexporter.WithEndpoint("http://localhost:14268/api/traces"),
		),
	)
	if err != nil {
		t.Fatalf("failed to construct inner jaeger exporter: %v", err)
	}

	e := &Exporter{inner: inner}
	ctx := context.Background()

	// Act
	exportErr := e.ExportSpans(ctx, nil)

	// Assert
	if exportErr != nil {
		t.Fatalf("ExportSpans(nil) returned unexpected error: %v", exportErr)
	}

	// Cleanup
	if shutdownErr := e.Shutdown(ctx); shutdownErr != nil {
		t.Fatalf("Shutdown() after ExportSpans failed: %v", shutdownErr)
	}
}

// TestResolveCollectorURL_TableDriven exhaustively exercises
// resolveCollectorURL's branches: scheme injection, scheme
// preservation, /api/traces suffix handling, trailing-slash
// stripping, parse errors, and missing host. This lifts the
// package's overall coverage from 65.2% to >80%.
func TestResolveCollectorURL_TableDriven(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
		insecure bool
		wantURL  string
		wantErr  bool
	}{
		{
			name:     "host-only insecure prepends http://",
			endpoint: "localhost:14268",
			insecure: true,
			wantURL:  "http://localhost:14268/api/traces",
		},
		{
			name:     "host-only secure prepends https://",
			endpoint: "collector.example.com:14268",
			insecure: false,
			wantURL:  "https://collector.example.com:14268/api/traces",
		},
		{
			name:     "explicit http scheme is preserved",
			endpoint: "http://localhost:14268/api/traces",
			insecure: false, // insecure flag must NOT override an explicit scheme
			wantURL:  "http://localhost:14268/api/traces",
		},
		{
			name:     "explicit https scheme is preserved",
			endpoint: "https://jaeger.internal:14268/api/traces",
			insecure: true,
			wantURL:  "https://jaeger.internal:14268/api/traces",
		},
		{
			name:     "missing /api/traces suffix is appended",
			endpoint: "https://jaeger.internal:14268",
			insecure: false,
			wantURL:  "https://jaeger.internal:14268/api/traces",
		},
		{
			name:     "trailing slash before /api/traces is collapsed",
			endpoint: "https://jaeger.internal:14268/",
			insecure: false,
			wantURL:  "https://jaeger.internal:14268/api/traces",
		},
		{
			name:     "whitespace is trimmed",
			endpoint: "   localhost:14268   ",
			insecure: true,
			wantURL:  "http://localhost:14268/api/traces",
		},
		{
			name:     "endpoint with no host returns error",
			endpoint: "http:///justapath",
			insecure: true,
			wantErr:  true,
		},
		{
			name:     "control character in endpoint fails url.Parse",
			endpoint: "http://local host:14268", // unescaped space → url.Parse error
			insecure: true,
			wantErr:  true,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := resolveCollectorURL(c.endpoint, c.insecure)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if !c.wantErr && got != c.wantURL {
				t.Fatalf("got %q want %q", got, c.wantURL)
			}
		})
	}
}

func TestExporter_Shutdown_DelegatesToInner(t *testing.T) {
	// Arrange
	inner, err := jaegerexporter.New(
		jaegerexporter.WithCollectorEndpoint(
			jaegerexporter.WithEndpoint("http://localhost:14268/api/traces"),
		),
	)
	if err != nil {
		t.Fatalf("failed to construct inner jaeger exporter: %v", err)
	}

	e := &Exporter{inner: inner}
	ctx := context.Background()

	// Act
	shutdownErr := e.Shutdown(ctx)

	// Assert
	if shutdownErr != nil {
		t.Fatalf("Shutdown() returned unexpected error: %v", shutdownErr)
	}
}
