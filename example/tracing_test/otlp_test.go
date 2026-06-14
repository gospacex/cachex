// Copyright 2024 cachex. All rights reserved.
//
// Tests the OTLP HTTP exporter (distinct from jaeger which also
// speaks OTLP gRPC). The assert helpers query jaeger's HTTP API for
// span presence — the jaeger all-in-one container natively ingests
// OTLP and surfaces the same data through its /api/traces endpoint,
// so we reuse assert.BackendJaeger here to avoid a second query
// helper. The distinction is in the cachex trace.exporter=otlp +
// protocol=http config below.
package tracing_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	cachex "github.com/gospacex/cachex"
	"github.com/gospacex/cachex/example/assert"
	"github.com/gospacex/cachex/initx"
	"go.opentelemetry.io/otel"
)

// TestTracing_OTLP_HTTP ships spans to a local jaeger-all-in-one via
// the OTLP HTTP exporter (port 4318) and asserts the trace lands.
//
//	Pre-req: docker run -d -p 16686:16686 -p 14268:14268 -p 4318:4318 \
//		jaegertracing/all-in-one:latest
func TestTracing_OTLP_HTTP(t *testing.T) {
	assert.StartStack(t, "tracing", assert.TopologySingle, assert.BackendJaeger)

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "trace.yaml")
	yaml := `backend: badger
in_memory: true
trace:
  enabled: true
  service_name: cachex-tracing-otlp
  exporter: otlp
  endpoint: http://localhost:4318/v1/traces
  protocol: http
  insecure: true
  sampler_type: always_on
`
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write yaml: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cleanup, err := initx.InitTracing(ctx, cfgPath)
	if err != nil {
		t.Skipf("InitTracing(otlp) failed (collector likely not running): %v", err)
	}
	defer cleanup(context.Background())

	cfg, err := cachex.LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cache, err := cachex.DefaultFactory.Create(cachex.BackendBadger, cfg)
	if err != nil {
		t.Fatalf("create badger cache: %v", err)
	}
	defer func() { _ = cache.Close() }()

	want := assert.NewTraceID(t)
	spanCtx := traceContextFromTraceID(t, want)
	ctx = contextWithSpanContext(ctx, spanCtx)

	tracer := otel.Tracer("cachex-tracing-test")
	_, span := tracer.Start(ctx, "tracing-test.op",
		traceWithAttribute("test.backend", "otlp_http"),
	)
	defer span.End()

	if err := cache.Set(ctx, "tracing-otlp-key", []byte("hello-otlp")); err != nil {
		t.Fatalf("cache.Set: %v", err)
	}
	if _, err := cache.Get(ctx, "tracing-otlp-key"); err != nil {
		t.Fatalf("cache.Get: %v", err)
	}

	// Reuse jaeger query API: the jaeger all-in-one container
	// accepts OTLP on 4317/4318 and exposes ingested spans on 16686,
	// so a single backend=jaeger query covers both protocol flavours.
	assert.AssertSpanInBackend(t, ctx, assert.BackendJaeger, "tracing", assert.TopologySingle, want)
}
