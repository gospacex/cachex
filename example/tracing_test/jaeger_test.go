// Copyright 2024 cachex. All rights reserved.
//
// Package tracing_test groups the 4-backend x 2-topology e2e tracing
// tests for cachex, plus a cross-process propagation test. This file
// covers the Jaeger OTLP gRPC backend.
//
// Each test builds a minimal in-memory badger cache and exercises the
// trace pipeline: initx.InitTracing -> otel.Tracer.Start -> cache.Set/Get
// -> assert.AssertSpanInBackend. Docker / jaeger unavailable => t.Skip.
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

// TestTracing_Jaeger_OTLP ships spans to a local jaeger-all-in-one via
// OTLP gRPC and verifies the trace lands in jaeger's HTTP query API.
//
//	Pre-req: docker run -d -p 16686:16686 -p 14268:14268 \
//		jaegertracing/all-in-one:latest
func TestTracing_Jaeger_OTLP(t *testing.T) {
	assert.StartStack(t, "tracing", assert.TopologySingle, assert.BackendJaeger)

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "trace.yaml")
	yaml := `backend: badger
in_memory: true
trace:
  enabled: true
  service_name: cachex-tracing-jaeger
  exporter: jaeger
  endpoint: http://localhost:14268/api/traces
  insecure: true
  protocol: grpc
  sampler_type: always_on
`
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write yaml: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cleanup, err := initx.InitTracing(ctx, cfgPath)
	if err != nil {
		t.Skipf("InitTracing(jaeger) failed (jaeger likely not running): %v", err)
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
		traceWithAttribute("test.backend", assert.BackendJaeger),
	)
	defer span.End()

	if err := cache.Set(ctx, "tracing-jaeger-key", []byte("hello-jaeger")); err != nil {
		t.Fatalf("cache.Set: %v", err)
	}
	if _, err := cache.Get(ctx, "tracing-jaeger-key"); err != nil {
		t.Fatalf("cache.Get: %v", err)
	}

	assert.AssertSpanInBackend(t, ctx, assert.BackendJaeger, "tracing", assert.TopologySingle, want)
}
