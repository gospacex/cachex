// Copyright 2024 cachex. All rights reserved.
//
// Tests the redis_stream trace backend. The trace exporter writes
// span batches to a Redis stream; the test injects a *redis.Client
// via initx.WithRedisClient and then asserts the span is present in
// the stream with the expected trace_id.
//
// Pre-req: docker run -d -p 6379:6379 redis:7-alpine
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
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
)

// TestTracing_RedisStream ships spans to a Redis stream via the
// redis_stream backend. Uses a *redis.Client injected through
// initx.WithRedisClient so the trace exporter can XADD span records.
func TestTracing_RedisStream(t *testing.T) {
	assert.StartStack(t, "tracing", assert.TopologySingle, assert.BackendRedisStream)

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "trace.yaml")
	yaml := `backend: badger
in_memory: true
trace:
  enabled: true
  service_name: cachex-tracing-redis
  exporter: redis_stream
  stream: trace:tracing:single
  sampler_type: always_on
`
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write yaml: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer func() { _ = rdb.Close() }()
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		pingCancel()
		t.Skipf("redis not reachable on localhost:6379: %v", err)
	}
	pingCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cleanup, err := initx.InitTracing(ctx, cfgPath, initx.WithRedisClient(rdb))
	if err != nil {
		t.Skipf("InitTracing(redis_stream) failed: %v", err)
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
		traceWithAttribute("test.backend", assert.BackendRedisStream),
	)
	defer span.End()

	if err := cache.Set(ctx, "tracing-redis-key", []byte("hello-redis")); err != nil {
		t.Fatalf("cache.Set: %v", err)
	}
	if _, err := cache.Get(ctx, "tracing-redis-key"); err != nil {
		t.Fatalf("cache.Get: %v", err)
	}

	assert.AssertSpanInBackend(t, ctx, assert.BackendRedisStream, "tracing", assert.TopologySingle, want)
}
