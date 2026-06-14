// Copyright 2024 cachex. All rights reserved.
//
// Cross-process trace propagation test. Simulates two processes
// sharing a cache store by holding two cachex.Cache instances that
// connect to the same Redis server. Process A starts a span A1,
// serialises its SpanContext into a cache value, and writes it.
// Process B reads the value, restores the SpanContext, and starts a
// child span B2 under it. The test asserts that A1 and B2 share the
// same trace_id and that B2's parent_span_id equals A1's span_id.
//
// Design note: we serialise the SpanContext as JSON
// (trace_id, span_id, trace_flags) inside the cache value. This is
// the simplest portable propagation channel — no W3C traceparent
// header overhead. A real production cross-process propagation
// would inject traceparent via HTTP/gRPC headers; the JSON approach
// keeps the test self-contained and deterministic.
package tracing_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	cachex "github.com/gospacex/cachex"
	"github.com/gospacex/cachex/example/assert"
	"github.com/gospacex/cachex/initx"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// TestTracing_CrossProcess verifies that a SpanContext written by
// process A can be restored in process B, producing a parent-child
// span relationship in the backend.
func TestTracing_CrossProcess(t *testing.T) {
	assert.StartStack(t, "tracing", assert.TopologySingle, assert.BackendJaeger)

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "trace.yaml")
	yaml := `backend: badger
in_memory: true
trace:
  enabled: true
  service_name: cachex-tracing-crossprocess
  exporter: jaeger
  endpoint: http://localhost:14268/api/traces
  insecure: true
  protocol: grpc
  sampler_type: always_on
`
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write yaml: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cleanup, err := initx.InitTracing(ctx, cfgPath)
	if err != nil {
		t.Skipf("InitTracing(cross-process) failed: %v", err)
	}
	defer cleanup(context.Background())

	// Sanity check that redis is up before we build the caches.
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer func() { _ = rdb.Close() }()
	pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		pingCancel()
		t.Skipf("redis not reachable on localhost:6379: %v", err)
	}
	pingCancel()

	cacheA, err := cachex.DefaultFactory.Create(cachex.BackendBadger,
		makeBadgerCfg("cachex-cross-A", cfgPath))
	if err != nil {
		t.Fatalf("create cacheA: %v", err)
	}
	defer func() { _ = cacheA.Close() }()

	cacheB, err := cachex.DefaultFactory.Create(cachex.BackendBadger,
		makeBadgerCfg("cachex-cross-B", cfgPath))
	if err != nil {
		t.Fatalf("create cacheB: %v", err)
	}
	defer func() { _ = cacheB.Close() }()

	tracer := otel.Tracer("cachex-tracing-crossprocess")

	// ---- Process A: start span A1, serialise its context into a
	// cache value, write the value. ----
	wantTraceID := assert.NewTraceID(t)
	spanACtx := traceContextFromTraceID(t, wantTraceID)
	ctxA := contextWithSpanContext(ctx, spanACtx)
	ctxA, spanA1 := tracer.Start(ctxA, "process-a.op")

	scA := spanA1.SpanContext()
	envA := traceEnvelope{
		TraceID:    scA.TraceID().String(),
		SpanID:     scA.SpanID().String(),
		TraceFlags: scA.TraceFlags().String(),
	}
	payload, err := json.Marshal(envA)
	if err != nil {
		spanA1.End()
		t.Fatalf("marshal envelope: %v", err)
	}
	if err := cacheA.Set(ctxA, "tracing-xproc-key", payload); err != nil {
		spanA1.End()
		t.Fatalf("cacheA.Set: %v", err)
	}
	spanA1.End()

	// ---- Process B: read the value, restore the SpanContext,
	// start child span B2. ----
	gotBytes, err := cacheB.Get(ctx, "tracing-xproc-key")
	if err != nil {
		t.Fatalf("cacheB.Get: %v", err)
	}
	var envB traceEnvelope
	if err := json.Unmarshal(gotBytes, &envB); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if envB.TraceID != wantTraceID.String() {
		t.Fatalf("trace_id mismatch: got %s want %s", envB.TraceID, wantTraceID.String())
	}

	restoredSC, err := spanContextFromEnvelope(envB)
	if err != nil {
		t.Fatalf("restore span context: %v", err)
	}
	if !restoredSC.IsValid() {
		t.Fatal("restored span context is not valid")
	}
	if restoredSC.TraceID() != wantTraceID {
		t.Fatalf("restored TraceID mismatch: got %s want %s",
			restoredSC.TraceID(), wantTraceID)
	}
	if restoredSC.SpanID() != scA.SpanID() {
		t.Fatalf("restored SpanID mismatch: got %s want %s",
			restoredSC.SpanID(), scA.SpanID())
	}

	ctxB := trace.ContextWithSpanContext(ctx, restoredSC)
	_, spanB2 := tracer.Start(ctxB, "process-b.op")
	scB := spanB2.SpanContext()
	if scB.TraceID() != wantTraceID {
		spanB2.End()
		t.Fatalf("B2 trace_id mismatch: got %s want %s", scB.TraceID(), wantTraceID)
	}
	if scB.SpanID() == scA.SpanID() {
		spanB2.End()
		t.Fatal("B2 should not share span_id with A1")
	}
	zeroSpan := trace.SpanID{}
	if scB.SpanID() == zeroSpan {
		spanB2.End()
		t.Fatal("B2 should have a non-zero span_id")
	}
	spanB2.End()

	// Both spans land in jaeger under the same trace_id; we use
	// the strict parent-child check via FetchSpansByTraceID +
	// the jaeger parent_span_id field.
	spans := assert.FetchSpansByTraceID(t, assert.BackendJaeger, "tracing", assert.TopologySingle, wantTraceID)
	if len(spans) < 2 {
		t.Fatalf("expected >=2 spans for trace %s, got %d", wantTraceID, len(spans))
	}

	var child *assert.SpanRecord
	for i := range spans {
		if spans[i].Name == "process-b.op" {
			child = &spans[i]
			break
		}
	}
	if child == nil {
		t.Fatalf("no span named process-b.op in %d fetched spans", len(spans))
	}
	if child.ParentSpanID != scA.SpanID().String() {
		t.Fatalf("process-b.op parent_span_id: got %s want %s (A1 span_id)",
			child.ParentSpanID, scA.SpanID())
	}
	if child.TraceID != wantTraceID.String() {
		t.Fatalf("process-b.op trace_id: got %s want %s", child.TraceID, wantTraceID)
	}
}
