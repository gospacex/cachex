// Copyright 2024 cachex. All rights reserved.
//
// Shared helpers for the 5 tracing_test/*.go files. Kept private to
// the package so the public surface stays minimal. Each helper is
// intentionally tiny — SpanContext restoration, trace-id injection
// into tracer.Start options, attribute shortcuts, and a small cfg
// factory for the cross-process test.
package tracing_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	cachex "github.com/gospacex/cachex"
	_ "github.com/gospacex/cachex/backends/embedded/badger" // Register badger factory for tracing_test bootstrap
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// traceContextFromTraceID returns an empty SpanContext stamped with
// the given TraceID. SpanID is left zero — the actual span_id is
// allocated by tracer.Start; the propagation envelope below carries
// the real span_id after Start returns.
func traceContextFromTraceID(t *testing.T, tid trace.TraceID) trace.SpanContext {
	t.Helper()
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     trace.SpanID{},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
}

// contextWithSpanContext returns a new context carrying sc, replacing
// any span already present.
func contextWithSpanContext(ctx context.Context, sc trace.SpanContext) context.Context {
	return trace.ContextWithSpanContext(ctx, sc)
}

// traceWithTraceID stamps ctx with a SpanContext whose TraceID is
// the supplied tid, so the next tracer.Start on that ctx inherits
// the TraceID. The OTel Go public API does not expose a SpanStartOption
// that overrides the TraceID directly (the WithTraceID helper lives
// in an internal package), so we use the context-stamping path.
// Caller is expected to pass the returned ctx to tracer.Start.
func traceWithTraceID(t *testing.T, ctx context.Context, tid trace.TraceID) context.Context {
	t.Helper()
	sc := traceContextFromTraceID(t, tid)
	return trace.ContextWithSpanContext(ctx, sc)
}

// traceWithAttribute is a tiny attribute shortcut so the per-test
// files don't have to import go.opentelemetry.io/otel/attribute
// in five places.
// traceWithAttribute is a tiny attribute shortcut so the per-test
// files don't have to import go.opentelemetry.io/otel/attribute
// in five places.
func traceWithAttribute(k, v string) trace.SpanStartOption {
	return trace.WithAttributes(attribute.String(k, v))
}

// traceEnvelope is the serialised form of a SpanContext propagated
// through a cache value. We use the hex string forms of TraceID and
// SpanID so they survive JSON round-trips.
type traceEnvelope struct {
	TraceID    string `json:"trace_id"`
	SpanID     string `json:"span_id"`
	TraceFlags string `json:"trace_flags"`
}

// spanContextFromEnvelope rebuilds a SpanContext from the
// JSON-serialised form written by process A in cross_process_test.go.
func spanContextFromEnvelope(env traceEnvelope) (trace.SpanContext, error) {
	var tid trace.TraceID
	if err := decodeHex(env.TraceID, tid[:]); err != nil {
		return trace.SpanContext{}, fmt.Errorf("trace_id: %w", err)
	}
	var sid trace.SpanID
	if err := decodeHex(env.SpanID, sid[:]); err != nil {
		return trace.SpanContext{}, fmt.Errorf("span_id: %w", err)
	}
	var flags trace.TraceFlags
	if env.TraceFlags == "01" {
		flags = trace.FlagsSampled
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: flags,
		Remote:     true,
	}), nil
}

func decodeHex(s string, dst []byte) error {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	if len(raw) != len(dst) {
		return fmt.Errorf("hex length: got %d want %d", len(raw), len(dst))
	}
	copy(dst, raw)
	return nil
}

// makeBadgerCfg returns a *Config that uses an in-memory badger
// store. The trace block is loaded from cfgPath so all 5 tests share
// the same trace pipeline. ServiceName is overridden per test.
func makeBadgerCfg(svcName, cfgPath string) *cachex.Config {
	cfg, err := cachex.LoadConfig(cfgPath)
	if err != nil {
		return nil
	}
	cfg.Backend = cachex.BackendBadger
	cfg.InMemory = true
	cfg.Dir = ""
	cfg.ValueDir = ""
	if cfg.Trace != nil {
		cfg.Trace.ServiceName = svcName
	}
	return cfg
}
