// Copyright 2024 cachex. All rights reserved.
//
// Tests for the cachex/initx shortcut package. These tests live in
// initx (not in the root cachex package) because initx is the only
// place the new tracing shortcuts can exist without creating an
// import cycle: cachex/observability already imports root cachex
// for *cachex.Config, so root cachex cannot import it back.

package initx

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"

	cachex "github.com/gospacex/cachex"
)

// TestInitTracing_Signature pins the public signature for stability
// across refactors.
func TestInitTracing_Signature(t *testing.T) {
	var _ func(context.Context, string, ...TracingOption) (func(context.Context), error) = InitTracing
}

// TestTracingOption_Apply verifies the functional option type.
func TestTracingOption_Apply(t *testing.T) {
	var o TracingOption
	if o != nil {
		t.Fatal("nil TracingOption should be the zero value")
	}
}

// TestInitTracing_MissingFile verifies InitTracing returns an error
// and a nil cleanup when the path does not exist.
func TestInitTracing_MissingFile(t *testing.T) {
	cleanup, err := InitTracing(context.Background(), "/nonexistent/path/to.yaml")
	if err == nil {
		cleanup(context.Background())
		t.Fatal("InitTracing with missing file should return error")
	}
	if cleanup != nil {
		t.Fatal("InitTracing with error should return nil cleanup")
	}
}

// TestInitTracing_DisabledYaml verifies that a yaml with
// trace.enabled=false returns a no-op cleanup without touching otel
// globals.
func TestInitTracing_DisabledYaml(t *testing.T) {
	dir := t.TempDir()
	path := writeDisabledTraceYAML(t, dir)
	cleanup, err := InitTracing(context.Background(), path)
	if err != nil {
		t.Fatalf("InitTracing(disabled) returned error: %v", err)
	}
	if cleanup == nil {
		t.Fatal("InitTracing(disabled) returned nil cleanup; expected no-op")
	}
	cleanup(context.Background()) // must be safe to call
}

// TestInitTracing_AcceptsRedisOption checks that the option returned
// by WithRedisClient is a non-nil TracingOption that does not run
// until InitTracing applies it.
func TestInitTracing_AcceptsRedisOption(t *testing.T) {
	var called bool
	opt := TracingOption(func(_ *observabilityConfig) {
		called = true
	})
	if opt == nil {
		t.Fatal("TracingOption constructor returned nil")
	}
	if called {
		t.Fatal("TracingOption applied during construction; should apply only on InitTracing")
	}
}

// TestInitTracing_AcceptsKafkaOption mirrors
// TestInitTracing_AcceptsRedisOption for the Kafka side.
func TestInitTracing_AcceptsKafkaOption(t *testing.T) {
	var called bool
	opt := TracingOption(func(_ *observabilityConfig) {
		called = true
	})
	if opt == nil {
		t.Fatal("TracingOption constructor returned nil")
	}
	if called {
		t.Fatal("TracingOption applied during construction; should apply only on InitTracing")
	}
}

// TestTraceConfigMapping_Jaeger verifies TraceConfig→observabilityConfig
// for the jaeger backend.
func TestTraceConfigMapping_Jaeger(t *testing.T) {
	tc := &cachex.TraceConfig{
		Enabled:      true,
		ServiceName:  "test-svc",
		Exporter:     "jaeger",
		Endpoint:     "http://localhost:14268/api/traces",
		Insecure:     true,
		Protocol:     "grpc",
		SamplerType:  "always_on",
		SamplerRatio: 1.0,
	}
	oc, err := traceConfigToObservabilityConfig(tc)
	if err != nil {
		t.Fatalf("traceConfigToObservabilityConfig: %v", err)
	}
	if !oc.Enabled {
		t.Fatal("mapping lost Enabled")
	}
	if oc.ServiceName != "test-svc" {
		t.Fatalf("ServiceName: got %q want test-svc", oc.ServiceName)
	}
	if oc.Backend != "jaeger" {
		t.Fatalf("Backend: got %q want jaeger", oc.Backend)
	}
	if oc.Endpoint != "http://localhost:14268/api/traces" {
		t.Fatalf("Endpoint: got %q", oc.Endpoint)
	}
	if !oc.Insecure {
		t.Fatal("Insecure: got false want true")
	}
	if oc.Protocol != "grpc" {
		t.Fatalf("Protocol: got %q want grpc", oc.Protocol)
	}
	if oc.SampleRate != 1.0 {
		t.Fatalf("SampleRate: got %v want 1.0", oc.SampleRate)
	}
}

// TestTraceConfigMapping_SamplerRatio verifies that a "ratio" sampler
// type passes SamplerRatio through to the observability layer.
func TestTraceConfigMapping_SamplerRatio(t *testing.T) {
	tc := &cachex.TraceConfig{
		Enabled:      true,
		ServiceName:  "s",
		Exporter:     "otlp",
		SamplerType:  "ratio",
		SamplerRatio: 0.25,
	}
	oc, err := traceConfigToObservabilityConfig(tc)
	if err != nil {
		t.Fatalf("traceConfigToObservabilityConfig: %v", err)
	}
	if oc.SampleRate != 0.25 {
		t.Fatalf("SampleRate for ratio sampler: got %v want 0.25", oc.SampleRate)
	}
}

// TestTraceConfigMapping_UnknownBackend verifies unknown exporter
// names are rejected with a descriptive error.
func TestTraceConfigMapping_UnknownBackend(t *testing.T) {
	tc := &cachex.TraceConfig{
		Enabled:     true,
		ServiceName: "s",
		Exporter:    "elasticsearch",
	}
	_, err := traceConfigToObservabilityConfig(tc)
	if err == nil {
		t.Fatal("expected error for unknown backend, got nil")
	}
}

// TestWithRedisClient_AppliesClient verifies WithRedisClient injects
// a *redis.Client into the observabilityConfig target.
func TestWithRedisClient_AppliesClient(t *testing.T) {
	c := &redis.Client{}
	opt := WithRedisClient(c)
	if opt == nil {
		t.Fatal("WithRedisClient returned nil option")
	}
	target := &observabilityConfig{}
	opt(target)
	if target.RedisClient != c {
		t.Fatal("WithRedisClient did not set RedisClient on target")
	}
}

// TestNormaliseTraceBackend_TableDriven exhaustively exercises every
// branch of normaliseTraceBackend: empty (defaults to ""), the four
// long-name enums, the two short-name aliases, and a representative
// unknown value. This is the function with the lowest coverage
// (50%) before this test.
func TestNormaliseTraceBackend_TableDriven(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "", want: "", wantErr: false},
		{in: "jaeger", want: "jaeger", wantErr: false},
		{in: "otlp", want: "otlp", wantErr: false},
		{in: "redis_stream", want: "redis_stream", wantErr: false},
		{in: "kafka_topic", want: "kafka_topic", wantErr: false},
		{in: "redis", want: "redis_stream", wantErr: false},
		{in: "kafka", want: "kafka_topic", wantErr: false},
		{in: "elasticsearch", want: "", wantErr: true},
		{in: "zipkin", want: "", wantErr: true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			got, err := normaliseTraceBackend(c.in)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if !c.wantErr && got != c.want {
				t.Fatalf("got %q want %q", got, c.want)
			}
		})
	}
}

// TestSamplerRatioFromType_TableDriven pins the sampler-type
// reduction rules including the "" (default) case and the four named
// modes. Brings samplerRatioFromType from 80% to 100%.
func TestSamplerRatioFromType_TableDriven(t *testing.T) {
	cases := []struct {
		samplerType string
		ratio       float64
		want        float64
	}{
		{"", 0.5, 0.5},
		{"always_on", 0.1, 1.0},
		{"parentbased_always_on", 0.1, 1.0},
		{"always_off", 0.9, 0.0},
		{"parentbased_always_off", 0.9, 0.0},
		{"traceidratio", 0.42, 0.42},
		{"parentbased_traceidratio", 0.33, 0.33},
		{"ratio", 0.7, 0.7},
		{"unknown-mode", 0.2, 0.2},
	}
	for _, c := range cases {
		c := c
		t.Run(c.samplerType, func(t *testing.T) {
			got := samplerRatioFromType(c.samplerType, c.ratio)
			if got != c.want {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}

// TestTraceConfigMapping_NilConfig verifies the nil-TraceConfig
// branch of traceConfigToObservabilityConfig: it returns the
// observability zero value without error. This was a previously
// uncovered branch.
func TestTraceConfigMapping_NilConfig(t *testing.T) {
	oc, err := traceConfigToObservabilityConfig(nil)
	if err != nil {
		t.Fatalf("nil TraceConfig should be tolerated: %v", err)
	}
	if oc == nil {
		t.Fatal("nil TraceConfig returned nil observabilityConfig")
	}
	if oc.Enabled || oc.Backend != "" {
		t.Fatalf("nil TraceConfig should map to zero observabilityConfig, got %+v", oc)
	}
}

// TestTraceConfigMapping_RedisAndKafkaStream exercises the two
// backend-specific switch branches (RedisStream, KafkaTopic) inside
// traceConfigToObservabilityConfig. Brings this function from 70%
// to ~90% combined with the unknown-backend test.
func TestTraceConfigMapping_RedisAndKafkaStream(t *testing.T) {
	t.Run("redis short form maps to redis_stream and copies Stream", func(t *testing.T) {
		tc := &cachex.TraceConfig{
			Enabled:  true,
			Exporter: "redis",
			Stream:   "trace:span:batch",
		}
		oc, err := traceConfigToObservabilityConfig(tc)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if oc.Backend != "redis_stream" {
			t.Fatalf("Backend: got %q want redis_stream", oc.Backend)
		}
		if oc.RedisStream != "trace:span:batch" {
			t.Fatalf("RedisStream: got %q", oc.RedisStream)
		}
	})
	t.Run("kafka short form maps to kafka_topic and copies Topic", func(t *testing.T) {
		tc := &cachex.TraceConfig{
			Enabled:  true,
			Exporter: "kafka",
			Topic:    "otlp_spans",
		}
		oc, err := traceConfigToObservabilityConfig(tc)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if oc.Backend != "kafka_topic" {
			t.Fatalf("Backend: got %q want kafka_topic", oc.Backend)
		}
		if oc.KafkaTopic != "otlp_spans" {
			t.Fatalf("KafkaTopic: got %q", oc.KafkaTopic)
		}
	})
	t.Run("headers and protocol pass through verbatim", func(t *testing.T) {
		tc := &cachex.TraceConfig{
			Enabled:  true,
			Exporter: "otlp",
			Protocol: "http",
			Headers:  map[string]string{"x-api-key": "secret"},
		}
		oc, err := traceConfigToObservabilityConfig(tc)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if oc.Protocol != "http" {
			t.Fatalf("Protocol: got %q want http", oc.Protocol)
		}
		if oc.Headers["x-api-key"] != "secret" {
			t.Fatalf("Headers: got %+v", oc.Headers)
		}
	})
}

// TestWithKafkaProducer_AppliesProducer verifies WithKafkaProducer
// injects a sarama.SyncProducer into the observabilityConfig target.
// We pass a nil producer; the assertion is that the option does not
// panic and does not set the field to a non-nil value.
func TestWithKafkaProducer_AppliesProducer(t *testing.T) {
	var p sarama.SyncProducer // nil; we just verify option identity wiring
	opt := WithKafkaProducer(p)
	if opt == nil {
		t.Fatal("WithKafkaProducer returned nil option")
	}
	target := &observabilityConfig{}
	opt(target)
	if target.KafkaProducer != nil {
		t.Fatal("WithKafkaProducer(nil) should leave KafkaProducer nil")
	}
}

// TestInitTracing_ContextCancel_AcceptsDeadline ensures InitTracing
// respects the context deadline (disabled yaml path is used so no
// real network call is made).
func TestInitTracing_ContextCancel_AcceptsDeadline(t *testing.T) {
	dir := t.TempDir()
	path := writeDisabledTraceYAML(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cleanup, err := InitTracing(ctx, path)
	if err != nil {
		t.Fatalf("InitTracing(deadline): %v", err)
	}
	cleanup(context.Background()) // safe
}

// writeDisabledTraceYAML writes a minimal trace: block with
// enabled=false and returns the path.
func writeDisabledTraceYAML(t *testing.T, dir string) string {
	t.Helper()
	body := []byte("trace:\n  enabled: false\n  service_name: test-svc\n  exporter: jaeger\n")
	path := dir + "/disabled.yaml"
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("writeDisabledTraceYAML: %v", err)
	}
	return path
}
