package observability

import (
	"context"
	"testing"

	"github.com/gospacex/cachex"
	"github.com/stretchr/testify/assert"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestNewLazyOtelExporter(t *testing.T) {
	cfg := &cachex.TracingConfig{
		Enabled:      true,
		ExporterType: "redis",
		RedisConfig: &cachex.RedisConfig{
			Addrs:   []string{"localhost:6379"},
			Channel: "test:traces",
		},
	}

	// Reset any existing exporter
	ResetExporter(ExporterTypeRedis, cfg)

	// Test lazy initialization
	exporter, err := NewLazyOtelExporter(ExporterTypeRedis, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exporter)

	// Test singleton - should return same instance
	exporter2, err := NewLazyOtelExporter(ExporterTypeRedis, cfg)
	assert.NoError(t, err)
	assert.Equal(t, exporter, exporter2)
}

func TestNewLazyOtelExporterUnknownType(t *testing.T) {
	cfg := &cachex.TracingConfig{}

	exporter, err := NewLazyOtelExporter("unknown", cfg)
	assert.Error(t, err)
	assert.Nil(t, exporter)
	assert.Contains(t, err.Error(), "unknown exporter type")
}

func TestMockExporter(t *testing.T) {
	exporter := NewMockExporter()
	assert.NotNil(t, exporter)

	// Test ExportSpans
	spans := []sdktrace.ReadOnlySpan{
		// Mock span - we can't create real ReadOnlySpan easily, but we can test the interface
	}

	err := exporter.ExportSpans(context.Background(), spans)
	assert.NoError(t, err)

	// Test Shutdown
	err = exporter.Shutdown(context.Background())
	assert.NoError(t, err)

	// Test Traces
	traces := exporter.Traces()
	assert.NotNil(t, traces)
	assert.Len(t, traces, 0)

	// Test Clear
	exporter.Clear()
	traces = exporter.Traces()
	assert.Len(t, traces, 0)
}

func TestTracerProvider(t *testing.T) {
	cfg := &cachex.TracingConfig{
		Enabled:      true,
		ServiceName:  "test-service",
		ExporterType: "redis",
		RedisConfig: &cachex.RedisConfig{
			Addrs:   []string{"localhost:6379"},
			Channel: "test:traces",
		},
	}

	tp, err := NewTracerProvider(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, tp)

	// Test Exporter getter
	exp, err := tp.Exporter()
	assert.NoError(t, err)
	assert.NotNil(t, exp)
}

func TestExporterType(t *testing.T) {
	assert.Equal(t, ExporterType("otlp"), ExporterTypeOTLP)
	assert.Equal(t, ExporterType("jaeger"), ExporterTypeJaeger)
	assert.Equal(t, ExporterType("redis"), ExporterTypeRedis)
	assert.Equal(t, ExporterType("kafka"), ExporterTypeKafka)
}

func TestJaegerExporterConfig(t *testing.T) {
	cfg := &cachex.TracingConfig{
		ExporterType: "jaeger",
		Endpoint:     "http://localhost:14268",
		JaegerConfig: &cachex.JaegerConfig{
			AgentHost: "localhost",
			AgentPort: 6831,
		},
	}

	exporter, err := NewLazyOtelExporter(ExporterTypeJaeger, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exporter)

	err = exporter.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestKafkaExporterConfig(t *testing.T) {
	cfg := &cachex.TracingConfig{
		ExporterType: "kafka",
		KafkaConfig: &cachex.KafkaConfig{
			Brokers:  []string{"localhost:9092"},
			Topic:    "test-traces",
			ClientID: "cachex-test",
		},
	}

	// NewLazyOtelExporter is lazy: kafka connection is deferred to first use,
	// so an unreachable broker does not surface at construction time.
	exporter, err := NewLazyOtelExporter(ExporterTypeKafka, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exporter)
}

func TestRedisExporterConfig(t *testing.T) {
	cfg := &cachex.TracingConfig{
		ExporterType: "redis",
		RedisConfig: &cachex.RedisConfig{
			Addrs:     []string{"localhost:6379"},
			Channel:   "test:traces",
			KeyPrefix: "cachex:",
		},
	}

	exporter, err := NewLazyOtelExporter(ExporterTypeRedis, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exporter)
}

func TestOTLPExporter(t *testing.T) {
	cfg := &cachex.TracingConfig{
		ExporterType: "otlp",
		Endpoint:     "localhost:4318",
	}

	exporter, err := NewLazyOtelExporter(ExporterTypeOTLP, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exporter)

	err = exporter.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestRedisExporterNoConfig(t *testing.T) {
	cfg := &cachex.TracingConfig{
		ExporterType: "redis",
		RedisConfig:  nil, // Missing config
	}

	exporter, err := NewLazyOtelExporter(ExporterTypeRedis, cfg)
	assert.Error(t, err)
	assert.Nil(t, exporter)
	assert.Contains(t, err.Error(), "redis config is required")
}

func TestTracerProviderShutdown(t *testing.T) {
	cfg := &cachex.TracingConfig{
		Enabled:      true,
		ServiceName:  "test-service",
		ExporterType: "redis",
		RedisConfig: &cachex.RedisConfig{
			Addrs:   []string{"localhost:6379"},
			Channel: "test:traces",
		},
	}

	tp, err := NewTracerProvider(cfg)
	assert.NoError(t, err)

	// Shutdown should work even without initializing provider
	err = tp.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestNewOtelProvider(t *testing.T) {
	cfg := &cachex.TracingConfig{
		Enabled:      true,
		ServiceName:  "test-service",
		ExporterType: "redis",
		RedisConfig: &cachex.RedisConfig{
			Addrs:   []string{"localhost:6379"},
			Channel: "test:traces",
		},
	}

	// Reset any existing provider
	ResetOtelProvider(cfg)

	provider, err := NewOtelProvider(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, provider)

	// Test singleton - should return same instance
	provider2, err := NewOtelProvider(cfg)
	assert.NoError(t, err)
	assert.Equal(t, provider, provider2)

	// Test Tracer
	tracer := provider.Tracer()
	assert.NotNil(t, tracer)

	// Test Exporter
	exporter, err := provider.Exporter()
	assert.NoError(t, err)
	assert.NotNil(t, exporter)

	// Test Shutdown
	err = provider.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestNewOtelProviderNilConfig(t *testing.T) {
	provider, err := NewOtelProvider(nil)
	assert.Error(t, err)
	assert.Nil(t, provider)
	assert.Contains(t, err.Error(), "nil tracing config")
}

func TestResetOtelProvider(t *testing.T) {
	cfg := &cachex.TracingConfig{
		Enabled:      true,
		ServiceName:  "test-service",
		ExporterType: "redis",
		RedisConfig: &cachex.RedisConfig{
			Addrs:   []string{"localhost:6379"},
			Channel: "test:traces",
		},
	}

	// Create provider
	provider1, err := NewOtelProvider(cfg)
	assert.NoError(t, err)

	// Reset
	ResetOtelProvider(cfg)

	// Create again - should be new instance
	provider2, err := NewOtelProvider(cfg)
	assert.NoError(t, err)
	assert.NotEqual(t, provider1, provider2)
}

func TestResetAllOtelProviders(t *testing.T) {
	cfg1 := &cachex.TracingConfig{
		Enabled:      true,
		ServiceName:  "test-service-1",
		ExporterType: "redis",
		RedisConfig: &cachex.RedisConfig{
			Addrs:   []string{"localhost:6379"},
			Channel: "test:traces1",
		},
	}

	cfg2 := &cachex.TracingConfig{
		Enabled:      true,
		ServiceName:  "test-service-2",
		ExporterType: "redis",
		RedisConfig: &cachex.RedisConfig{
			Addrs:   []string{"localhost:6379"},
			Channel: "test:traces2",
		},
	}

	// Create two providers
	_, err := NewOtelProvider(cfg1)
	assert.NoError(t, err)
	_, err = NewOtelProvider(cfg2)
	assert.NoError(t, err)

	// Reset all
	ResetAllOtelProviders()

	// Create again - should be new instances
	provider1, err := NewOtelProvider(cfg1)
	assert.NoError(t, err)
	provider2, err := NewOtelProvider(cfg2)
	assert.NoError(t, err)
	assert.NotEqual(t, provider1, provider2)
}

func TestOtelProviderTracer(t *testing.T) {
	cfg := &cachex.TracingConfig{
		Enabled:      true,
		ServiceName:  "test-service",
		ExporterType: "otlp",
	}

	provider, err := NewOtelProvider(cfg)
	assert.NoError(t, err)

	// Get tracer twice - should return same instance
	tracer1 := provider.Tracer()
	tracer2 := provider.Tracer()
	assert.Equal(t, tracer1, tracer2)
}
