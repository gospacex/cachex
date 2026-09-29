package cachex

import (
	"sync"
	"testing"
)

func TestIsTracingEnabled_DefaultFalse(t *testing.T) {
	if IsTracingEnabled() {
		t.Fatal("expected tracing disabled by default")
	}
}

func TestSetTracingConfig_Enabled(t *testing.T) {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

	SetTracingConfig(TracingConfig{Enabled: true, Exporter: "jaeger", JaegerEndpoint: "localhost:4318"})
	if !IsTracingEnabled() {
		t.Fatal("expected tracing enabled")
	}
}

func TestSetTracingConfig_Disabled(t *testing.T) {
	SetTracingConfig(TracingConfig{Enabled: false})
	if IsTracingEnabled() {
		t.Fatal("expected tracing disabled")
	}
}

func TestSetTracingConfig_NoPanic_InvalidExporter(t *testing.T) {
	// 不存在的 exporter 不应 panic，即使失败也应在 tpOnce 内被吞掉
	SetTracingConfig(TracingConfig{
		Enabled:        true,
		Exporter:       "unknown",
		JaegerEndpoint: "localhost:9999",
		Insecure:       true,
	})
	// 仅验证不 panic，tracing 状态保持 enabled
	if !IsTracingEnabled() {
		t.Fatal("expected tracing still enabled after failed init")
	}
}

func TestDefaultKafkaTopic(t *testing.T) {
	if defaultKafkaTopic("") != "cachex-traces" {
		t.Fatal("expected default topic")
	}
	if defaultKafkaTopic("my-topic") != "my-topic" {
		t.Fatal("expected custom topic")
	}
}

func TestDefaultServiceName(t *testing.T) {
	if defaultServiceName("") != "cachex" {
		t.Fatal("expected default service name")
	}
	if defaultServiceName("my-svc") != "my-svc" {
		t.Fatal("expected custom service name")
	}
}

func TestInitTracerProvider_JaegerNoEndpoint(t *testing.T) {
	// 默认 endpoint 应回退到 localhost:4318，因无实际 Jaeger 所以会失败，但不 panic
	err := initTracerProvider(TracingConfig{Enabled: true, Exporter: "jaeger", Insecure: true})
	// 预期失败（无 Jaeger 实例），但不 panic
	_ = err
}

func TestInitTracerProvider_KafkaNoBrokers(t *testing.T) {
	err := initTracerProvider(TracingConfig{Enabled: true, Exporter: "kafka"})
	if err == nil {
		t.Fatal("expected error when no kafka brokers")
	}
}
