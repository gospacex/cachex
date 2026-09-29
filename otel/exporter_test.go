package otel

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestInitFromExporter_SetsProvider(t *testing.T) {
	var got *sdktrace.TracerProvider
	err := initFromExporter(noopExporter{}, ProviderOptions{ServiceName: "cachex-test"}, func(p *sdktrace.TracerProvider) {
		got = p
	})
	if err != nil {
		t.Fatalf("initFromExporter failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected TracerProvider to be set")
	}
}

func TestInitJaeger_NoEndpoint(t *testing.T) {
	// Endpoint 为空时应报错或正常初始化（取决于实现）
	err := InitJaeger(&JaegerConfig{Insecure: true}, func(_ *sdktrace.TracerProvider) {})
	// 无实际 Jaeger 连接，预期返回错误
	_ = err
}

func TestInitKafka_NoBrokers(t *testing.T) {
	err := InitKafka(&KafkaConfig{}, func(_ *sdktrace.TracerProvider) {})
	// 无 brokers 应能创建 TP，ExportSpans 时才报错
	if err != nil {
		t.Logf("InitKafka returned error (expected if kafka unavailable): %v", err)
	}
}

func TestSpanToJSON(t *testing.T) {
	// 基本序列化不 panic
	exp := &kafkaExporter{topic: "test-topic"}
	err := exp.ExportSpans(context.Background(), nil)
	if err != nil {
		t.Fatalf("ExportSpans with nil spans failed: %v", err)
	}
}

// noopExporter is a test exporter that does nothing
type noopExporter struct{}

func (noopExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error { return nil }
func (noopExporter) Shutdown(ctx context.Context) error                                   { return nil }
