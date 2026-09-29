// cachex/otel — OTel 上报通道（Jaeger OTLP HTTP / gRPC、Kafka）
//
// 设计要点：
//   - exporter 在初始化时创建一次并长期复用，避免每次导出重建连接
//   - span 经 BatchSpanProcessor 异步批量导出，杜绝逐条同步导出的性能抖动
//   - TracerProvider 的 resource 携带 service.name / service.version，对齐 OTel 语义约定
package otel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

const (
	defaultServiceName    = "cachex"
	defaultServiceVersion = "0.1.0"
	defaultBatchSize      = 512
	defaultBatchTimeout   = 5 * time.Second
)

// SetTracerProviderFunc 注册 TracerProvider 到外部 SDK 的回调
type SetTracerProviderFunc func(provider *sdktrace.TracerProvider)

// ProviderOptions TracerProvider 的构建参数
type ProviderOptions struct {
	ServiceName    string
	ServiceVersion string
	BatchSize      int
	// SamplerRatio 采样率：<=0 或 >=1 表示全采，(0,1) 为按比例采样
	SamplerRatio float64
}

func (o ProviderOptions) withDefaults() ProviderOptions {
	if o.ServiceName == "" {
		o.ServiceName = defaultServiceName
	}
	if o.ServiceVersion == "" {
		o.ServiceVersion = defaultServiceVersion
	}
	if o.BatchSize <= 0 {
		o.BatchSize = defaultBatchSize
	}
	return o
}

func (o ProviderOptions) sampler() sdktrace.Sampler {
	if o.SamplerRatio > 0 && o.SamplerRatio < 1 {
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(o.SamplerRatio))
	}
	return sdktrace.AlwaysSample()
}

// initFromExporter 用批处理导出器创建 TracerProvider，并通过回调注册到外部 SDK
func initFromExporter(exp sdktrace.SpanExporter, opt ProviderOptions, setTP SetTracerProviderFunc) error {
	opt = opt.withDefaults()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithMaxExportBatchSize(opt.BatchSize),
			sdktrace.WithBatchTimeout(defaultBatchTimeout),
		),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(opt.ServiceName),
			semconv.ServiceVersionKey.String(opt.ServiceVersion),
		)),
		sdktrace.WithSampler(opt.sampler()),
	)
	setTP(tp)
	return nil
}

// ----- Jaeger HTTP Exporter（OTLP/HTTP，默认端口 4318）-----

// InitJaeger 通过 OTLP HTTP 上报到 Jaeger
func InitJaeger(cfg *JaegerConfig, setTP SetTracerProviderFunc) error {
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	// gRPC/HTTP 构造器仅在参数非法或无法解析 TLS 配置时报错，不建立网络连接，
	// 因此这里用 Background context 是安全的：连接延迟到首次导出时才按需建立。
	exp, err := otlptracehttp.New(context.Background(), opts...)
	if err != nil {
		return fmt.Errorf("cachex/otel: create jaeger http exporter: %w", err)
	}
	return initFromExporter(exp, jaegerOptions(cfg), setTP)
}

// ----- Jaeger gRPC Exporter（OTLP/gRPC，默认端口 4317）-----

// InitJaegerGRPC 通过 OTLP gRPC 上报到 Jaeger
func InitJaegerGRPC(cfg *JaegerConfig, setTP SetTracerProviderFunc) error {
	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	exp, err := otlptracegrpc.New(context.Background(), opts...)
	if err != nil {
		return fmt.Errorf("cachex/otel: create jaeger grpc exporter: %w", err)
	}
	return initFromExporter(exp, jaegerOptions(cfg), setTP)
}

// ----- Kafka Exporter -----

// InitKafka 将 span 序列化为 JSON 后异步写入 Kafka topic
func InitKafka(cfg *KafkaConfig, setTP SetTracerProviderFunc) error {
	return initFromExporter(
		&kafkaExporter{brokers: cfg.Brokers, topic: cfg.Topic},
		kafkaOptions(cfg), setTP,
	)
}

// Shutdown 关闭全局 TracerProvider 并冲刷缓冲区中残留的 span。
// 应在进程退出前调用一次（cachex.CloseAll 已封装），否则最后一批 span 会丢失。
func Shutdown(ctx context.Context) error {
	provider, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider)
	if !ok {
		// 全局 provider 为 noop 或由宿主应用托管，交由宿主负责关闭
		return nil
	}
	return provider.Shutdown(ctx)
}

// ----- Config structs（与 cachex.TracingConfig 解耦，避免跨包循环依赖）-----

// JaegerConfig 已解析的 Jaeger 上报配置
type JaegerConfig struct {
	Endpoint       string
	Insecure       bool
	TLSEnabled     bool
	ServiceName    string
	ServiceVersion string
	BatchSize      int
	SamplerRatio   float64
}

// KafkaConfig 已解析的 Kafka 上报配置
type KafkaConfig struct {
	Brokers        []string
	Topic          string
	ServiceName    string
	ServiceVersion string
	BatchSize      int
	SamplerRatio   float64
}

// jaegerOptions 由 Jaeger 导出器配置构造 TracerProvider 参数
func jaegerOptions(cfg *JaegerConfig) ProviderOptions {
	return ProviderOptions{
		ServiceName:    cfg.ServiceName,
		ServiceVersion: cfg.ServiceVersion,
		BatchSize:      cfg.BatchSize,
		SamplerRatio:   cfg.SamplerRatio,
	}
}

// kafkaOptions 由 Kafka 导出器配置构造 TracerProvider 参数
func kafkaOptions(cfg *KafkaConfig) ProviderOptions {
	return ProviderOptions{
		ServiceName:    cfg.ServiceName,
		ServiceVersion: cfg.ServiceVersion,
		BatchSize:      cfg.BatchSize,
		SamplerRatio:   cfg.SamplerRatio,
	}
}
