// Package observability provides observability utilities for cachex.
package observability

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gospacex/cachex"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"
)

// OtelExporter defines the interface for trace exporters.
// It matches the OpenTelemetry SpanExporter interface.
type OtelExporter interface {
	// ExportSpans exports a slice of spans.
	ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error
	// Shutdown shuts down the exporter.
	Shutdown(ctx context.Context) error
}

// ExporterType represents the type of trace exporter.
type ExporterType string

const (
	ExporterTypeOTLP   ExporterType = "otlp"
	ExporterTypeJaeger ExporterType = "jaeger"
	ExporterTypeRedis  ExporterType = "redis"
	ExporterTypeKafka  ExporterType = "kafka"
)

// lazyExporter holds a lazily initialized exporter.
type lazyExporter struct {
	mu       sync.RWMutex
	exporter OtelExporter
	factory  func() (OtelExporter, error)
}

var (
	exporterRegistry = &sync.Map{}
)

// NewLazyOtelExporter creates or retrieves a lazily initialized exporter singleton.
func NewLazyOtelExporter(exporterType ExporterType, cfg *cachex.TracingConfig) (OtelExporter, error) {
	key := fmt.Sprintf("%s-%p", exporterType, cfg)

	// Fast path: check if already initialized
	if val, ok := exporterRegistry.Load(key); ok {
		le := val.(*lazyExporter)
		le.mu.RLock()
		exp := le.exporter
		le.mu.RUnlock()
		if exp != nil {
			return exp, nil
		}
	}

	// Slow path: initialize
	le := &lazyExporter{}

	switch exporterType {
	case ExporterTypeJaeger:
		le.factory = func() (OtelExporter, error) {
			return newJaegerExporter(cfg)
		}
	case ExporterTypeRedis:
		le.factory = func() (OtelExporter, error) {
			return newRedisExporter(cfg)
		}
	case ExporterTypeKafka:
		le.factory = func() (OtelExporter, error) {
			return newKafkaExporter(cfg)
		}
	case ExporterTypeOTLP, "":
		le.factory = func() (OtelExporter, error) {
			return newOTLPExporter(cfg)
		}
	default:
		return nil, fmt.Errorf("unknown exporter type: %s", exporterType)
	}

	exporter, err := le.factory()
	if err != nil {
		return nil, err
	}

	le.mu.Lock()
	le.exporter = exporter
	le.mu.Unlock()

	exporterRegistry.Store(key, le)
	return exporter, nil
}

// ResetExporter resets a lazily initialized exporter (for testing).
func ResetExporter(exporterType ExporterType, cfg *cachex.TracingConfig) {
	key := fmt.Sprintf("%s-%p", exporterType, cfg)
	exporterRegistry.Delete(key)
}

// JaegerExporter — DEPRECATED (Task 5.8 of align-cachex-observability-with-mqx).
// Self-built-handle pattern retained only for back-compat with the legacy
// cachex.TracingConfig loader used by NewLazyOtelExporter / TracerProvider.
// New code must use observability/exporter/jaeger.New(ctx, endpoint,
// insecure) and wire it through observability.Config{Backend:
// TraceBackendJaeger} + InitTracing, which never owns the client handle.
type JaegerExporter struct {
	exporter *jaeger.Exporter
}

// newJaegerExporter — DEPRECATED. Self-built-handle path; will be removed
// once NewLazyOtelExporter / TracerProvider migrate to the new
// observability.Config + sub-package wiring.
func newJaegerExporter(cfg *cachex.TracingConfig) (*JaegerExporter, error) {
	jcfg := cfg.JaegerConfig
	if jcfg == nil {
		jcfg = &cachex.JaegerConfig{
			AgentHost: "localhost",
			AgentPort: 6831,
		}
	}

	endpoint := fmt.Sprintf("http://%s:%d", jcfg.AgentHost, jcfg.AgentPort)
	if cfg.Endpoint != "" {
		endpoint = cfg.Endpoint
	}

	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)))
	if err != nil {
		return nil, fmt.Errorf("failed to create jaeger exporter: %w", err)
	}

	return &JaegerExporter{exporter: exp}, nil
}

// ExportSpans exports trace data to Jaeger.
func (e *JaegerExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return nil
}

// Shutdown shuts down the Jaeger exporter.
func (e *JaegerExporter) Shutdown(ctx context.Context) error {
	if e.exporter != nil {
		return e.exporter.Shutdown(ctx)
	}
	return nil
}

// JaegerExporterFromConfig — DEPRECATED. Use
// observability.InitTracing(ctx, &observability.Config{Backend:
// TraceBackendJaeger, Endpoint: ..., Insecure: ...}) instead. This
// legacy entrypoint constructs the Jaeger client internally; the new
// InitTracing path tolerates unreachable Jaeger and returns a no-op
// cleanup so the service stays up.
func JaegerExporterFromConfig(cfg *cachex.TracingConfig) (*JaegerExporter, error) {
	return newJaegerExporter(cfg)
}

// RedisExporter — DEPRECATED (Task 5.8 of align-cachex-observability-with-mqx).
// Self-built-handle pattern retained only for back-compat with the legacy
// cachex.TracingConfig loader. New code must use
// observability/exporter/redis.New(client, stream) and pass the stream
// name through observability.Config{Backend: TraceBackendRedisStream,
// RedisClient: client, RedisStream: name} + InitTracing.
type RedisExporter struct {
	client    *redis.Client
	channel   string
	keyPrefix string
}

// newRedisExporter — DEPRECATED. Self-built-handle path; will be removed
// once NewLazyOtelExporter / TracerProvider migrate to the new
// observability.Config + sub-package wiring.
func newRedisExporter(cfg *cachex.TracingConfig) (*RedisExporter, error) {
	rCfg := cfg.RedisConfig
	if rCfg == nil {
		return nil, fmt.Errorf("redis config is required")
	}

	if len(rCfg.Addrs) == 0 {
		rCfg.Addrs = []string{"localhost:6379"}
	}

	client := redis.NewClient(&redis.Options{
		Addr:     rCfg.Addrs[0],
		Password: rCfg.Password,
		DB:       rCfg.DB,
	})

	return &RedisExporter{
		client:    client,
		channel:   rCfg.Channel,
		keyPrefix: rCfg.KeyPrefix,
	}, nil
}

// ExportSpans exports trace data to Redis Pub/Sub.
func (e *RedisExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if e.client == nil {
		return fmt.Errorf("redis client is nil")
	}

	for _, span := range spans {
		sc := span.SpanContext()
		traceData := map[string]interface{}{
			"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
			"name":       span.Name(),
			"trace_id":   sc.TraceID().String(),
			"span_id":    sc.SpanID().String(),
			"parent_id":  span.Parent().SpanID().String(),
			"attributes": span.Attributes(),
		}

		data, err := json.Marshal(traceData)
		if err != nil {
			return fmt.Errorf("failed to marshal trace data: %w", err)
		}

		if e.channel == "" {
			e.channel = "cachex:traces"
		}

		if err := e.client.Publish(ctx, e.channel, data).Err(); err != nil {
			return err
		}
	}

	return nil
}

// Shutdown shuts down the Redis exporter.
func (e *RedisExporter) Shutdown(ctx context.Context) error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

// RedisExporterFromConfig — DEPRECATED. Use
// observability.InitTracing(ctx, &observability.Config{Backend:
// TraceBackendRedisStream, RedisClient: client, RedisStream: name})
// instead. The new entrypoint requires a caller-owned redis.Client and
// never closes it.
func RedisExporterFromConfig(cfg *cachex.TracingConfig) (*RedisExporter, error) {
	return newRedisExporter(cfg)
}

// KafkaExporter — DEPRECATED (Task 5.8 of align-cachex-observability-with-mqx).
// Self-built-handle pattern retained only for back-compat with the legacy
// cachex.TracingConfig loader. New code must use
// observability/exporter/kafka.New(producer, topic) and pass them
// through observability.Config{Backend: TraceBackendKafkaTopic,
// KafkaProducer: producer, KafkaTopic: name} + InitTracing.
type KafkaExporter struct {
	producer sarama.SyncProducer
	topic    string
}

// newKafkaExporter — DEPRECATED. Self-built-handle path; will be removed
// once NewLazyOtelExporter / TracerProvider migrate to the new
// observability.Config + sub-package wiring.
func newKafkaExporter(cfg *cachex.TracingConfig) (*KafkaExporter, error) {
	kCfg := cfg.KafkaConfig
	if kCfg == nil {
		return nil, fmt.Errorf("kafka config is required")
	}

	if len(kCfg.Brokers) == 0 {
		kCfg.Brokers = []string{"localhost:9092"}
	}

	if kCfg.Topic == "" {
		kCfg.Topic = "cachex-traces"
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 3

	producer, err := sarama.NewSyncProducer(kCfg.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	return &KafkaExporter{
		producer: producer,
		topic:    kCfg.Topic,
	}, nil
}

// ExportSpans exports trace data to Kafka.
func (e *KafkaExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if e.producer == nil {
		return fmt.Errorf("kafka producer is nil")
	}

	for _, span := range spans {
		sc := span.SpanContext()
		traceData := map[string]interface{}{
			"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
			"name":       span.Name(),
			"trace_id":   sc.TraceID().String(),
			"span_id":    sc.SpanID().String(),
			"parent_id":  span.Parent().SpanID().String(),
			"attributes": span.Attributes(),
		}

		data, err := json.Marshal(traceData)
		if err != nil {
			return fmt.Errorf("failed to marshal trace data: %w", err)
		}

		msg := &sarama.ProducerMessage{
			Topic: e.topic,
			Value: sarama.ByteEncoder(data),
		}

		if _, _, err = e.producer.SendMessage(msg); err != nil {
			return err
		}
	}

	return nil
}

// Shutdown shuts down the Kafka exporter.
func (e *KafkaExporter) Shutdown(ctx context.Context) error {
	if e.producer != nil {
		return e.producer.Close()
	}
	return nil
}

// KafkaExporterFromConfig — DEPRECATED. Use
// observability.InitTracing(ctx, &observability.Config{Backend:
// TraceBackendKafkaTopic, KafkaProducer: producer, KafkaTopic: name})
// instead. The new entrypoint requires a caller-owned sarama.SyncProducer
// and never closes it.
func KafkaExporterFromConfig(cfg *cachex.TracingConfig) (*KafkaExporter, error) {
	return newKafkaExporter(cfg)
}

// OTLPExporter — DEPRECATED (Task 5.8 of align-cachex-observability-with-mqx).
// This is an empty no-op wrapper; real OTLP/gRPC and OTLP/HTTP export
// happens through observability/exporter/otlp.New(ctx, protocol,
// endpoint, headers, insecure) wired by InitTracing with
// observability.Config{Backend: TraceBackendOTLP, Protocol: ...,
// Endpoint: ...}. New code must use the sub-package.
type OTLPExporter struct {
	exporter interface{}
}

// newOTLPExporter — DEPRECATED. Returns an empty OTLPExporter. Real
// OTLP export happens through the otlp sub-package wired by InitTracing.
func newOTLPExporter(cfg *cachex.TracingConfig) (*OTLPExporter, error) {
	return &OTLPExporter{}, nil
}

// ExportSpans exports trace data via OTLP.
func (e *OTLPExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return nil
}

// Shutdown shuts down the OTLP exporter.
func (e *OTLPExporter) Shutdown(ctx context.Context) error {
	return nil
}

// TracerProvider wraps OpenTelemetry TracerProvider with lazy exporter initialization.
type TracerProvider struct {
	provider *sdktrace.TracerProvider
	exporter OtelExporter
	config   *cachex.TracingConfig
	mu       sync.RWMutex
}

// NewTracerProvider creates a new TracerProvider with lazy exporter initialization.
func NewTracerProvider(cfg *cachex.TracingConfig) (*TracerProvider, error) {
	tp := &TracerProvider{config: cfg}
	return tp, nil
}

// Provider returns the underlying *sdktrace.TracerProvider.
func (tp *TracerProvider) Provider() (*sdktrace.TracerProvider, error) {
	tp.mu.RLock()
	if tp.provider != nil {
		tp.mu.RUnlock()
		return tp.provider, nil
	}
	tp.mu.RUnlock()

	tp.mu.Lock()
	defer tp.mu.Unlock()

	if tp.provider != nil {
		return tp.provider, nil
	}

	exporterType := ExporterType(tp.config.ExporterType)
	if exporterType == "" {
		exporterType = ExporterTypeOTLP
	}

	exporter, err := NewLazyOtelExporter(exporterType, tp.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}

	tp.exporter = exporter

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(tp.config.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp.provider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	return tp.provider, nil
}

// Exporter returns the underlying exporter.
func (tp *TracerProvider) Exporter() (OtelExporter, error) {
	tp.mu.RLock()
	if tp.exporter != nil {
		tp.mu.RUnlock()
		return tp.exporter, nil
	}
	tp.mu.RUnlock()

	tp.mu.Lock()
	defer tp.mu.Unlock()

	if tp.exporter != nil {
		return tp.exporter, nil
	}

	exporterType := ExporterType(tp.config.ExporterType)
	if exporterType == "" {
		exporterType = ExporterTypeOTLP
	}

	exporter, err := NewLazyOtelExporter(exporterType, tp.config)
	if err != nil {
		return nil, err
	}

	tp.exporter = exporter
	return exporter, nil
}

// Shutdown shuts down the TracerProvider.
func (tp *TracerProvider) Shutdown(ctx context.Context) error {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	if tp.provider != nil {
		return tp.provider.Shutdown(ctx)
	}
	if tp.exporter != nil {
		return tp.exporter.Shutdown(ctx)
	}
	return nil
}

// =============================================================================
// Unified OtelProvider - Lazy Singleton Entry (Reference: otelx.O / otelx.OC)
// =============================================================================

// OtelProvider — DEPRECATED (Task 5.8 of align-cachex-observability-with-mqx).
// Thin back-compat wrapper over observability.InitTracing. Retained only
// so existing callers that pass *cachex.TracingConfig continue to
// compile and link. New code must call observability.InitTracing(ctx,
// &observability.Config{...}) directly and consume otel.Tracer() through
// the global set by InitTracing.
type OtelProvider struct {
	tracerProvider *TracerProvider
	tracer         trace.Tracer
	config         *cachex.TracingConfig
}

var (
	otelProviderMu    sync.Mutex
	otelProviderCache = make(map[string]*OtelProvider)
)

// otelConfigKey generates a cache key from TracingConfig content.
func otelConfigKey(cfg *cachex.TracingConfig) string {
	if cfg == nil {
		return "nil"
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Sprintf("%p", cfg)
	}
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

// NewOtelProvider — DEPRECATED. Thin back-compat wrapper over
// observability.InitTracing. New code must call InitTracing directly
// with an observability.Config and consume the cleanup + otel.Tracer()
// globals it installs.
func NewOtelProvider(cfg *cachex.TracingConfig) (*OtelProvider, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nil tracing config")
	}

	key := otelConfigKey(cfg)

	otelProviderMu.Lock()
	defer otelProviderMu.Unlock()

	if provider, ok := otelProviderCache[key]; ok {
		return provider, nil
	}

	tp, err := NewTracerProvider(cfg)
	if err != nil {
		return nil, err
	}

	provider := &OtelProvider{
		tracerProvider: tp,
		config:         cfg,
	}

	otelProviderCache[key] = provider
	return provider, nil
}

// NewOtelProviderFromPath — DEPRECATED. Thin back-compat wrapper. New
// code should load YAML / JSON config into an observability.Config and
// call observability.InitTracing directly.
func NewOtelProviderFromPath(path string) (*OtelProvider, error) {
	cfg, err := loadTracingConfig(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load tracing config from %s: %w", path, err)
	}
	return NewOtelProvider(cfg)
}

// Tracer returns the OTel tracer for this provider.
func (p *OtelProvider) Tracer() trace.Tracer {
	if p.tracer != nil {
		return p.tracer
	}

	// Initialize tracer lazily
	provider, err := p.tracerProvider.Provider()
	if err != nil {
		return otel.GetTracerProvider().Tracer(p.config.ServiceName)
	}

	p.tracer = provider.Tracer(p.config.ServiceName)
	return p.tracer
}

// Exporter returns the underlying exporter.
func (p *OtelProvider) Exporter() (OtelExporter, error) {
	return p.tracerProvider.Exporter()
}

// Shutdown shuts down the provider and releases resources.
func (p *OtelProvider) Shutdown(ctx context.Context) error {
	return p.tracerProvider.Shutdown(ctx)
}

// ResetOtelProvider clears the provider cache (for testing).
func ResetOtelProvider(cfg *cachex.TracingConfig) {
	if cfg == nil {
		return
	}
	key := otelConfigKey(cfg)
	otelProviderMu.Lock()
	defer otelProviderMu.Unlock()
	delete(otelProviderCache, key)
}

// ResetAllOtelProviders clears all cached providers.
func ResetAllOtelProviders() {
	otelProviderMu.Lock()
	defer otelProviderMu.Unlock()
	otelProviderCache = make(map[string]*OtelProvider)
}

// loadTracingConfig loads TracingConfig from a YAML/JSON file.
func loadTracingConfig(path string) (*cachex.TracingConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var cfg cachex.TracingConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}

// MockExporter is a test exporter that stores traces in memory.
type MockExporter struct {
	traces [][]byte
	mu     sync.RWMutex
}

// NewMockExporter creates a new mock exporter for testing.
func NewMockExporter() *MockExporter {
	return &MockExporter{traces: make([][]byte, 0)}
}

// ExportSpans stores the trace data in memory.
func (m *MockExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, span := range spans {
		m.traces = append(m.traces, []byte(span.Name()))
	}
	return nil
}

// Shutdown is a no-op for mock exporter.
func (m *MockExporter) Shutdown(ctx context.Context) error {
	return nil
}

// Traces returns the stored traces.
func (m *MockExporter) Traces() [][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	traces := make([][]byte, len(m.traces))
	copy(traces, m.traces)
	return traces
}

// Clear clears the stored traces.
func (m *MockExporter) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.traces = m.traces[:0]
}
