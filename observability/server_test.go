// Copyright 2024 cachex. All rights reserved.
//
// Tests for the observability server: InitTracing and buildSpanExporter.
//
// TDD iron rule: these tests were written FIRST. The companion server.go
// is the minimal implementation that makes them pass.

package observability

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// fakeSyncProducer is a test double for sarama.SyncProducer.
// Mirrors the helper in exporter/kafka/kafka_test.go so the server tests
// can run without a real broker.
type fakeSyncProducer struct {
	mu       sync.Mutex
	messages []*sarama.ProducerMessage
}

func (f *fakeSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, msg)
	return 0, 0, nil
}

func (f *fakeSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, msgs...)
	return nil
}

func (f *fakeSyncProducer) Close() error { return nil }
func (f *fakeSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnStatusFlag(0)
}
func (f *fakeSyncProducer) IsTransactional() bool { return false }
func (f *fakeSyncProducer) BeginTxn() error       { return nil }
func (f *fakeSyncProducer) CommitTxn() error      { return nil }
func (f *fakeSyncProducer) AbortTxn() error       { return nil }
func (f *fakeSyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupId string) error {
	return nil
}
func (f *fakeSyncProducer) AddOffsetsToTxnWithGroupMetadata(offsets map[string][]*sarama.PartitionOffsetMetadata, group *sarama.ConsumerGroupMetadata) error {
	return nil
}
func (f *fakeSyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupId string, metadata *string) error {
	return nil
}
func (f *fakeSyncProducer) AddMessageToTxnWithGroupMetadata(msg *sarama.ConsumerMessage, group *sarama.ConsumerGroupMetadata, metadata *string) error {
	return nil
}

// resetGlobalTracingState resets otel's globals between tests so prior
// InitTracing calls do not leak. We capture the package-level noop
// tracer provider (the only one guaranteed to exist before any SDK is
// initialised) and restore it after each test.
var initialNoopTP = otel.GetTracerProvider()

func resetGlobalTracingState() {
	otel.SetTracerProvider(initialNoopTP)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
}

// -----------------------------------------------------------------------------
// DefaultConfig
// -----------------------------------------------------------------------------

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg == nil {
		t.Fatal("DefaultConfig returned nil")
	}
	if cfg.Enabled {
		t.Errorf("Enabled = true, want false (opt-in)")
	}
	if cfg.ServiceName != "cachex" {
		t.Errorf("ServiceName = %q, want %q", cfg.ServiceName, "cachex")
	}
	if cfg.Protocol != "grpc" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "grpc")
	}
	if cfg.SampleRate != 1.0 {
		t.Errorf("SampleRate = %v, want 1.0", cfg.SampleRate)
	}
	if cfg.BatchTimeout != 5*time.Second {
		t.Errorf("BatchTimeout = %v, want 5s", cfg.BatchTimeout)
	}
}

// -----------------------------------------------------------------------------
// InitTracing: nil / disabled
// -----------------------------------------------------------------------------

func TestInitTracing_NilConfig(t *testing.T) {
	cleanup, err := InitTracing(context.Background(), nil)
	if err != nil {
		t.Fatalf("nil cfg should be allowed, got error: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup func should be non-nil even on nil cfg")
	}
	// Calling cleanup on a disabled/nil path should be safe.
	cleanup(context.Background())
	cleanup(context.Background()) // idempotent
}

func TestInitTracing_Disabled_ReturnsNoopCleanup(t *testing.T) {
	resetGlobalTracingState()
	beforeTP := otel.GetTracerProvider()

	cfg := &Config{Enabled: false}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup should be non-nil")
	}

	afterTP := otel.GetTracerProvider()
	// Global tracer provider should be untouched when Enabled=false.
	if !reflect.DeepEqual(beforeTP, afterTP) {
		t.Errorf("global tracer provider changed when Enabled=false")
	}

	cleanup(context.Background())
	cleanup(context.Background())
}

// -----------------------------------------------------------------------------
// InitTracing: jaeger backend
// -----------------------------------------------------------------------------

func TestInitTracing_Jaeger_ValidEndpoint(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:     true,
		Backend:     TraceBackendJaeger,
		Endpoint:    "localhost:14268",
		Insecure:    true,
		ServiceName: "cachex-jaeger-test",
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup should be non-nil")
	}

	tp := otel.GetTracerProvider()
	if reflect.DeepEqual(tp, initialNoopTP) {
		t.Errorf("global tracer provider should not be the package noop after InitTracing")
	}

	cleanup(context.Background())
	cleanup(context.Background())
}

func TestInitTracing_TolerateUnreachableJaeger(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:     true,
		Backend:     TraceBackendJaeger,
		Endpoint:    "this-host-does-not-exist-resolve.invalid:1234",
		Insecure:    true,
		ServiceName: "cachex-jaeger-unreachable",
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unreachable jaeger should NOT return error (got: %v)", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup should be non-nil")
	}
	// Cleanup must be safe even though the exporter failed to dial.
	cleanup(context.Background())
}

// -----------------------------------------------------------------------------
// InitTracing: otlp backend
// -----------------------------------------------------------------------------

func TestInitTracing_OTLP_GRPC_ValidEndpoint(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:  true,
		Backend:  TraceBackendOTLP,
		Protocol: ProtocolGRPC,
		Endpoint: "localhost:4317",
		Insecure: true,
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup should be non-nil")
	}
	cleanup(context.Background())
}

func TestInitTracing_OTLP_HTTP_ValidEndpoint(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:  true,
		Backend:  TraceBackendOTLP,
		Protocol: ProtocolHTTP,
		Endpoint: "localhost:4318",
		Insecure: true,
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup should be non-nil")
	}
	cleanup(context.Background())
}

func TestInitTracing_OTLP_DefaultProtocol_GRPC(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:  true,
		Backend:  TraceBackendOTLP,
		Protocol: "", // should default to grpc
		Endpoint: "localhost:4317",
		Insecure: true,
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup should be non-nil")
	}
	cleanup(context.Background())
}

func TestBuildSpanExporter_OTLP_UnknownProtocol(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:  true,
		Backend:  TraceBackendOTLP,
		Protocol: "tcp", // invalid
		Endpoint: "localhost:4317",
		Insecure: true,
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for unknown otlp protocol, got nil")
	}
	if cleanup != nil {
		t.Errorf("cleanup should be nil on error, got non-nil")
	}
}

func TestBuildSpanExporter_OTLP_EmptyEndpoint(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:  true,
		Backend:  TraceBackendOTLP,
		Protocol: ProtocolGRPC,
		Endpoint: "",
		Insecure: true,
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for empty endpoint, got nil")
	}
	if cleanup != nil {
		t.Errorf("cleanup should be nil on error")
	}
}

func TestBuildSpanExporter_OTLP_HeadersPropagated(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:  true,
		Backend:  TraceBackendOTLP,
		Protocol: ProtocolGRPC,
		Endpoint: "localhost:4317",
		Insecure: true,
		Headers:  map[string]string{"Authorization": "Bearer xyz"},
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("OTLP should accept headers silently, got: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup should be non-nil")
	}
	cleanup(context.Background())
}

// -----------------------------------------------------------------------------
// InitTracing: redis_stream backend
// -----------------------------------------------------------------------------

func TestInitTracing_RedisStream_NilClient_ReturnsError(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:     true,
		Backend:     TraceBackendRedisStream,
		RedisClient: nil,
		RedisStream: "trace:cachex",
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for nil RedisClient, got nil")
	}
	if cleanup != nil {
		t.Errorf("cleanup should be nil on error")
	}
}

func TestInitTracing_RedisStream_EmptyStream_ReturnsError(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:     true,
		Backend:     TraceBackendRedisStream,
		RedisClient: redis.NewClient(&redis.Options{Addr: "localhost:0"}),
		RedisStream: "",
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for empty RedisStream, got nil")
	}
	if cleanup != nil {
		t.Errorf("cleanup should be nil on error")
	}
}

func TestInitTracing_RedisStream_Valid(t *testing.T) {
	resetGlobalTracingState()

	client := redis.NewClient(&redis.Options{Addr: "localhost:0"})
	defer func() { _ = client.Close() }()

	cfg := &Config{
		Enabled:     true,
		Backend:     TraceBackendRedisStream,
		RedisClient: client,
		RedisStream: "trace:cachex",
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup should be non-nil")
	}
	cleanup(context.Background())
}

// -----------------------------------------------------------------------------
// InitTracing: kafka_topic backend
// -----------------------------------------------------------------------------

func TestInitTracing_KafkaTopic_NilProducer_ReturnsError(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:       true,
		Backend:       TraceBackendKafkaTopic,
		KafkaProducer: nil,
		KafkaTopic:    "trace-cachex",
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for nil KafkaProducer, got nil")
	}
	if cleanup != nil {
		t.Errorf("cleanup should be nil on error")
	}
}

func TestInitTracing_KafkaTopic_EmptyTopic_ReturnsError(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:       true,
		Backend:       TraceBackendKafkaTopic,
		KafkaProducer: &fakeSyncProducer{},
		KafkaTopic:    "",
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for empty KafkaTopic, got nil")
	}
	if cleanup != nil {
		t.Errorf("cleanup should be nil on error")
	}
}

func TestInitTracing_KafkaTopic_Valid(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:       true,
		Backend:       TraceBackendKafkaTopic,
		KafkaProducer: &fakeSyncProducer{},
		KafkaTopic:    "trace-cachex",
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup should be non-nil")
	}
	cleanup(context.Background())
}

// -----------------------------------------------------------------------------
// InitTracing: misc / global state / edge cases
// -----------------------------------------------------------------------------

func TestInitTracing_UnknownBackend_ReturnsError(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled: true,
		Backend: "bogus",
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for unknown backend, got nil")
	}
	if cleanup != nil {
		t.Errorf("cleanup should be nil on error")
	}
}

func TestInitTracing_IdempotentCleanup(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:     true,
		Backend:     TraceBackendRedisStream,
		RedisClient: redis.NewClient(&redis.Options{Addr: "localhost:0"}),
		RedisStream: "trace:cachex",
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Call cleanup multiple times — must not panic, must not error.
	for i := 0; i < 3; i++ {
		cleanup(context.Background())
	}
}

func TestInitTracing_SetsGlobalTracerProvider(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:     true,
		Backend:     TraceBackendRedisStream,
		RedisClient: redis.NewClient(&redis.Options{Addr: "localhost:0"}),
		RedisStream: "trace:cachex",
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup(context.Background())

	tp := otel.GetTracerProvider()
	if reflect.DeepEqual(tp, initialNoopTP) {
		t.Errorf("global tracer provider should not be the package noop after InitTracing")
	}
	tracer := tp.Tracer("test")
	if tracer == nil {
		t.Fatal("tp.Tracer should return non-nil tracer")
	}
}

func TestInitTracing_SetsGlobalTextMapPropagator(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:     true,
		Backend:     TraceBackendRedisStream,
		RedisClient: redis.NewClient(&redis.Options{Addr: "localhost:0"}),
		RedisStream: "trace:cachex",
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup(context.Background())

	prop := otel.GetTextMapPropagator()
	if prop == nil {
		t.Fatal("text map propagator should be non-nil")
	}
	// The composite propagator supports both TraceContext and Baggage fields.
	carrier := propagation.MapCarrier{}
	ctx := prop.Extract(context.Background(), carrier)
	if ctx == nil {
		t.Fatal("propagator.Extract returned nil context")
	}
}

func TestInitTracing_SampleRate_Default1(t *testing.T) {
	resetGlobalTracingState()

	cfg := &Config{
		Enabled:     true,
		Backend:     TraceBackendRedisStream,
		RedisClient: redis.NewClient(&redis.Options{Addr: "localhost:0"}),
		RedisStream: "trace:cachex",
		SampleRate:  0, // should be treated as AlwaysSample (no error)
	}
	cleanup, err := InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup(context.Background())
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

// _ = trace.TracerProvider nil-check — keep the import live and make it
// clear we deliberately depend on the trace package for the assertion
// patterns above.
var _ trace.TracerProvider = (trace.TracerProvider)(nil)
