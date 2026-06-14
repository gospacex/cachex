package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// fakeSyncProducer is a test double for sarama.SyncProducer.
// It records every SendMessage call so tests can assert on it.
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

func (f *fakeSyncProducer) snapshot() []*sarama.ProducerMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*sarama.ProducerMessage, len(f.messages))
	copy(out, f.messages)
	return out
}

// recordOneEndedSpan creates a TracerProvider backed by a SpanRecorder,
// starts and ends a single span, and returns the recorded ReadOnlySpan.
func recordOneEndedSpan(t *testing.T) trace.ReadOnlySpan {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	tracer := tp.Tracer("kafka-exporter-test")
	_, span := tracer.Start(context.Background(), "cache.Get")
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 recorded span, got %d", len(ended))
	}
	return ended[0]
}

func TestNew_NilProducer_ReturnsError(t *testing.T) {
	exp, err := New(nil, "cachex-traces")
	if err == nil {
		t.Fatal("expected error for nil producer, got nil")
	}
	if exp != nil {
		t.Fatalf("expected nil exporter on error, got %#v", exp)
	}
}

func TestNew_EmptyTopic_ReturnsError(t *testing.T) {
	producer := &fakeSyncProducer{}
	exp, err := New(producer, "")
	if err == nil {
		t.Fatal("expected error for empty topic, got nil")
	}
	if exp != nil {
		t.Fatalf("expected nil exporter on error, got %#v", exp)
	}
}

func TestNew_ValidArgs_ReturnsExporter(t *testing.T) {
	producer := &fakeSyncProducer{}
	exp, err := New(producer, "cachex-traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exp == nil {
		t.Fatal("expected non-nil exporter")
	}
	if exp.topic != "cachex-traces" {
		t.Fatalf("topic = %q, want %q", exp.topic, "cachex-traces")
	}
}

func TestExporter_ExportSpans_EmptyBatch_NoError(t *testing.T) {
	producer := &fakeSyncProducer{}
	exp, err := New(producer, "cachex-traces")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := exp.ExportSpans(context.Background(), nil); err != nil {
		t.Fatalf("expected no error for empty batch, got %v", err)
	}
	if got := producer.snapshot(); len(got) != 0 {
		t.Fatalf("expected 0 messages sent, got %d", len(got))
	}
}

func TestExporter_ExportSpans_SingleSpan_CallsSendMessage(t *testing.T) {
	producer := &fakeSyncProducer{}
	exp, err := New(producer, "cachex-traces")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	recorded := recordOneEndedSpan(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := exp.ExportSpans(ctx, []trace.ReadOnlySpan{recorded}); err != nil {
		t.Fatalf("ExportSpans: %v", err)
	}

	msgs := producer.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 SendMessage call, got %d", len(msgs))
	}
	if msgs[0].Topic != "cachex-traces" {
		t.Fatalf("message topic = %q, want %q", msgs[0].Topic, "cachex-traces")
	}
}

func TestExporter_Shutdown_NoError(t *testing.T) {
	producer := &fakeSyncProducer{}
	exp, err := New(producer, "cachex-traces")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := exp.Shutdown(context.Background()); err != nil {
		t.Fatalf("expected no error from Shutdown, got %v", err)
	}
}
