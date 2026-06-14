package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Exporter implements observability.OtelExporter by sending each span as a
// JSON-encoded message to a configured Kafka topic. The producer is
// caller-injected; the exporter does not own it and never closes it.
type Exporter struct {
	producer sarama.SyncProducer
	topic    string
}

// New constructs an Exporter that publishes span batches to topic via
// producer. The caller owns the producer and is responsible for its
// lifecycle; the exporter never closes it.
//
// Both producer and topic must be non-nil/non-empty respectively.
func New(producer sarama.SyncProducer, topic string) (*Exporter, error) {
	if producer == nil {
		return nil, errors.New("kafka: nil producer")
	}
	if topic == "" {
		return nil, errors.New("kafka: empty topic")
	}
	return &Exporter{producer: producer, topic: topic}, nil
}

// ExportSpans serializes each span as JSON and SendMessages it to the
// configured topic. An empty batch is a no-op and returns nil.
func (e *Exporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	for _, span := range spans {
		data, err := json.Marshal(buildSpanRecord(span))
		if err != nil {
			return fmt.Errorf("kafka exporter: marshal: %w", err)
		}
		if _, _, err := e.producer.SendMessage(&sarama.ProducerMessage{
			Topic: e.topic,
			Value: sarama.ByteEncoder(data),
		}); err != nil {
			return fmt.Errorf("kafka exporter: send: %w", err)
		}
	}
	return nil
}

// Shutdown is a no-op. The caller owns the sarama.SyncProducer lifecycle
// and is responsible for calling producer.Close() when appropriate.
func (e *Exporter) Shutdown(ctx context.Context) error {
	return nil
}

// buildSpanRecord converts a ReadOnlySpan to the JSON map shape the
// cachex observability layer publishes to Redis and Kafka exporters.
func buildSpanRecord(span sdktrace.ReadOnlySpan) map[string]interface{} {
	sc := span.SpanContext()
	parent := span.Parent()
	return map[string]interface{}{
		"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
		"name":       span.Name(),
		"trace_id":   sc.TraceID().String(),
		"span_id":    sc.SpanID().String(),
		"parent_id":  parent.SpanID().String(),
		"attributes": attrToMap(span.Attributes()),
	}
}

// attrToMap flattens OTel attribute key/value pairs into a plain map so
// the JSON encoder can serialize it. Each value is rendered via the
// attribute.Value.Emit() method which handles all standard OTel value
// types (string, bool, int64, float64, and slices of those).
func attrToMap(attrs []attribute.KeyValue) map[string]interface{} {
	out := make(map[string]interface{}, len(attrs))
	for _, a := range attrs {
		out[string(a.Key)] = a.Value.Emit()
	}
	return out
}
