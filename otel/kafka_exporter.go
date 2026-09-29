// cachex/otel/kafka — Kafka span exporter
package otel

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// kafkaExporter 将 span 序列化为 JSON 后写入 Kafka
type kafkaExporter struct {
	brokers []string
	topic   string
	w       *kafka.Writer
}

func (k *kafkaExporter) resolveWriter() *kafka.Writer {
	if k.w == nil {
		k.w = &kafka.Writer{
			Addr:         kafka.TCP(k.brokers...),
			Topic:        k.topic,
			BatchSize:    100,
			Async:        false,
			RequiredAcks: kafka.RequireAll,
			Compression:  kafka.Snappy,
		}
	}
	return k.w
}

func (k *kafkaExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	w := k.resolveWriter()
	for _, s := range spans {
		msg := kafka.Message{
			Value: spanToJSON(s),
			Headers: []kafka.Header{
				{Key: "trace_id", Value: []byte(s.SpanContext().TraceID().String())},
				{Key: "span_id", Value: []byte(s.SpanContext().SpanID().String())},
				{Key: "name", Value: []byte(s.Name())},
				{Key: "service", Value: []byte("cachex")},
			},
		}
		if err := w.WriteMessages(ctx, msg); err != nil {
			return fmt.Errorf("kafka export: %w", err)
		}
	}
	return nil
}

func (k *kafkaExporter) Shutdown(ctx context.Context) error {
	if k.w != nil {
		return k.w.Close()
	}
	return nil
}

func spanToJSON(s sdktrace.ReadOnlySpan) []byte {
	data := map[string]interface{}{
		"name":           s.Name(),
		"trace_id":       s.SpanContext().TraceID().String(),
		"span_id":        s.SpanContext().SpanID().String(),
		"parent_span_id": s.Parent().SpanID().String(),
		"start_time":     s.StartTime().UTC().Format(time.RFC3339Nano),
		"end_time":       s.EndTime().UTC().Format(time.RFC3339Nano),
		"duration_us":    s.EndTime().Sub(s.StartTime()).Microseconds(),
		"span_kind":      s.SpanKind().String(),
		"status":         s.Status().Code.String(),
		"attributes":     attrsToMap(s.Attributes()),
		"events":         eventListToMap(s.Events()),
	}
	b, _ := json.Marshal(data)
	return b
}

func attrsToMap(attrs []attribute.KeyValue) map[string]interface{} {
	m := make(map[string]interface{}, len(attrs))
	for _, a := range attrs {
		m[string(a.Key)] = a.Value.AsInterface()
	}
	return m
}

func eventListToMap(events []sdktrace.Event) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(events))
	for _, e := range events {
		out = append(out, map[string]interface{}{
			"name":       e.Name,
			"timestamp":  e.Time.UTC().Format(time.RFC3339Nano),
			"attributes": attrsToMap(e.Attributes),
		})
	}
	return out
}
