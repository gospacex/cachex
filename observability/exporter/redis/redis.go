// Package redis provides a Redis stream exporter for OpenTelemetry spans.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Exporter implements observability.OtelExporter by XADD-ing each span as a
// JSON-encoded map against the configured Redis stream. The caller owns the
// *redis.Client and is responsible for its lifecycle; the exporter never
// closes it.
type Exporter struct {
	client *redis.Client
	stream string
}

// New constructs an Exporter that publishes span batches to a Redis stream.
// The caller owns the *redis.Client and is responsible for its lifecycle;
// the exporter never closes it. stream is the XADD target (e.g.
// "cachex:traces").
func New(client *redis.Client, stream string) (*Exporter, error) {
	if client == nil {
		return nil, errors.New("redis exporter: nil client")
	}
	if stream == "" {
		return nil, errors.New("redis exporter: empty stream name")
	}
	return &Exporter{client: client, stream: stream}, nil
}

// ExportSpans serializes each span as a JSON record and XADDs it to the
// configured stream. An empty (or nil) batch is a no-op. The first marshal or
// network error is returned wrapped with context.
func (e *Exporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	for _, span := range spans {
		record := buildSpanRecord(span)
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("redis exporter: marshal: %w", err)
		}
		if err := e.client.XAdd(ctx, &redis.XAddArgs{
			Stream: e.stream,
			Values: map[string]interface{}{"data": data},
		}).Err(); err != nil {
			return fmt.Errorf("redis exporter: xadd: %w", err)
		}
	}
	return nil
}

// Shutdown is a no-op. The *redis.Client lifecycle is the caller's concern,
// so the exporter never closes it.
func (e *Exporter) Shutdown(_ context.Context) error {
	return nil
}

// buildSpanRecord converts a ReadOnlySpan into a JSON-friendly map. The
// timestamp is captured at call time using the wall clock; callers needing
// span start time should use span.StartTime() instead.
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

// attrToMap flattens attribute.KeyValue slices into a map for JSON encoding.
// STRING values are kept as strings; INT values are int64; everything else
// falls back to fmt.Sprintf("%v", ...) to keep the encoder happy.
func attrToMap(kvs []attribute.KeyValue) map[string]interface{} {
	out := make(map[string]interface{}, len(kvs))
	for _, kv := range kvs {
		switch kv.Value.Type() {
		case attribute.STRING:
			out[string(kv.Key)] = kv.Value.AsString()
		case attribute.BOOL:
			out[string(kv.Key)] = kv.Value.AsBool()
		case attribute.INT64:
			out[string(kv.Key)] = kv.Value.AsInt64()
		case attribute.FLOAT64:
			out[string(kv.Key)] = kv.Value.AsFloat64()
		default:
			out[string(kv.Key)] = kv.Value.Emit()
		}
	}
	return out
}
