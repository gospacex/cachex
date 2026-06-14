package observability

import (
	"context"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// InjectKafkaTrace injects W3C TraceContext + Baggage into Kafka message
// headers from the given context. If the context has no active span, headers
// are left untouched.
func InjectKafkaTrace(ctx context.Context, headers *[]sarama.RecordHeader) {
	if headers == nil {
		return
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for k, v := range carrier {
		*headers = append(*headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
}

// ExtractKafkaTrace returns a new context with W3C TraceContext + Baggage
// extracted from Kafka message headers. If the headers carry no trace info,
// the input context is returned unchanged.
func ExtractKafkaTrace(ctx context.Context, headers []sarama.RecordHeader) context.Context {
	carrier := make(propagation.MapCarrier, len(headers))
	for _, h := range headers {
		carrier[string(h.Key)] = string(h.Value)
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
