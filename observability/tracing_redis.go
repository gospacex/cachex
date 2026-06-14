package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// InjectRedisTrace injects W3C TraceContext + Baggage into a Redis values map
// under the "trace" key (nested map[string]string). If the context has no
// active span, the values map is left untouched.
func InjectRedisTrace(ctx context.Context, values map[string]interface{}) {
	if values == nil {
		return
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) > 0 {
		values["trace"] = map[string]string(carrier)
	}
}

// ExtractRedisTrace returns a new context with W3C TraceContext + Baggage
// extracted from a Redis values map (expects "trace" key with nested
// map[string]string). If no trace info present, input context is returned.
func ExtractRedisTrace(ctx context.Context, values map[string]interface{}) context.Context {
	raw, ok := values["trace"]
	if !ok {
		return ctx
	}
	carrier, ok := raw.(map[string]string)
	if !ok {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}
