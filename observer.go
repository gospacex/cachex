package cachex

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	otelattribute "go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// =============================================================================
// Observer - Observability interface
// =============================================================================

// Observer defines the interface for observing cache operations.
// Implement this to add metrics, tracing, or logging.
type Observer interface {
	// OnOperation is called before and after each cache operation.
	OnOperation(ctx context.Context, op string, backend string, err error, duration time.Duration)
	// OnError is called when an error occurs.
	OnError(ctx context.Context, op string, backend string, err error)
}

// Operation represents a cache operation type.
type Operation string

const (
	OpGet    Operation = "get"
	OpSet    Operation = "set"
	OpSetEX  Operation = "setex"
	OpSetNX  Operation = "setnx"
	OpDelete Operation = "delete"
	OpExists Operation = "exists"
	OpExpire Operation = "expire"
	OpTTL    Operation = "ttl"
	OpMGet   Operation = "mget"
	OpMSet   Operation = "mset"
	OpKeys   Operation = "keys"
	OpIncr   Operation = "incr"
	OpDecr   Operation = "decr"
	OpPing   Operation = "ping"
	OpClose  Operation = "close"
)

// observedCache wraps a Cache with observer notifications.
type observedCache struct {
	cache     Cache
	observers []Observer
	backend   string
}

func (c *observedCache) Get(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()
	result, err := c.cache.Get(ctx, key)
	c.notify(ctx, OpGet, err, time.Since(start))
	return result, err
}

func (c *observedCache) Set(ctx context.Context, key string, value []byte) error {
	start := time.Now()
	err := c.cache.Set(ctx, key, value)
	c.notify(ctx, OpSet, err, time.Since(start))
	return err
}

func (c *observedCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	start := time.Now()
	err := c.cache.SetEX(ctx, key, value, ttlSeconds)
	c.notify(ctx, OpSetEX, err, time.Since(start))
	return err
}

func (c *observedCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	start := time.Now()
	result, err := c.cache.SetNX(ctx, key, value, ttlSeconds)
	c.notify(ctx, OpSetNX, err, time.Since(start))
	return result, err
}

func (c *observedCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	start := time.Now()
	n, err := c.cache.Delete(ctx, keys...)
	c.notify(ctx, OpDelete, err, time.Since(start))
	return n, err
}

func (c *observedCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	start := time.Now()
	n, err := c.cache.Exists(ctx, keys...)
	c.notify(ctx, OpExists, err, time.Since(start))
	return n, err
}

func (c *observedCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	start := time.Now()
	b, err := c.cache.Expire(ctx, key, ttlSeconds)
	c.notify(ctx, OpExpire, err, time.Since(start))
	return b, err
}

func (c *observedCache) TTL(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	n, err := c.cache.TTL(ctx, key)
	c.notify(ctx, OpTTL, err, time.Since(start))
	return n, err
}

func (c *observedCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	start := time.Now()
	result, err := c.cache.MGet(ctx, keys...)
	c.notify(ctx, OpMGet, err, time.Since(start))
	return result, err
}

func (c *observedCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	start := time.Now()
	err := c.cache.MSet(ctx, kvs)
	c.notify(ctx, OpMSet, err, time.Since(start))
	return err
}

func (c *observedCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	start := time.Now()
	result, err := c.cache.Keys(ctx, pattern)
	c.notify(ctx, OpKeys, err, time.Since(start))
	return result, err
}

func (c *observedCache) Incr(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	n, err := c.cache.Incr(ctx, key)
	c.notify(ctx, OpIncr, err, time.Since(start))
	return n, err
}

func (c *observedCache) Decr(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	n, err := c.cache.Decr(ctx, key)
	c.notify(ctx, OpDecr, err, time.Since(start))
	return n, err
}

func (c *observedCache) Ping(ctx context.Context) error {
	start := time.Now()
	err := c.cache.Ping(ctx)
	c.notify(ctx, OpPing, err, time.Since(start))
	return err
}

func (c *observedCache) Close() error {
	start := time.Now()
	err := c.cache.Close()
	c.notify(context.Background(), OpClose, err, time.Since(start))
	return err
}

func (c *observedCache) Stats() Stats {
	return c.cache.Stats()
}

func (c *observedCache) notify(ctx context.Context, op Operation, err error, duration time.Duration) {
	opStr := string(op)
	for _, obs := range c.observers {
		obs.OnOperation(ctx, opStr, c.backend, err, duration)
	}
}

// =============================================================================
// OTel Tracing Helpers
// =============================================================================

func newOtelTracerFromConfig(cfg *TracingConfig) oteltrace.Tracer {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "cachex"
	}
	return otel.Tracer(cfg.ServiceName)
}

func newTraceObserverFromConfig(cfg *TracingConfig, tracer oteltrace.Tracer) *autoTraceObserver {
	return &autoTraceObserver{tracer: tracer}
}

// autoTraceObserver is a tracing observer that uses OTel.
type autoTraceObserver struct {
	tracer oteltrace.Tracer
}

func (o *autoTraceObserver) OnOperation(ctx context.Context, op string, backend string, err error, duration time.Duration) {
	span := oteltrace.SpanFromContext(ctx)
	if span != nil && span.SpanContext().IsValid() {
		span.SetAttributes(
			otelattribute.String("cache.operation", op),
			otelattribute.String("cache.backend", backend),
			otelattribute.Int64("cache.duration_ms", duration.Milliseconds()),
		)
		if err != nil {
			span.RecordError(err)
		}
	}
}

func (o *autoTraceObserver) OnError(ctx context.Context, op string, backend string, err error) {
	span := oteltrace.SpanFromContext(ctx)
	if span != nil && span.SpanContext().IsValid() {
		span.SetAttributes(
			otelattribute.String("cache.operation", op),
			otelattribute.String("cache.backend", backend),
		)
		span.RecordError(err)
	}
}
