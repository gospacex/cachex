// Copyright 2024 cachex. All rights reserved.
//
// Package initx hosts the root-level observability shortcut
// functions (InitTracing, TracingOption, WithRedisClient,
// WithKafkaProducer). It is a separate sub-package from root cachex
// because cachex/observability already imports root cachex for
// *cachex.Config, so a root → observability import would cycle. The
// initx package sits one hop below the import boundary and is
// imported by application code that wants a single-call tracing
// initialisation.
//
// Architecture note: the spec's "RP" / "KP" shortcut names (which
// would return *redis.Client / sarama.SyncProducer) collide with the
// existing shortapi.go RP/KP that return Cache. Those existing names
// are kept for backward compatibility; the driver-side constructors
// drivers/redisx.PPS and drivers/kafkax.PPS already satisfy the spec's
// intent (path → native handle). See plan-ready.md §5 row 8 for the
// deviation note.

package initx

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"

	cachex "github.com/gospacex/cachex"
	"github.com/gospacex/cachex/observability"
)

// observabilityConfig is the local view of an observability.Config
// constructed by traceConfigToObservabilityConfig. It exists so the
// option helpers (WithRedisClient / WithKafkaProducer) can mutate
// the right fields before the call to observability.InitTracing.
type observabilityConfig struct {
	Enabled       bool
	Backend       string
	ServiceName   string
	Endpoint      string
	Insecure      bool
	Headers       map[string]string
	Protocol      string
	RedisClient   *redis.Client
	RedisStream   string
	KafkaProducer sarama.SyncProducer
	KafkaTopic    string
	SampleRate    float64
	BatchTimeout  time.Duration
}

// TracingOption mutates an observabilityConfig before InitTracing
// dispatches to the observability sub-package.
type TracingOption func(*observabilityConfig)

// WithRedisClient injects a *redis.Client into the trace config. Used
// for the redis_stream backend where the trace exporter publishes
// span batches to a Redis stream.
func WithRedisClient(c *redis.Client) TracingOption {
	return func(o *observabilityConfig) { o.RedisClient = c }
}

// WithKafkaProducer injects a sarama.SyncProducer into the trace
// config. Used for the kafka_topic backend where the trace exporter
// writes span batches to a Kafka topic.
func WithKafkaProducer(p sarama.SyncProducer) TracingOption {
	return func(o *observabilityConfig) { o.KafkaProducer = p }
}

// traceConfigToObservabilityConfig converts the new-schema
// cachex.TraceConfig (yaml: trace:) into the local
// observabilityConfig used by InitTracing. Sampler type strings are
// mapped to a numeric ratio; the long-name exporter values are
// validated; "redis" / "kafka" short names are accepted and
// normalised to redis_stream / kafka_topic for parity with
// cachex.TraceConfig.Validate.
func traceConfigToObservabilityConfig(tc *cachex.TraceConfig) (*observabilityConfig, error) {
	if tc == nil {
		return &observabilityConfig{}, nil
	}
	backend, err := normaliseTraceBackend(tc.Exporter)
	if err != nil {
		return nil, err
	}
	cfg := &observabilityConfig{
		Enabled:     tc.Enabled,
		ServiceName: tc.ServiceName,
		Backend:     backend,
		Endpoint:    tc.Endpoint,
		Protocol:    tc.Protocol,
		Insecure:    tc.Insecure,
		Headers:     tc.Headers,
		SampleRate:  samplerRatioFromType(tc.SamplerType, tc.SamplerRatio),
	}
	switch backend {
	case "redis_stream":
		cfg.RedisStream = tc.Stream
	case "kafka_topic":
		cfg.KafkaTopic = tc.Topic
	}
	return cfg, nil
}

// normaliseTraceBackend accepts the same set of values as
// cachex.TraceConfig.Validate and returns the canonical long-name
// form. Empty string is accepted (observability.InitTracing defaults
// to jaeger).
func normaliseTraceBackend(name string) (string, error) {
	switch name {
	case "":
		return "", nil
	case "jaeger", "otlp", "redis_stream", "kafka_topic":
		return name, nil
	case "redis":
		return "redis_stream", nil
	case "kafka":
		return "kafka_topic", nil
	default:
		return "", fmt.Errorf("unknown trace.exporter: %q (valid: jaeger, otlp, redis_stream, kafka_topic; short: redis, kafka)", name)
	}
}

// samplerRatioFromType collapses a (SamplerType, SamplerRatio) pair
// into a single ratio that the OTel SDK understands:
//
//	always_on / parentbased_always_on       → 1.0
//	always_off / parentbased_always_off     → 0.0
//	traceidratio / parentbased_traceidratio → ratio
//	"" / unknown                            → ratio
func samplerRatioFromType(samplerType string, ratio float64) float64 {
	switch samplerType {
	case "always_on", "parentbased_always_on":
		return 1.0
	case "always_off", "parentbased_always_off":
		return 0.0
	case "traceidratio", "parentbased_traceidratio", "ratio":
		return ratio
	default:
		return ratio
	}
}

// InitTracing loads the trace: block from the given YAML file,
// applies the supplied TracingOptions, and dispatches to
// observability.InitTracing. The returned cleanup is idempotent and
// safe to call multiple times.
//
// When trace.enabled is false (or no trace block is present), a
// no-op cleanup is returned and no OTel globals are touched.
// When the file is missing or unparseable, an error is returned and
// the cleanup is nil.
func InitTracing(ctx context.Context, path string, opts ...TracingOption) (func(context.Context), error) {
	cfg, err := cachex.LoadConfig(path)
	if err != nil {
		return nil, err
	}
	oc, err := traceConfigToObservabilityConfig(cfg.Trace)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(oc)
	}
	obsCfg := &observability.Config{
		Enabled:       oc.Enabled,
		Backend:       oc.Backend,
		ServiceName:   oc.ServiceName,
		Endpoint:      oc.Endpoint,
		Insecure:      oc.Insecure,
		Headers:       oc.Headers,
		Protocol:      oc.Protocol,
		RedisClient:   oc.RedisClient,
		RedisStream:   oc.RedisStream,
		KafkaProducer: oc.KafkaProducer,
		KafkaTopic:    oc.KafkaTopic,
		SampleRate:    oc.SampleRate,
		BatchTimeout:  oc.BatchTimeout,
	}
	return observability.InitTracing(ctx, obsCfg)
}
