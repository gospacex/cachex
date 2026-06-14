package observability

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/gospacex/cachex"
	"github.com/redis/go-redis/v9"
)

func init() {
	cachex.RegisterTracingInit(traceConfigToInitTracing)
}

// traceConfigToInitTracing converts a cachex.TraceConfig to an observability.Config
// and calls InitTracing. This is the bridge between the root cachex package
// (which cannot import observability due to cycle constraints) and the
// observability package.
func traceConfigToInitTracing(ctx context.Context, trace *cachex.TraceConfig) (func(context.Context), error) {
	if trace == nil || !trace.Enabled {
		return func(context.Context) {}, nil
	}

	cfg := &Config{
		Enabled:     trace.Enabled,
		Backend:     trace.Exporter,
		ServiceName: trace.ServiceName,
		Endpoint:    trace.Endpoint,
		Protocol:    trace.Protocol,
		Insecure:    trace.Insecure,
		Headers:     trace.Headers,
		SampleRate:  trace.SamplerRatio,
	}

	if trace.Stream != "" {
		cfg.RedisStream = trace.Stream
	}
	if trace.Topic != "" {
		cfg.KafkaTopic = trace.Topic
	}

	if trace.SamplerRatio <= 0 {
		cfg.SampleRate = 1.0
	}
	if cfg.SampleRate > 0 && cfg.SampleRate < 1.0 {
		cfg.BatchTimeout = 5 * time.Second
	}

	return InitTracing(ctx, cfg)
}

// TraceConfigFromCachex builds an observability.Config from a cachex.TraceConfig
// and optionally injects the provided Redis client and/or Kafka producer.
// This is useful when the caller already has driver-level handles.
func TraceConfigFromCachex(trace *cachex.TraceConfig, redisClient *redis.Client, kafkaProducer sarama.SyncProducer) *Config {
	if trace == nil {
		return DefaultConfig()
	}

	cfg := &Config{
		Enabled:     trace.Enabled,
		Backend:     trace.Exporter,
		ServiceName: trace.ServiceName,
		Endpoint:    trace.Endpoint,
		Protocol:    trace.Protocol,
		Insecure:    trace.Insecure,
		Headers:     trace.Headers,
		SampleRate:  trace.SamplerRatio,
	}

	if redisClient != nil {
		cfg.RedisClient = redisClient
	}
	if trace.Stream != "" {
		cfg.RedisStream = trace.Stream
	}
	if kafkaProducer != nil {
		cfg.KafkaProducer = kafkaProducer
	}
	if trace.Topic != "" {
		cfg.KafkaTopic = trace.Topic
	}

	return cfg
}
