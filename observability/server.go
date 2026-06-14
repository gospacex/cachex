// Copyright 2024 cachex. All rights reserved.
//
// Observability server: InitTracing wires up OpenTelemetry tracing
// against one of four backends (jaeger / otlp / redis_stream / kafka_topic)
// and returns a no-op idempotent cleanup function.
//
// Patterned after mqx/observability/server.go but with the OTLP backend
// promoted to first-class support (gRPC + HTTP) and a hard requirement
// that Jaeger failure be tolerated (log + no-op) so that the service
// stays up even when the collector is unreachable.

package observability

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

	jaegerexporter "github.com/gospacex/cachex/observability/exporter/jaeger"
	kafkaexporter "github.com/gospacex/cachex/observability/exporter/kafka"
	otlpexporter "github.com/gospacex/cachex/observability/exporter/otlp"
	redisexporter "github.com/gospacex/cachex/observability/exporter/redis"
)

// Four trace backend identifiers.
const (
	TraceBackendJaeger      = "jaeger"
	TraceBackendOTLP        = "otlp"
	TraceBackendRedisStream = "redis_stream"
	TraceBackendKafkaTopic  = "kafka_topic"
)

// OTLP wire protocols.
const (
	ProtocolGRPC = "grpc"
	ProtocolHTTP = "http"
)

// Config drives InitTracing. Field semantics:
//
//   - Enabled=false          → no-op cleanup, no global side-effects.
//   - Backend=""             → defaults to TraceBackendJaeger.
//   - Protocol="" (OTLP)     → defaults to ProtocolGRPC.
//   - SampleRate<=0 or ==1.0 → AlwaysSample; otherwise ratio-based.
//   - BatchTimeout==0        → defaults to 5s (sdktrace.WithBatcher).
//   - ServiceName==""        → defaults to "cachex".
type Config struct {
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

// DefaultConfig returns a Config with the production-grade defaults baked
// in. Tracing stays opt-in (Enabled=false). Mutate fields as needed and
// pass the result to InitTracing.
func DefaultConfig() *Config {
	return &Config{
		Enabled:      false,
		ServiceName:  "cachex",
		Backend:      TraceBackendJaeger,
		Protocol:     ProtocolGRPC,
		SampleRate:   1.0,
		BatchTimeout: 5 * time.Second,
	}
}

// Process-wide guard for the "current" tracer provider / cleanup pair.
// InitTracing captures both at call time so a subsequent InitTracing
// cannot clobber the cleanup function it returned.
var (
	tpMu          sync.Mutex
	currentTP     *sdktrace.TracerProvider
	currentClean  func(context.Context)
	currentCleanO sync.Once
)

// InitTracing assembles a TracerProvider for the requested backend,
// installs it (and a composite W3C propagator) as the otel globals, and
// returns an idempotent shutdown function.
//
// Returned cleanup:
//   - is safe to call multiple times (sync.Once-wrapped per Init call).
//   - flushes + shuts down the TracerProvider captured at call time, so
//     a later InitTracing for a different backend will not affect it.
//   - does NOT close injected Redis clients or Kafka producers (callers
//     own those lifecycles).
func InitTracing(ctx context.Context, cfg *Config) (func(context.Context), error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	if !cfg.Enabled {
		log.Println("[observability] tracing disabled, skipping OTel initialization")
		return func(context.Context) {}, nil
	}

	exp, err := buildSpanExporter(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("observability: build span exporter: %w", err)
	}
	if exp == nil {
		// buildSpanExporter already logged the warning; return no-op so
		// startup proceeds even when the collector is unreachable.
		return func(context.Context) {}, nil
	}

	// Resource / TracerProvider.
	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(serviceNameOrDefault(cfg.ServiceName)),
	))
	if err != nil {
		return nil, fmt.Errorf("observability: create resource: %w", err)
	}

	batchTimeout := cfg.BatchTimeout
	if batchTimeout <= 0 {
		batchTimeout = 5 * time.Second
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(batchTimeout)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(samplerFromRate(cfg.SampleRate)),
	)

	// Install as global. Captured under tpMu so concurrent InitTracing
	// callers see a consistent (tp, cleanup) pair.
	tpMu.Lock()
	previousTP := currentTP
	previousClean := currentClean
	currentTP = tp
	currentClean = func(context.Context) {}
	tpMu.Unlock()

	// Close the previously-installed provider, if any. Best-effort:
	// shutdown errors only get logged so the new init succeeds.
	if previousTP != nil {
		go func(prev *sdktrace.TracerProvider) {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := prev.Shutdown(shutdownCtx); err != nil {
				log.Printf("[observability] previous TracerProvider shutdown error: %v", err)
			}
		}(previousTP)
	}

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Printf("[observability] tracing initialized, backend=%s service=%s", backendOrDefault(cfg.Backend), cfg.ServiceName)

	// Per-call cleanup. sync.Once makes repeated calls a no-op.
	var once sync.Once
	cleanup := func(ctx context.Context) {
		once.Do(func() {
			log.Println("[observability] flushing tracer provider...")
			if err := tp.ForceFlush(ctx); err != nil {
				log.Printf("[observability] force flush error: %v", err)
			}
			if err := tp.Shutdown(ctx); err != nil {
				log.Printf("[observability] tracer provider shutdown error: %v", err)
			}
		})
	}

	// Track the *current* cleanup so a future Init can release the
	// previous one. The previousClean value is kept for diagnostics.
	_ = previousClean
	tpMu.Lock()
	currentClean = cleanup
	tpMu.Unlock()

	return cleanup, nil
}

// buildSpanExporter picks the right backend exporter. Returns
// (nil, nil) only for the Jaeger-unreachable case (logged warning);
// all other failure paths return a real error so the caller fails
// InitTracing loudly.
func buildSpanExporter(ctx context.Context, cfg *Config) (sdktrace.SpanExporter, error) {
	switch backendOrDefault(cfg.Backend) {
	case TraceBackendJaeger:
		exp, err := jaegerexporter.New(ctx, cfg.Endpoint, cfg.Insecure)
		if err != nil {
			// Tolerate unreachable Jaeger: log warning, return (nil, nil)
			// so the service stays up.
			log.Printf("[observability] warning: failed to create Jaeger exporter (endpoint=%q): %v", cfg.Endpoint, err)
			return nil, nil
		}
		return exp, nil

	case TraceBackendOTLP:
		protocol := cfg.Protocol
		if protocol == "" {
			protocol = ProtocolGRPC
		}
		if cfg.Endpoint == "" {
			return nil, fmt.Errorf("otlp: endpoint is required")
		}
		return otlpexporter.New(ctx, otlpexporter.Protocol(protocol), cfg.Endpoint, cfg.Headers, cfg.Insecure)

	case TraceBackendRedisStream:
		if cfg.RedisClient == nil {
			return nil, fmt.Errorf("backend=redis_stream requires cfg.RedisClient != nil")
		}
		if cfg.RedisStream == "" {
			return nil, fmt.Errorf("backend=redis_stream requires cfg.RedisStream != \"\"")
		}
		return redisexporter.New(cfg.RedisClient, cfg.RedisStream)

	case TraceBackendKafkaTopic:
		if cfg.KafkaProducer == nil {
			return nil, fmt.Errorf("backend=kafka_topic requires cfg.KafkaProducer != nil")
		}
		if cfg.KafkaTopic == "" {
			return nil, fmt.Errorf("backend=kafka_topic requires cfg.KafkaTopic != \"\"")
		}
		return kafkaexporter.New(cfg.KafkaProducer, cfg.KafkaTopic)

	default:
		return nil, fmt.Errorf("unknown trace backend %q (want: %s | %s | %s | %s)",
			cfg.Backend,
			TraceBackendJaeger, TraceBackendOTLP,
			TraceBackendRedisStream, TraceBackendKafkaTopic)
	}
}

// backendOrDefault returns cfg.Backend or TraceBackendJaeger when empty.
func backendOrDefault(b string) string {
	if b == "" {
		return TraceBackendJaeger
	}
	return b
}

// serviceNameOrDefault returns cfg.ServiceName or "cachex" when empty.
func serviceNameOrDefault(s string) string {
	if s == "" {
		return "cachex"
	}
	return s
}

// samplerFromRate maps a [0,1] ratio to a Sampler. Zero or one picks
// AlwaysSample; anything in (0,1) uses ParentBased(TraceIDRatioBased)
// so the decision is stable across a trace.
func samplerFromRate(rate float64) sdktrace.Sampler {
	if rate <= 0 || rate >= 1 {
		return sdktrace.AlwaysSample()
	}
	return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(rate))
}
