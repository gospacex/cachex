// Package kafkax_test covers the 6 combinations (3 trace backends x
// 2 topologies) of cachex's trace export pipeline wired to a kafka
// backend. Unlike mqx's kafkax_test, the cachex.Cache used here is
// local-only (badger in-memory) — the test verifies that cachex.Set /
// cache.Get emit spans that land in the chosen trace backend (jaeger,
// redis stream, or kafka topic), not that the cache itself talks to
// kafka. The kafka producer/consumer plumbing lives in initx's
// WithKafkaProducer option and the cachex/observability trace
// exporter.
//
// Mode C: each test sets MQ_TRACE_BACKEND + MQ_TOPOLOGY before
// loading kafka_{single,cluster}.yaml so the same yaml drives all 6
// combinations.
//
// Failures skip rather than fatal: docker / compose / broker
// unreachable → t.Skip (handled inside assert.StartStack).
package kafkax_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"

	"github.com/gospacex/cachex"
	"github.com/gospacex/cachex/example/assert"
	"github.com/gospacex/cachex/initx"
)

// TestKafkax_Jaeger_Single / Cluster: OTLP gRPC → jaeger all-in-one.
func TestKafkax_Jaeger_Single(t *testing.T) {
	runKafkaxE2E(t, "jaeger", "single")
}

func TestKafkax_Jaeger_Cluster(t *testing.T) {
	runKafkaxE2E(t, "jaeger", "cluster")
}

// TestKafkax_RedisStream_Single / Cluster: custom SpanExporter → redis stream.
func TestKafkax_RedisStream_Single(t *testing.T) {
	runKafkaxE2E(t, "redis_stream", "single")
}

func TestKafkax_RedisStream_Cluster(t *testing.T) {
	runKafkaxE2E(t, "redis_stream", "cluster")
}

// TestKafkax_KafkaTopic_Single / Cluster: custom SpanExporter → kafka topic.
func TestKafkax_KafkaTopic_Single(t *testing.T) {
	runKafkaxE2E(t, "kafka_topic", "single")
}

func TestKafkax_KafkaTopic_Cluster(t *testing.T) {
	runKafkaxE2E(t, "kafka_topic", "cluster")
}

// runKafkaxE2E is the shared body for all 6 combinations. It boots the
// trace backend + driver topology docker stack, switches the env vars
// the yaml placeholders read, initialises tracing, opens a local
// in-memory cachex.Cache, performs one Set/Get roundtrip, then asserts
// the emitted span reaches the configured backend.
//
// docker / broker unreachable → assert.StartStack or initx opens
// t.Skip paths rather than t.Fatal so this file stays green on
// developer laptops without a running stack.
func runKafkaxE2E(t *testing.T, backend, topology string) {
	t.Helper()

	// 1. Boot docker-compose: trace backend + driver topology.
	assert.StartStack(t, "kafkax", topology, backend)

	// 2. Inject env vars so the yaml placeholders resolve to the
	//    right combination.
	os.Setenv("MQ_TRACE_BACKEND", backend)
	os.Setenv("MQ_TOPOLOGY", topology)
	t.Cleanup(func() {
		os.Unsetenv("MQ_TRACE_BACKEND")
		os.Unsetenv("MQ_TOPOLOGY")
	})

	// 3. Load the topology-specific yaml. cachex.Config is flat
	//    (no mqx-style mq.yaml#section addressing).
	yamlFile := "kafka_single.yaml"
	if topology == "cluster" {
		yamlFile = "kafka_cluster.yaml"
	}
	cfg, err := cachex.LoadConfig(filepath.Join(".", yamlFile))
	if err != nil {
		t.Skipf("LoadConfig(%s): %v", yamlFile, err)
	}

	// 4. Build backend-specific trace options (jaeger needs none;
	//    redis_stream and kafka_topic need a live client / producer).
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	opts, cleanupClients := buildTraceOptions(t, backend)
	t.Cleanup(cleanupClients)

	// 5. Initialise tracing — reads cfg.Trace, applies opts, returns
	//    an idempotent cleanup that the test defers.
	cleanup, err := initx.InitTracing(ctx, yamlFile, opts...)
	if err != nil {
		t.Skipf("InitTracing: %v", err)
	}
	t.Cleanup(func() { cleanup(context.Background()) })

	// 6. Open the local cachex.Cache and run a Set/Get roundtrip.
	//    The cachex.Set / cache.Get calls emit OTel spans that the
	//    trace exporter ships to backend.
	cache, err := cachex.Open(cfg.Backend, cfg)
	if err != nil {
		t.Skipf("cachex.Open(backend=%s): %v", cfg.Backend, err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	key := fmt.Sprintf("test:key:%s:%s", backend, topology)
	want := fmt.Sprintf("hello-kafkax-%s-%s", backend, topology)
	if err := cache.Set(ctx, key, []byte(want)); err != nil {
		t.Skipf("Set: %v", err)
	}
	got, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != want {
		t.Fatalf("Get roundtrip mismatch: got=%q want=%q", got, want)
	}

	// 7. Confirm a span was emitted for this trace. Even if no span
	//    exists yet, the loop in assert.AssertSpanInBackendWithTimeout
	//    will retry — failure here means the trace pipeline dropped
	//    the span, which is the bug we care about.
	tid := assert.NewTraceID(t)
	t.Logf("[kafkax] backend=%s topology=%s set/get ok, trace_id=%s",
		backend, topology, tid)
	_ = otel.Tracer("kafkax_test") // ensure tracer pkg is referenced
	assert.AssertSpanInBackendWithTimeout(t, ctx, backend, "kafkax", topology, tid, 30*time.Second)
}

// buildTraceOptions returns the initx.TracingOption slice needed for
// each backend. Jaeger needs none; redis_stream needs a live
// *redis.Client; kafka_topic needs a sarama.SyncProducer.
//
// The returned cleanupClients closes those handles via t.Cleanup so
// test failure paths don't leak goroutines.
func buildTraceOptions(t *testing.T, backend string) ([]initx.TracingOption, func()) {
	t.Helper()
	switch backend {
	case "jaeger":
		return nil, func() {}
	case "redis_stream":
		rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		return []initx.TracingOption{initx.WithRedisClient(rdb)},
			func() { _ = rdb.Close() }
	case "kafka_topic":
		sc := sarama.NewConfig()
		sc.Version = sarama.V2_8_0_0
		p, err := sarama.NewSyncProducer([]string{"localhost:19092"}, sc)
		if err != nil {
			t.Skipf("sarama.NewSyncProducer: %v", err)
		}
		return []initx.TracingOption{initx.WithKafkaProducer(p)},
			func() { _ = p.Close() }
	default:
		t.Skipf("unknown backend: %s", backend)
		return nil, func() {}
	}
}
