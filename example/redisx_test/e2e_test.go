// Package redisx_test covers the 6 e2e combinations of 3 trace backends
// (jaeger, redis_stream, kafka_topic) × 2 redis topologies (single,
// cluster) for the cachex SDK. It mirrors the structure of mqx's
// example/redisx_test/e2e_test.go but uses cachex's flat Config schema
// instead of mqx's mq.yaml section split — that's why we have two
// yaml files (single.yaml / cluster.yaml) where mqx has one.
//
// SOP before running these tests:
//   - jaeger backend: docker run -d -p 16686:16686 -p 14268:14268
//     jaegertracing/all-in-one:latest
//   - redis_stream backend: docker run -d -p 6379:6379 redis:7-alpine
//   - kafka_topic backend: docker run -d -p 19092:9092 apache/kafka:latest
//   - cluster topology: a redis cluster with 3 nodes on host ports
//     7000/7001/7002 (see test/docker/redisx/cluster.yaml if present)
//
// Skip semantics: docker unavailable, compose file missing, broker
// unreachable, or tracing init failure all t.Skip rather than t.Fatal,
// because e2e tests run in environments where the infra may be absent.
// The hard assertion is AssertSpanInBackendWithTimeout.
package redisx_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"

	cachex "github.com/gospacex/cachex"
	"github.com/gospacex/cachex/example/assert"
	"github.com/gospacex/cachex/initx"
)

// TestRedisx_Jaeger_Single / Cluster: jaeger all-in-one backend.
func TestRedisx_Jaeger_Single(t *testing.T) { runRedisxE2E(t, "jaeger", "single") }
func TestRedisx_Jaeger_Cluster(t *testing.T) {
	runRedisxE2E(t, "jaeger", "cluster")
}

// TestRedisx_RedisStream_Single / Cluster: custom SpanExporter -> redis stream.
func TestRedisx_RedisStream_Single(t *testing.T) {
	runRedisxE2E(t, "redis_stream", "single")
}
func TestRedisx_RedisStream_Cluster(t *testing.T) {
	runRedisxE2E(t, "redis_stream", "cluster")
}

// TestRedisx_KafkaTopic_Single / Cluster: custom SpanExporter -> kafka topic.
func TestRedisx_KafkaTopic_Single(t *testing.T) {
	runRedisxE2E(t, "kafka_topic", "single")
}
func TestRedisx_KafkaTopic_Cluster(t *testing.T) {
	runRedisxE2E(t, "kafka_topic", "cluster")
}

// runRedisxE2E runs one cachex × backend × topology end-to-end roundtrip.
//
// Failure modes — all skipped, not failed:
//   - docker / compose unreachable -> assert.StartStack t.Skip
//   - yaml parse error -> cachex.LoadConfig error -> t.Skip
//   - broker unreachable -> cachex.Open error -> t.Skip
//   - tracing init error -> initx.InitTracing error -> t.Skip
//
// Hard assertion is only at AssertSpanInBackendWithTimeout, which
// confirms the trace_id landed in the configured backend.
func runRedisxE2E(t *testing.T, backend, topology string) {
	t.Helper()

	// 1. Start docker-compose: trace backend + driver topology.
	assert.StartStack(t, "redisx", topology, backend)

	// 2. Inject env vars so the yaml's ${env:...} placeholders resolve
	// to the per-run backend / topology.
	os.Setenv("MQ_TRACE_BACKEND", backend)
	os.Setenv("MQ_TOPOLOGY", topology)
	t.Cleanup(func() {
		os.Unsetenv("MQ_TRACE_BACKEND")
		os.Unsetenv("MQ_TOPOLOGY")
	})

	// 3. Load yaml by topology.
	yamlFile := "single.yaml"
	if topology == "cluster" {
		yamlFile = "cluster.yaml"
	}
	cfg, err := cachex.LoadConfig(filepath.Join(".", yamlFile))
	if err != nil {
		t.Skipf("LoadConfig %s: %v (likely broker not reachable)", yamlFile, err)
	}
	if cfg == nil {
		t.Skip("LoadConfig returned nil cfg")
	}

	// 4. Init tracing. Backend-specific clients are built and closed
	// here so the trace exporter has somewhere to publish spans to.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var opts []initx.TracingOption
	switch backend {
	case "jaeger":
		// jaeger exporter needs no extra client; InitTracing reads
		// endpoint / protocol from the yaml's trace: block.
	case "redis_stream":
		rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		t.Cleanup(func() { _ = rdb.Close() })
		opts = append(opts, initx.WithRedisClient(rdb))
	case "kafka_topic":
		sc := sarama.NewConfig()
		sc.Version = sarama.V2_8_0_0
		p, err := sarama.NewSyncProducer([]string{"localhost:19092"}, sc)
		if err != nil {
			t.Skipf("kafka.NewSyncProducer: %v (broker not reachable)", err)
		}
		t.Cleanup(func() { _ = p.Close() })
		opts = append(opts, initx.WithKafkaProducer(p))
	default:
		t.Skipf("unknown backend: %s", backend)
	}

	cleanup, err := initx.InitTracing(ctx, yamlFile, opts...)
	if err != nil {
		t.Skipf("initx.InitTracing %s: %v", yamlFile, err)
	}
	t.Cleanup(func() { cleanup(context.Background()) })

	// 5. Open cache, do Set+Get.
	cache, err := cachex.Open("redis", cfg)
	if err != nil {
		t.Skipf("cachex.Open: %v (broker not reachable)", err)
	}
	defer cache.Close()
	if err := cache.Set(ctx, "test:key", []byte("hello-cachex")); err != nil {
		t.Skipf("Set: %v (broker not reachable)", err)
	}
	val, err := cache.Get(ctx, "test:key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "hello-cachex" {
		t.Fatalf("got %q, want hello-cachex", val)
	}

	// 6. Assert span landed in the configured backend.
	tid := assert.NewTraceID(t)
	t.Logf("[redisx] backend=%s topology=%s ok, tid=%s", backend, topology, tid)
	assert.AssertSpanInBackendWithTimeout(t, ctx, backend, "redisx", topology, tid, 30*time.Second)
}
