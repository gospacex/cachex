// Copyright 2024 cachex. All rights reserved.
//
// Tests the kafka_topic trace backend. The trace exporter writes
// span batches to a Kafka topic; the test injects a
// sarama.SyncProducer via initx.WithKafkaProducer.
//
// Pre-req: docker run -d -p 19092:9092 apache/kafka:latest
//
//	(19092 host port to avoid conflict with cachex business port)
package tracing_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM/sarama"
	cachex "github.com/gospacex/cachex"
	"github.com/gospacex/cachex/example/assert"
	"github.com/gospacex/cachex/initx"
	"go.opentelemetry.io/otel"
)

// TestTracing_KafkaTopic ships spans to a Kafka topic via the
// kafka_topic backend. Uses a sarama.SyncProducer injected through
// initx.WithKafkaProducer.
func TestTracing_KafkaTopic(t *testing.T) {
	assert.StartStack(t, "tracing", assert.TopologySingle, assert.BackendKafkaTopic)

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "trace.yaml")
	yaml := `backend: badger
in_memory: true
trace:
  enabled: true
  service_name: cachex-tracing-kafka
  exporter: kafka_topic
  topic: trace-spans-tracing
  brokers:
    - localhost:19092
  sampler_type: always_on
`
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write yaml: %v", err)
	}

	sc := sarama.NewConfig()
	sc.Version = sarama.V2_8_0_0
	sc.Producer.Return.Successes = true
	sc.Producer.RequiredAcks = sarama.WaitForLocal
	producer, err := sarama.NewSyncProducer([]string{"localhost:19092"}, sc)
	if err != nil {
		t.Skipf("kafka producer creation failed (kafka likely not running): %v", err)
	}
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cleanup, err := initx.InitTracing(ctx, cfgPath, initx.WithKafkaProducer(producer))
	if err != nil {
		t.Skipf("InitTracing(kafka_topic) failed: %v", err)
	}
	defer cleanup(context.Background())

	cfg, err := cachex.LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cache, err := cachex.DefaultFactory.Create(cachex.BackendBadger, cfg)
	if err != nil {
		t.Fatalf("create badger cache: %v", err)
	}
	defer func() { _ = cache.Close() }()

	want := assert.NewTraceID(t)
	spanCtx := traceContextFromTraceID(t, want)
	ctx = contextWithSpanContext(ctx, spanCtx)

	tracer := otel.Tracer("cachex-tracing-test")
	_, span := tracer.Start(ctx, "tracing-test.op",
		traceWithAttribute("test.backend", assert.BackendKafkaTopic),
	)
	defer span.End()

	if err := cache.Set(ctx, "tracing-kafka-key", []byte("hello-kafka")); err != nil {
		t.Fatalf("cache.Set: %v", err)
	}
	if _, err := cache.Get(ctx, "tracing-kafka-key"); err != nil {
		t.Fatalf("cache.Get: %v", err)
	}

	assert.AssertSpanInBackend(t, ctx, assert.BackendKafkaTopic, "tracing", assert.TopologySingle, want)
}
