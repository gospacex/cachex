// Package kafka publishes OpenTelemetry span batches to a Kafka topic.
//
// The exporter is caller-injected: it does NOT own a sarama.SyncProducer and
// never closes it on shutdown. The caller is responsible for constructing
// (or reusing) the producer and for its lifecycle. This keeps the cachex
// observability layer free of broker addresses, credentials, and producer
// configuration concerns.
//
// Typical wiring:
//
//	producer, err := sarama.NewSyncProducer(brokers, sarama.NewConfig())
//	if err != nil { return err }
//	exp, err := kafka.New(producer, "cachex-traces")
//	if err != nil { return err }
//	// ... use exp until process exit
//	_ = producer.Close()  // caller's responsibility
//
// The exporter implements observability.OtelExporter. It serializes each
// span as a JSON object and produces one Kafka message per span to the
// configured topic.
package kafka
