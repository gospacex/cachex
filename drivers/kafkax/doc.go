// Package kafkax is the top-level Kafka driver for cachex.
//
// It manages shared sarama.SyncProducer instances keyed by a content-aware
// fingerprint (see utils.ConfigFingerprint) so that callers asking for the
// same configuration reuse the same underlying producer, while a
// configuration change produces a fresh producer with the old one scheduled
// for delayed flush+close so in-flight messages are not lost.
//
// Five primary entry points mirror the mqx contract:
//
//   - PPS(path)        sync-producer pool (path#configKey supported)
//   - PPC(path)        async-producer pool
//   - Reload(path)     re-parse the YAML, atomically swap, flush+close old
//   - Shutdown(ctx)    gracefully flush+close every pooled producer
//   - HealthCheck()    metadata-probe every pooled producer
//
// All public functions are safe for concurrent use. The package does not
// depend on cachex/observability; observability can subscribe to the gauge
// snapshot exposed by Gauges().
package kafkax
