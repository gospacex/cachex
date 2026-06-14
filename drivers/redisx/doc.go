// Package redisx is the top-level Redis driver for cachex.
//
// It manages shared *redis.Client and *redis.ClusterClient instances keyed by
// a content-aware fingerprint (see utils.ConfigFingerprint) so that callers
// asking for the same configuration reuse the same underlying pool, while a
// configuration change produces a fresh pool with the old one scheduled for
// delayed shutdown.
//
// Five primary entry points mirror the mqx contract:
//
//   - PPS(path)        single-instance client pool (path#configKey supported)
//   - PPC(path)        cluster client pool
//   - Reload(path)     re-parse the YAML, atomically swap, close old after grace
//   - Shutdown(ctx)    gracefully close every pooled client
//   - HealthCheck()    PING every pooled client, report status per cache key
//
// All public functions are safe for concurrent use. The package does not
// depend on cachex/observability; observability can subscribe to the gauge
// snapshot exposed by Gauges().
package redisx
