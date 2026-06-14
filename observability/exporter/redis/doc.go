// Package redis provides a Redis stream exporter for OpenTelemetry spans.
//
// The exporter publishes each ReadOnlySpan as a JSON-encoded record on a
// configured Redis stream via XADD, so consumers can read span batches with
// XREAD. The exporter is caller-injected: it does not own the *redis.Client
// and never calls Close on it. The caller is responsible for the client's
// lifecycle.
package redis
