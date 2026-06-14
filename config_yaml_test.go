package cachex

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTempYAML is a test helper that writes a YAML document to a temp file
// and returns its path. The temp dir is auto-cleaned via t.TempDir().
func writeTempYAML(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

// =============================================================================
// Task 7.11 — config_test.go cases
// =============================================================================

//  1. NewSchema_ParseOK — new `trace:` block parses into *Config.Trace
//     with the long-name enum `jaeger`, env-expanded endpoint, and the
//     13 fields from the plan spec.
func TestNewSchema_ParseOK(t *testing.T) {
	path := writeTempYAML(t, "trace_new.yaml", `
backend: redis
addrs:
  - localhost:6379
trace:
  enabled: true
  service_name: cachex-test
  exporter: jaeger
  endpoint: ${env:MQ_JAEGER_ENDPOINT:-http://localhost:14268/api/traces}
  insecure: true
  protocol: grpc
  sampler_type: always_on
  sampler_ratio: 1.0
`)
	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	require.NotNil(t, cfg.Trace, "Config.Trace must be set for new schema")
	assert.True(t, cfg.Trace.Enabled)
	assert.Equal(t, "cachex-test", cfg.Trace.ServiceName)
	assert.Equal(t, "jaeger", cfg.Trace.Exporter)
	assert.Equal(t, "http://localhost:14268/api/traces", cfg.Trace.Endpoint)
	assert.True(t, cfg.Trace.Insecure)
	assert.Equal(t, "grpc", cfg.Trace.Protocol)
	assert.Equal(t, "always_on", cfg.Trace.SamplerType)
	assert.InDelta(t, 1.0, cfg.Trace.SamplerRatio, 0.0001)
}

//  2. LegacySchema_AutoPromote — old `tracing:` block must auto-promote
//     to the new `trace:` block. Short name `redis` maps to long name
//     `redis_stream`; `kafka` maps to `kafka_topic`.
func TestLegacySchema_AutoPromote(t *testing.T) {
	path := writeTempYAML(t, "legacy.yaml", `
backend: redis
tracing:
  enabled: true
  service_name: legacy-cachex
  exporter_type: redis
  endpoint: ""
  redis_config:
    addrs:
      - localhost:6379
    channel: trace:legacy
`)
	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	require.NotNil(t, cfg.Trace, "legacy tracing block must auto-promote to Trace")
	assert.True(t, cfg.Trace.Enabled)
	assert.Equal(t, "legacy-cachex", cfg.Trace.ServiceName)
	assert.Equal(t, "redis_stream", cfg.Trace.Exporter, "short 'redis' must map to 'redis_stream'")
	assert.Equal(t, "trace:legacy", cfg.Trace.Stream)

	// Legacy KafkaConfig must auto-promote to Trace with `kafka_topic`.
	path2 := writeTempYAML(t, "legacy_kafka.yaml", `
backend: redis
tracing:
  enabled: true
  service_name: legacy-kafka
  exporter_type: kafka
  kafka_config:
    brokers:
      - localhost:9092
    topic: trace-spans-legacy
`)
	cfg2, err := LoadConfig(path2)
	require.NoError(t, err)
	require.NotNil(t, cfg2.Trace)
	assert.Equal(t, "kafka_topic", cfg2.Trace.Exporter)
	assert.Equal(t, "trace-spans-legacy", cfg2.Trace.Topic)
	assert.Equal(t, []string{"localhost:9092"}, cfg2.Trace.Brokers)
}

//  3. MixedPrecedence_TraceWins — when both `trace:` and `tracing:` are
//     present, the new `trace:` block wins and the legacy block is ignored.
func TestMixedPrecedence_TraceWins(t *testing.T) {
	path := writeTempYAML(t, "mixed.yaml", `
backend: redis
tracing:
  enabled: true
  service_name: legacy-wins
  exporter_type: jaeger
trace:
  enabled: true
  service_name: new-wins
  exporter: redis_stream
  stream: trace:precedence
`)
	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	require.NotNil(t, cfg.Trace)
	assert.Equal(t, "new-wins", cfg.Trace.ServiceName, "trace: must win over tracing:")
	assert.Equal(t, "redis_stream", cfg.Trace.Exporter)
	assert.Equal(t, "trace:precedence", cfg.Trace.Stream)
}

// 4. EnvExpansion_Var — ${env:HOME} (when set) expands to its value.
func TestEnvExpansion_Var(t *testing.T) {
	const homeEnv = "CACHEX_TEST_ENV_EXPANSION_VAR"
	t.Setenv(homeEnv, "/var/log/cachex")

	path := writeTempYAML(t, "env_var.yaml", `
backend: redis
trace:
  enabled: true
  service_name: cachex-env
  exporter: otlp
  endpoint: ${env:`+homeEnv+`}
`)
	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	require.NotNil(t, cfg.Trace)
	assert.Equal(t, "/var/log/cachex", cfg.Trace.Endpoint)
}

//  5. EnvExpansion_Default — ${env:UNSET:-fallback} expands to fallback
//     when the env var is unset or empty.
func TestEnvExpansion_Default(t *testing.T) {
	const unsetEnv = "CACHEX_TEST_ENV_EXPANSION_UNSET_T7_11"
	require.NoError(t, os.Unsetenv(unsetEnv))

	path := writeTempYAML(t, "env_default.yaml", `
backend: redis
trace:
  enabled: true
  service_name: cachex-env-default
  exporter: otlp
  endpoint: ${env:`+unsetEnv+`:-http://collector:4318}
`)
	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	require.NotNil(t, cfg.Trace)
	assert.Equal(t, "http://collector:4318", cfg.Trace.Endpoint)
}

//  6. UnknownBackend_Rejected — `exporter: foo` returns a validation
//     error with a clear message that names the offending field.
func TestUnknownBackend_Rejected(t *testing.T) {
	path := writeTempYAML(t, "unknown_exporter.yaml", `
backend: redis
trace:
  enabled: true
  service_name: cachex-bad
  exporter: foo
`)
	_, err := LoadConfig(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "trace.exporter")
	assert.Contains(t, err.Error(), "foo")
}

//  7. ShortName_AutoMap — for the new `trace:` block, the short names
//     `redis` and `kafka` must auto-map to the long names `redis_stream`
//     and `kafka_topic`. Other invalid values are rejected.
func TestShortName_AutoMap(t *testing.T) {
	// `redis` → `redis_stream`
	path := writeTempYAML(t, "short_redis.yaml", `
trace:
  enabled: true
  service_name: cachex-short
  exporter: redis
  stream: trace:short
`)
	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	require.NotNil(t, cfg.Trace)
	assert.Equal(t, "redis_stream", cfg.Trace.Exporter)

	// `kafka` → `kafka_topic`
	path2 := writeTempYAML(t, "short_kafka.yaml", `
trace:
  enabled: true
  service_name: cachex-short
  exporter: kafka
  topic: trace:short
  brokers: [localhost:9092]
`)
	cfg2, err := LoadConfig(path2)
	require.NoError(t, err)
	require.NotNil(t, cfg2.Trace)
	assert.Equal(t, "kafka_topic", cfg2.Trace.Exporter)

	// Invalid short/long name → error
	path3 := writeTempYAML(t, "short_zipkin.yaml", `
trace:
  enabled: true
  service_name: cachex-bad
  exporter: zipkin
`)
	_, err = LoadConfig(path3)
	require.Error(t, err)
}
