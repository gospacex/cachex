package kafkax

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSplitPath verifies the path#configKey syntax.
func TestSplitPath(t *testing.T) {
	t.Run("plain path returns empty configKey", func(t *testing.T) {
		file, key := splitPath("/etc/cachex/kafka.yaml")
		assert.Equal(t, "/etc/cachex/kafka.yaml", file)
		assert.Equal(t, "", key)
	})

	t.Run("hash splits file from configKey", func(t *testing.T) {
		file, key := splitPath("/etc/cachex/kafka.yaml#orders")
		assert.Equal(t, "/etc/cachex/kafka.yaml", file)
		assert.Equal(t, "orders", key)
	})
}

// TestParseFile covers single-section, multi-section, and env expansion paths.
func TestParseFile(t *testing.T) {
	t.Run("single section", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "kafka.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
brokers:
  - localhost:9092
topic: orders
batch_size: 1024
`), 0o600))

		cfg, key, err := parseFileFromDisk(path)
		require.NoError(t, err)
		assert.Equal(t, "default", key)
		assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
		assert.Equal(t, "orders", cfg.Topic)
		assert.Equal(t, 1024, cfg.BatchSize)
	})

	t.Run("multi section with configKey selector", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "kafka.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
orders:
  brokers:
    - orders-kafka:9092
  topic: orders-topic
audit:
  brokers:
    - audit-kafka:9092
  topic: audit-topic
`), 0o600))

		cfg, key, err := parseFileFromDisk(path + "#orders")
		require.NoError(t, err)
		assert.Equal(t, "orders", key)
		assert.Equal(t, []string{"orders-kafka:9092"}, cfg.Brokers)
		assert.Equal(t, "orders-topic", cfg.Topic)
	})

	t.Run("env expansion via ExpandEnvVars", func(t *testing.T) {
		t.Setenv("KAFKA_HOST", "10.0.0.1")
		t.Setenv("KAFKA_PORT", "9094")

		dir := t.TempDir()
		path := filepath.Join(dir, "kafka.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
brokers:
  - ${env:KAFKA_HOST}:${env:KAFKA_PORT}
topic: events
`), 0o600))

		cfg, _, err := parseFileFromDisk(path)
		require.NoError(t, err)
		assert.Equal(t, []string{"10.0.0.1:9094"}, cfg.Brokers)
	})

	t.Run("missing file returns error", func(t *testing.T) {
		_, _, err := parseFileFromDisk(filepath.Join(t.TempDir(), "nope.yaml"))
		require.Error(t, err)
	})

	t.Run("multi section with unknown configKey returns error", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "kafka.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
orders:
  brokers:
    - localhost:9092
  topic: orders
`), 0o600))

		_, _, err := parseFileFromDisk(path + "#missing")
		require.Error(t, err)
	})
}

// TestCacheKey_Stability ensures cache key generation is order-independent
// and reflects field changes.
func TestCacheKey_Stability(t *testing.T) {
	t.Run("same config produces same cache key", func(t *testing.T) {
		cfg1 := &Config{Brokers: []string{"a:1"}, Topic: "x", BatchSize: 100}
		cfg2 := &Config{Brokers: []string{"a:1"}, Topic: "x", BatchSize: 100}
		assert.Equal(t, buildCacheKey("single", "main", cfg1), buildCacheKey("single", "main", cfg2))
	})

	t.Run("batch size change produces different cache key", func(t *testing.T) {
		cfg1 := &Config{Brokers: []string{"a:1"}, BatchSize: 100}
		cfg2 := &Config{Brokers: []string{"a:1"}, BatchSize: 200}
		assert.NotEqual(t, buildCacheKey("single", "main", cfg1), buildCacheKey("single", "main", cfg2))
	})

	t.Run("cluster mode produces different cache key than single", func(t *testing.T) {
		cfg := &Config{Brokers: []string{"a:1"}}
		assert.NotEqual(t, buildCacheKey("single", "k", cfg), buildCacheKey("cluster", "k", cfg))
	})

	t.Run("different configKey produces different cache key", func(t *testing.T) {
		cfg := &Config{Brokers: []string{"a:1"}}
		assert.NotEqual(t, buildCacheKey("single", "orders", cfg), buildCacheKey("single", "audit", cfg))
	})

	t.Run("Fingerprint is 64-char lowercase hex", func(t *testing.T) {
		cfg := &Config{Brokers: []string{"a:1"}}
		ck := buildCacheKey("single", "k", cfg)
		// strip "producer:single:k:" prefix and assert the rest is 64-char hex
		const prefix = "producer:single:k:"
		require.True(t, len(ck) > len(prefix))
		hex := ck[len(prefix):]
		assert.Len(t, hex, 64)
		assert.Regexp(t, `^[0-9a-f]{64}$`, hex)
	})
}

// TestShutdownEmpty verifies Shutdown is safe when no clients have been created.
func TestShutdownEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	// Should not panic, should not block forever.
	Shutdown(ctx)
}

// TestHealthCheckEmpty verifies HealthCheck returns a non-nil map.
func TestHealthCheckEmpty(t *testing.T) {
	result := HealthCheck()
	assert.NotNil(t, result)
}

// TestReload_MissingFile verifies Reload returns an error for a missing path.
func TestReload_MissingFile(t *testing.T) {
	err := Reload(filepath.Join(t.TempDir(), "nope.yaml"))
	require.Error(t, err)
}

// TestGauges_EmptyByDefault verifies the gauge snapshot is read-safe when empty.
func TestGauges_EmptyByDefault(t *testing.T) {
	snap := Gauges()
	assert.NotNil(t, snap)
}
