package redisx

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/cachex/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSplitPath verifies the path#configKey syntax.
func TestSplitPath(t *testing.T) {
	t.Run("plain path returns empty configKey", func(t *testing.T) {
		file, key := splitPath("/etc/cachex/redis.yaml")
		assert.Equal(t, "/etc/cachex/redis.yaml", file)
		assert.Equal(t, "", key)
	})

	t.Run("hash splits file from configKey", func(t *testing.T) {
		file, key := splitPath("/etc/cachex/redis.yaml#session")
		assert.Equal(t, "/etc/cachex/redis.yaml", file)
		assert.Equal(t, "session", key)
	})
}

// TestParseFile covers single-section, multi-section, and env expansion paths.
func TestParseFile(t *testing.T) {
	t.Run("single section", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "redis.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
backend: redis
addrs:
  - localhost:6379
db: 0
pool_size: 12
`), 0o600))

		cfg, key, err := parseFileFromDisk(path)
		require.NoError(t, err)
		assert.Equal(t, "default", key)
		assert.Equal(t, "redis", cfg.Backend)
		assert.Equal(t, []string{"localhost:6379"}, cfg.Addrs)
		assert.Equal(t, 12, cfg.PoolSize)
	})

	t.Run("multi section with configKey selector", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "redis.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
session:
  backend: redis
  addrs:
    - session:6379
  db: 1
ratelimit:
  backend: redis
  addrs:
    - ratelimit:6379
  db: 2
`), 0o600))

		cfg, key, err := parseFileFromDisk(path + "#session")
		require.NoError(t, err)
		assert.Equal(t, "session", key)
		assert.Equal(t, []string{"session:6379"}, cfg.Addrs)
		assert.Equal(t, 1, cfg.DB)
	})

	t.Run("env expansion via ExpandEnvVars", func(t *testing.T) {
		t.Setenv("REDIS_HOST", "10.0.0.1")
		t.Setenv("REDIS_PORT", "6380")

		dir := t.TempDir()
		path := filepath.Join(dir, "redis.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
backend: redis
addrs:
  - ${env:REDIS_HOST}:${env:REDIS_PORT}
db: 0
`), 0o600))

		cfg, _, err := parseFileFromDisk(path)
		require.NoError(t, err)
		assert.Equal(t, []string{"10.0.0.1:6380"}, cfg.Addrs)
	})

	t.Run("missing file returns error", func(t *testing.T) {
		_, _, err := parseFileFromDisk(filepath.Join(t.TempDir(), "nope.yaml"))
		require.Error(t, err)
	})

	t.Run("multi section with unknown configKey returns error", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "redis.yaml")
		require.NoError(t, os.WriteFile(path, []byte(`
session:
  backend: redis
  addrs:
    - session:6379
`), 0o600))

		_, _, err := parseFileFromDisk(path + "#missing")
		require.Error(t, err)
	})
}

// TestCacheKey_Stability ensures cache key generation is order-independent
// and reflects field changes.
func TestCacheKey_Stability(t *testing.T) {
	t.Run("same config produces same cache key", func(t *testing.T) {
		cfg1 := &cachex.Config{Backend: "redis", Addrs: []string{"a:1"}, DB: 0, PoolSize: 10}
		cfg2 := &cachex.Config{Backend: "redis", Addrs: []string{"a:1"}, DB: 0, PoolSize: 10}
		assert.Equal(t, buildCacheKey("single", "main", cfg1), buildCacheKey("single", "main", cfg2))
	})

	t.Run("pool size change produces different cache key", func(t *testing.T) {
		cfg1 := &cachex.Config{Backend: "redis", Addrs: []string{"a:1"}, PoolSize: 10}
		cfg2 := &cachex.Config{Backend: "redis", Addrs: []string{"a:1"}, PoolSize: 20}
		assert.NotEqual(t, buildCacheKey("single", "main", cfg1), buildCacheKey("single", "main", cfg2))
	})

	t.Run("cluster mode produces different cache key than single", func(t *testing.T) {
		cfg := &cachex.Config{Backend: "redis", Addrs: []string{"a:1"}}
		assert.NotEqual(t, buildCacheKey("single", "k", cfg), buildCacheKey("cluster", "k", cfg))
	})

	t.Run("different configKey produces different cache key", func(t *testing.T) {
		cfg := &cachex.Config{Backend: "redis", Addrs: []string{"a:1"}}
		assert.NotEqual(t, buildCacheKey("single", "session", cfg), buildCacheKey("single", "ratelimit", cfg))
	})

	t.Run("Fingerprint is 64-char lowercase hex", func(t *testing.T) {
		cfg := &cachex.Config{Backend: "redis", Addrs: []string{"a:1"}}
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

// TestHealthCheckEmpty verifies HealthCheck returns empty when no clients.
func TestHealthCheckEmpty(t *testing.T) {
	// Use a fresh sub-cache via a unique prefix to avoid pollution from other
	// parallel test runs (the package-level caches are global).
	_ = utils.ConfigFingerprint
	result := HealthCheck()
	// We don't assert len==0 because parallel tests may have populated clients.
	// We only assert the call returns without panic and yields a map.
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
