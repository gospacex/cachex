package cachex

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	tests := []struct {
		backend string
		check   func(t *testing.T, cfg *Config)
	}{
		{
			backend: BackendRedis,
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "redis", cfg.Backend)
				assert.Equal(t, "redis", cfg.Driver)
				assert.Contains(t, cfg.Addrs, "localhost:6379")
				assert.Equal(t, 10, cfg.PoolSize)
				assert.Equal(t, 5, cfg.MinIdleConns)
				assert.Equal(t, 3, cfg.MaxRetries)
			},
		},
		{
			backend: BackendBadger,
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "badger", cfg.Backend)
				assert.NotEmpty(t, cfg.Dir)
				assert.Equal(t, int64(256*1024*1024), cfg.BlockCacheSize)
				assert.NotNil(t, cfg.CircuitBreaker)
			},
		},
		{
			backend: BackendBBolt,
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "bbolt", cfg.Backend)
				assert.Equal(t, "cachex", cfg.BucketName)
				assert.True(t, cfg.SyncWrites)
			},
		},
		{
			backend: BackendPebble,
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "pebble", cfg.Backend)
				assert.True(t, cfg.Compression)
				assert.Equal(t, int64(64*1024*1024), cfg.BlockCacheSize)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.backend, func(t *testing.T) {
			cfg := DefaultConfig(tt.backend)
			tt.check(t, cfg)
		})
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name: "valid redis",
			cfg: &Config{
				Backend: BackendRedis,
				Addrs:   []string{"localhost:6379"},
			},
			wantErr: false,
		},
		{
			name: "valid redis cluster",
			cfg: &Config{
				Backend:     BackendRedis,
				Addrs:       []string{"localhost:6379", "localhost:6378"},
				ClusterMode: true,
			},
			wantErr: false,
		},
		{
			name: "redis missing addrs",
			cfg: &Config{
				Backend: BackendRedis,
			},
			wantErr: true,
		},
		{
			name: "badger in memory",
			cfg: &Config{
				Backend:  BackendBadger,
				InMemory: true,
			},
			wantErr: false,
		},
		{
			name: "badger missing dir",
			cfg: &Config{
				Backend:  BackendBadger,
				InMemory: false,
			},
			wantErr: true,
		},
		{
			name: "empty backend",
			cfg: &Config{
				Backend: "",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigToTimeout(t *testing.T) {
	cfg := &Config{}

	assert.Equal(t, 0.0, cfg.ToTimeout(0).Seconds())
	assert.Equal(t, 5.0, cfg.ToTimeout(5).Seconds())
	assert.Equal(t, 30.0, cfg.ToTimeout(30).Seconds())
}

func TestConfigClone(t *testing.T) {
	cfg := &Config{
		Backend:  BackendRedis,
		Addrs:    []string{"localhost:6379"},
		Password: "secret",
		PoolSize: 20,
		TLS: TLSConfig{
			Enabled:  true,
			CAFile:   "/path/to/ca.crt",
			CertFile: "/path/to/cert.crt",
			KeyFile:  "/path/to/key.key",
		},
		CircuitBreaker: &CircuitBreakerConfig{
			Enabled:   true,
			Threshold: 5,
			Timeout:   30,
		},
		Logger: &LoggerConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
	}

	clone := cfg.Clone()

	// Verify values are copied
	assert.Equal(t, cfg.Backend, clone.Backend)
	assert.Equal(t, cfg.Addrs, clone.Addrs)
	assert.Equal(t, cfg.Password, clone.Password)
	assert.Equal(t, cfg.PoolSize, clone.PoolSize)
	assert.Equal(t, cfg.TLS.Enabled, clone.TLS.Enabled)
	assert.Equal(t, cfg.TLS.CAFile, clone.TLS.CAFile)
	assert.Equal(t, cfg.CircuitBreaker.Threshold, clone.CircuitBreaker.Threshold)
	assert.Equal(t, cfg.Logger.Level, clone.Logger.Level)

	// Modify clone shouldn't affect original
	clone.Addrs[0] = "localhost:9999"
	assert.Equal(t, "localhost:6379", cfg.Addrs[0])

	clone.Password = "newsecret"
	assert.Equal(t, "secret", cfg.Password)
}

func TestConfigMergeWithEnv(t *testing.T) {
	// Set environment variables
	os.Setenv("CACHEX_ADDRS", "env-host:6379")
	os.Setenv("CACHEX_PASSWORD", "env-password")
	os.Setenv("CACHEX_DB", "5")
	os.Setenv("CACHEX_POOL_SIZE", "50")
	os.Setenv("CACHEX_TLS_ENABLED", "true")
	os.Setenv("CACHEX_TLS_CA_FILE", "/env/ca.crt")
	os.Setenv("CACHEX_DIR", "/env/dir")

	defer func() {
		os.Unsetenv("CACHEX_ADDRS")
		os.Unsetenv("CACHEX_PASSWORD")
		os.Unsetenv("CACHEX_DB")
		os.Unsetenv("CACHEX_POOL_SIZE")
		os.Unsetenv("CACHEX_TLS_ENABLED")
		os.Unsetenv("CACHEX_TLS_CA_FILE")
		os.Unsetenv("CACHEX_DIR")
	}()

	cfg := DefaultConfig(BackendRedis)
	cfg.MergeWithEnv()

	assert.Equal(t, []string{"env-host:6379"}, cfg.Addrs)
	assert.Equal(t, "env-password", cfg.Password)
	assert.Equal(t, 5, cfg.DB)
	assert.Equal(t, 50, cfg.PoolSize)
	assert.True(t, cfg.TLS.Enabled)
	assert.Equal(t, "/env/ca.crt", cfg.TLS.CAFile)
	assert.Equal(t, "/env/dir", cfg.Dir)
}

func TestTLSConfig(t *testing.T) {
	cfg := &TLSConfig{
		Enabled:            true,
		CAFile:             "/path/to/ca.crt",
		CertFile:           "/path/to/cert.crt",
		KeyFile:            "/path/to/key.key",
		InsecureSkipVerify: false,
	}

	assert.True(t, cfg.Enabled)
	assert.Equal(t, "/path/to/ca.crt", cfg.CAFile)
	assert.Equal(t, "/path/to/cert.crt", cfg.CertFile)
	assert.Equal(t, "/path/to/key.key", cfg.KeyFile)
	assert.False(t, cfg.InsecureSkipVerify)
}

func TestCircuitBreakerConfig(t *testing.T) {
	cfg := &CircuitBreakerConfig{
		Enabled:             true,
		Threshold:           5,
		Timeout:             30,
		HalfOpenMaxRequests: 3,
	}

	assert.True(t, cfg.Enabled)
	assert.Equal(t, 5, cfg.Threshold)
	assert.Equal(t, 30, cfg.Timeout)
	assert.Equal(t, 3, cfg.HalfOpenMaxRequests)
}

func TestLoggerConfig(t *testing.T) {
	cfg := &LoggerConfig{
		Level:  "debug",
		Format: "text",
		Output: "stderr",
	}

	assert.Equal(t, "debug", cfg.Level)
	assert.Equal(t, "text", cfg.Format)
	assert.Equal(t, "stderr", cfg.Output)
}
