package cachex

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockCache is a mock implementation of Cache for compile-time verification.
type mockCache struct {
	getErr   error
	setErr   error
	closeErr error
	pingErr  error
}

func (m *mockCache) Get(ctx context.Context, key string) ([]byte, error) {
	return nil, m.getErr
}
func (m *mockCache) Set(ctx context.Context, key string, value []byte) error { return m.setErr }
func (m *mockCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	return m.setErr
}
func (m *mockCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	return false, m.setErr
}
func (m *mockCache) Delete(ctx context.Context, keys ...string) (int64, error) { return 0, nil }
func (m *mockCache) Exists(ctx context.Context, keys ...string) (int64, error) { return 0, nil }
func (m *mockCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	return false, nil
}
func (m *mockCache) TTL(ctx context.Context, key string) (int64, error)         { return 0, nil }
func (m *mockCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) { return nil, nil }
func (m *mockCache) MSet(ctx context.Context, kvs map[string][]byte) error      { return nil }
func (m *mockCache) Keys(ctx context.Context, pattern string) ([]string, error) { return nil, nil }
func (m *mockCache) Incr(ctx context.Context, key string) (int64, error)        { return 0, nil }
func (m *mockCache) Decr(ctx context.Context, key string) (int64, error)        { return 0, nil }
func (m *mockCache) Ping(ctx context.Context) error                             { return m.pingErr }
func (m *mockCache) Close() error                                               { return m.closeErr }
func (m *mockCache) Stats() Stats {
	return &mockStats{}
}

type mockStats struct{}

func (s *mockStats) Hits() int64    { return 0 }
func (s *mockStats) Misses() int64  { return 0 }
func (s *mockStats) Errors() int64  { return 0 }
func (s *mockStats) Latency() int64 { return 0 }

// Compile-time interface check
var _ Cache = (*mockCache)(nil)

// TestCacheInterfaceCompliance verifies that mockCache implements all methods.
func TestCacheInterfaceCompliance(t *testing.T) {
	cache := &mockCache{}
	ctx := context.Background()

	// Verify all methods exist
	_ = cache.Get
	_ = cache.Set
	_ = cache.SetEX
	_ = cache.SetNX
	_ = cache.Delete
	_ = cache.Exists
	_ = cache.Expire
	_ = cache.TTL
	_ = cache.MGet
	_ = cache.MSet
	_ = cache.Keys
	_ = cache.Incr
	_ = cache.Decr
	_ = cache.Ping
	_ = cache.Close()

	// Verify Ping works
	err := cache.Ping(ctx)
	assert.NoError(t, err)

	_ = ctx
}

// TestBackendConstants tests backend type constants.
func TestBackendConstants(t *testing.T) {
	backends := map[string]string{
		BackendRedis:     "redis",
		BackendDragonfly: "dragonfly",
		BackendKeyDB:     "keydb",
		BackendGarnet:    "garnet",
		BackendBadger:    "badger",
		BackendBBolt:     "bbolt",
		BackendPebble:    "pebble",
	}

	for name, expected := range backends {
		assert.Equal(t, expected, name, "Backend constant mismatch for %s", name)
	}
}

// TestFactoryCreation tests factory creation with manually registered backends.
func TestFactoryCreation(t *testing.T) {
	tests := []struct {
		name    string
		backend string
		cfg     *Config
		wantErr bool
		errType error
	}{
		{
			name:    "Unknown backend",
			backend: "unknown",
			cfg:     &Config{Backend: "unknown"},
			wantErr: true,
			errType: ErrUnknownBackend,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := NewFactory()
			cache, err := factory.Create(tt.backend, tt.cfg)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
				if cache != nil {
					cache.Close()
				}
			} else {
				assert.NoError(t, err)
				if cache != nil {
					cache.Close()
				}
			}
		})
	}
}

// TestOpenFunction tests the global Open function.
func TestOpenFunction(t *testing.T) {
	tests := []struct {
		name    string
		backend string
		cfg     *Config
		wantErr bool
	}{
		{
			name:    "Redis without Addrs",
			backend: BackendRedis,
			cfg:     &Config{Backend: BackendRedis},
			wantErr: true,
		},
		{
			name:    "Unknown backend",
			backend: "unknown",
			cfg:     &Config{Backend: "unknown"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := Open(tt.backend, tt.cfg)

			if tt.wantErr {
				assert.Error(t, err)
				if cache != nil {
					cache.Close()
				}
			}
		})
	}
}

// TestSingletonOperations tests C() and Reset() functions.
func TestSingletonOperations(t *testing.T) {
	Reset()

	// Test GetSingleton with no singleton
	cache, ok := GetSingleton(BackendRedis)
	assert.False(t, ok)
	assert.Nil(t, cache)

	// Test that C returns error for invalid config
	_, err := C(BackendRedis, &Config{Backend: BackendRedis})
	assert.Error(t, err)

	// Test Reset clears state
	Reset()
}

// TestListBackends tests the ListBackends function.
func TestListBackends(t *testing.T) {
	factory := NewFactory()
	backends := factory.ListBackends()

	// Factory starts with no registered backends
	assert.Empty(t, backends)

	// Test that custom registration works
	err := factory.Register("custom", &mockCreator{})
	require.NoError(t, err)

	backends = factory.ListBackends()
	assert.Len(t, backends, 1)
	assert.Equal(t, "custom", backends[0].Name)

	// Test duplicate registration
	err = factory.Register("custom", &mockCreator{})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrBackendAlreadyRegistered)
}

// TestObserver tests the Observer interface.
func TestObserver(t *testing.T) {
	var observedOps []string
	var observedErrors []error

	observer := &testObserver{
		onOperation: func(ctx context.Context, op string, backend string, err error, duration time.Duration) {
			observedOps = append(observedOps, op)
		},
		onError: func(ctx context.Context, op string, backend string, err error) {
			observedErrors = append(observedErrors, err)
		},
	}

	factory := NewFactory()
	factory.AddObserver(observer)
	factory.AddObserver(observer)

	// Create a custom backend
	err := factory.Register("test", &mockCreator{})
	require.NoError(t, err)

	assert.Len(t, observedOps, 0)
}

// TestConfigValidation tests configuration validation.
func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
		err     error
	}{
		{
			name:    "Redis without Addrs",
			cfg:     &Config{Backend: BackendRedis},
			wantErr: true,
			err:     ErrAddrsRequired,
		},
		{
			name:    "Badger without Dir",
			cfg:     &Config{Backend: BackendBadger, InMemory: false},
			wantErr: true,
			err:     ErrDirRequired,
		},
		{
			name:    "BBolt without Dir",
			cfg:     &Config{Backend: BackendBBolt},
			wantErr: true,
			err:     ErrDirRequired,
		},
		{
			name:    "Pebble without Dir",
			cfg:     &Config{Backend: BackendPebble},
			wantErr: true,
			err:     ErrDirRequired,
		},
		{
			name:    "Unknown backend",
			cfg:     &Config{Backend: "unknown"},
			wantErr: true,
			err:     ErrUnknownBackend,
		},
		{
			name: "Valid Redis config",
			cfg: &Config{
				Backend: BackendRedis,
				Addrs:   []string{"localhost:6379"},
			},
			wantErr: false,
		},
		{
			name: "Valid Badger in-memory",
			cfg: &Config{
				Backend:  BackendBadger,
				InMemory: true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.err != nil {
					assert.ErrorIs(t, err, tt.err)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestHealthCheck tests health check functionality.
func TestHealthCheck(t *testing.T) {
	cache := &mockCache{}
	ctx := context.Background()

	// Should succeed with working cache
	err := HealthCheck(ctx, cache)
	assert.NoError(t, err)

	// Should fail with broken cache
	cache.pingErr = ErrConnectionFailed
	err = HealthCheck(ctx, cache)
	assert.Error(t, err)
}

// TestContextHelpers tests context helper functions.
func TestContextHelpers(t *testing.T) {
	parent := context.Background()

	// Test WithTimeout
	ctx, cancel := WithTimeout(parent, 5)
	defer cancel()
	assert.NotNil(t, ctx)
	assert.NotNil(t, cancel)

	// Test WithDeadline
	deadline := time.Now().Add(10 * time.Second)
	ctx2, cancel2 := WithDeadline(parent, deadline)
	defer cancel2()
	assert.NotNil(t, ctx2)
	assert.NotNil(t, cancel2)
}

// mockCreator implements Creator for testing.
type mockCreator struct{}

func (c *mockCreator) Create(cfg *Config) (Cache, error) {
	return &mockCache{}, nil
}

// testObserver implements Observer for testing.
type testObserver struct {
	onOperation func(ctx context.Context, op string, backend string, err error, duration time.Duration)
	onError     func(ctx context.Context, op string, backend string, err error)
}

func (o *testObserver) OnOperation(ctx context.Context, op string, backend string, err error, duration time.Duration) {
	if o.onOperation != nil {
		o.onOperation(ctx, op, backend, err, duration)
	}
}

func (o *testObserver) OnError(ctx context.Context, op string, backend string, err error) {
	if o.onError != nil {
		o.onError(ctx, op, backend, err)
	}
}
