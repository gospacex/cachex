package cachex

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetTracingConfigAndIsTracingEnabled(t *testing.T) {
	// Reset global state before test
	setTracingConfigForTest(TracingConfig{Enabled: false})
	defer setTracingConfigForTest(TracingConfig{Enabled: false})

	assert.False(t, IsTracingEnabled())

	SetTracingConfig(TracingConfig{Enabled: true})
	assert.True(t, IsTracingEnabled())

	SetTracingConfig(TracingConfig{Enabled: false})
	assert.False(t, IsTracingEnabled())
}

func TestSetTracingConfigConcurrent(t *testing.T) {
	setTracingConfigForTest(TracingConfig{Enabled: false})
	defer setTracingConfigForTest(TracingConfig{Enabled: false})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			SetTracingConfig(TracingConfig{Enabled: true})
		}()
		go func() {
			defer wg.Done()
			_ = IsTracingEnabled()
		}()
	}
	wg.Wait()
}

func TestNewCircuitBreakerInitialState(t *testing.T) {
	cb := NewCircuitBreaker("test-cb")
	assert.Equal(t, 0, cb.State()) // Closed
}

func TestCircuitBreakerExecuteSuccess(t *testing.T) {
	cb := NewCircuitBreaker("success-cb")
	err := cb.Execute(nil, func() error {
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, cb.State())
}

func TestCircuitBreakerExecuteFails(t *testing.T) {
	cb := NewCircuitBreaker("fail-cb")
	expectedErr := assert.AnError
	err := cb.Execute(nil, func() error {
		return expectedErr
	})
	require.ErrorIs(t, err, expectedErr)
	// State may have advanced but should still be closed after single failure (threshold=5)
	assert.Equal(t, 0, cb.State())
}

func TestExpandConfigPasswordBasic(t *testing.T) {
	setTracingConfigForTest(TracingConfig{Enabled: false})
	defer setTracingConfigForTest(TracingConfig{Enabled: false})

	cfg := &InstanceConfig{
		Name:     "test",
		Password: "secret",
	}
	err := expandConfigPassword(cfg)
	require.NoError(t, err)
	assert.Equal(t, "secret", cfg.Password)
}

func TestExpandConfigPasswordWithEnvVar(t *testing.T) {
	setTracingConfigForTest(TracingConfig{Enabled: false})
	defer setTracingConfigForTest(TracingConfig{Enabled: false})

	t.Setenv("TEST_PASS_EXPAND", "resolved_secret")
	cfg := &InstanceConfig{
		Name:     "test",
		Password: "${TEST_PASS_EXPAND}",
	}
	err := expandConfigPassword(cfg)
	require.NoError(t, err)
	assert.Equal(t, "resolved_secret", cfg.Password)
}

func TestExpandConfigPasswordUnresolved(t *testing.T) {
	setTracingConfigForTest(TracingConfig{Enabled: false})
	defer setTracingConfigForTest(TracingConfig{Enabled: false})

	cfg := &InstanceConfig{
		Name:     "test",
		Password: "${NONEXISTENT_VAR_xyz}",
	}
	err := expandConfigPassword(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unresolved env var")
}

func TestExpandConfigPasswordMixed(t *testing.T) {
	setTracingConfigForTest(TracingConfig{Enabled: false})
	defer setTracingConfigForTest(TracingConfig{Enabled: false})

	t.Setenv("TEST_PW_MIXED", "mid")
	cfg := &InstanceConfig{
		Name:     "test",
		Password: "prefix-${TEST_PW_MIXED}-suffix",
	}
	err := expandConfigPassword(cfg)
	require.NoError(t, err)
	assert.Equal(t, "prefix-mid-suffix", cfg.Password)
}

func TestFillDefaults(t *testing.T) {
	c := &Config{Name: "test", Driver: "redis"}
	c.FillDefaults()
	assert.Equal(t, 5*time.Second, c.Timeout)
	assert.Equal(t, 5*time.Second, c.ReadTimeout)
	assert.Equal(t, 5*time.Second, c.WriteTimeout)
	assert.Equal(t, 10, c.PoolSize)
	assert.Equal(t, 3, c.RetryCount)
}

func TestFillDefaultsPreservesSetValues(t *testing.T) {
	c := &Config{
		Name:        "test",
		Driver:      "redis",
		Timeout:     2 * time.Second,
		ReadTimeout: 1 * time.Second,
		PoolSize:    20,
		RetryCount:  5,
	}
	c.FillDefaults()
	assert.Equal(t, 2*time.Second, c.Timeout)
	assert.Equal(t, 1*time.Second, c.ReadTimeout)
	assert.Equal(t, 2*time.Second, c.WriteTimeout)
	assert.Equal(t, 20, c.PoolSize)
	assert.Equal(t, 5, c.RetryCount)
}

func TestRecordCommandNoPanic(t *testing.T) {
	RecordCommand("redis", "test", "GET", "success", 10*time.Millisecond)
	RecordCommand("redis", "test", "SET", "error", 5*time.Millisecond)
}

func TestUpdateCircuitBreakerStateNoPanic(t *testing.T) {
	UpdateCircuitBreakerState("redis", "test", 0)
	UpdateCircuitBreakerState("redis", "test", 1)
	UpdateCircuitBreakerState("redis", "test", 2)
}

func TestUpdatePoolMetricsNoPanic(t *testing.T) {
	UpdatePoolMetrics("redis", "test", 5, 3)
}

// helper: directly mutate the tracing config var for test isolation
func setTracingConfigForTest(cfg TracingConfig) {
	tracingCfgMu.Lock()
	defer tracingCfgMu.Unlock()
	tracingCfg = cfg
}
