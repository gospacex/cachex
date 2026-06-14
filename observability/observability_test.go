package observability

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/cachex/observability/metrics"
	"github.com/stretchr/testify/assert"
)

func TestMetricsCollector(t *testing.T) {
	m := metrics.NewCollector("cachex", "collector1")

	ctx := context.Background()

	// Test ObserveOperation
	m.ObserveOperation(ctx, cachex.OpGet, "redis", nil, 10*time.Millisecond)
	m.ObserveOperation(ctx, cachex.OpGet, "redis", nil, 20*time.Millisecond)
	m.ObserveOperation(ctx, cachex.OpSet, "redis", nil, 5*time.Millisecond)
	m.ObserveOperation(ctx, cachex.OpGet, "redis", assert.AnError, 15*time.Millisecond)

	// Verify metrics were collected (operationsTotal counter incremented)
	// Note: We can't access unexported fields directly, but we can verify
	// that ObserveOperation doesn't panic and completes successfully
}

func TestMetricsCollectorObserveGet(t *testing.T) {
	m := metrics.NewCollector("cachex", "collector2")

	ctx := context.Background()

	// Test hit
	m.ObserveGet(ctx, "redis", true, nil, 5*time.Millisecond)

	// Test miss
	m.ObserveGet(ctx, "redis", false, nil, 3*time.Millisecond)

	// Test error
	m.ObserveGet(ctx, "redis", false, assert.AnError, 10*time.Millisecond)
}

func TestMetricsCollectorOnOperation(t *testing.T) {
	m := metrics.NewCollector("cachex", "collector3")

	ctx := context.Background()

	m.OnOperation(ctx, cachex.OpGet, "redis", nil, 10*time.Millisecond)
	m.OnError(ctx, cachex.OpGet, "redis", assert.AnError)
}

func TestLatencyRecorder(t *testing.T) {
	recorder := &metrics.LatencyRecorder{}

	recorder.RecordHit()
	recorder.RecordHit()
	recorder.RecordMiss()
	recorder.RecordHit()

	hits, misses, total := recorder.Stats()
	assert.Equal(t, int64(3), hits)
	assert.Equal(t, int64(1), misses)
	assert.Equal(t, int64(4), total)

	rate := recorder.HitRate()
	assert.Equal(t, 0.75, rate)

	emptyRecorder := &metrics.LatencyRecorder{}
	assert.Equal(t, 0.0, emptyRecorder.HitRate())
}

func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker("test",
		WithThreshold(3),
		WithTimeout(time.Second),
		WithHalfOpenMaxRequests(2),
	)

	ctx := context.Background()

	assert.Equal(t, StateClosed, cb.State())

	err := cb.Acquire(ctx)
	assert.NoError(t, err)

	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, 2, cb.Failures())
	assert.Equal(t, StateClosed, cb.State())

	cb.RecordFailure()
	assert.Equal(t, StateOpen, cb.State())

	err = cb.Acquire(ctx)
	assert.ErrorIs(t, err, cachex.ErrCircuitOpen)

	time.Sleep(time.Second + 100*time.Millisecond)

	cb.Reset()
	assert.Equal(t, StateClosed, cb.State())
	assert.Equal(t, 0, cb.Failures())
}

func TestCircuitBreakerExecute(t *testing.T) {
	cb := NewCircuitBreaker("test",
		WithThreshold(3),
		WithTimeout(time.Second),
	)

	ctx := context.Background()
	callCount := 0

	// First call - success
	err := cb.Execute(ctx, func() error {
		callCount++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)

	// Second call - failure (first failure)
	err = cb.Execute(ctx, func() error {
		callCount++
		return assert.AnError
	})
	assert.Error(t, err)
	assert.Equal(t, 2, callCount)

	// Third call - failure (second failure, threshold is 3, so still not open)
	err = cb.Execute(ctx, func() error {
		callCount++
		return assert.AnError
	})
	assert.Error(t, err)
	assert.Equal(t, 3, callCount)
	// After 2 failures with threshold=3, circuit should be closed still
	assert.Equal(t, StateClosed, cb.State())

	// Fourth call - failure (third failure, now circuit opens)
	err = cb.Execute(ctx, func() error {
		callCount++
		return assert.AnError
	})
	assert.Error(t, err)
	assert.Equal(t, 4, callCount)
	assert.Equal(t, StateOpen, cb.State())
}

func TestCircuitBreakerMetrics(t *testing.T) {
	cb := NewCircuitBreaker("test",
		WithThreshold(3),
	)

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess()

	metrics := cb.Metrics()
	assert.Equal(t, "test", metrics.Name)
	assert.Equal(t, "closed", metrics.State)
	assert.Equal(t, 0, metrics.Failures) // Reset by success in closed state
	assert.NotEmpty(t, metrics.LastFailAt)
}

func TestCircuitBreakerFromConfig(t *testing.T) {
	cfg := &cachex.CircuitBreakerConfig{
		Enabled:             true,
		Threshold:           5,
		Timeout:             30,
		HalfOpenMaxRequests: 3,
	}

	cb := CircuitBreakerFromConfig("test", cfg)
	assert.NotNil(t, cb)

	disabledCfg := &cachex.CircuitBreakerConfig{
		Enabled: false,
	}
	cb = CircuitBreakerFromConfig("test", disabledCfg)
	assert.Nil(t, cb)

	cb = CircuitBreakerFromConfig("test", nil)
	assert.Nil(t, cb)
}

func TestIsCircuitOpenError(t *testing.T) {
	assert.True(t, IsCircuitOpenError(cachex.ErrCircuitOpen))
	assert.False(t, IsCircuitOpenError(assert.AnError))
}

func TestLogger(t *testing.T) {
	logger := NewLogger(
		WithLevel(LevelDebug),
		WithFormat("json"),
	)

	ctx := context.Background()

	logger.Debug(ctx, "debug message", map[string]interface{}{"key": "value"})
	logger.Info(ctx, "info message", map[string]interface{}{"key": "value"})
	logger.Warn(ctx, "warn message", map[string]interface{}{"key": "value"})
	logger.Error(ctx, "error message", map[string]interface{}{"key": "value"})
}

func TestLoggerSetLevel(t *testing.T) {
	logger := NewLogger(WithLevel(LevelInfo))
	logger.SetLevel(LevelError)
}

func TestLoggingObserver(t *testing.T) {
	logger := NewLogger(WithLevel(LevelDebug))
	observer := NewLoggingObserver(logger)

	ctx := context.Background()

	// Redirect stdout for testing
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	observer.OnOperation(ctx, "get", "redis", nil, time.Millisecond)
	observer.OnError(ctx, "get", "redis", assert.AnError)

	w.Close()
	os.Stdout = oldStdout
	_ = r
}

func TestCacheLogger(t *testing.T) {
	logger := NewCacheLogger("redis")
	assert.NotNil(t, logger)

	ctx := context.Background()
	logger.Info(ctx, "test message")
}
