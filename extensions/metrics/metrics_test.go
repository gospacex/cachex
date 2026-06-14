// Package metrics provides observability metrics for cachex.
package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gospacex/cachex"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
)

// MockCache is a mock implementation of cachex.Cache for testing.
type MockCache struct {
	mock.Mock
}

func (m *MockCache) Get(ctx context.Context, key string) ([]byte, error) {
	args := m.Called(ctx, key)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockCache) Set(ctx context.Context, key string, value []byte) error {
	args := m.Called(ctx, key, value)
	return args.Error(0)
}

func (m *MockCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	args := m.Called(ctx, key, value, ttlSeconds)
	return args.Error(0)
}

func (m *MockCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	args := m.Called(ctx, key, value, ttlSeconds)
	return args.Bool(0), args.Error(1)
}

func (m *MockCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	args := m.Called(m.appendCtx(ctx, keys)...)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	args := m.Called(m.appendCtx(ctx, keys)...)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	args := m.Called(ctx, key, ttlSeconds)
	return args.Bool(0), args.Error(1)
}

func (m *MockCache) TTL(ctx context.Context, key string) (int64, error) {
	args := m.Called(ctx, key)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	args := m.Called(m.appendCtx(ctx, keys)...)
	return args.Get(0).([][]byte), args.Error(1)
}

func (m *MockCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	args := m.Called(ctx, kvs)
	return args.Error(0)
}

func (m *MockCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	args := m.Called(ctx, pattern)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockCache) Incr(ctx context.Context, key string) (int64, error) {
	args := m.Called(ctx, key)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockCache) Decr(ctx context.Context, key string) (int64, error) {
	args := m.Called(ctx, key)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockCache) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockCache) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockCache) Stats() cachex.Stats {
	args := m.Called()
	return args.Get(0).(cachex.Stats)
}

// appendCtx helper converts a string slice to []interface{} with ctx prepended.
func (m *MockCache) appendCtx(ctx context.Context, keys []string) []interface{} {
	result := make([]interface{}, 0, len(keys)+1)
	result = append(result, ctx)
	for _, k := range keys {
		result = append(result, k)
	}
	return result
}

// MockCollector is a mock implementation of MetricsCollector for testing.
type MockCollector struct {
	mock.Mock
}

func (m *MockCollector) RecordGet(ctx context.Context, hit bool, latency time.Duration) {
	m.Called(ctx, hit, latency)
}

func (m *MockCollector) RecordSet(ctx context.Context, latency time.Duration) {
	m.Called(ctx, latency)
}

func (m *MockCollector) RecordDelete(ctx context.Context, keysDeleted int64, latency time.Duration) {
	m.Called(ctx, keysDeleted, latency)
}

func (m *MockCollector) RecordError(ctx context.Context, operation string, err error) {
	m.Called(ctx, operation, err)
}

// mockStats implements cachex.Stats for testing.
type mockStats struct{}

func (s *mockStats) Hits() int64    { return 0 }
func (s *mockStats) Misses() int64  { return 0 }
func (s *mockStats) Errors() int64  { return 0 }
func (s *mockStats) Latency() int64 { return 0 }

// TestMetricsCacheGetHit tests that Get records a hit when key is found.
func TestMetricsCacheGetHit(t *testing.T) {
	mockCache := new(MockCache)
	mockCollector := new(MockCollector)

	metricsCache := NewMetricsCache(mockCache, mockCollector)
	ctx := context.Background()

	mockCache.On("Get", ctx, "key1").Return([]byte("value1"), nil)
	mockCollector.On("RecordGet", ctx, true, mock.AnythingOfType("time.Duration")).Return()

	result, err := metricsCache.Get(ctx, "key1")

	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), result)
	mockCache.AssertExpectations(t)
	mockCollector.AssertExpectations(t)
}

// TestMetricsCacheGetMiss tests that Get records a miss when key is not found.
func TestMetricsCacheGetMiss(t *testing.T) {
	mockCache := new(MockCache)
	mockCollector := new(MockCollector)

	metricsCache := NewMetricsCache(mockCache, mockCollector)
	ctx := context.Background()

	mockCache.On("Get", ctx, "nonexistent").Return(nil, cachex.ErrKeyNotFound)
	mockCollector.On("RecordError", ctx, "get", cachex.ErrKeyNotFound).Return()

	result, err := metricsCache.Get(ctx, "nonexistent")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, cachex.ErrKeyNotFound, err)
	mockCache.AssertExpectations(t)
	mockCollector.AssertExpectations(t)
}

// TestMetricsCacheSet tests that Set records metrics correctly.
func TestMetricsCacheSet(t *testing.T) {
	mockCache := new(MockCache)
	mockCollector := new(MockCollector)

	metricsCache := NewMetricsCache(mockCache, mockCollector)
	ctx := context.Background()

	mockCache.On("Set", ctx, "key1", []byte("value1")).Return(nil)
	mockCollector.On("RecordSet", ctx, mock.AnythingOfType("time.Duration")).Return()

	err := metricsCache.Set(ctx, "key1", []byte("value1"))

	assert.NoError(t, err)
	mockCache.AssertExpectations(t)
	mockCollector.AssertExpectations(t)
}

// TestMetricsCacheSetError tests that Set records error when operation fails.
func TestMetricsCacheSetError(t *testing.T) {
	mockCache := new(MockCache)
	mockCollector := new(MockCollector)

	metricsCache := NewMetricsCache(mockCache, mockCollector)
	ctx := context.Background()
	testErr := errors.New("set failed")

	mockCache.On("Set", ctx, "key1", []byte("value1")).Return(testErr)
	mockCollector.On("RecordError", ctx, "set", testErr).Return()

	err := metricsCache.Set(ctx, "key1", []byte("value1"))

	assert.Error(t, err)
	assert.Equal(t, testErr, err)
	mockCache.AssertExpectations(t)
	mockCollector.AssertExpectations(t)
}

// TestMetricsCacheDelete tests that Delete records metrics correctly.
func TestMetricsCacheDelete(t *testing.T) {
	mockCache := new(MockCache)
	mockCollector := new(MockCollector)

	metricsCache := NewMetricsCache(mockCache, mockCollector)
	ctx := context.Background()

	mockCache.On("Delete", ctx, "key1", "key2").Return(int64(2), nil)
	mockCollector.On("RecordDelete", ctx, int64(2), mock.AnythingOfType("time.Duration")).Return()

	deleted, err := metricsCache.Delete(ctx, "key1", "key2")

	assert.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
	mockCache.AssertExpectations(t)
	mockCollector.AssertExpectations(t)
}

// TestMetricsCacheDeleteError tests that Delete records error when operation fails.
func TestMetricsCacheDeleteError(t *testing.T) {
	mockCache := new(MockCache)
	mockCollector := new(MockCollector)

	metricsCache := NewMetricsCache(mockCache, mockCollector)
	ctx := context.Background()
	testErr := errors.New("delete failed")

	mockCache.On("Delete", ctx, "key1").Return(int64(0), testErr)
	mockCollector.On("RecordError", ctx, "delete", testErr).Return()

	deleted, err := metricsCache.Delete(ctx, "key1")

	assert.Error(t, err)
	assert.Equal(t, int64(0), deleted)
	mockCache.AssertExpectations(t)
	mockCollector.AssertExpectations(t)
}

// TestMetricsCacheSetEX tests that SetEX records metrics correctly.
func TestMetricsCacheSetEX(t *testing.T) {
	mockCache := new(MockCache)
	mockCollector := new(MockCollector)

	metricsCache := NewMetricsCache(mockCache, mockCollector)
	ctx := context.Background()

	mockCache.On("SetEX", ctx, "key1", []byte("value1"), int64(3600)).Return(nil)
	mockCollector.On("RecordSet", ctx, mock.AnythingOfType("time.Duration")).Return()

	err := metricsCache.SetEX(ctx, "key1", []byte("value1"), 3600)

	assert.NoError(t, err)
	mockCache.AssertExpectations(t)
	mockCollector.AssertExpectations(t)
}

// TestMetricsCacheSetNX tests that SetNX records metrics correctly.
func TestMetricsCacheSetNX(t *testing.T) {
	mockCache := new(MockCache)
	mockCollector := new(MockCollector)

	metricsCache := NewMetricsCache(mockCache, mockCollector)
	ctx := context.Background()

	mockCache.On("SetNX", ctx, "key1", []byte("value1"), int64(3600)).Return(true, nil)
	mockCollector.On("RecordSet", ctx, mock.AnythingOfType("time.Duration")).Return()

	set, err := metricsCache.SetNX(ctx, "key1", []byte("value1"), 3600)

	assert.NoError(t, err)
	assert.True(t, set)
	mockCache.AssertExpectations(t)
	mockCollector.AssertExpectations(t)
}

// TestMetricsCacheClose tests that Close delegates to underlying cache.
func TestMetricsCacheClose(t *testing.T) {
	mockCache := new(MockCache)
	mockCollector := new(MockCollector)

	metricsCache := NewMetricsCache(mockCache, mockCollector)

	mockCache.On("Close").Return(nil)

	err := metricsCache.Close()

	assert.NoError(t, err)
	mockCache.AssertExpectations(t)
}

// TestMetricsCacheStats tests that Stats delegates to underlying cache.
func TestMetricsCacheStats(t *testing.T) {
	mockCache := new(MockCache)
	mockCollector := new(MockCollector)

	metricsCache := NewMetricsCache(mockCache, mockCollector)

	testStats := &mockStats{}
	mockCache.On("Stats").Return(testStats)

	stats := metricsCache.Stats()

	assert.Equal(t, testStats, stats)
	mockCache.AssertExpectations(t)
}

// TestOTeletryCollectorRecordGet tests the OTel collector RecordGet.
func TestOTeletryCollectorRecordGet(t *testing.T) {
	// Create a no-op meter for testing using the global meter provider
	meter := otel.GetMeterProvider().Meter("test")
	collector, err := NewOTeletryCollector(meter)
	assert.NoError(t, err)

	// Should not panic
	collector.RecordGet(context.Background(), true, time.Millisecond)
	collector.RecordGet(context.Background(), false, time.Millisecond)
}

// TestOTeletryCollectorRecordSet tests the OTel collector RecordSet.
func TestOTeletryCollectorRecordSet(t *testing.T) {
	meter := otel.GetMeterProvider().Meter("test")
	collector, err := NewOTeletryCollector(meter)
	assert.NoError(t, err)

	// Should not panic
	collector.RecordSet(context.Background(), time.Millisecond)
}

// TestOTeletryCollectorRecordDelete tests the OTel collector RecordDelete.
func TestOTeletryCollectorRecordDelete(t *testing.T) {
	meter := otel.GetMeterProvider().Meter("test")
	collector, err := NewOTeletryCollector(meter)
	assert.NoError(t, err)

	// Should not panic
	collector.RecordDelete(context.Background(), 5, time.Millisecond)
}

// TestOTeletryCollectorRecordError tests the OTel collector RecordError.
func TestOTeletryCollectorRecordError(t *testing.T) {
	meter := otel.GetMeterProvider().Meter("test")
	collector, err := NewOTeletryCollector(meter)
	assert.NoError(t, err)

	// Should not panic
	collector.RecordError(context.Background(), "get", errors.New("not found"))
	collector.RecordError(context.Background(), "set", errors.New("timeout error"))
}

// TestClassifyError tests the error classification function.
func TestClassifyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: "none",
		},
		{
			name:     "not found error",
			err:      errors.New("key not found"),
			expected: "not_found",
		},
		{
			name:     "KeyNotFound error",
			err:      errors.New("KeyNotFound"),
			expected: "not_found",
		},
		{
			name:     "timeout error",
			err:      errors.New("timeout exceeded"),
			expected: "timeout",
		},
		{
			name:     "connection error",
			err:      errors.New("connection refused"),
			expected: "connection",
		},
		{
			name:     "closed error",
			err:      errors.New("connection closed"),
			expected: "closed",
		},
		{
			name:     "other error",
			err:      errors.New("some other error"),
			expected: "other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifyError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestContains tests the contains helper function.
func TestContains(t *testing.T) {
	assert.True(t, contains("hello world", "world"))
	assert.True(t, contains("hello world", "hello"))
	assert.False(t, contains("hello world", "foo"))
	assert.False(t, contains("hello", "hello world"))
	assert.True(t, contains("key not found", "not found"))
}
