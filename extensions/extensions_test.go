package extensions

import (
	"context"
	"testing"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/cachex/extensions/bloom"
	"github.com/gospacex/cachex/extensions/ratelimit"
	"github.com/stretchr/testify/assert"
)

// mockCache implements cachex.Cache for testing
type mockCache struct {
	data map[string][]byte
}

func (m *mockCache) Get(ctx context.Context, key string) ([]byte, error) {
	if v, ok := m.data[key]; ok {
		return v, nil
	}
	return nil, cachex.ErrKeyNotFound
}

func (m *mockCache) Set(ctx context.Context, key string, value []byte) error {
	m.data[key] = value
	return nil
}

func (m *mockCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	return m.Set(ctx, key, value)
}

func (m *mockCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	if _, ok := m.data[key]; ok {
		return false, nil
	}
	m.data[key] = value
	return true, nil
}

func (m *mockCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	var count int64
	for _, key := range keys {
		if _, ok := m.data[key]; ok {
			delete(m.data, key)
			count++
		}
	}
	return count, nil
}

func (m *mockCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	var count int64
	for _, key := range keys {
		if _, ok := m.data[key]; ok {
			count++
		}
	}
	return count, nil
}

func (m *mockCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	_, ok := m.data[key]
	return ok, nil
}

func (m *mockCache) TTL(ctx context.Context, key string) (int64, error) {
	if _, ok := m.data[key]; ok {
		return 60, nil
	}
	return -2, cachex.ErrKeyNotFound
}

func (m *mockCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	result := make([][]byte, len(keys))
	for i, key := range keys {
		if v, ok := m.data[key]; ok {
			result[i] = v
		}
	}
	return result, nil
}

func (m *mockCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	for k, v := range kvs {
		m.data[k] = v
	}
	return nil
}

func (m *mockCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	var keys []string
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys, nil
}

func (m *mockCache) Incr(ctx context.Context, key string) (int64, error) {
	return 0, cachex.ErrNotSupported
}

func (m *mockCache) Decr(ctx context.Context, key string) (int64, error) {
	return 0, cachex.ErrNotSupported
}

func (m *mockCache) Ping(ctx context.Context) error {
	return nil
}

func (m *mockCache) Close() error {
	return nil
}

func (m *mockCache) Stats() cachex.Stats {
	return &mockStats{}
}

type mockStats struct{}

func (s *mockStats) Hits() int64    { return 0 }
func (s *mockStats) Misses() int64  { return 0 }
func (s *mockStats) Errors() int64  { return 0 }
func (s *mockStats) Latency() int64 { return 0 }

func newMockCache() *mockCache {
	return &mockCache{data: make(map[string][]byte)}
}

func TestHealthChecker(t *testing.T) {
	cache := newMockCache()
	checker := NewHealthChecker(cache)

	// Add custom check
	checker.AddCheck("custom", func(ctx context.Context) error {
		return nil
	})

	ctx := context.Background()
	results := checker.Check(ctx)

	assert.NotEmpty(t, results)
	assert.Equal(t, "ping", results[0].Name)
	assert.Equal(t, "healthy", results[0].Status)

	// Test all healthy
	err := checker.CheckAll(ctx)
	assert.NoError(t, err)
}

func TestReadyChecker(t *testing.T) {
	cache := newMockCache()
	ready := NewReadyChecker(cache, 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := ready.Ready(ctx)
	assert.NoError(t, err)
}

func TestBloomFilter(t *testing.T) {
	filter := bloom.New(1000, 0.01)

	// Add items
	filter.Add([]byte("item1"))
	filter.Add([]byte("item2"))
	filter.Add([]byte("item3"))

	// Test existing items (should be found)
	assert.True(t, filter.Test([]byte("item1")))
	assert.True(t, filter.Test([]byte("item2")))
	assert.True(t, filter.Test([]byte("item3")))

	// Test non-existing items (might be false positive)
	// With 1% fp rate and 3 items in 1000, we might get a false positive
	_ = filter.Test([]byte("item4"))

	// Clear and test
	filter.Clear()
	assert.False(t, filter.Test([]byte("item1")))
}

func TestTokenBucket(t *testing.T) {
	// 10 tokens per second, capacity of 5
	limiter := ratelimit.NewTokenBucket(5, 10)

	// Should allow initial burst
	assert.True(t, limiter.Allow())
	assert.True(t, limiter.Allow())
	assert.True(t, limiter.Allow())
	assert.True(t, limiter.Allow())
	assert.True(t, limiter.Allow())

	// Now exhausted
	assert.False(t, limiter.Allow())

	// Available should be 0
	assert.Equal(t, int64(0), limiter.Available())

	// Wait for refill
	time.Sleep(200 * time.Millisecond)

	// Should have refilled
	available := limiter.Available()
	assert.GreaterOrEqual(t, available, int64(1))
}

func TestSlidingWindow(t *testing.T) {
	limiter := ratelimit.NewSlidingWindow(3, time.Second)

	// Allow 3 requests
	assert.True(t, limiter.Allow())
	assert.True(t, limiter.Allow())
	assert.True(t, limiter.Allow())

	// Now exhausted
	assert.False(t, limiter.Allow())

	// Current count should be 3
	assert.Equal(t, int64(3), limiter.Current())
}

func TestRateLimitedCache(t *testing.T) {
	cache := newMockCache()
	rateLimited := ratelimit.NewRateLimitedCache(cache, 10, 5)

	// Should allow some requests
	for i := 0; i < 10; i++ {
		assert.True(t, rateLimited.Allow())
	}

	// Should now be rate limited
	assert.False(t, rateLimited.Allow())
}
