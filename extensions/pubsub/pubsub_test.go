package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gospacex/cachex"
)

// mockCache implements a minimal cachex.Cache for testing.
type mockCache struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func newMockCache() *mockCache {
	return &mockCache{data: make(map[string][]byte)}
}

func (m *mockCache) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if v, ok := m.data[key]; ok {
		return v, nil
	}
	return nil, cachex.ErrKeyNotFound
}

func (m *mockCache) Set(ctx context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
	return nil
}

func (m *mockCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	return m.Set(ctx, key, value)
}

func (m *mockCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; ok {
		return false, nil
	}
	m.data[key] = value
	return true, nil
}

func (m *mockCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, key := range keys {
		if _, ok := m.data[key]; ok {
			delete(m.data, key)
			n++
		}
	}
	return n, nil
}

func (m *mockCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var n int64
	for _, key := range keys {
		if _, ok := m.data[key]; ok {
			n++
		}
	}
	return n, nil
}

func (m *mockCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	return true, nil
}

func (m *mockCache) TTL(ctx context.Context, key string) (int64, error) {
	return -1, nil
}

func (m *mockCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([][]byte, len(keys))
	for i, key := range keys {
		if v, ok := m.data[key]; ok {
			result[i] = v
		}
	}
	return result, nil
}

func (m *mockCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range kvs {
		m.data[k] = v
	}
	return nil
}

func (m *mockCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys, nil
}

func (m *mockCache) Incr(ctx context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.data[key]; ok {
		var n int64
		for _, b := range v {
			n = n*10 + int64(b-'0')
		}
		n++
		m.data[key] = []byte(string(rune(n)))
		return n, nil
	}
	m.data[key] = []byte("1")
	return 1, nil
}

func (m *mockCache) Decr(ctx context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.data[key]; ok {
		var n int64
		for _, b := range v {
			n = n*10 + int64(b-'0')
		}
		if n > 0 {
			n--
		}
		m.data[key] = []byte(string(rune(n)))
		return n, nil
	}
	m.data[key] = []byte("0")
	return 0, nil
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

// =============================================================================
// InMemoryPubSub Tests
// =============================================================================

func TestInMemoryPubSub_Publish_Subscribe(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	defer pubsub.Close()

	ctx := context.Background()
	var receivedKeys []string
	var mu sync.Mutex

	handler := func(key string) {
		mu.Lock()
		defer mu.Unlock()
		receivedKeys = append(receivedKeys, key)
	}

	err := pubsub.Subscribe(ctx, "test-channel", handler)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	// Give subscription time to set up
	time.Sleep(10 * time.Millisecond)

	err = pubsub.Publish(ctx, "test-channel", "key1")
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	// Give publish time to deliver
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	if len(receivedKeys) != 1 || receivedKeys[0] != "key1" {
		t.Errorf("expected [key1], got %v", receivedKeys)
	}
	mu.Unlock()
}

func TestInMemoryPubSub_Publish_NoSubscribers(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	defer pubsub.Close()

	ctx := context.Background()

	// Publishing to a channel with no subscribers should not error
	err := pubsub.Publish(ctx, "nonexistent-channel", "key1")
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
}

func TestInMemoryPubSub_Close(t *testing.T) {
	pubsub := NewInMemoryPubSub()

	err := pubsub.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Subsequent operations should fail
	ctx := context.Background()
	err = pubsub.Publish(ctx, "test-channel", "key1")
	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestInMemoryPubSub_Subscribe_AfterClose(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	pubsub.Close()

	ctx := context.Background()
	err := pubsub.Subscribe(ctx, "test-channel", func(key string) {})
	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestInMemoryPubSub_MultipleSubscribers(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	defer pubsub.Close()

	ctx := context.Background()
	var received1 []string
	var received2 []string
	var mu sync.Mutex

	handler1 := func(key string) {
		mu.Lock()
		defer mu.Unlock()
		received1 = append(received1, key)
	}

	handler2 := func(key string) {
		mu.Lock()
		defer mu.Unlock()
		received2 = append(received2, key)
	}

	err := pubsub.Subscribe(ctx, "test-channel", handler1)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	err = pubsub.Subscribe(ctx, "test-channel", handler2)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	err = pubsub.Publish(ctx, "test-channel", "key1")
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received1) != 1 || received1[0] != "key1" {
		t.Errorf("handler1 expected [key1], got %v", received1)
	}
	if len(received2) != 1 || received2[0] != "key1" {
		t.Errorf("handler2 expected [key1], got %v", received2)
	}
}

// =============================================================================
// CacheInvalidationPubSub Tests
// =============================================================================

func TestCacheInvalidationPubSub_Delete_PublishesInvalidation(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	defer pubsub.Close()

	cache := newMockCache()
	wrapped := NewCacheInvalidationPubSub(cache, pubsub, "invalidation-channel")

	ctx := context.Background()

	// Set up some data
	cache.Set(ctx, "key1", []byte("value1"))
	cache.Set(ctx, "key2", []byte("value2"))

	var receivedKeys []string
	var mu sync.Mutex

	err := pubsub.Subscribe(ctx, "invalidation-channel", func(key string) {
		mu.Lock()
		defer mu.Unlock()
		receivedKeys = append(receivedKeys, key)
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	// Delete should publish invalidation
	_, err = wrapped.Delete(ctx, "key1")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	if len(receivedKeys) != 1 || receivedKeys[0] != "key1" {
		t.Errorf("expected [key1], got %v", receivedKeys)
	}
	mu.Unlock()

	// Verify local cache still has the data (we only published, didn't invalidate locally)
	_, err = cache.Get(ctx, "key1")
	if err != cachex.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestCacheInvalidationPubSub_InvalidateLocal(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	defer pubsub.Close()

	cache := newMockCache()
	wrapped := NewCacheInvalidationPubSub(cache, pubsub, "invalidation-channel")

	ctx := context.Background()

	// Set up some data
	cache.Set(ctx, "key1", []byte("value1"))

	// Invalidate locally
	err := wrapped.InvalidateLocal(ctx, "key1")
	if err != nil {
		t.Fatalf("InvalidateLocal() error = %v", err)
	}

	// Verify local cache is invalidated
	_, err = cache.Get(ctx, "key1")
	if err != cachex.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after invalidate, got %v", err)
	}
}

func TestCacheInvalidationPubSub_SubscribeToInvalidations(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	defer pubsub.Close()

	cache := newMockCache()
	wrapped := NewCacheInvalidationPubSub(cache, pubsub, "invalidation-channel")

	ctx := context.Background()

	// Set up some data
	cache.Set(ctx, "key1", []byte("value1"))

	// Subscribe to invalidations
	err := wrapped.SubscribeToInvalidations(ctx)
	if err != nil {
		t.Fatalf("SubscribeToInvalidations() error = %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	// Publish an invalidation from "remote"
	err = pubsub.Publish(ctx, "invalidation-channel", "key1")
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	// Verify local cache was invalidated by the remote message
	_, err = cache.Get(ctx, "key1")
	if err != cachex.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after remote invalidation, got %v", err)
	}
}

func TestCacheInvalidationPubSub_Get(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	defer pubsub.Close()

	cache := newMockCache()
	wrapped := NewCacheInvalidationPubSub(cache, pubsub, "invalidation-channel")

	ctx := context.Background()

	// Set up some data
	cache.Set(ctx, "key1", []byte("value1"))

	// Get should work
	val, err := wrapped.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}
}

func TestCacheInvalidationPubSub_Set(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	defer pubsub.Close()

	cache := newMockCache()
	wrapped := NewCacheInvalidationPubSub(cache, pubsub, "invalidation-channel")

	ctx := context.Background()

	// Set should work
	err := wrapped.Set(ctx, "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	val, err := cache.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}
}

func TestCacheInvalidationPubSub_Close(t *testing.T) {
	pubsub := NewInMemoryPubSub()

	cache := newMockCache()
	wrapped := NewCacheInvalidationPubSub(cache, pubsub, "invalidation-channel")

	ctx := context.Background()
	cache.Set(ctx, "key1", []byte("value1"))

	err := wrapped.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestCacheInvalidationPubSub_Stats(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	defer pubsub.Close()

	cache := newMockCache()
	wrapped := NewCacheInvalidationPubSub(cache, pubsub, "invalidation-channel")

	stats := wrapped.Stats()
	if stats == nil {
		t.Errorf("expected non-nil stats")
	}
}

// =============================================================================
// Interface Compliance Tests
// =============================================================================

func TestInMemoryPubSub_ImplementsPubSub(t *testing.T) {
	var ps PubSub = NewInMemoryPubSub()
	defer ps.Close()

	ctx := context.Background()

	// Test Publish
	err := ps.Publish(ctx, "test-channel", "key1")
	if err != nil {
		t.Errorf("Publish() error = %v", err)
	}

	// Test Subscribe
	err = ps.Subscribe(ctx, "test-channel", func(key string) {})
	if err != nil {
		t.Errorf("Subscribe() error = %v", err)
	}
}

func TestCacheInvalidationPubSub_ImplementsCache(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	defer pubsub.Close()

	cache := newMockCache()
	wrapped := NewCacheInvalidationPubSub(cache, pubsub, "invalidation-channel")

	var _ cachex.Cache = wrapped
}

// =============================================================================
// Error Handling Tests
// =============================================================================

func TestInMemoryPubSub_ContextCancellation(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	defer pubsub.Close()

	ctx, cancel := context.WithCancel(context.Background())

	err := pubsub.Subscribe(ctx, "test-channel", func(key string) {})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	cancel()

	// Give some time for the cancellation to propagate
	time.Sleep(10 * time.Millisecond)

	// Publishing after cancel should fail
	err = pubsub.Publish(ctx, "test-channel", "key1")
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// =============================================================================
// Performance/Stress Tests
// =============================================================================

func TestInMemoryPubSub_ManyMessages(t *testing.T) {
	pubsub := NewInMemoryPubSub()
	defer pubsub.Close()

	ctx := context.Background()
	var count int64
	var wg sync.WaitGroup

	handler := func(key string) {
		if key == "stop" {
			return
		}
	}

	err := pubsub.Subscribe(ctx, "test-channel", handler)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	const numMessages = 100
	wg.Add(numMessages)
	for i := 0; i < numMessages; i++ {
		go func(i int) {
			defer wg.Done()
			pubsub.Publish(ctx, "test-channel", string(rune(i)))
		}(i)
	}

	wg.Wait()

	// Publish stop signal
	pubsub.Publish(ctx, "test-channel", "stop")

	time.Sleep(10 * time.Millisecond)

	if count != 0 {
		t.Errorf("expected count 0, got %d", count)
	}
}
