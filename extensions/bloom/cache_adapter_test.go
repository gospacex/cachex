package bloom

import (
	"context"
	"testing"

	"github.com/gospacex/cachex"
)

// mockCache implements cachex.Cache for testing
type mockCache struct {
	data map[string][]byte
}

func newMockCache() *mockCache {
	return &mockCache{data: make(map[string][]byte)}
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
	m.data[key] = value
	return nil
}

func (m *mockCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	if _, ok := m.data[key]; ok {
		return false, nil
	}
	m.data[key] = value
	return true, nil
}

func (m *mockCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	var deleted int64
	for _, key := range keys {
		if _, ok := m.data[key]; ok {
			delete(m.data, key)
			deleted++
		}
	}
	return deleted, nil
}

func (m *mockCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	var exists int64
	for _, key := range keys {
		if _, ok := m.data[key]; ok {
			exists++
		}
	}
	return exists, nil
}

func (m *mockCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	_, ok := m.data[key]
	return ok, nil
}

func (m *mockCache) TTL(ctx context.Context, key string) (int64, error) {
	if _, ok := m.data[key]; ok {
		return -1, nil
	}
	return -2, nil
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
	m.data[key] = []byte("1")
	return 1, nil
}

func (m *mockCache) Decr(ctx context.Context, key string) (int64, error) {
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
	return &stats{}
}

type stats struct{}

func (s *stats) Hits() int64    { return 0 }
func (s *stats) Misses() int64  { return 0 }
func (s *stats) Errors() int64  { return 0 }
func (s *stats) Latency() int64 { return 0 }

func TestCacheBloomFilterAdapter_Get(t *testing.T) {
	cache := newMockCache()
	adapter := NewCacheBloomFilterAdapter(cache, 100, 0.01)

	// Add key through adapter to populate Bloom filter
	_ = adapter.Set(context.Background(), "key1", []byte("value1"))

	tests := []struct {
		name      string
		key       string
		wantValue string
		wantErr   error
	}{
		{
			name:      "key exists in cache",
			key:       "key1",
			wantValue: "value1",
			wantErr:   nil,
		},
		{
			name:      "key not in cache or bloom filter",
			key:       "nonexistent",
			wantValue: "",
			wantErr:   cachex.ErrKeyNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := adapter.Get(context.Background(), tt.key)
			if err != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr == nil && string(got) != tt.wantValue {
				t.Errorf("Get() = %v, want %v", string(got), tt.wantValue)
			}
		})
	}
}

func TestCacheBloomFilterAdapter_Set(t *testing.T) {
	cache := newMockCache()
	adapter := NewCacheBloomFilterAdapter(cache, 100, 0.01)

	err := adapter.Set(context.Background(), "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	got, err := adapter.Get(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Get() after Set() error = %v", err)
	}
	if string(got) != "value1" {
		t.Errorf("Get() = %v, want value1", string(got))
	}
}

func TestCacheBloomFilterAdapter_SetNX(t *testing.T) {
	cache := newMockCache()
	adapter := NewCacheBloomFilterAdapter(cache, 100, 0.01)

	// First SetNX should succeed
	set, err := adapter.SetNX(context.Background(), "key1", []byte("value1"), 60)
	if err != nil {
		t.Fatalf("SetNX() error = %v", err)
	}
	if !set {
		t.Errorf("SetNX() = false, want true")
	}

	// Second SetNX should fail
	set, err = adapter.SetNX(context.Background(), "key1", []byte("value2"), 60)
	if err != nil {
		t.Fatalf("SetNX() second call error = %v", err)
	}
	if set {
		t.Errorf("SetNX() second call = true, want false")
	}
}

func TestCacheBloomFilterAdapter_Delete(t *testing.T) {
	cache := newMockCache()
	cache.data["key1"] = []byte("value1")

	adapter := NewCacheBloomFilterAdapter(cache, 100, 0.01)

	deleted, err := adapter.Delete(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if deleted != 1 {
		t.Errorf("Delete() = %d, want 1", deleted)
	}

	_, err = adapter.Get(context.Background(), "key1")
	if err != cachex.ErrKeyNotFound {
		t.Errorf("Get() after Delete() = %v, want ErrKeyNotFound", err)
	}
}

func TestCacheBloomFilterAdapter_Exists(t *testing.T) {
	cache := newMockCache()
	adapter := NewCacheBloomFilterAdapter(cache, 100, 0.01)

	// Add key through adapter to populate Bloom filter
	_ = adapter.Set(context.Background(), "key1", []byte("value1"))

	exists, err := adapter.Exists(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists != 1 {
		t.Errorf("Exists() = %d, want 1", exists)
	}

	exists, err = adapter.Exists(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("Exists() for nonexistent error = %v", err)
	}
	if exists != 0 {
		t.Errorf("Exists() for nonexistent = %d, want 0", exists)
	}
}

func TestCacheBloomFilterAdapter_MSet_MGet(t *testing.T) {
	cache := newMockCache()
	adapter := NewCacheBloomFilterAdapter(cache, 100, 0.01)

	kvs := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	err := adapter.MSet(context.Background(), kvs)
	if err != nil {
		t.Fatalf("MSet() error = %v", err)
	}

	values, err := adapter.MGet(context.Background(), "key1", "key2", "key3")
	if err != nil {
		t.Fatalf("MGet() error = %v", err)
	}

	if len(values) != 3 {
		t.Errorf("MGet() returned %d values, want 3", len(values))
	}
	if string(values[0]) != "value1" {
		t.Errorf("MGet()[0] = %v, want value1", string(values[0]))
	}
	if string(values[1]) != "value2" {
		t.Errorf("MGet()[1] = %v, want value2", string(values[1]))
	}
	if string(values[2]) != "value3" {
		t.Errorf("MGet()[2] = %v, want value3", string(values[2]))
	}
}

func TestCacheBloomFilterAdapter_FilterStats(t *testing.T) {
	cache := newMockCache()
	adapter := NewCacheBloomFilterAdapter(cache, 100, 0.01)

	adapter.Set(context.Background(), "key1", []byte("value1"))

	stats := adapter.FilterStats()
	if stats == nil {
		t.Fatal("FilterStats() returned nil")
	}

	if size, ok := stats["size"].(uint64); !ok || size == 0 {
		t.Errorf("FilterStats()[size] = %v, want non-zero", stats["size"])
	}
}

func TestCacheBloomFilterAdapter_IncrDecr(t *testing.T) {
	cache := newMockCache()
	adapter := NewCacheBloomFilterAdapter(cache, 100, 0.01)

	result, err := adapter.Incr(context.Background(), "counter")
	if err != nil {
		t.Fatalf("Incr() error = %v", err)
	}
	if result != 1 {
		t.Errorf("Incr() = %d, want 1", result)
	}

	result, err = adapter.Decr(context.Background(), "counter")
	if err != nil {
		t.Fatalf("Decr() error = %v", err)
	}
	if result != 0 {
		t.Errorf("Decr() = %d, want 0", result)
	}
}

func TestCacheBloomFilterAdapter_Ping(t *testing.T) {
	cache := newMockCache()
	adapter := NewCacheBloomFilterAdapter(cache, 100, 0.01)

	err := adapter.Ping(context.Background())
	if err != nil {
		t.Errorf("Ping() error = %v, want nil", err)
	}
}

func TestCacheBloomFilterAdapter_Close(t *testing.T) {
	cache := newMockCache()
	adapter := NewCacheBloomFilterAdapter(cache, 100, 0.01)

	err := adapter.Close()
	if err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}
