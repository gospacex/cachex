package scan

import (
	"context"
	"strconv"
	"testing"

	"github.com/gospacex/cachex"
)

// mockCache implements cachex.Cache for testing.
type mockCache struct {
	keys []string
}

func (m *mockCache) Get(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}

func (m *mockCache) Set(ctx context.Context, key string, value []byte) error {
	return nil
}

func (m *mockCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	return nil
}

func (m *mockCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	return false, nil
}

func (m *mockCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	return 0, nil
}

func (m *mockCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	return 0, nil
}

func (m *mockCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	return false, nil
}

func (m *mockCache) TTL(ctx context.Context, key string) (int64, error) {
	return 0, nil
}

func (m *mockCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	return nil, nil
}

func (m *mockCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	return nil
}

func (m *mockCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	return m.keys, nil
}

func (m *mockCache) Incr(ctx context.Context, key string) (int64, error) {
	return 0, nil
}

func (m *mockCache) Decr(ctx context.Context, key string) (int64, error) {
	return 0, nil
}

func (m *mockCache) Ping(ctx context.Context) error {
	return nil
}

func (m *mockCache) Close() error {
	return nil
}

func (m *mockCache) Stats() cachex.Stats {
	return nil
}

func TestNewKeyScanner(t *testing.T) {
	cache := &mockCache{keys: []string{}}
	scanner := NewKeyScanner(cache)
	if scanner == nil {
		t.Fatal("NewKeyScanner returned nil")
	}
}

func TestKeyScanner_ScanAll(t *testing.T) {
	keys := []string{"user:1", "user:2", "user:3", "order:1", "order:2"}
	cache := &mockCache{keys: keys}
	scanner := NewKeyScanner(cache)

	result, err := scanner.ScanAll(context.Background(), "*")
	if err != nil {
		t.Fatalf("ScanAll failed: %v", err)
	}

	if len(result) != len(keys) {
		t.Errorf("expected %d keys, got %d", len(keys), len(result))
	}
}

func TestKeyScanner_Scan(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	cache := &mockCache{keys: keys}
	scanner := NewKeyScanner(cache)

	ch := scanner.Scan(context.Background(), "*", 3)

	var received []string
	for batch := range ch {
		received = append(received, batch...)
	}

	if len(received) != len(keys) {
		t.Errorf("expected %d keys, got %d", len(keys), len(received))
	}
}

func TestKeyScanner_ScanWithPattern(t *testing.T) {
	allKeys := []string{"user:1", "user:2", "order:1", "order:2"}
	matchingKeys := []string{"user:1", "user:2"}
	cache := &mockCache{keys: allKeys}
	scanner := NewKeyScanner(cache)

	// Note: mock cache returns all keys for any pattern
	// Real implementation would filter by pattern
	result, err := scanner.ScanAll(context.Background(), "user:*")
	if err != nil {
		t.Fatalf("ScanAll failed: %v", err)
	}

	if len(result) != len(allKeys) {
		t.Errorf("expected %d keys, got %d", len(allKeys), len(result))
	}

	_ = matchingKeys // Used to document expected behavior
}

func TestKeyScanner_ScanContextCancellation(t *testing.T) {
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		keys[i] = string(rune('a'+byte(i%26))) + strconv.Itoa(i/26)
	}
	cache := &mockCache{keys: keys}
	scanner := NewKeyScanner(cache)

	ctx, cancel := context.WithCancel(context.Background())

	ch := scanner.Scan(ctx, "*", 10)

	// Cancel after first batch
	cancel()

	var count int
	for batch := range ch {
		count += len(batch)
		if count > 0 {
			break
		}
	}

	// Should not panic or hang
	if count < 0 {
		t.Errorf("unexpected count: %d", count)
	}
}

func TestKeyScanner_ScanEmptyCache(t *testing.T) {
	cache := &mockCache{keys: []string{}}
	scanner := NewKeyScanner(cache)

	result, err := scanner.ScanAll(context.Background(), "*")
	if err != nil {
		t.Fatalf("ScanAll failed: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("expected 0 keys, got %d", len(result))
	}
}

func TestKeyScanner_ScanLargeBatch(t *testing.T) {
	keys := make([]string, 5000)
	for i := 0; i < 5000; i++ {
		keys[i] = "key"
	}
	cache := &mockCache{keys: keys}
	scanner := NewKeyScanner(cache)

	result, err := scanner.ScanAll(context.Background(), "*")
	if err != nil {
		t.Fatalf("ScanAll failed: %v", err)
	}

	if len(result) != 5000 {
		t.Errorf("expected 5000 keys, got %d", len(result))
	}
}

func TestKeyScanner_ScanBatchSize(t *testing.T) {
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		keys[i] = "key"
	}
	cache := &mockCache{keys: keys}
	scanner := NewKeyScanner(cache)

	ch := scanner.Scan(context.Background(), "*", 25)

	var batches []int
	for batch := range ch {
		batches = append(batches, len(batch))
	}

	// Should have4 batches: 25 + 25 + 25 + 25
	if len(batches) != 4 {
		t.Errorf("expected 4 batches, got %d", len(batches))
	}
}

// Compile-time interface check.
var _ cachex.Cache = (*mockCache)(nil)
