//go:build integration
// +build integration

package cachex

import (
	"context"
	"os"
	"testing"
	"time"

	_ "github.com/gospacex/cachex/backends/network/redis"
)

func TestRedisIntegration(t *testing.T) {
	// Skip if no Redis available
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	cfg := &Config{
		Backend: BackendRedis,
		Addrs:   []string{addr},
	}

	cache, err := Open(BackendRedis, cfg)
	if err != nil {
		t.Skipf("Skipping integration test: Redis not available: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()

	t.Run("BasicOperations", func(t *testing.T) {
		// Clean up
		cache.Delete(ctx, "test:key1", "test:key2")

		// Set
		err := cache.Set(ctx, "test:key1", []byte("value1"))
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Get
		val, err := cache.Get(ctx, "test:key1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if string(val) != "value1" {
			t.Errorf("Expected 'value1', got '%s'", string(val))
		}

		// Delete
		n, err := cache.Delete(ctx, "test:key1")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		if n != 1 {
			t.Errorf("Expected 1 deleted, got %d", n)
		}

		// Verify deletion
		_, err = cache.Get(ctx, "test:key1")
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("SetEX", func(t *testing.T) {
		cache.Delete(ctx, "test:ttl")

		err := cache.SetEX(ctx, "test:ttl", []byte("value"), 2)
		if err != nil {
			t.Fatalf("SetEX failed: %v", err)
		}

		ttl, err := cache.TTL(ctx, "test:ttl")
		if err != nil {
			t.Fatalf("TTL failed: %v", err)
		}
		if ttl < 1 || ttl > 2 {
			t.Errorf("Expected TTL between 1 and 2, got %d", ttl)
		}

		time.Sleep(3 * time.Second)

		_, err = cache.Get(ctx, "test:ttl")
		if err != ErrKeyNotFound {
			t.Errorf("Expected key to be expired, got %v", err)
		}
	})

	t.Run("SetNX", func(t *testing.T) {
		cache.Delete(ctx, "test:setnx")

		// First set should succeed
		set, err := cache.SetNX(ctx, "test:setnx", []byte("value"), 60)
		if err != nil {
			t.Fatalf("SetNX failed: %v", err)
		}
		if !set {
			t.Errorf("Expected SetNX to return true")
		}

		// Second set should fail
		set, err = cache.SetNX(ctx, "test:setnx", []byte("value2"), 60)
		if err != nil {
			t.Fatalf("SetNX failed: %v", err)
		}
		if set {
			t.Errorf("Expected SetNX to return false")
		}
	})

	t.Run("IncrDecr", func(t *testing.T) {
		cache.Delete(ctx, "test:counter")

		val, err := cache.Incr(ctx, "test:counter")
		if err != nil {
			t.Fatalf("Incr failed: %v", err)
		}
		if val != 1 {
			t.Errorf("Expected 1, got %d", val)
		}

		val, err = cache.Incr(ctx, "test:counter")
		if err != nil {
			t.Fatalf("Incr failed: %v", err)
		}
		if val != 2 {
			t.Errorf("Expected 2, got %d", val)
		}

		val, err = cache.Decr(ctx, "test:counter")
		if err != nil {
			t.Fatalf("Decr failed: %v", err)
		}
		if val != 1 {
			t.Errorf("Expected 1, got %d", val)
		}
	})

	t.Run("MSetMGet", func(t *testing.T) {
		cache.Delete(ctx, "test:mkey1", "test:mkey2", "test:mkey3")

		kvs := map[string][]byte{
			"test:mkey1": []byte("value1"),
			"test:mkey2": []byte("value2"),
			"test:mkey3": []byte("value3"),
		}

		err := cache.MSet(ctx, kvs)
		if err != nil {
			t.Fatalf("MSet failed: %v", err)
		}

		vals, err := cache.MGet(ctx, "test:mkey1", "test:mkey2", "test:mkey3")
		if err != nil {
			t.Fatalf("MGet failed: %v", err)
		}

		if len(vals) != 3 {
			t.Errorf("Expected 3 values, got %d", len(vals))
		}

		for i, v := range vals {
			expected := string(rune('1' + i))
			if string(v) != "value"+expected {
				t.Errorf("Expected 'value%s', got '%s'", expected, string(v))
			}
		}
	})

	t.Run("Exists", func(t *testing.T) {
		cache.Delete(ctx, "test:exists1", "test:exists2")

		cache.Set(ctx, "test:exists1", []byte("value"))

		n, err := cache.Exists(ctx, "test:exists1", "test:exists2", "test:exists3")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if n != 1 {
			t.Errorf("Expected 1, got %d", n)
		}
	})

	t.Run("Expire", func(t *testing.T) {
		cache.Delete(ctx, "test:expire")

		cache.Set(ctx, "test:expire", []byte("value"))

		set, err := cache.Expire(ctx, "test:expire", 5)
		if err != nil {
			t.Fatalf("Expire failed: %v", err)
		}
		if !set {
			t.Errorf("Expected Expire to return true")
		}

		ttl, err := cache.TTL(ctx, "test:expire")
		if err != nil {
			t.Fatalf("TTL failed: %v", err)
		}
		if ttl < 4 || ttl > 5 {
			t.Errorf("Expected TTL between 4 and 5, got %d", ttl)
		}
	})

	t.Run("Keys", func(t *testing.T) {
		cache.Delete(ctx, "test:keys:1", "test:keys:2", "test:other")

		cache.Set(ctx, "test:keys:1", []byte("value1"))
		cache.Set(ctx, "test:keys:2", []byte("value2"))
		cache.Set(ctx, "test:other", []byte("value3"))

		keys, err := cache.Keys(ctx, "test:keys:*")
		if err != nil {
			t.Fatalf("Keys failed: %v", err)
		}
		if len(keys) != 2 {
			t.Errorf("Expected 2 keys, got %d: %v", len(keys), keys)
		}
	})

	t.Run("Ping", func(t *testing.T) {
		err := cache.Ping(ctx)
		if err != nil {
			t.Fatalf("Ping failed: %v", err)
		}
	})

	t.Run("Stats", func(t *testing.T) {
		stats := cache.Stats()
		if stats == nil {
			t.Errorf("Expected non-nil stats")
		}
	})
}

func TestRedisClusterIntegration(t *testing.T) {
	// Skip if no cluster available
	addrs := []string{"localhost:7000", "localhost:7001", "localhost:7002"}

	cfg := &Config{
		Backend:     BackendRedis,
		Addrs:       addrs,
		ClusterMode: true,
	}

	cache, err := Open(BackendRedis, cfg)
	if err != nil {
		t.Skipf("Skipping cluster integration test: Redis cluster not available: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()

	// Basic operations should work
	err = cache.Set(ctx, "cluster:test", []byte("value"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, err := cache.Get(ctx, "cluster:test")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("Expected 'value', got '%s'", string(val))
	}
}
