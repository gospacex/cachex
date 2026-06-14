// Package main provides examples of using cachex with Badger.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gospacex/cachex"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// Example 1: Basic embedded usage
	fmt.Println("=== Example 1: Basic Badger Usage ===")
	if err := basicExample(ctx); err != nil {
		return fmt.Errorf("basic example failed: %w", err)
	}

	// Example 2: In-memory mode
	fmt.Println("\n=== Example 2: In-Memory Mode ===")
	if err := memoryExample(ctx); err != nil {
		return fmt.Errorf("memory example failed: %w", err)
	}

	// Example 3: With TTL support
	fmt.Println("\n=== Example 3: TTL Support ===")
	if err := ttlExample(ctx); err != nil {
		return fmt.Errorf("TTL example failed: %w", err)
	}

	// Example 4: Batch operations
	fmt.Println("\n=== Example 4: Batch Operations ===")
	if err := batchExample(ctx); err != nil {
		return fmt.Errorf("batch example failed: %w", err)
	}

	// Example 5: Iterator usage
	fmt.Println("\n=== Example 5: Iterator Usage ===")
	if err := iteratorExample(ctx); err != nil {
		return fmt.Errorf("iterator example failed: %w", err)
	}

	return nil
}

func basicExample(ctx context.Context) error {
	// Create a temp directory for the database
	tmpDir := "/tmp/cachex-badger-example-" + time.Now().Format("20060102150405")
	defer os.RemoveAll(tmpDir)

	// Create cache client
	cache, err := cachex.Open("badger", &cachex.Config{
		Backend:        "badger",
		Dir:            tmpDir,
		SyncWrites:     false,
		BlockCacheSize: 64 * 1024 * 1024, // 64MB
		IndexCacheSize: 32 * 1024 * 1024, // 32MB
	})
	if err != nil {
		return fmt.Errorf("failed to open cache: %w", err)
	}
	defer cache.Close()

	// Basic CRUD operations
	fmt.Println("Setting values...")
	if err := cache.Set(ctx, "user:1", []byte(`{"name":"Alice","age":30}`)); err != nil {
		return fmt.Errorf("failed to set: %w", err)
	}

	if err := cache.Set(ctx, "user:2", []byte(`{"name":"Bob","age":25}`)); err != nil {
		return fmt.Errorf("failed to set: %w", err)
	}

	fmt.Println("Getting values...")
	val, err := cache.Get(ctx, "user:1")
	if err != nil {
		return fmt.Errorf("failed to get: %w", err)
	}
	fmt.Printf("User 1: %s\n", string(val))

	val, err = cache.Get(ctx, "user:2")
	if err != nil {
		return fmt.Errorf("failed to get: %w", err)
	}
	fmt.Printf("User 2: %s\n", string(val))

	// Check existence
	count, err := cache.Exists(ctx, "user:1", "user:2", "user:3")
	if err != nil {
		return fmt.Errorf("failed to check existence: %w", err)
	}
	fmt.Printf("Found %d existing keys\n", count)

	// Health check
	if err := cache.Ping(ctx); err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	fmt.Println("Health check: OK")

	return nil
}

func memoryExample(ctx context.Context) error {
	// Create in-memory cache
	cache, err := cachex.Open("badger", &cachex.Config{
		Backend:  "badger",
		InMemory: true,
	})
	if err != nil {
		return fmt.Errorf("failed to open cache: %w", err)
	}
	defer cache.Close()

	// Use as a temporary cache
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf(`{"id":%d,"data":"value-%d"}`, i, i)
		if err := cache.Set(ctx, key, []byte(value)); err != nil {
			return fmt.Errorf("failed to set: %w", err)
		}
	}

	// Verify data
	val, err := cache.Get(ctx, "key-999")
	if err != nil {
		return fmt.Errorf("failed to get: %w", err)
	}
	fmt.Printf("Last value: %s\n", string(val))

	fmt.Println("In-memory cache: OK")
	return nil
}

func ttlExample(ctx context.Context) error {
	tmpDir := "/tmp/cachex-badger-ttl-example-" + time.Now().Format("20060102150405")
	defer os.RemoveAll(tmpDir)

	cache, err := cachex.Open("badger", &cachex.Config{
		Backend: "badger",
		Dir:     tmpDir,
	})
	if err != nil {
		return err
	}
	defer cache.Close()

	// Set with TTL (1 second)
	fmt.Println("Setting key with 1-second TTL...")
	if err := cache.SetEX(ctx, "temp-key", []byte("temporary value"), 1); err != nil {
		return fmt.Errorf("failed to set with TTL: %w", err)
	}

	// Check TTL immediately
	ttl, err := cache.TTL(ctx, "temp-key")
	if err != nil {
		return fmt.Errorf("failed to get TTL: %w", err)
	}
	fmt.Printf("Initial TTL: %d seconds\n", ttl)

	// Verify key exists
	val, err := cache.Get(ctx, "temp-key")
	if err != nil {
		return fmt.Errorf("failed to get: %w", err)
	}
	fmt.Printf("Value before expiry: %s\n", string(val))

	// Wait for expiry
	fmt.Println("Waiting for TTL to expire...")
	time.Sleep(2 * time.Second)

	// Try to get expired key
	_, err = cache.Get(ctx, "temp-key")
	if err == cachex.ErrKeyNotFound {
		fmt.Println("Key expired as expected: OK")
	} else {
		return fmt.Errorf("expected key not found, got: %v", err)
	}

	return nil
}

func batchExample(ctx context.Context) error {
	tmpDir := "/tmp/cachex-badger-batch-example-" + time.Now().Format("20060102150405")
	defer os.RemoveAll(tmpDir)

	cache, err := cachex.Open("badger", &cachex.Config{
		Backend: "badger",
		Dir:     tmpDir,
	})
	if err != nil {
		return err
	}
	defer cache.Close()

	// Batch set
	kvs := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		kvs[fmt.Sprintf("batch:key:%d", i)] = []byte(fmt.Sprintf("batch:value:%d", i))
	}

	fmt.Printf("Setting %d key-value pairs...\n", len(kvs))
	start := time.Now()
	if err := cache.MSet(ctx, kvs); err != nil {
		return fmt.Errorf("failed to batch set: %w", err)
	}
	fmt.Printf("Batch set completed in %v\n", time.Since(start))

	// Batch get
	keys := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		keys = append(keys, fmt.Sprintf("batch:key:%d", i))
	}

	fmt.Printf("Getting %d keys...\n", len(keys))
	start = time.Now()
	values, err := cache.MGet(ctx, keys...)
	if err != nil {
		return fmt.Errorf("failed to batch get: %w", err)
	}

	found := 0
	for _, v := range values {
		if v != nil {
			found++
		}
	}
	fmt.Printf("Batch get completed in %v, found %d values\n", time.Since(start), found)

	return nil
}

func iteratorExample(ctx context.Context) error {
	tmpDir := "/tmp/cachex-badger-iter-example-" + time.Now().Format("20060102150405")
	defer os.RemoveAll(tmpDir)

	cache, err := cachex.Open("badger", &cachex.Config{
		Backend: "badger",
		Dir:     tmpDir,
	})
	if err != nil {
		return err
	}
	defer cache.Close()

	// Insert some test data
	prefixes := []string{"user:", "session:", "cache:"}
	for _, prefix := range prefixes {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("%s%d", prefix, i)
			value := fmt.Sprintf(`{"id":%d,"type":"%s"}`, i, prefix[:len(prefix)-1])
			cache.Set(ctx, key, []byte(value))
		}
	}

	// Iterate with pattern matching
	fmt.Println("Iterating all keys...")
	allKeys, err := cache.Keys(ctx, "*")
	if err != nil {
		return fmt.Errorf("failed to get all keys: %w", err)
	}
	fmt.Printf("Total keys: %d\n", len(allKeys))

	fmt.Println("Iterating keys with prefix 'user:*'...")
	userKeys, err := cache.Keys(ctx, "user:*")
	if err != nil {
		return fmt.Errorf("failed to get user keys: %w", err)
	}
	fmt.Printf("User keys: %d\n", len(userKeys))

	fmt.Println("Iterating keys with prefix 'session:*'...")
	sessionKeys, err := cache.Keys(ctx, "session:*")
	if err != nil {
		return fmt.Errorf("failed to get session keys: %w", err)
	}
	fmt.Printf("Session keys: %d\n", len(sessionKeys))

	return nil
}

// loadConfig loads configuration from environment or defaults.
func loadConfig() *cachex.Config {
	cfg := cachex.DefaultConfig(cachex.BackendBadger)

	if dir := os.Getenv("BADGER_DIR"); dir != "" {
		cfg.Dir = dir
	}

	return cfg
}
