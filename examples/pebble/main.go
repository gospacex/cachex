// Package main provides examples of using cachex with Pebble.
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
	fmt.Println("=== Example 1: Basic Pebble Usage ===")
	if err := basicExample(ctx); err != nil {
		return fmt.Errorf("basic example failed: %w", err)
	}

	// Example 2: Batch operations with sync
	fmt.Println("\n=== Example 2: Batch Operations ===")
	if err := batchExample(ctx); err != nil {
		return fmt.Errorf("batch example failed: %w", err)
	}

	// Example 3: Counter operations
	fmt.Println("\n=== Example 3: Counter Operations ===")
	if err := counterExample(ctx); err != nil {
		return fmt.Errorf("counter example failed: %w", err)
	}

	// Example 4: Iteration
	fmt.Println("\n=== Example 4: Iteration ===")
	if err := iteratorExample(ctx); err != nil {
		return fmt.Errorf("iterator example failed: %w", err)
	}

	return nil
}

func basicExample(ctx context.Context) error {
	// Create a temp directory for the database
	tmpDir := "/tmp/cachex-pebble-example-" + time.Now().Format("20060102150405")
	defer os.RemoveAll(tmpDir)

	// Create cache client
	cache, err := cachex.Open("pebble", &cachex.Config{
		Backend:        "pebble",
		Dir:            tmpDir,
		BlockCacheSize: 64 * 1024 * 1024, // 64MB
		Compression:    true,
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

	// Delete
	deleted, err := cache.Delete(ctx, "user:2")
	if err != nil {
		return fmt.Errorf("failed to delete: %w", err)
	}
	fmt.Printf("Deleted %d keys\n", deleted)

	// Verify deletion
	_, err = cache.Get(ctx, "user:2")
	if err == cachex.ErrKeyNotFound {
		fmt.Println("Key deletion verified: OK")
	}

	// Health check
	if err := cache.Ping(ctx); err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	fmt.Println("Health check: OK")

	// Get stats
	stats := cache.Stats()
	fmt.Printf("Cache stats - Hits: %d, Misses: %d, Errors: %d\n",
		stats.Hits(), stats.Misses(), stats.Errors())

	return nil
}

func batchExample(ctx context.Context) error {
	tmpDir := "/tmp/cachex-pebble-batch-example-" + time.Now().Format("20060102150405")
	defer os.RemoveAll(tmpDir)

	cache, err := cachex.Open("pebble", &cachex.Config{
		Backend:     "pebble",
		Dir:         tmpDir,
		Compression: true,
	})
	if err != nil {
		return err
	}
	defer cache.Close()

	// Batch set
	kvs := make(map[string][]byte)
	for i := 0; i < 1000; i++ {
		kvs[fmt.Sprintf("batch:key:%d", i)] = []byte(fmt.Sprintf("batch:value:%d", i))
	}

	fmt.Printf("Setting %d key-value pairs...\n", len(kvs))
	start := time.Now()
	if err := cache.MSet(ctx, kvs); err != nil {
		return fmt.Errorf("failed to batch set: %w", err)
	}
	fmt.Printf("Batch set completed in %v\n", time.Since(start))

	// Batch get
	keys := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
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

func counterExample(ctx context.Context) error {
	tmpDir := "/tmp/cachex-pebble-counter-example-" + time.Now().Format("20060102150405")
	defer os.RemoveAll(tmpDir)

	cache, err := cachex.Open("pebble", &cachex.Config{
		Backend: "pebble",
		Dir:     tmpDir,
	})
	if err != nil {
		return err
	}
	defer cache.Close()

	// Increment counter
	fmt.Println("Testing counter operations...")

	counter, err := cache.Incr(ctx, "counter:requests")
	if err != nil {
		return fmt.Errorf("failed to increment: %w", err)
	}
	fmt.Printf("Counter value (1st): %d\n", counter)

	for i := 0; i < 9; i++ {
		counter, err = cache.Incr(ctx, "counter:requests")
		if err != nil {
			return fmt.Errorf("failed to increment: %w", err)
		}
	}
	fmt.Printf("Counter value (10th): %d\n", counter)

	// Decrement
	counter, err = cache.Decr(ctx, "counter:requests")
	if err != nil {
		return fmt.Errorf("failed to decrement: %w", err)
	}
	fmt.Printf("Counter value after decrement: %d\n", counter)

	return nil
}

func iteratorExample(ctx context.Context) error {
	tmpDir := "/tmp/cachex-pebble-iter-example-" + time.Now().Format("20060102150405")
	defer os.RemoveAll(tmpDir)

	cache, err := cachex.Open("pebble", &cachex.Config{
		Backend: "pebble",
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
