// Package main provides examples of using cachex with BBolt.
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
	fmt.Println("=== Example 1: Basic BBolt Usage ===")
	if err := basicExample(ctx); err != nil {
		return fmt.Errorf("basic example failed: %w", err)
	}

	// Example 2: Transaction support
	fmt.Println("\n=== Example 2: Transaction Support ===")
	if err := transactionExample(ctx); err != nil {
		return fmt.Errorf("transaction example failed: %w", err)
	}

	// Example 3: Counter operations
	fmt.Println("\n=== Example 3: Counter Operations ===")
	if err := counterExample(ctx); err != nil {
		return fmt.Errorf("counter example failed: %w", err)
	}

	// Example 4: Batch operations
	fmt.Println("\n=== Example 4: Batch Operations ===")
	if err := batchExample(ctx); err != nil {
		return fmt.Errorf("batch example failed: %w", err)
	}

	return nil
}

func basicExample(ctx context.Context) error {
	// Create a temp directory for the database
	tmpDir := "/tmp/cachex-bbolt-example-" + time.Now().Format("20060102150405")
	defer os.RemoveAll(tmpDir)

	// Create cache client
	cache, err := cachex.Open("bbolt", &cachex.Config{
		Backend:    "bbolt",
		Dir:        tmpDir + "/bbolt.db",
		BucketName: "cachex",
		SyncWrites: true,
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

func transactionExample(ctx context.Context) error {
	tmpDir := "/tmp/cachex-bbolt-tx-example-" + time.Now().Format("20060102150405")
	defer os.RemoveAll(tmpDir)

	cache, err := cachex.Open("bbolt", &cachex.Config{
		Backend:    "bbolt",
		Dir:        tmpDir + "/bbolt.db",
		BucketName: "cachex",
	})
	if err != nil {
		return err
	}
	defer cache.Close()

	// Batch operations (simulates transaction-like behavior)
	kvs := map[string][]byte{
		"tx:key1": []byte("value1"),
		"tx:key2": []byte("value2"),
		"tx:key3": []byte("value3"),
	}

	fmt.Println("Executing batch operations...")
	if err := cache.MSet(ctx, kvs); err != nil {
		return fmt.Errorf("batch set failed: %w", err)
	}

	// Read all keys
	keys, err := cache.Keys(ctx, "tx:*")
	if err != nil {
		return fmt.Errorf("failed to get keys: %w", err)
	}
	fmt.Printf("Batch operation created %d keys\n", len(keys))

	return nil
}

func counterExample(ctx context.Context) error {
	tmpDir := "/tmp/cachex-bbolt-counter-example-" + time.Now().Format("20060102150405")
	defer os.RemoveAll(tmpDir)

	cache, err := cachex.Open("bbolt", &cachex.Config{
		Backend:    "bbolt",
		Dir:        tmpDir + "/bbolt.db",
		BucketName: "cachex",
	})
	if err != nil {
		return err
	}
	defer cache.Close()

	// Increment counter
	fmt.Println("Testing counter operations...")

	counter, err := cache.Incr(ctx, "counter:visits")
	if err != nil {
		return fmt.Errorf("failed to increment: %w", err)
	}
	fmt.Printf("Counter value (1st): %d\n", counter)

	counter, err = cache.Incr(ctx, "counter:visits")
	if err != nil {
		return fmt.Errorf("failed to increment: %w", err)
	}
	fmt.Printf("Counter value (2nd): %d\n", counter)

	counter, err = cache.Incr(ctx, "counter:visits")
	if err != nil {
		return fmt.Errorf("failed to increment: %w", err)
	}
	fmt.Printf("Counter value (3rd): %d\n", counter)

	// Decrement
	counter, err = cache.Decr(ctx, "counter:visits")
	if err != nil {
		return fmt.Errorf("failed to decrement: %w", err)
	}
	fmt.Printf("Counter value after decrement: %d\n", counter)

	return nil
}

func batchExample(ctx context.Context) error {
	tmpDir := "/tmp/cachex-bbolt-batch-example-" + time.Now().Format("20060102150405")
	defer os.RemoveAll(tmpDir)

	cache, err := cachex.Open("bbolt", &cachex.Config{
		Backend:    "bbolt",
		Dir:        tmpDir + "/bbolt.db",
		BucketName: "cachex",
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
