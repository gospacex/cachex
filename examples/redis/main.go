// Package main provides examples of using cachex with Redis.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/cachex/observability"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// Example 1: Basic usage with config file
	fmt.Println("=== Example 1: Basic Redis Connection ===")
	if err := basicExample(ctx); err != nil {
		return fmt.Errorf("basic example failed: %w", err)
	}

	// Example 2: Connection with TLS
	fmt.Println("\n=== Example 2: TLS Connection ===")
	if err := tlsExample(ctx); err != nil {
		fmt.Printf("TLS example failed (expected if no TLS server): %v\n", err)
	}

	// Example 3: Cluster mode
	fmt.Println("\n=== Example 3: Cluster Mode ===")
	if err := clusterExample(ctx); err != nil {
		fmt.Printf("Cluster example failed (expected if no cluster): %v\n", err)
	}

	// Example 4: With metrics
	fmt.Println("\n=== Example 4: With Prometheus Metrics ===")
	if err := metricsExample(ctx); err != nil {
		fmt.Printf("Metrics example failed: %v\n", err)
	}

	// Example 5: With circuit breaker
	fmt.Println("\n=== Example 5: With Circuit Breaker ===")
	if err := circuitBreakerExample(ctx); err != nil {
		fmt.Printf("Circuit breaker example failed: %v\n", err)
	}

	return nil
}

func basicExample(ctx context.Context) error {
	// Create a new cache client
	cache, err := cachex.Open("redis", &cachex.Config{
		Backend:      "redis",
		Addrs:        []string{"localhost:6379"},
		Password:     os.Getenv("REDIS_PASSWORD"),
		PoolSize:     10,
		DB:           0,
		DialTimeout:  5,
		ReadTimeout:  3,
		WriteTimeout: 3,
	})
	if err != nil {
		return fmt.Errorf("failed to open cache: %w", err)
	}
	defer cache.Close()

	// Basic operations
	if err := cache.Set(ctx, "key1", []byte("value1")); err != nil {
		return fmt.Errorf("failed to set: %w", err)
	}

	val, err := cache.Get(ctx, "key1")
	if err != nil {
		return fmt.Errorf("failed to get: %w", err)
	}
	fmt.Printf("Got value: %s\n", string(val))

	// With expiration
	if err := cache.SetEX(ctx, "key2", []byte("value2"), 60); err != nil {
		return fmt.Errorf("failed to set with expiry: %w", err)
	}

	ttl, err := cache.TTL(ctx, "key2")
	if err != nil {
		return fmt.Errorf("failed to get TTL: %w", err)
	}
	fmt.Printf("TTL: %d seconds\n", ttl)

	// Atomic operations
	_, err = cache.Incr(ctx, "counter")
	if err != nil {
		return fmt.Errorf("failed to increment: %w", err)
	}

	counter, err := cache.Incr(ctx, "counter")
	if err != nil {
		return fmt.Errorf("failed to increment: %w", err)
	}
	fmt.Printf("Counter value: %d\n", counter)

	// Batch operations
	kvs := map[string][]byte{
		"batch1": []byte("value1"),
		"batch2": []byte("value2"),
		"batch3": []byte("value3"),
	}
	if err := cache.MSet(ctx, kvs); err != nil {
		return fmt.Errorf("failed to mset: %w", err)
	}

	values, err := cache.MGet(ctx, "batch1", "batch2", "batch3")
	if err != nil {
		return fmt.Errorf("failed to mget: %w", err)
	}
	fmt.Printf("Got %d values from batch\n", len(values))

	// Health check
	if err := cache.Ping(ctx); err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	fmt.Println("Health check: OK")

	return nil
}

func tlsExample(ctx context.Context) error {
	cache, err := cachex.Open("redis", &cachex.Config{
		Backend: "redis",
		Addrs:   []string{"localhost:6379"},
		TLS: cachex.TLSConfig{
			Enabled:            true,
			CAFile:             "/path/to/ca.crt",
			CertFile:           "/path/to/client.crt",
			KeyFile:            "/path/to/client.key",
			InsecureSkipVerify: false,
		},
	})
	if err != nil {
		return err
	}
	defer cache.Close()

	if err := cache.Ping(ctx); err != nil {
		return err
	}
	fmt.Println("TLS connection: OK")
	return nil
}

func clusterExample(ctx context.Context) error {
	cache, err := cachex.Open("redis", &cachex.Config{
		Backend:     "redis",
		Addrs:       []string{"localhost:7000", "localhost:7001", "localhost:7002"},
		ClusterMode: true,
	})
	if err != nil {
		return err
	}
	defer cache.Close()

	// Set and get operations work across cluster
	if err := cache.Set(ctx, "cluster-key", []byte("cluster-value")); err != nil {
		return err
	}

	val, err := cache.Get(ctx, "cluster-key")
	if err != nil {
		return err
	}
	fmt.Printf("Cluster value: %s\n", string(val))
	return nil
}

func metricsExample(ctx context.Context) error {
	// Create metrics collector
	metrics := observability.NewMetricsCollector("cachex", "redis")

	// Create cache with metrics observer
	factory := cachex.NewFactory()
	factory.AddObserver(metrics)

	cache, err := factory.Create("redis", &cachex.Config{
		Backend: "redis",
		Addrs:   []string{"localhost:6379"},
	})
	if err != nil {
		return err
	}
	defer cache.Close()

	// Perform some operations
	for i := 0; i < 100; i++ {
		cache.Set(ctx, fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("value-%d", i)))
		cache.Get(ctx, fmt.Sprintf("key-%d", i))
	}

	// Get stats
	stats := cache.Stats()
	fmt.Printf("Cache stats - Hits: %d, Misses: %d, Errors: %d\n",
		stats.Hits(), stats.Misses(), stats.Errors())

	return nil
}

func circuitBreakerExample(ctx context.Context) error {
	// Create circuit breaker
	cb := observability.NewCircuitBreaker("redis",
		observability.WithThreshold(5),
		observability.WithTimeout(30*time.Second),
		observability.WithHalfOpenMaxRequests(3),
	)

	// Create cache
	cache, err := cachex.Open("redis", &cachex.Config{
		Backend: "redis",
		Addrs:   []string{"localhost:6379"},
	})
	if err != nil {
		return err
	}
	defer cache.Close()

	// Wrap with circuit breaker
	protectedCache := observability.WrapCacheWithCircuitBreaker(cache, cb)

	// Use the protected cache
	for i := 0; i < 10; i++ {
		err := cb.Execute(ctx, func() error {
			return protectedCache.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
		})
		if err != nil {
			if observability.IsCircuitOpenError(err) {
				fmt.Println("Circuit is open, request rejected")
			} else {
				fmt.Printf("Request failed: %v\n", err)
			}
		}
	}

	// Print circuit breaker metrics
	metrics := cb.Metrics()
	fmt.Printf("Circuit breaker state: %s, failures: %d\n", metrics.State, metrics.Failures)

	return nil
}

// loadConfig loads configuration from environment or defaults.
func loadConfig() *cachex.Config {
	cfg := cachex.DefaultConfig(cachex.BackendRedis)

	// Override with environment variables
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		cfg.Addrs = []string{addr}
	}
	if pwd := os.Getenv("REDIS_PASSWORD"); pwd != "" {
		cfg.Password = pwd
	}

	return cfg
}
