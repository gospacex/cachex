// Package main demonstrates advanced cachex usage patterns.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/cachex/extensions/distlock"
	"github.com/gospacex/cachex/extensions/healthcheck"
	"github.com/gospacex/cachex/extensions/ratelimit"
	"github.com/gospacex/cachex/extensions/retry"
	"github.com/gospacex/cachex/observability"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// ================================================================
	// Example 1: Production-ready cache with all observability
	// ================================================================
	fmt.Println("=== Example 1: Production Cache with Observability ===")
	cache, err := setupProductionCache(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup cache: %w", err)
	}
	defer cache.Close()

	// ================================================================
	// Example 2: Rate-limited cache
	// ================================================================
	fmt.Println("\n=== Example 2: Rate-Limited Cache ===")
	rateLimitedCache, err := setupRateLimitedCache(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup rate-limited cache: %w", err)
	}
	defer rateLimitedCache.Close()

	// ================================================================
	// Example 3: Distributed locking
	// ================================================================
	fmt.Println("\n=== Example 3: Distributed Locking ===")
	if err := demonstrateDistributedLock(ctx, cache); err != nil {
		fmt.Printf("Distributed lock example: %v (expected if no Redis)\n", err)
	}

	// ================================================================
	// Example 4: Health checks
	// ================================================================
	fmt.Println("\n=== Example 4: Health Checks ===")
	if err := demonstrateHealthChecks(ctx, cache); err != nil {
		fmt.Printf("Health check example: %v\n", err)
	}

	// ================================================================
	// Example 5: Retry with backoff
	// ================================================================
	fmt.Println("\n=== Example 5: Retry with Backoff ===")
	if err := demonstrateRetry(ctx, cache); err != nil {
		fmt.Printf("Retry example: %v\n", err)
	}

	// ================================================================
	// Example 6: Start metrics server
	// ================================================================
	fmt.Println("\n=== Example 6: Prometheus Metrics Server ===")
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server starting on :8080")
		log.Println(http.ListenAndServe(":8080", nil))
	}()

	// Wait for user input to exit
	fmt.Println("\nPress Enter to exit...")
	fmt.Scanln()

	return nil
}

func setupProductionCache(ctx context.Context) (cachex.Cache, error) {
	// Create metrics collector
	metrics := observability.NewMetricsCollector("cachex", "production")

	// Create logger
	logger := observability.NewLogger(
		observability.WithLevel(observability.LevelDebug),
		observability.WithFormat("json"),
	)

	// Create circuit breaker
	cb := observability.NewCircuitBreaker("redis",
		observability.WithThreshold(5),
		observability.WithTimeout(30*time.Second),
		observability.WithHalfOpenMaxRequests(3),
		observability.WithOnStateChange(func(name string, from, to observability.State) {
			logger.Info(ctx, "circuit breaker state changed", map[string]interface{}{
				"backend": name,
				"from":    from.String(),
				"to":      to.String(),
			})
		}),
	)

	// Create factory with observers
	factory := cachex.NewFactory()
	factory.AddObserver(metrics)
	factory.AddObserver(observability.NewLoggingObserver(logger))

	// Load config
	cfg := cachex.DefaultConfig(cachex.BackendRedis)
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		cfg.Addrs = []string{addr}
	} else {
		cfg.Addrs = []string{"localhost:6379"}
	}
	cfg.Password = os.Getenv("REDIS_PASSWORD")
	cfg.PoolSize = 20
	cfg.MaxRetries = 3
	cfg.CircuitBreaker.Enabled = true
	cfg.Metrics = true
	cfg.MetricsPrefix = "cachex_production"

	// Create cache
	cache, err := factory.Create(cachex.BackendRedis, cfg)
	if err != nil {
		return nil, err
	}

	// Wrap with circuit breaker
	return observability.WrapCacheWithCircuitBreaker(cache, cb), nil
}

func setupRateLimitedCache(ctx context.Context) (cachex.Cache, error) {
	// Use embedded cache for demo
	cfg := cachex.DefaultConfig(cachex.BackendBadger)
	cfg.InMemory = true

	cache, err := cachex.Open(cachex.BackendBadger, cfg)
	if err != nil {
		return nil, err
	}

	// Wrap with rate limiter (100 requests/sec, burst of 200)
	return ratelimit.NewRateLimitedCache(cache, 200, 100), nil
}

func demonstrateDistributedLock(ctx context.Context, cache cachex.Cache) error {
	lockMgr := distlock.NewDistributedLock(cache)

	// Acquire lock
	lock, err := lockMgr.Lock(ctx, "my-resource-lock", 30*time.Second)
	if err != nil {
		return err
	}

	fmt.Printf("Lock acquired: %v\n", lock.IsAcquired())

	// Do work
	fmt.Println("Doing critical work...")
	time.Sleep(time.Second)

	// Release lock
	if err := lock.Release(ctx); err != nil {
		return err
	}
	fmt.Println("Lock released")

	return nil
}

func demonstrateHealthChecks(ctx context.Context, cache cachex.Cache) error {
	checker := healthcheck.NewHealthChecker(cache)

	// Add custom health check
	checker.AddCheck("data_integrity", func(ctx context.Context) error {
		// Check if we can write and read
		testKey := "health:check:" + fmt.Sprintf("%d", time.Now().Unix())
		if err := cache.Set(ctx, testKey, []byte("health_check_value")); err != nil {
			return fmt.Errorf("write failed: %w", err)
		}
		val, err := cache.Get(ctx, testKey)
		if err != nil {
			return fmt.Errorf("read failed: %w", err)
		}
		if string(val) != "health_check_value" {
			return fmt.Errorf("data integrity check failed")
		}
		cache.Delete(ctx, testKey)
		return nil
	})

	// Run health checks
	results := checker.Check(ctx)
	fmt.Println("Health check results:")
	for _, result := range results {
		fmt.Printf("  [%s] %s: %s (latency: %v)\n", result.Status, result.Name, result.Message, result.Latency)
	}

	// Full check
	if err := checker.CheckAll(ctx); err != nil {
		fmt.Printf("Overall health: UNHEALTHY - %v\n", err)
	} else {
		fmt.Println("Overall health: HEALTHY")
	}

	return nil
}

func demonstrateRetry(ctx context.Context, cache cachex.Cache) error {
	// Wrap with retry
	retryCfg := &retry.Config{
		MaxAttempts:    5,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2.0,
		Jitter:         true,
	}

	retryableCache := retry.NewRetryableCache(cache, retryCfg)

	// Try to set with retry
	err := retryableCache.Set(ctx, "retry:key", []byte("retry:value"))
	if err != nil {
		return fmt.Errorf("retry set failed: %w", err)
	}

	// Try to get with retry
	val, err := retryableCache.Get(ctx, "retry:key")
	if err != nil {
		return fmt.Errorf("retry get failed: %w", err)
	}

	fmt.Printf("Retried operation succeeded: %s\n", string(val))
	return nil
}

// Example: HTTP handler with caching
func httpCacheHandler(cache cachex.Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		key := r.URL.Path

		// Try to get from cache
		val, err := cache.Get(ctx, key)
		if err == nil {
			w.Header().Set("X-Cache", "HIT")
			w.Write(val)
			return
		}

		// Cache miss - generate content
		w.Header().Set("X-Cache", "MISS")
		content := []byte("Generated content for " + key)

		// Cache the result
		cache.Set(ctx, key, content)

		w.Write(content)
	}
}

// Example: Cache-aside pattern
func cacheAsidePattern(ctx context.Context, cache cachex.Cache, userID string) ([]byte, error) {
	cacheKey := "user:" + userID

	// Try cache first
	val, err := cache.Get(ctx, cacheKey)
	if err == nil {
		return val, nil
	}

	// Cache miss - fetch from "database"
	userData, err := fetchFromDatabase(userID)
	if err != nil {
		return nil, err
	}

	// Store in cache (with TTL)
	cache.SetEX(ctx, cacheKey, userData, 300) // 5 minutes

	return userData, nil
}

func fetchFromDatabase(userID string) ([]byte, error) {
	// Simulated database fetch
	return []byte(fmt.Sprintf(`{"id":"%s","name":"User %s"}`, userID, userID)), nil
}

// Example: Write-through caching
func writeThroughCache(ctx context.Context, cache cachex.Cache, key string, value []byte) error {
	// Write to cache first
	if err := cache.Set(ctx, key, value); err != nil {
		return err
	}

	// Then write to "database"
	return writeToDatabase(key, value)
}

func writeToDatabase(key string, value []byte) error {
	// Simulated database write
	time.Sleep(10 * time.Millisecond)
	return nil
}

// Example: Write-behind caching
func writeBehindCache(ctx context.Context, cache cachex.Cache, key string, value []byte) error {
	// Write to cache immediately
	if err := cache.Set(ctx, key, value); err != nil {
		return err
	}

	// Schedule database write asynchronously (would use a queue in production)
	go func() {
		writeToDatabase(key, value)
	}()

	return nil
}
