//go:build ignore
// +build ignore

package cachex

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/gospacex/cachex/backends/embedded/badger"
	_ "github.com/gospacex/cachex/backends/embedded/bbolt"
	_ "github.com/gospacex/cachex/backends/embedded/pebble"
)

// Benchmark helpers
func setupBadgerBench(b *testing.B) (*Config, func()) {
	tmpDir := "/tmp/cachex-bench-badger-" + time.Now().Format("20060102150405")
	os.MkdirAll(tmpDir, 0755)
	cfg := &Config{
		Backend:  BackendBadger,
		Dir:      tmpDir,
		InMemory: true,
	}
	return cfg, func() { os.RemoveAll(tmpDir) }
}

func setupBBoltBench(b *testing.B) (*Config, func()) {
	tmpDir := "/tmp/cachex-bench-bbolt-" + time.Now().Format("20060102150405")
	os.MkdirAll(tmpDir, 0755)
	cfg := &Config{
		Backend:    BackendBBolt,
		Dir:        tmpDir + "/bbolt.db",
		BucketName: "cachex",
	}
	return cfg, func() { os.RemoveAll(tmpDir) }
}

func setupPebbleBench(b *testing.B) (*Config, func()) {
	tmpDir := "/tmp/cachex-bench-pebble-" + time.Now().Format("20060102150405")
	os.MkdirAll(tmpDir, 0755)
	cfg := &Config{
		Backend: "pebble",
		Dir:     tmpDir,
	}
	return cfg, func() { os.RemoveAll(tmpDir) }
}

// =============================================================================
// Badger Benchmarks
// =============================================================================

func BenchmarkBadgerSet(b *testing.B) {
	cfg, cleanup := setupBadgerBench(b)
	defer cleanup()

	cache, err := Open(BackendBadger, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
	}
}

func BenchmarkBadgerGet(b *testing.B) {
	cfg, cleanup := setupBadgerBench(b)
	defer cleanup()

	cache, err := Open(BackendBadger, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()

	// Pre-populate
	for i := 0; i < b.N; i++ {
		cache.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Get(ctx, fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkBadgerMSet(b *testing.B) {
	cfg, cleanup := setupBadgerBench(b)
	defer cleanup()

	cache, err := Open(BackendBadger, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()
	batchSize := 100

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i += batchSize {
		kvs := make(map[string][]byte, batchSize)
		for j := 0; j < batchSize && i+j < b.N; j++ {
			kvs[fmt.Sprintf("key-%d", i+j)] = []byte("value")
		}
		cache.MSet(ctx, kvs)
	}
}

func BenchmarkBadgerMGet(b *testing.B) {
	cfg, cleanup := setupBadgerBench(b)
	defer cleanup()

	cache, err := Open(BackendBadger, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()
	batchSize := 100

	// Pre-populate
	kvs := make(map[string][]byte, b.N)
	for i := 0; i < b.N; i++ {
		kvs[fmt.Sprintf("key-%d", i)] = []byte("value")
	}
	cache.MSet(ctx, kvs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i += batchSize {
		keys := make([]string, 0, batchSize)
		for j := 0; j < batchSize && i+j < b.N; j++ {
			keys = append(keys, fmt.Sprintf("key-%d", i+j))
		}
		cache.MGet(ctx, keys...)
	}
}

// =============================================================================
// BBolt Benchmarks
// =============================================================================

func BenchmarkBBoltSet(b *testing.B) {
	cfg, cleanup := setupBBoltBench(b)
	defer cleanup()

	cache, err := Open(BackendBBolt, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
	}
}

func BenchmarkBBoltGet(b *testing.B) {
	cfg, cleanup := setupBBoltBench(b)
	defer cleanup()

	cache, err := Open(BackendBBolt, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()

	// Pre-populate
	for i := 0; i < b.N; i++ {
		cache.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Get(ctx, fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkBBoltIncr(b *testing.B) {
	cfg, cleanup := setupBBoltBench(b)
	defer cleanup()

	cache, err := Open(BackendBBolt, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Incr(ctx, "counter")
	}
}

// =============================================================================
// Pebble Benchmarks
// =============================================================================

func BenchmarkPebbleSet(b *testing.B) {
	cfg, cleanup := setupPebbleBench(b)
	defer cleanup()

	cache, err := Open(BackendPebble, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
	}
}

func BenchmarkPebbleGet(b *testing.B) {
	cfg, cleanup := setupPebbleBench(b)
	defer cleanup()

	cache, err := Open(BackendPebble, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()

	// Pre-populate
	for i := 0; i < b.N; i++ {
		cache.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Get(ctx, fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkPebbleMSet(b *testing.B) {
	cfg, cleanup := setupPebbleBench(b)
	defer cleanup()

	cache, err := Open(BackendPebble, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()
	batchSize := 100

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i += batchSize {
		kvs := make(map[string][]byte, batchSize)
		for j := 0; j < batchSize && i+j < b.N; j++ {
			kvs[fmt.Sprintf("key-%d", i+j)] = []byte("value")
		}
		cache.MSet(ctx, kvs)
	}
}

func BenchmarkPebbleIncr(b *testing.B) {
	cfg, cleanup := setupPebbleBench(b)
	defer cleanup()

	cache, err := Open(BackendPebble, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Incr(ctx, "counter")
	}
}

// =============================================================================
// Cross-backend Comparison
// =============================================================================

func BenchmarkCompareSet(b *testing.B) {
	backends := []string{BackendBadger, BackendBBolt, BackendPebble}

	for _, backend := range backends {
		b.Run(backend, func(b *testing.B) {
			var cfg *Config
			var cleanup func()

			switch backend {
			case BackendBadger:
				cfg, cleanup = setupBadgerBench(b)
			case BackendBBolt:
				cfg, cleanup = setupBBoltBench(b)
			case BackendPebble:
				cfg, cleanup = setupPebbleBench(b)
			}
			defer cleanup()

			cache, err := Open(backend, cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer cache.Close()

			ctx := context.Background()
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				cache.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
			}
		})
	}
}

func BenchmarkCompareGet(b *testing.B) {
	backends := []string{BackendBadger, BackendBBolt, BackendPebble}

	for _, backend := range backends {
		b.Run(backend, func(b *testing.B) {
			var cfg *Config
			var cleanup func()

			switch backend {
			case BackendBadger:
				cfg, cleanup = setupBadgerBench(b)
			case BackendBBolt:
				cfg, cleanup = setupBBoltBench(b)
			case BackendPebble:
				cfg, cleanup = setupPebbleBench(b)
			}
			defer cleanup()

			cache, err := Open(backend, cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer cache.Close()

			ctx := context.Background()

			// Pre-populate
			for i := 0; i < b.N; i++ {
				cache.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				cache.Get(ctx, fmt.Sprintf("key-%d", i))
			}
		})
	}
}

// =============================================================================
// Mixed Workload Benchmark
// =============================================================================

func BenchmarkMixedWorkload(b *testing.B) {
	cfg, cleanup := setupBadgerBench(b)
	defer cleanup()

	cache, err := Open(BackendBadger, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		op := i % 10
		key := fmt.Sprintf("key-%d", i)

		switch op {
		case 0, 1, 2: // 30% GET
			cache.Get(ctx, key)
		case 3, 4: // 20% SET
			cache.Set(ctx, key, []byte("value"))
		case 5: // 10% DELETE
			cache.Delete(ctx, key)
		case 6, 7: // 20% EXISTS
			cache.Exists(ctx, key)
		case 8: // 10% KEYS
			cache.Keys(ctx, "key-*")
		case 9: // 10% INCR
			cache.Incr(ctx, "counter")
		}
	}
}
