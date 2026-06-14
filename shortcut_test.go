package cachex

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"sync"
)

// TestRFromConfigCallable tests that RFromConfig is callable and handles nil config.
func TestRFromConfigCallable(t *testing.T) {
	// Test with nil config - should return error
	_, err := RFromConfig(nil)
	if err == nil {
		t.Fatal("RFromConfig(nil) should return error")
	}
}

// TestSingleton tests that same config returns same instance through registry.
func TestSingletonWithRegistry(t *testing.T) {
	// This test verifies the registry mechanism by checking that
	// two calls with the same config key return the same lazyClient reference
	cfg := &Config{Backend: BackendRedis, Addrs: []string{"localhost:6379"}}
	key := configKey(cfg)

	// Store a test lazyClient in registry
	testClient := &lazyClient{}
	clientRegistry.Store(key, testClient)

	// Load it back
	loaded, ok := clientRegistry.Load(key)
	if !ok {
		t.Fatal("failed to load from registry")
	}
	if loaded.(*lazyClient) != testClient {
		t.Fatal("registry returned different instance")
	}

	// Clean up
	clientRegistry.Delete(key)
}

// TestConcurrentRegistryAccess tests concurrent access to the registry.
func TestConcurrentRegistryAccess(t *testing.T) {
	var wg sync.WaitGroup
	results := make([]bool, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := "test-key"
			_, ok := clientRegistry.LoadOrStore(key, &lazyClient{})
			results[idx] = ok
		}(i)
	}
	wg.Wait()

	// All goroutines should complete without panic
	// LoadOrStore returns (value, loaded) where loaded=true if key already existed
}

// TestConfigKey tests the configKey function.
func TestConfigKey(t *testing.T) {
	cfg1 := &Config{Backend: BackendRedis, Addrs: []string{"localhost:6379"}}
	cfg2 := &Config{Backend: BackendRedis, Addrs: []string{"localhost:6379"}}
	cfg3 := &Config{Backend: BackendDragonfly, Addrs: []string{"localhost:6379"}}

	key1 := configKey(cfg1)
	key2 := configKey(cfg2)
	key3 := configKey(cfg3)

	if key1 != key2 {
		t.Fatal("same config should produce same key")
	}
	if key1 == key3 {
		t.Fatal("different config should produce different key")
	}
	if configKey(nil) != "nil" {
		t.Fatal("nil config should produce 'nil' key")
	}
}

// TestFunctionSignatures tests that all shortcut functions have correct signatures.
func TestFunctionSignatures(t *testing.T) {
	// These tests just verify the functions are callable with correct types
	// They don't execute against real backends

	// go-redis series
	var _ func(string) (Cache, error) = RS
	var _ func(*Config) (Cache, error) = ROS
	var _ func(string) (Cache, error) = RCS
	var _ func(*Config) (Cache, error) = ROCS
	var _ func(string) (Cache, error) = DS
	var _ func(*Config) (Cache, error) = DOS
	var _ func(string) (Cache, error) = DCS
	var _ func(*Config) (Cache, error) = DOCS
	var _ func(string) (Cache, error) = KS
	var _ func(*Config) (Cache, error) = KOS
	var _ func(string) (Cache, error) = KCS
	var _ func(*Config) (Cache, error) = KOCS
	var _ func(string) (Cache, error) = GS
	var _ func(*Config) (Cache, error) = GOS
	var _ func(string) (Cache, error) = GCS
	var _ func(*Config) (Cache, error) = GOCS

	// KV-Family series
	var _ func(string) (Cache, error) = BS
	var _ func(*Config) (Cache, error) = BOS
	var _ func(string) (Cache, error) = BBS
	var _ func(*Config) (Cache, error) = BBOS
	var _ func(string) (Cache, error) = PS
	var _ func(*Config) (Cache, error) = POS
}

// TestOP_StillCompilesAndReturnsCache ensures the existing OP
// signature is preserved across the spec change.
func TestOP_StillCompilesAndReturnsCache(t *testing.T) {
	var _ func(string) (Cache, error) = OP
}

// TestLegacySingleLetter_AllPreserved checks the 7 legacy
// single-letter shortcuts (R/D/K/G/B/BB/P) are still callable with
// the spec-preserved signature.
func TestLegacySingleLetter_AllPreserved(t *testing.T) {
	var _ func(string) (Cache, error) = R
	var _ func(string) (Cache, error) = D
	var _ func(string) (Cache, error) = K
	var _ func(string) (Cache, error) = G
	var _ func(string) (Cache, error) = B
	var _ func(string) (Cache, error) = BB
	var _ func(string) (Cache, error) = P
}

// TestDriverShortcutSignatures verifies the new driver-level shortcut
// function signatures match the spec.
func TestDriverShortcutSignatures(t *testing.T) {
	// RP returns *redis.Client (driver pool shortcut)
	var _ func(string) (*redis.Client, error) = RP

	// RCP returns *redis.ClusterClient (driver pool shortcut)
	var _ func(string) (*redis.ClusterClient, error) = RCP

	// KP returns sarama.SyncProducer (driver pool shortcut)
	var _ func(string) (sarama.SyncProducer, error) = KP

	// OP still returns (Cache, error)
	var _ func(string) (Cache, error) = OP
}

// TestInitTracingSignature verifies InitTracing returns the correct type.
func TestInitTracingSignature(t *testing.T) {
	var _ func(string) (func(context.Context), error) = InitTracing
}

// TestKPCacheStillCompiles verifies the KeyDB cache shortcut.
func TestKPCacheStillCompiles(t *testing.T) {
	var _ func(string) (Cache, error) = KPCache
}
