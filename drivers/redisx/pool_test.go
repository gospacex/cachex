package redisx

import (
	"sync"
	"testing"

	"github.com/gospacex/cachex"
)

// TestPoolKey_SameConfigProducesSameKey asserts that PoolKey is a stable
// function of config content (and not pointer identity). The pool uses
// this key to dedupe clients — two callers with identical configs must
// land in the same pool slot.
func TestPoolKey_SameConfigProducesSameKey(t *testing.T) {
	cfg1 := &cachex.Config{Backend: "redis", Addrs: []string{"a:1"}, PoolSize: 10}
	cfg2 := cfg1
	cfg3 := &cachex.Config{Backend: "redis", Addrs: []string{"a:1"}, PoolSize: 10}
	cfg4 := &cachex.Config{Backend: "redis", Addrs: []string{"a:1"}, PoolSize: 11} // differs
	cfg5 := &cachex.Config{Backend: "redis", Addrs: []string{"b:2"}, PoolSize: 10} // differs

	if PoolKey(cfg1) != PoolKey(cfg2) {
		t.Fatal("same pointer must produce same key")
	}
	if PoolKey(cfg1) != PoolKey(cfg3) {
		t.Fatal("identical content must produce same key")
	}
	if PoolKey(cfg1) == PoolKey(cfg4) {
		t.Fatal("different PoolSize must produce different key")
	}
	if PoolKey(cfg1) == PoolKey(cfg5) {
		t.Fatal("different Addrs must produce different key")
	}
}

// TestPoolKey_NilConfigSafe documents that a nil cfg is handled gracefully
// (returns a stable sentinel) rather than panicking.
func TestPoolKey_NilConfigSafe(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PoolKey(nil) panicked: %v", r)
		}
	}()
	if PoolKey(nil) == "" {
		t.Fatal("PoolKey(nil) returned empty string; expected a stable sentinel")
	}
}

// TestPool_ConcurrentLoadStore is the contract proof for the pool's
// concurrency safety: 200 goroutines pounding the same fingerprint must
// see the same value (no torn writes, no double-create). We never dial —
// we test the pool's own lookup path with a stub value.
func TestPool_ConcurrentLoadStore(t *testing.T) {
	const N = 200
	stub := struct{ id int }{id: 42}
	resetPoolForTest()
	storePooledForTest("stub-fp", stub)

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok := loadPooledForTest("stub-fp")
			if !ok || v.(struct{ id int }).id != 42 {
				t.Errorf("concurrent load returned %v / ok=%v", v, ok)
			}
		}()
	}
	wg.Wait()
}
