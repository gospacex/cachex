// Package pebble provides a Pebble backend for cachex.
package pebble

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/gospacex/cachex"
)

// BackendName is the name of this backend.
const BackendName = "pebble"

// creator implements cachex.BackendCreator for Pebble.
type creator struct{}

func (c *creator) Create(cfg *cachex.Config) (cachex.Cache, error) {
	if cfg.Dir == "" {
		return nil, cachex.ErrDirRequired
	}

	// Ensure directory exists
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	opts := &pebble.Options{
		ReadOnly:      cfg.ReadOnly,
		NoSyncOnClose: !cfg.SyncWrites,
	}

	// Configure cache
	if cfg.BlockCacheSize > 0 {
		opts.Cache = pebble.NewCache(cfg.BlockCacheSize)
	}

	// Configure memtable
	if cfg.MemTableSize > 0 {
		opts.MemTableSize = uint64(cfg.MemTableSize)
	}

	db, err := pebble.Open(cfg.Dir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble: %w", err)
	}

	return newPebbleCache(db), nil
}

// pebbleCache implements cachex.Cache for Pebble.
type pebbleCache struct {
	db    *pebble.DB
	stats *pebbleStats
}

func newPebbleCache(db *pebble.DB) *pebbleCache {
	return &pebbleCache{
		db:    db,
		stats: newPebbleStats(),
	}
}

// pebbleStats implements cachex.Stats for Pebble.
type pebbleStats struct {
	hits   int64
	misses int64
	errors int64
}

func newPebbleStats() *pebbleStats {
	return &pebbleStats{}
}

func (s *pebbleStats) Hits() int64    { return atomic.LoadInt64(&s.hits) }
func (s *pebbleStats) Misses() int64  { return atomic.LoadInt64(&s.misses) }
func (s *pebbleStats) Errors() int64  { return atomic.LoadInt64(&s.errors) }
func (s *pebbleStats) Latency() int64 { return 0 }

func (c *pebbleCache) Get(ctx context.Context, key string) ([]byte, error) {
	val, closer, err := c.db.Get([]byte(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			atomic.AddInt64(&c.stats.misses, 1)
			return nil, cachex.ErrKeyNotFound
		}
		atomic.AddInt64(&c.stats.errors, 1)
		return nil, err
	}
	defer closer.Close()

	result := make([]byte, len(val))
	copy(result, val)
	atomic.AddInt64(&c.stats.hits, 1)
	return result, nil
}

func (c *pebbleCache) Set(ctx context.Context, key string, value []byte) error {
	err := c.db.Set([]byte(key), value, pebble.NoSync)
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return err
}

func (c *pebbleCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	// Pebble doesn't support TTL natively
	return c.Set(ctx, key, value)
}

func (c *pebbleCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	// Check if key exists first
	_, closer, err := c.db.Get([]byte(key))
	if err == nil {
		closer.Close()
		return false, nil // Key exists
	}
	if err != pebble.ErrNotFound {
		atomic.AddInt64(&c.stats.errors, 1)
		return false, err
	}
	closer.Close()

	// Key doesn't exist, set it
	err = c.db.Set([]byte(key), value, pebble.NoSync)
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
		return false, err
	}
	return true, nil
}

func (c *pebbleCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	var deleted int64
	batch := c.db.NewBatch()
	defer batch.Close()

	for _, key := range keys {
		if err := batch.Delete([]byte(key), pebble.NoSync); err != nil {
			atomic.AddInt64(&c.stats.errors, 1)
			return deleted, err
		}
		deleted++
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return deleted, err
}

func (c *pebbleCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	var exists int64
	for _, key := range keys {
		_, closer, err := c.db.Get([]byte(key))
		if err == nil {
			closer.Close()
			exists++
		} else if err == pebble.ErrNotFound {
			// Not found, skip
		} else {
			atomic.AddInt64(&c.stats.errors, 1)
			return exists, err
		}
	}
	return exists, nil
}

func (c *pebbleCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	// Pebble doesn't support TTL natively
	return false, cachex.ErrNotSupported
}

func (c *pebbleCache) TTL(ctx context.Context, key string) (int64, error) {
	// Pebble doesn't support TTL natively
	return -1, cachex.ErrNotSupported
}

func (c *pebbleCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	result := make([][]byte, len(keys))

	for i, key := range keys {
		val, closer, err := c.db.Get([]byte(key))
		if err == pebble.ErrNotFound {
			continue
		}
		if err != nil {
			atomic.AddInt64(&c.stats.errors, 1)
			return result, err
		}
		result[i] = make([]byte, len(val))
		copy(result[i], val)
		closer.Close()
	}

	return result, nil
}

func (c *pebbleCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	batch := c.db.NewBatch()
	defer batch.Close()

	for k, v := range kvs {
		if err := batch.Set([]byte(k), v, pebble.NoSync); err != nil {
			atomic.AddInt64(&c.stats.errors, 1)
			return err
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return err
}

func (c *pebbleCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	var keys []string

	iter, _ := c.db.NewIter(nil)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if matchPattern(key, pattern) {
			keys = append(keys, key)
		}
	}

	return keys, iter.Error()
}

func (c *pebbleCache) Incr(ctx context.Context, key string) (int64, error) {
	// Get current value
	var current int64
	val, closer, err := c.db.Get([]byte(key))
	if err == nil {
		defer closer.Close()
		// Parse existing value
		if len(val) >= 8 {
			current = int64(binary.BigEndian.Uint64(val[:8]))
		}
	} else if err != pebble.ErrNotFound {
		atomic.AddInt64(&c.stats.errors, 1)
		return 0, err
	}

	newVal := current + 1
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(newVal))

	if err := c.db.Set([]byte(key), buf, pebble.NoSync); err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
		return 0, err
	}

	return newVal, nil
}

func (c *pebbleCache) Decr(ctx context.Context, key string) (int64, error) {
	// Get current value
	var current int64
	val, closer, err := c.db.Get([]byte(key))
	if err == nil {
		defer closer.Close()
		// Parse existing value
		if len(val) >= 8 {
			current = int64(binary.BigEndian.Uint64(val[:8]))
		}
	} else if err != pebble.ErrNotFound {
		atomic.AddInt64(&c.stats.errors, 1)
		return 0, err
	}

	newVal := current - 1
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(newVal))

	if err := c.db.Set([]byte(key), buf, pebble.NoSync); err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
		return 0, err
	}

	return newVal, nil
}

func (c *pebbleCache) Ping(ctx context.Context) error {
	iter, _ := c.db.NewIter(nil)
	defer iter.Close()
	return iter.Error()
}

func (c *pebbleCache) Close() error {
	return c.db.Close()
}

func (c *pebbleCache) Stats() cachex.Stats {
	return c.stats
}

// matchPattern matches a key against a glob pattern.
func matchPattern(key, pattern string) bool {
	if pattern == "*" {
		return true
	}

	parts := splitPattern(pattern)
	return matchParts(key, parts)
}

func splitPattern(pattern string) []string {
	var parts []string
	start := 0
	for i := 0; i <= len(pattern); i++ {
		if i == len(pattern) || pattern[i] == '*' {
			if i > start {
				parts = append(parts, pattern[start:i])
			}
			if i < len(pattern) {
				parts = append(parts, "*")
			}
			start = i + 1
		}
	}
	return parts
}

func matchParts(key string, parts []string) bool {
	if len(parts) == 0 {
		return key == ""
	}

	part := parts[0]
	if part == "*" {
		if len(parts) == 1 {
			return true
		}
		for i := 1; i < len(parts); i++ {
			if parts[i] != "*" {
				idx := findSubstring(key, parts[i])
				if idx == -1 {
					return false
				}
				return matchParts(key[idx+len(parts[i]):], parts[i+1:])
			}
		}
		return true
	}

	if !hasPrefix(key, part) {
		return false
	}
	return matchParts(key[len(part):], parts[1:])
}

func findSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// Singleton support
var (
	singleton    *pebbleCache
	singletonMu  sync.Once
	singletonErr error
)

// Open opens a Pebble database with default settings.
func Open(dir string) (*pebbleCache, error) {
	singletonMu.Do(func() {
		cfg := cachex.DefaultConfig(cachex.BackendPebble)
		cfg.Dir = dir

		creator := &creator{}
		cache, err := creator.Create(cfg)
		if err != nil {
			singletonErr = err
			return
		}
		singleton = cache.(*pebbleCache)
	})

	if singletonErr != nil {
		return nil, singletonErr
	}
	return singleton, nil
}

// Reset clears the singleton (for testing).
func Reset() {
	singletonMu = sync.Once{}
	singleton = nil
	singletonErr = nil
}

// Auto-registration
func init() {
	cachex.DefaultFactory.Register(BackendName, &creator{})
}

// Compile-time interface check.
var _ cachex.Cache = (*pebbleCache)(nil)
