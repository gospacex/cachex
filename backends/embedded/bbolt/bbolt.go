// Package bbolt provides a BBolt backend for cachex.
package bbolt

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/gospacex/cachex"
	bolt "go.etcd.io/bbolt"
)

// BackendName is the name of this backend.
const BackendName = "bbolt"

// creator implements cachex.BackendCreator for BBolt.
type creator struct{}

func (c *creator) Create(cfg *cachex.Config) (cachex.Cache, error) {
	if cfg.Dir == "" {
		return nil, cachex.ErrDirRequired
	}

	bucketName := cfg.BucketName
	if bucketName == "" {
		bucketName = "cachex"
	}

	// Ensure directory exists
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	db, err := bolt.Open(cfg.Dir, os.FileMode(cfg.FileMode), &bolt.Options{
		ReadOnly:        cfg.ReadOnly,
		NoGrowSync:      false,
		InitialMmapSize: int(cfg.MmapSize),
		NoSync:          !cfg.SyncWrites,
		FreelistType:    bolt.FreelistMapType,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt: %w", err)
	}

	// Ensure bucket exists
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	return newBBoltCache(db, bucketName), nil
}

// bboltCache implements cachex.Cache for BBolt.
type bboltCache struct {
	db         *bolt.DB
	bucketName string
	stats      *bboltStats
}

func newBBoltCache(db *bolt.DB, bucketName string) *bboltCache {
	return &bboltCache{
		db:         db,
		bucketName: bucketName,
		stats:      newBBoltStats(),
	}
}

// bboltStats implements cachex.Stats for BBolt.
type bboltStats struct {
	hits   int64
	misses int64
	errors int64
}

func newBBoltStats() *bboltStats {
	return &bboltStats{}
}

func (s *bboltStats) Hits() int64    { return atomic.LoadInt64(&s.hits) }
func (s *bboltStats) Misses() int64  { return atomic.LoadInt64(&s.misses) }
func (s *bboltStats) Errors() int64  { return atomic.LoadInt64(&s.errors) }
func (s *bboltStats) Latency() int64 { return 0 }

func (c *bboltCache) Get(ctx context.Context, key string) ([]byte, error) {
	var val []byte
	err := c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucketName))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		val = bucket.Get([]byte(key))
		if val == nil {
			atomic.AddInt64(&c.stats.misses, 1)
			return cachex.ErrKeyNotFound
		}
		atomic.AddInt64(&c.stats.hits, 1)
		return nil
	})
	if err != nil {
		if err == cachex.ErrKeyNotFound {
			return nil, err
		}
		atomic.AddInt64(&c.stats.errors, 1)
		return nil, err
	}
	// Copy the value since BBolt reuses the underlying bytes
	result := make([]byte, len(val))
	copy(result, val)
	return result, nil
}

func (c *bboltCache) Set(ctx context.Context, key string, value []byte) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucketName))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		return bucket.Put([]byte(key), value)
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return err
}

func (c *bboltCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	// BBolt doesn't support TTL natively, use special key format
	// Store TTL in key metadata or use a separate bucket
	return c.Set(ctx, key, value)
}

func (c *bboltCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	var exists bool
	err := c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucketName))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		existing := bucket.Get([]byte(key))
		if existing != nil {
			exists = false
			return nil
		}
		exists = true
		return bucket.Put([]byte(key), value)
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return exists, err
}

func (c *bboltCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	var deleted int64
	err := c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucketName))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		for _, key := range keys {
			if err := bucket.Delete([]byte(key)); err != nil {
				return err
			}
			deleted++
		}
		return nil
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return deleted, err
}

func (c *bboltCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	var exists int64
	err := c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucketName))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		for _, key := range keys {
			if bucket.Get([]byte(key)) != nil {
				exists++
			}
		}
		return nil
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return exists, err
}

func (c *bboltCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	// BBolt doesn't support TTL natively
	return false, cachex.ErrNotSupported
}

func (c *bboltCache) TTL(ctx context.Context, key string) (int64, error) {
	// BBolt doesn't support TTL natively
	return -1, cachex.ErrNotSupported
}

func (c *bboltCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	result := make([][]byte, len(keys))
	err := c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucketName))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		for i, key := range keys {
			val := bucket.Get([]byte(key))
			if val != nil {
				result[i] = make([]byte, len(val))
				copy(result[i], val)
			}
		}
		return nil
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return result, err
}

func (c *bboltCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucketName))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		for k, v := range kvs {
			if err := bucket.Put([]byte(k), v); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return err
}

func (c *bboltCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	var keys []string

	err := c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucketName))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}

		cursor := bucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			key := string(k)
			if matchPattern(key, pattern) {
				keys = append(keys, key)
			}
		}
		return nil
	})

	return keys, err
}

func (c *bboltCache) Incr(ctx context.Context, key string) (int64, error) {
	var result int64
	err := c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucketName))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}

		val := bucket.Get([]byte(key))
		if val == nil {
			result = 1
		} else {
			result = int64(binary.BigEndian.Uint64(val)) + 1
		}

		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(result))
		return bucket.Put([]byte(key), buf)
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return result, err
}

func (c *bboltCache) Decr(ctx context.Context, key string) (int64, error) {
	var result int64
	err := c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucketName))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}

		val := bucket.Get([]byte(key))
		if val == nil {
			result = -1
		} else {
			result = int64(binary.BigEndian.Uint64(val)) - 1
		}

		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(result))
		return bucket.Put([]byte(key), buf)
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return result, err
}

func (c *bboltCache) Ping(ctx context.Context) error {
	return c.db.View(func(tx *bolt.Tx) error { return nil })
}

func (c *bboltCache) Close() error {
	return c.db.Close()
}

func (c *bboltCache) Stats() cachex.Stats {
	return c.stats
}

// matchPattern matches a key against a glob pattern.
func matchPattern(key, pattern string) bool {
	if pattern == "*" {
		return true
	}

	// Simple pattern matching for *
	// Does not support full glob syntax
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
		// Find next non-* part
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
	singleton    *bboltCache
	singletonMu  sync.Once
	singletonErr error
)

// Open opens a BBolt database with default settings.
func Open(path string) (*bboltCache, error) {
	singletonMu.Do(func() {
		cfg := cachex.DefaultConfig(cachex.BackendBBolt)
		cfg.Dir = path

		creator := &creator{}
		cache, err := creator.Create(cfg)
		if err != nil {
			singletonErr = err
			return
		}
		singleton = cache.(*bboltCache)
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
var _ cachex.Cache = (*bboltCache)(nil)
