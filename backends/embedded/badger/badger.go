// Package badger provides a Badger backend for cachex.
package badger

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/gospacex/cachex"
)

// BackendName is the name of this backend.
const BackendName = "badger"

// creator implements cachex.BackendCreator for Badger.
type creator struct{}

func (c *creator) Create(cfg *cachex.Config) (cachex.Cache, error) {
	if cfg.Dir == "" && !cfg.InMemory {
		return nil, cachex.ErrDirRequired
	}

	dir := cfg.Dir
	if dir == "" && !cfg.InMemory {
		dir = "/tmp/badger"
	}

	opts := badger.DefaultOptions(dir)
	if !cfg.InMemory {
		opts.Dir = dir
		opts.ValueDir = cfg.ValueDir
	}
	if cfg.ValueDir != "" {
		opts.ValueDir = cfg.ValueDir
	}

	// Configure based on config
	opts.SyncWrites = cfg.SyncWrites
	opts.ReadOnly = cfg.ReadOnly
	opts.InMemory = cfg.InMemory
	opts.BlockCacheSize = cfg.BlockCacheSize
	opts.IndexCacheSize = cfg.IndexCacheSize
	opts.MemTableSize = cfg.MemTableSize

	// Performance tuning
	opts.BaseTableSize = 1 << 30 // 1GB
	opts.ValueLogFileSize = cfg.ValueLogFileSize
	if cfg.ValueLogFileSize == 0 {
		opts.ValueLogFileSize = 1 << 20 // 1MB default
	}

	opts.ValueThreshold = cfg.ValueThreshold
	if cfg.ValueThreshold == 0 {
		opts.ValueThreshold = 0 // All values in vlog
	}

	opts.BypassLockGuard = cfg.BypassLockGuard

	if !cfg.Compression {
		opts.Compression = options.None
	}

	if opts.BlockCacheSize == 0 {
		opts.BlockCacheSize = 64 << 20 // 64MB default
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger: %w", err)
	}

	return newBadgerCache(db), nil
}

// badgerCache implements cachex.Cache for Badger.
type badgerCache struct {
	db    *badger.DB
	stats *badgerStats
}

func newBadgerCache(db *badger.DB) *badgerCache {
	return &badgerCache{
		db:    db,
		stats: newBadgerStats(),
	}
}

// badgerStats implements cachex.Stats for Badger.
type badgerStats struct {
	hits   int64
	misses int64
	errors int64
}

func newBadgerStats() *badgerStats {
	return &badgerStats{}
}

func (s *badgerStats) Hits() int64    { return atomic.LoadInt64(&s.hits) }
func (s *badgerStats) Misses() int64  { return atomic.LoadInt64(&s.misses) }
func (s *badgerStats) Errors() int64  { return atomic.LoadInt64(&s.errors) }
func (s *badgerStats) Latency() int64 { return 0 }

func (c *badgerCache) Get(ctx context.Context, key string) ([]byte, error) {
	var val []byte
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			atomic.AddInt64(&c.stats.misses, 1)
			return cachex.ErrKeyNotFound
		}
		if err != nil {
			atomic.AddInt64(&c.stats.errors, 1)
			return err
		}
		val, err = item.ValueCopy(nil)
		if err != nil {
			atomic.AddInt64(&c.stats.errors, 1)
			return err
		}
		atomic.AddInt64(&c.stats.hits, 1)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (c *badgerCache) Set(ctx context.Context, key string, value []byte) error {
	err := c.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return err
}

func (c *badgerCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	err := c.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), value).WithTTL(time.Duration(ttlSeconds) * time.Second)
		return txn.SetEntry(e)
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return err
}

func (c *badgerCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	var exists bool
	err := c.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err == nil {
			exists = false
			return nil
		}
		if err == badger.ErrKeyNotFound {
			exists = true
			var e *badger.Entry
			if ttlSeconds > 0 {
				e = badger.NewEntry([]byte(key), value).WithTTL(time.Duration(ttlSeconds) * time.Second)
			} else {
				e = badger.NewEntry([]byte(key), value)
			}
			return txn.SetEntry(e)
		}
		return err
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return exists, err
}

func (c *badgerCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	var deleted int64
	err := c.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			err := txn.Delete([]byte(key))
			if err == nil {
				deleted++
			}
		}
		return nil
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return deleted, err
}

func (c *badgerCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	var exists int64
	err := c.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			_, err := txn.Get([]byte(key))
			if err == nil {
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

func (c *badgerCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	var value []byte
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		var err2 error
		value, err2 = item.ValueCopy(nil)
		return err2
	})
	if err != nil {
		return false, err
	}

	err = c.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), value).WithTTL(time.Duration(ttlSeconds) * time.Second)
		return txn.SetEntry(e)
	})
	return err == nil, err
}

func (c *badgerCache) TTL(ctx context.Context, key string) (int64, error) {
	var ttl int64 = -2
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		expiresAt := item.ExpiresAt()
		if expiresAt == 0 {
			ttl = -1
		} else {
			ttl = int64(expiresAt) - time.Now().Unix()
		}
		return nil
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return -2, nil
		}
		return -2, err
	}
	return ttl, nil
}

func (c *badgerCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	result := make([][]byte, len(keys))
	err := c.db.View(func(txn *badger.Txn) error {
		for i, key := range keys {
			item, err := txn.Get([]byte(key))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result[i] = val
		}
		return nil
	})
	if err != nil {
		atomic.AddInt64(&c.stats.errors, 1)
	}
	return result, err
}

func (c *badgerCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	err := c.db.Update(func(txn *badger.Txn) error {
		for k, v := range kvs {
			if err := txn.Set([]byte(k), v); err != nil {
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

func (c *badgerCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	var keys []string
	iterOpts := badger.DefaultIteratorOptions
	iterOpts.PrefetchValues = false

	err := c.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(iterOpts)
		defer iter.Close()

		prefix := []byte(pattern)
		if pattern != "*" && !hasWildcard(pattern) {
			prefix = append(prefix, '*')
		}

		for iter.Seek(prefix); iter.Valid(); iter.Next() {
			key := iter.Item().Key()
			keys = append(keys, string(key))
		}
		return nil
	})

	return keys, err
}

func (c *badgerCache) Incr(ctx context.Context, key string) (int64, error) {
	return 0, cachex.ErrNotSupported
}

func (c *badgerCache) Decr(ctx context.Context, key string) (int64, error) {
	return 0, cachex.ErrNotSupported
}

func (c *badgerCache) Ping(ctx context.Context) error {
	return c.db.View(func(txn *badger.Txn) error { return nil })
}

func (c *badgerCache) Close() error {
	return c.db.Close()
}

func (c *badgerCache) Stats() cachex.Stats {
	return c.stats
}

func hasWildcard(s string) bool {
	for _, c := range s {
		if c == '*' || c == '?' {
			return true
		}
	}
	return false
}

// Singleton support
var (
	singleton    *badgerCache
	singletonMu  sync.Once
	singletonErr error
)

// Open opens a Badger database with default settings.
func Open(dir string) (*badgerCache, error) {
	singletonMu.Do(func() {
		cfg := cachex.DefaultConfig(cachex.BackendBadger)
		cfg.Dir = dir

		creator := &creator{}
		cache, err := creator.Create(cfg)
		if err != nil {
			singletonErr = err
			return
		}
		singleton = cache.(*badgerCache)
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
var _ cachex.Cache = (*badgerCache)(nil)
