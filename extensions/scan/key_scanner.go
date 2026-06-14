// Package scan provides iterator-based key scanning to prevent OOM on large datasets.
package scan

import (
	"context"
	"sync"

	"github.com/gospacex/cachex"
)

// KeyScanner implements Scanner by wrapping a cachex.Cache instance.
// It provides streaming iteration over keys using backend-specific optimizations:
// - Redis: Uses SCAN command with cursor-based iteration
// - Embedded backends: Uses Keys() with chunked streaming
type KeyScanner struct {
	cache cachex.Cache
}

// NewKeyScanner creates a new KeyScanner wrapping the given cache.
func NewKeyScanner(cache cachex.Cache) *KeyScanner {
	return &KeyScanner{cache: cache}
}

// Scan implements Scanner interface.
func (s *KeyScanner) Scan(ctx context.Context, pattern string, batchSize int) <-chan []string {
	ch := make(chan []string, 1)

	go func() {
		defer close(ch)

		// Try Redis SCAN first
		if s.tryRedisScan(ctx, pattern, batchSize, ch) {
			return
		}

		// Fallback to chunked Keys() scanning
		s.chunkedScan(ctx, pattern, batchSize, ch)
	}()

	return ch
}

// tryRedisScan attempts Redis SCAN if the backend supports it.
// Returns true if Redis SCAN was used, false otherwise.
func (s *KeyScanner) tryRedisScan(ctx context.Context, pattern string, batchSize int, ch chan<- []string) bool {
	// Try type assertion for the Scan method
	var scanFunc func(ctx context.Context, cursor uint64, match string, count int64) (<-chan string, error)

	switch c := s.cache.(type) {
	case interface {
		Scan(ctx context.Context, cursor uint64, match string, count int64) (<-chan string, error)
	}:
		scanFunc = c.Scan
	}

	if scanFunc == nil {
		return false
	}

	batch := int64(batchSize)
	if batch <= 0 {
		batch = 100
	}

	keysCh, err := scanFunc(ctx, 0, pattern, batch)
	if err != nil {
		return true
	}

	// Collect keys in batches
	var batchKeys []string
	for key := range keysCh {
		batchKeys = append(batchKeys, key)
		if len(batchKeys) >= int(batch) {
			select {
			case ch <- batchKeys:
				batchKeys = nil
			case <-ctx.Done():
				return true
			}
		}
	}

	if len(batchKeys) > 0 {
		select {
		case ch <- batchKeys:
		case <-ctx.Done():
		}
	}

	return true
}

// chunkedScan uses Keys() with chunking for backends without SCAN support.
func (s *KeyScanner) chunkedScan(ctx context.Context, pattern string, batchSize int, ch chan<- []string) {
	if batchSize <= 0 {
		batchSize = 1000
	}

	// Get all keys first (embedded backends don't have SCAN)
	keys, err := s.cache.Keys(ctx, pattern)
	if err != nil {
		return
	}

	// Stream in batches
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		batch := keys[i:end]
		select {
		case ch <- batch:
		case <-ctx.Done():
			return
		}
	}
}

// ScanAll implements Scanner interface.
func (s *KeyScanner) ScanAll(ctx context.Context, pattern string) ([]string, error) {
	var allKeys []string
	ch := s.Scan(ctx, pattern, 100)

	for keys := range ch {
		allKeys = append(allKeys, keys...)
	}

	return allKeys, ctx.Err()
}

// embeddedIterator provides iteration for embedded backends.
type embeddedIterator struct {
	mu      sync.Mutex
	ctx     context.Context
	pattern string
	batch   []string
	pos     int
	closeFn func() error
}

func (i *embeddedIterator) Next() bool {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.pos++
	return i.pos < len(i.batch)
}

func (i *embeddedIterator) Key() string {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.pos >= 0 && i.pos < len(i.batch) {
		return i.batch[i.pos]
	}
	return ""
}

func (i *embeddedIterator) Close() error {
	if i.closeFn != nil {
		return i.closeFn()
	}
	return nil
}

// Compile-time interface check.
var _ Scanner = (*KeyScanner)(nil)
