// Package bloom provides Bloom filter implementation for cachex.
package bloom

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
)

// Filter implements a Bloom filter for cache operations.
type Filter struct {
	bitset    []bool
	size      uint64
	hashCount int
}

// New creates a new Bloom filter.
// n: expected number of elements
// fpRate: false positive rate (0.0 to 1.0)
func New(n int, fpRate float64) *Filter {
	// Calculate optimal size and hash count
	size := optimalSize(uint64(n), fpRate)
	hashCount := optimalHashCount(uint64(n), size)

	return &Filter{
		bitset:    make([]bool, size),
		size:      size,
		hashCount: hashCount,
	}
}

// Add adds an element to the filter.
func (f *Filter) Add(data []byte) {
	hashes := f.getHashes(data)
	for _, h := range hashes {
		f.bitset[h%uint64(len(f.bitset))] = true
	}
}

// Test checks if an element might be in the set.
// Returns true if possibly present, false if definitely not.
func (f *Filter) Test(data []byte) bool {
	hashes := f.getHashes(data)
	for _, h := range hashes {
		if !f.bitset[h%uint64(len(f.bitset))] {
			return false
		}
	}
	return true
}

// Clear clears the filter.
func (f *Filter) Clear() {
	for i := range f.bitset {
		f.bitset[i] = false
	}
}

// Size returns the size of the bitset.
func (f *Filter) Size() uint64 {
	return uint64(len(f.bitset))
}

// Count returns the number of bits set to true.
func (f *Filter) Count() int {
	count := 0
	for _, bit := range f.bitset {
		if bit {
			count++
		}
	}
	return count
}

// getHashes generates k hash values for the data.
func (f *Filter) getHashes(data []byte) []uint64 {
	hashes := make([]uint64, f.hashCount)

	// Double hashing technique
	h1 := sha256.Sum256(data)
	h2 := sha256.Sum256(append(data, 0xFF))

	v1 := binary.BigEndian.Uint64(h1[:8])
	v2 := binary.BigEndian.Uint64(h2[:8])

	for i := 0; i < f.hashCount; i++ {
		hashes[i] = v1 + uint64(i)*v2
	}

	return hashes
}

// optimalSize calculates the optimal bitset size.
func optimalSize(n uint64, fpRate float64) uint64 {
	return uint64(math.Ceil(-1 * float64(n) * math.Log(fpRate) / (math.Log(2) * math.Log(2))))
}

// optimalHashCount calculates the optimal number of hash functions.
func optimalHashCount(n, m uint64) int {
	if n == 0 {
		return 1
	}
	return int(math.Ceil((float64(m) / float64(n)) * math.Log(2)))
}

// CacheBloomFilter wraps a cache with Bloom filter for efficient existence checks.
type CacheBloomFilter struct {
	cache interface {
		Get(key string) (string, error)
	}
	filter *Filter
}

// NewCacheBloomFilter creates a new cache-aware Bloom filter.
// Note: This is a simplified version; full implementation would integrate with cachex.Cache
func NewCacheBloomFilter(expectedItems int, fpRate float64) *CacheBloomFilter {
	return &CacheBloomFilter{
		filter: New(expectedItems, fpRate),
	}
}

// Add adds an item to the Bloom filter.
func (c *CacheBloomFilter) Add(data []byte) {
	c.filter.Add(data)
}

// MightContain checks if an item might be in the filter.
func (c *CacheBloomFilter) MightContain(data []byte) bool {
	return c.filter.Test(data)
}

// Clear clears the Bloom filter.
func (c *CacheBloomFilter) Clear() {
	c.filter.Clear()
}

// Stats returns filter statistics.
func (c *CacheBloomFilter) Stats() map[string]interface{} {
	return map[string]interface{}{
		"size":  c.filter.Size(),
		"count": c.filter.Count(),
		"load":  float64(c.filter.Count()) / float64(c.filter.Size()),
	}
}
