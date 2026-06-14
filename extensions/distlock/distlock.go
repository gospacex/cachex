// Package distlock provides distributed locking functionality for cachex.
package distlock

import (
	"context"
	"sync"
	"time"

	"github.com/gospacex/cachex"
)

// Lock represents a distributed lock.
type Lock struct {
	cache    cachex.Cache
	key      string
	value    string
	ttl      time.Duration
	acquired bool
	mu       sync.Mutex
}

// NewLock creates a new lock.
func NewLock(cache cachex.Cache, key, value string, ttl time.Duration) *Lock {
	return &Lock{
		cache: cache,
		key:   key,
		value: value,
		ttl:   ttl,
	}
}

// Acquire attempts to acquire the lock.
func (l *Lock) Acquire(ctx context.Context) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.acquired {
		return true, nil
	}

	set, err := l.cache.SetNX(ctx, l.key, []byte(l.value), int64(l.ttl.Seconds()))
	if err != nil {
		return false, err
	}

	l.acquired = set
	return set, nil
}

// Release releases the lock.
func (l *Lock) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.acquired {
		return nil
	}

	// Only release if we own the lock
	val, err := l.cache.Get(ctx, l.key)
	if err != nil {
		l.acquired = false
		return nil // Lock expired or doesn't exist
	}

	if string(val) == l.value {
		_, err = l.cache.Delete(ctx, l.key)
	}

	l.acquired = false
	return err
}

// Extend extends the lock TTL.
func (l *Lock) Extend(ctx context.Context, ttl time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.acquired {
		return nil
	}

	val, err := l.cache.Get(ctx, l.key)
	if err != nil {
		return err
	}

	if string(val) != l.value {
		return ErrNotLockOwner
	}

	return l.cache.SetEX(ctx, l.key, []byte(l.value), int64(ttl.Seconds()))
}

// IsAcquired returns whether the lock is acquired.
func (l *Lock) IsAcquired() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.acquired
}

// ErrNotLockOwner is returned when trying to release a lock we don't own.
var ErrNotLockOwner = cachex.ErrNotSupported

// DistributedLock provides distributed locking functionality.
type DistributedLock struct {
	cache cachex.Cache
	mu    sync.Mutex
}

// NewDistributedLock creates a new distributed lock manager.
func NewDistributedLock(cache cachex.Cache) *DistributedLock {
	return &DistributedLock{
		cache: cache,
	}
}

// Lock attempts to acquire a lock with the given key.
func (d *DistributedLock) Lock(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	value := generateLockValue()

	lock := NewLock(d.cache, key, value, ttl)

	acquired, err := lock.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	if !acquired {
		return nil, ErrLockNotAcquired
	}

	return lock, nil
}

// TryLock attempts to acquire a lock without blocking.
func (d *DistributedLock) TryLock(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	value := generateLockValue()

	lock := NewLock(d.cache, key, value, ttl)

	acquired, err := lock.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	if !acquired {
		return nil, ErrLockNotAcquired
	}

	return lock, nil
}

// ErrLockNotAcquired is returned when a lock cannot be acquired.
var ErrLockNotAcquired = &lockError{msg: "lock not acquired"}

// lockError represents a lock-related error.
type lockError struct {
	msg string
}

func (e *lockError) Error() string {
	return e.msg
}

// generateLockValue generates a unique value for lock ownership.
func generateLockValue() string {
	return time.Now().Format(time.RFC3339Nano) + "-" + randomString(16)
}

// randomString generates a random string of given length.
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
	}
	return string(b)
}

// Semaphore implements a distributed semaphore.
type Semaphore struct {
	cache cachex.Cache
	name  string
	limit int64
	mu    sync.Mutex
}

// NewSemaphore creates a new semaphore.
func NewSemaphore(cache cachex.Cache, name string, limit int64) *Semaphore {
	return &Semaphore{
		cache: cache,
		name:  name,
		limit: limit,
	}
}

// Acquire acquires one slot from the semaphore.
func (s *Semaphore) Acquire(ctx context.Context) error {
	for {
		count, err := s.cache.Incr(ctx, s.name+":count")
		if err != nil {
			return err
		}

		if count <= s.limit {
			return nil
		}

		// Over limit, decrement and wait
		s.cache.Decr(ctx, s.name+":count")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			continue
		}
	}
}

// Release releases one slot from the semaphore.
func (s *Semaphore) Release(ctx context.Context) error {
	_, err := s.cache.Decr(ctx, s.name+":count")
	return err
}

// Current returns the current count.
func (s *Semaphore) Current(ctx context.Context) (int64, error) {
	// Get current count
	v, err := s.cache.Get(ctx, s.name+":count")
	if err == cachex.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var count int64
	for _, b := range v {
		count = count*10 + int64(b-'0')
	}
	return count, nil
}
