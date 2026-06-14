package cachex

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheErrors(t *testing.T) {
	// Test predefined errors
	assert.Equal(t, "key not found", ErrKeyNotFound.Error())
	assert.Equal(t, "invalid key", ErrInvalidKey.Error())
	assert.Equal(t, "connection failed", ErrConnectionFailed.Error())
	assert.Equal(t, "operation timeout", ErrTimeout.Error())
	assert.Equal(t, "unknown cache backend", ErrUnknownBackend.Error())
}

func TestCacheError(t *testing.T) {
	innerErr := errors.New("connection refused")
	err := NewCacheError("get", "redis", ErrConnectionFailed)

	assert.Equal(t, "get", err.Operation)
	assert.Equal(t, "redis", err.Backend)
	assert.Equal(t, ErrConnectionFailed, err.Err)
	assert.Equal(t, ErrConnectionFailed, err.Unwrap())

	// Test with key
	errWithKey := err.WithKey("test-key")
	assert.Equal(t, "test-key", errWithKey.Key)

	// Test with backend
	errWithBackend := err.WithBackend("dragonfly")
	assert.Equal(t, "dragonfly", errWithBackend.Backend)

	// Test with cause
	errWithCause := err.WithCause(innerErr)
	assert.Equal(t, innerErr, errWithCause.Cause)
}

func TestIsCacheError(t *testing.T) {
	cacheErr := NewCacheError("get", "redis", ErrKeyNotFound)

	assert.True(t, IsCacheError(cacheErr))
	assert.False(t, IsCacheError(errors.New("regular error")))
}

func TestGetCacheError(t *testing.T) {
	cacheErr := NewCacheError("get", "redis", ErrKeyNotFound)

	result := GetCacheError(cacheErr)
	assert.NotNil(t, result)
	assert.Equal(t, cacheErr, result)

	// Test with wrapped error
	wrapped := errors.New("wrapped: " + cacheErr.Error())
	result = GetCacheError(wrapped)
	assert.Nil(t, result)
}

func TestRetryableError(t *testing.T) {
	innerErr := errors.New("network error")
	err := &RetryableError{
		Err:       innerErr,
		Attempts:  3,
		BackoffMs: 100,
	}

	assert.Contains(t, err.Error(), "retryable error after 3 attempts")
	assert.Equal(t, innerErr, err.Unwrap())
}

func TestIsRetryable(t *testing.T) {
	// Standard retryable errors
	assert.True(t, IsRetryable(ErrConnectionFailed))
	assert.True(t, IsRetryable(ErrTimeout))

	// Non-retryable errors
	assert.False(t, IsRetryable(ErrKeyNotFound))
	assert.False(t, IsRetryable(ErrNotSupported))

	// RetryableError
	retryErr := &RetryableError{Err: errors.New("test")}
	assert.True(t, IsRetryable(retryErr))
}

func TestConnectionError(t *testing.T) {
	innerErr := errors.New("connection refused")
	err := NewConnectionError("localhost:6379", "redis", innerErr)

	assert.Equal(t, "localhost:6379", err.Addr)
	assert.Equal(t, "redis", err.Backend)
	assert.Equal(t, innerErr, err.Unwrap())
	assert.Contains(t, err.Error(), "localhost:6379")
	assert.Contains(t, err.Error(), "redis")
}

func TestErrorWrapping(t *testing.T) {
	// Test that errors wrap correctly
	baseErr := errors.New("base error")
	wrapped1 := NewCacheError("get", "redis", baseErr)
	wrapped2 := wrapped1.WithKey("test-key")

	// Unwrap chain
	assert.Equal(t, baseErr, errors.Unwrap(wrapped2))
	assert.Equal(t, baseErr, errors.Unwrap(wrapped1))
}

func TestErrorIs(t *testing.T) {
	err := NewCacheError("get", "redis", ErrKeyNotFound)

	// Test ErrorIs
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.False(t, errors.Is(err, ErrConnectionFailed))
}

func TestErrorAs(t *testing.T) {
	cacheErr := NewCacheError("get", "redis", ErrKeyNotFound)

	var extracted *CacheError
	assert.True(t, errors.As(cacheErr, &extracted))
	assert.Equal(t, cacheErr, extracted)

	var connErr *ConnectionError
	assert.False(t, errors.As(cacheErr, &connErr))
}
