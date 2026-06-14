package cachex

import (
	"errors"
	"fmt"
)

// Common errors for cache operations.
var (
	// ErrKeyNotFound is returned when a key is not found in the cache.
	ErrKeyNotFound = errors.New("key not found")

	// ErrInvalidKey is returned when a key is invalid.
	ErrInvalidKey = errors.New("invalid key")

	// ErrInvalidValue is returned when a value is invalid.
	ErrInvalidValue = errors.New("invalid value")

	// ErrConnectionFailed is returned when the cache connection fails.
	ErrConnectionFailed = errors.New("connection failed")

	// ErrTimeout is returned when an operation times out.
	ErrTimeout = errors.New("operation timeout")

	// ErrNotSupported is returned when an operation is not supported.
	ErrNotSupported = errors.New("operation not supported")

	// ErrClosed is returned when the cache is closed.
	ErrClosed = errors.New("cache closed")

	// ErrAddrsRequired is returned when addresses are required but not provided.
	ErrAddrsRequired = errors.New("addresses are required")

	// ErrDirRequired is returned when directory is required but not provided.
	ErrDirRequired = errors.New("directory is required")

	// ErrUnknownBackend is returned when an unknown backend type is requested.
	ErrUnknownBackend = errors.New("unknown cache backend")

	// ErrBackendAlreadyRegistered is returned when a backend is already registered.
	ErrBackendAlreadyRegistered = errors.New("backend already registered")

	// ErrCircuitOpen is returned when the circuit breaker is open.
	ErrCircuitOpen = errors.New("circuit breaker open")

	// ErrMaxRetriesExceeded is returned when max retries is exceeded.
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")

	// ErrInvalidConfig is returned when the configuration is invalid.
	ErrInvalidConfig = errors.New("invalid configuration")

	// ErrRedisPoolNotRegistered is returned when RP/RCP is called without
	// blank-importing drivers/redisx.
	ErrRedisPoolNotRegistered = errors.New("cachex: redis pool not registered; blank-import github.com/gospacex/cachex/drivers/redisx")

	// ErrRedisClusterPoolNotRegistered is returned when RCP is called without
	// blank-importing drivers/redisx.
	ErrRedisClusterPoolNotRegistered = errors.New("cachex: redis cluster pool not registered; blank-import github.com/gospacex/cachex/drivers/redisx")

	// ErrKafkaPoolNotRegistered is returned when KP is called without
	// blank-importing drivers/kafkax.
	ErrKafkaPoolNotRegistered = errors.New("cachex: kafka pool not registered; blank-import github.com/gospacex/cachex/drivers/kafkax")

	// ErrTracingNotRegistered is returned when InitTracing is called without
	// blank-importing github.com/gospacex/cachex/observability.
	ErrTracingNotRegistered = errors.New("cachex: tracing not registered; blank-import github.com/gospacex/cachex/observability")
)

// CacheError wraps cache operations with context.
type CacheError struct {
	Operation string
	Backend   string
	Key       string
	Err       error
	Cause     error
}

func (e *CacheError) Error() string {
	if e.Key != "" {
		return fmt.Sprintf("%s: %s (%s) key=%s: %v", e.Operation, e.Backend, e.Err, e.Key, e.Cause)
	}
	return fmt.Sprintf("%s: %s (%s): %v", e.Operation, e.Backend, e.Err, e.Cause)
}

func (e *CacheError) Unwrap() error {
	return e.Err
}

// WithKey adds key context to the error.
func (e *CacheError) WithKey(key string) *CacheError {
	return &CacheError{
		Operation: e.Operation,
		Backend:   e.Backend,
		Key:       key,
		Err:       e.Err,
		Cause:     e.Cause,
	}
}

// WithBackend adds backend context to the error.
func (e *CacheError) WithBackend(backend string) *CacheError {
	return &CacheError{
		Operation: e.Operation,
		Backend:   backend,
		Key:       e.Key,
		Err:       e.Err,
		Cause:     e.Cause,
	}
}

// WithCause wraps the original error.
func (e *CacheError) WithCause(cause error) *CacheError {
	return &CacheError{
		Operation: e.Operation,
		Backend:   e.Backend,
		Key:       e.Key,
		Err:       e.Err,
		Cause:     cause,
	}
}

// NewCacheError creates a new CacheError.
func NewCacheError(operation, backend string, err error) *CacheError {
	return &CacheError{
		Operation: operation,
		Backend:   backend,
		Err:       err,
		Cause:     err,
	}
}

// IsCacheError checks if an error is a CacheError.
func IsCacheError(err error) bool {
	var cacheErr *CacheError
	return errors.As(err, &cacheErr)
}

// GetCacheError extracts the CacheError from an error chain.
func GetCacheError(err error) *CacheError {
	var cacheErr *CacheError
	if errors.As(err, &cacheErr) {
		return cacheErr
	}
	return nil
}

// RetryableError marks an error as retryable.
type RetryableError struct {
	Err       error
	Attempts  int
	BackoffMs int
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable error after %d attempts: %v", e.Attempts, e.Err)
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// IsRetryable checks if an error is retryable.
func IsRetryable(err error) bool {
	var retryErr *RetryableError
	if errors.As(err, &retryErr) {
		return true
	}
	// Check for standard retryable errors
	return errors.Is(err, ErrConnectionFailed) ||
		errors.Is(err, ErrTimeout)
}

// ConnectionError represents a connection-related error.
type ConnectionError struct {
	Addr    string
	Backend string
	Cause   error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("connection failed to %s (%s): %v", e.Addr, e.Backend, e.Cause)
}

func (e *ConnectionError) Unwrap() error {
	return e.Cause
}

// NewConnectionError creates a new ConnectionError.
func NewConnectionError(addr, backend string, cause error) *ConnectionError {
	return &ConnectionError{
		Addr:    addr,
		Backend: backend,
		Cause:   cause,
	}
}
