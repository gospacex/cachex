package observability

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gospacex/cachex"
)

// State represents the circuit breaker state.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreaker implements the circuit breaker pattern.
type CircuitBreaker struct {
	mu sync.RWMutex

	name string

	threshold       int
	timeout         time.Duration
	halfOpenMaxReqs int

	state        State
	failures     int
	successes    int
	lastFailTime time.Time

	onStateChange func(name string, from, to State)
	onFailure     func(name string)
	onSuccess     func(name string)
}

// CircuitBreakerOption is a functional option for CircuitBreaker.
type CircuitBreakerOption func(*CircuitBreaker)

// WithThreshold sets the failure threshold.
func WithThreshold(threshold int) CircuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.threshold = threshold
	}
}

// WithTimeout sets the timeout before attempting to close the circuit.
func WithTimeout(timeout time.Duration) CircuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.timeout = timeout
	}
}

// WithHalfOpenMaxRequests sets the max requests in half-open state.
func WithHalfOpenMaxRequests(max int) CircuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.halfOpenMaxReqs = max
	}
}

// WithOnStateChange sets the state change callback.
func WithOnStateChange(cb func(name string, from, to State)) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		c.onStateChange = cb
	}
}

// WithOnFailure sets the failure callback.
func WithOnFailure(cb func(name string)) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		c.onFailure = cb
	}
}

// WithOnSuccess sets the success callback.
func WithOnSuccess(cb func(name string)) CircuitBreakerOption {
	return func(c *CircuitBreaker) {
		c.onSuccess = cb
	}
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(name string, opts ...CircuitBreakerOption) *CircuitBreaker {
	cb := &CircuitBreaker{
		name:            name,
		threshold:       5,
		timeout:         30 * time.Second,
		halfOpenMaxReqs: 3,
		state:           StateClosed,
	}

	for _, opt := range opts {
		opt(cb)
	}

	return cb
}

// Execute runs the function with circuit breaker protection.
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	if err := cb.Acquire(ctx); err != nil {
		return err
	}
	defer cb.Release()

	err := fn()
	if err != nil {
		cb.RecordFailure()
		return err
	}

	cb.RecordSuccess()
	return nil
}

// Acquire checks if the circuit allows the request.
func (cb *CircuitBreaker) Acquire(ctx context.Context) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return nil

	case StateOpen:
		if time.Since(cb.lastFailTime) >= cb.timeout {
			cb.setState(StateHalfOpen)
			cb.successes = 0
			return nil
		}
		return cachex.ErrCircuitOpen

	case StateHalfOpen:
		if cb.successes >= cb.halfOpenMaxReqs {
			return cachex.ErrCircuitOpen
		}
		return nil
	}

	return nil
}

// Release releases the circuit breaker after a request completes.
func (cb *CircuitBreaker) Release() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateHalfOpen {
		cb.successes++
		if cb.successes >= cb.halfOpenMaxReqs {
			cb.setState(StateClosed)
			cb.failures = 0
		}
	}
}

// RecordFailure records a failure.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailTime = time.Now()

	if cb.state == StateClosed && cb.failures >= cb.threshold {
		cb.setState(StateOpen)
	} else if cb.state == StateHalfOpen {
		cb.setState(StateOpen)
	}

	if cb.onFailure != nil {
		cb.onFailure(cb.name)
	}
}

// RecordSuccess records a success.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateClosed {
		cb.failures = 0
	}

	if cb.onSuccess != nil {
		cb.onSuccess(cb.name)
	}
}

// State returns the current state.
func (cb *CircuitBreaker) State() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Failures returns the current failure count.
func (cb *CircuitBreaker) Failures() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.failures
}

// Reset resets the circuit breaker to closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state != StateClosed {
		cb.setState(StateClosed)
	}
	cb.failures = 0
	cb.successes = 0
}

func (cb *CircuitBreaker) setState(state State) {
	if cb.state == state {
		return
	}

	oldState := cb.state
	cb.state = state

	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, oldState, state)
	}
}

// Metrics returns circuit breaker metrics.
type CircuitBreakerMetrics struct {
	Name       string `json:"name"`
	State      string `json:"state"`
	Failures   int    `json:"failures"`
	Successes  int    `json:"successes"`
	LastFailAt string `json:"last_failure_at,omitempty"`
}

// Metrics returns the current metrics.
func (cb *CircuitBreaker) Metrics() CircuitBreakerMetrics {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	metrics := CircuitBreakerMetrics{
		Name:      cb.name,
		State:     cb.state.String(),
		Failures:  cb.failures,
		Successes: cb.successes,
	}

	if !cb.lastFailTime.IsZero() {
		metrics.LastFailAt = cb.lastFailTime.Format(time.RFC3339)
	}

	return metrics
}

// WrapCacheWithCircuitBreaker wraps a cache with circuit breaker protection.
func WrapCacheWithCircuitBreaker(cache cachex.Cache, cb *CircuitBreaker) *CircuitBreakerCache {
	return &CircuitBreakerCache{
		cache: cache,
		cb:    cb,
	}
}

// CircuitBreakerCache wraps a cache with circuit breaker protection.
type CircuitBreakerCache struct {
	cache cachex.Cache
	cb    *CircuitBreaker
}

var _ cachex.Cache = (*CircuitBreakerCache)(nil)

func (c *CircuitBreakerCache) Get(ctx context.Context, key string) ([]byte, error) {
	var result []byte
	err := c.cb.Execute(ctx, func() error {
		var err error
		result, err = c.cache.Get(ctx, key)
		return err
	})
	return result, err
}

func (c *CircuitBreakerCache) Set(ctx context.Context, key string, value []byte) error {
	return c.cb.Execute(ctx, func() error {
		return c.cache.Set(ctx, key, value)
	})
}

func (c *CircuitBreakerCache) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	return c.cb.Execute(ctx, func() error {
		return c.cache.SetEX(ctx, key, value, ttlSeconds)
	})
}

func (c *CircuitBreakerCache) SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error) {
	var result bool
	err := c.cb.Execute(ctx, func() error {
		var err error
		result, err = c.cache.SetNX(ctx, key, value, ttlSeconds)
		return err
	})
	return result, err
}

func (c *CircuitBreakerCache) Delete(ctx context.Context, keys ...string) (int64, error) {
	var result int64
	err := c.cb.Execute(ctx, func() error {
		var err error
		result, err = c.cache.Delete(ctx, keys...)
		return err
	})
	return result, err
}

func (c *CircuitBreakerCache) Exists(ctx context.Context, keys ...string) (int64, error) {
	var result int64
	err := c.cb.Execute(ctx, func() error {
		var err error
		result, err = c.cache.Exists(ctx, keys...)
		return err
	})
	return result, err
}

func (c *CircuitBreakerCache) Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	var result bool
	err := c.cb.Execute(ctx, func() error {
		var err error
		result, err = c.cache.Expire(ctx, key, ttlSeconds)
		return err
	})
	return result, err
}

func (c *CircuitBreakerCache) TTL(ctx context.Context, key string) (int64, error) {
	var result int64
	err := c.cb.Execute(ctx, func() error {
		var err error
		result, err = c.cache.TTL(ctx, key)
		return err
	})
	return result, err
}

func (c *CircuitBreakerCache) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	var result [][]byte
	err := c.cb.Execute(ctx, func() error {
		var err error
		result, err = c.cache.MGet(ctx, keys...)
		return err
	})
	return result, err
}

func (c *CircuitBreakerCache) MSet(ctx context.Context, kvs map[string][]byte) error {
	return c.cb.Execute(ctx, func() error {
		return c.cache.MSet(ctx, kvs)
	})
}

func (c *CircuitBreakerCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	var result []string
	err := c.cb.Execute(ctx, func() error {
		var err error
		result, err = c.cache.Keys(ctx, pattern)
		return err
	})
	return result, err
}

func (c *CircuitBreakerCache) Incr(ctx context.Context, key string) (int64, error) {
	var result int64
	err := c.cb.Execute(ctx, func() error {
		var err error
		result, err = c.cache.Incr(ctx, key)
		return err
	})
	return result, err
}

func (c *CircuitBreakerCache) Decr(ctx context.Context, key string) (int64, error) {
	var result int64
	err := c.cb.Execute(ctx, func() error {
		var err error
		result, err = c.cache.Decr(ctx, key)
		return err
	})
	return result, err
}

func (c *CircuitBreakerCache) Ping(ctx context.Context) error {
	return c.cb.Execute(ctx, func() error {
		return c.cache.Ping(ctx)
	})
}

func (c *CircuitBreakerCache) Close() error {
	return c.cache.Close()
}

func (c *CircuitBreakerCache) Stats() cachex.Stats {
	return c.cache.Stats()
}

// CircuitBreakerFromConfig creates a circuit breaker from configuration.
func CircuitBreakerFromConfig(name string, cfg *cachex.CircuitBreakerConfig) *CircuitBreaker {
	if cfg == nil || !cfg.Enabled {
		return nil
	}

	return NewCircuitBreaker(
		name,
		WithThreshold(cfg.Threshold),
		WithTimeout(time.Duration(cfg.Timeout)*time.Second),
		WithHalfOpenMaxRequests(cfg.HalfOpenMaxRequests),
	)
}

// IsCircuitOpenError checks if an error is due to open circuit.
func IsCircuitOpenError(err error) bool {
	return errors.Is(err, cachex.ErrCircuitOpen)
}
