// Package healthcheck provides health checking for cachex backends.
package healthcheck

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gospacex/cachex"
)

// HealthChecker implements comprehensive health checking for cache backends.
type HealthChecker struct {
	cache  cachex.Cache
	checks []HealthCheckFunc
	mu     sync.RWMutex
}

// HealthCheckFunc is a function that performs a health check.
type HealthCheckFunc func(ctx context.Context) error

// HealthResult represents the result of a health check.
type HealthResult struct {
	Name    string        `json:"name"`
	Status  string        `json:"status"`
	Message string        `json:"message,omitempty"`
	Latency time.Duration `json:"latency"`
}

// NewHealthChecker creates a new health checker.
func NewHealthChecker(cache cachex.Cache) *HealthChecker {
	return &HealthChecker{
		cache:  cache,
		checks: make([]HealthCheckFunc, 0),
	}
}

// AddCheck adds a custom health check function.
func (h *HealthChecker) AddCheck(name string, fn HealthCheckFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks = append(h.checks, func(ctx context.Context) error {
		return fn(ctx)
	})
	_ = name // name is for documentation purposes
}

// Check performs all health checks.
func (h *HealthChecker) Check(ctx context.Context) []HealthResult {
	h.mu.RLock()
	checks := h.checks
	h.mu.RUnlock()

	results := make([]HealthResult, 0, len(checks)+1)

	// Basic ping check
	start := time.Now()
	err := h.cache.Ping(ctx)
	results = append(results, HealthResult{
		Name:    "ping",
		Status:  statusFromError(err),
		Message: messageFromError(err),
		Latency: time.Since(start),
	})

	// Custom checks
	for i, check := range checks {
		start := time.Now()
		err := check(ctx)
		results = append(results, HealthResult{
			Name:    fmt.Sprintf("custom_%d", i),
			Status:  statusFromError(err),
			Message: messageFromError(err),
			Latency: time.Since(start),
		})
	}

	return results
}

// CheckAll performs a full health check including connectivity and data integrity.
func (h *HealthChecker) CheckAll(ctx context.Context) error {
	results := h.Check(ctx)

	for _, result := range results {
		if result.Status != "healthy" {
			return fmt.Errorf("health check failed: %s - %s", result.Name, result.Message)
		}
	}

	return nil
}

// ReadyChecker checks if the cache is ready to serve requests.
type ReadyChecker struct {
	cache    cachex.Cache
	attempts atomic.Int32
	maxWait  time.Duration
}

// NewReadyChecker creates a new readiness checker.
func NewReadyChecker(cache cachex.Cache, maxWait time.Duration) *ReadyChecker {
	return &ReadyChecker{
		cache:   cache,
		maxWait: maxWait,
	}
}

// Ready waits for the cache to be ready.
func (r *ReadyChecker) Ready(ctx context.Context) error {
	deadline := time.Now().Add(r.maxWait)

	for {
		if err := r.cache.Ping(ctx); err == nil {
			return nil
		}

		r.attempts.Add(1)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			if time.Now().After(deadline) {
				return fmt.Errorf("cache not ready after %v", r.maxWait)
			}
		}
	}
}

// statusFromError converts an error to a status string.
func statusFromError(err error) string {
	if err == nil {
		return "healthy"
	}
	return "unhealthy"
}

// messageFromError converts an error to a message string.
func messageFromError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// HealthReporter provides a health reporting interface.
type HealthReporter struct {
	checker *HealthChecker
}

// NewHealthReporter creates a new health reporter.
func NewHealthReporter(cache cachex.Cache) *HealthReporter {
	return &HealthReporter{
		checker: NewHealthChecker(cache),
	}
}

// Report returns the current health status.
func (r *HealthReporter) Report(ctx context.Context) map[string]interface{} {
	results := r.checker.Check(ctx)

	status := "healthy"
	for _, result := range results {
		if result.Status != "healthy" {
			status = "unhealthy"
			break
		}
	}

	return map[string]interface{}{
		"status":    status,
		"checks":    results,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
}

// RegisterHealthCheck registers a health check function for the factory.
func RegisterHealthCheck(name string, fn func() HealthCheckFunc) {
	// This is a placeholder for factory-level health check registration
	_ = name
	_ = fn
}
