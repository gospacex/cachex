package cachex

import (
	"fmt"
	"sync"
)

// =============================================================================
// Factory - Creates cache instances
// =============================================================================

// Factory creates cache instances based on backend type.
type Factory struct {
	registry  *Registry
	observers []Observer
	mu        sync.RWMutex
}

// NewFactory creates a new factory with an empty registry.
// Backend packages register themselves via init() into DefaultFactory.
// For custom factories, call Register to add backends.
func NewFactory() *Factory {
	return &Factory{
		registry: NewRegistry(),
	}
}

// Create creates a new cache instance.
func (f *Factory) Create(backend string, cfg *Config) (Cache, error) {
	creator, err := f.registry.Get(backend)
	if err != nil {
		return nil, fmt.Errorf("unknown backend %q: %w", backend, err)
	}

	cache, err := creator.Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache for %q: %w", backend, err)
	}

	// Build effective observers list
	observers := make([]Observer, 0, len(f.observers))

	// Add factory-level observers
	observers = append(observers, f.observers...)

	// Auto-enable tracing if configured
	if cfg.Tracing != nil && cfg.Tracing.IsEnabled() {
		tracer := newOtelTracerFromConfig(cfg.Tracing)
		observers = append(observers, newTraceObserverFromConfig(cfg.Tracing, tracer))
	}

	// Wrap with observers if any
	if len(observers) > 0 {
		cache = &observedCache{
			cache:     cache,
			observers: observers,
			backend:   backend,
		}
	}

	return cache, nil
}

// ListBackends returns a list of all registered backends.
func (f *Factory) ListBackends() []BackendInfo {
	return f.registry.List()
}

// Register registers a custom backend creator.
func (f *Factory) Register(backend string, creator Creator) error {
	return f.registry.RegisterBackend(backend, &customCreator{creator: creator})
}

// AddObserver adds an observer for cache operations.
func (f *Factory) AddObserver(obs Observer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observers = append(f.observers, obs)
}

// RemoveObserver removes an observer from the factory.
func (f *Factory) RemoveObserver(obs Observer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, o := range f.observers {
		if o == obs {
			f.observers = append(f.observers[:i], f.observers[i+1:]...)
			return
		}
	}
}
