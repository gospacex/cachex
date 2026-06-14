package cachex

import (
	"fmt"
	"sync"
)

// BackendInfo contains information about a registered backend.
type BackendInfo struct {
	// Name is the backend identifier (e.g., "redis", "badger").
	Name string `json:"name"`

	// Description is a human-readable description.
	Description string `json:"description"`

	// Protocol is the backend protocol ("redis", "kv", "custom").
	Protocol string `json:"protocol"`

	// IsEmbedded indicates if this is an embedded backend.
	IsEmbedded bool `json:"isEmbedded"`

	// Version is the backend version.
	Version string `json:"version,omitempty"`

	// Features is a list of supported features.
	Features []string `json:"features,omitempty"`
}

// Registry manages the registration of cache backend creators.
type Registry struct {
	mu       sync.RWMutex
	backends map[string]*backendEntry
}

type backendEntry struct {
	info    BackendInfo
	creator BackendCreator
}

// BackendCreator is the interface for creating cache instances.
type BackendCreator interface {
	Create(cfg *Config) (Cache, error)
}

// NewRegistry creates a new registry.
func NewRegistry() *Registry {
	return &Registry{
		backends: make(map[string]*backendEntry),
	}
}

// RegisterBackend registers a new backend.
func (r *Registry) RegisterBackend(name string, creator BackendCreator) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if name == "" {
		return fmt.Errorf("backend name cannot be empty")
	}

	if creator == nil {
		return fmt.Errorf("backend creator cannot be nil")
	}

	if _, exists := r.backends[name]; exists {
		return fmt.Errorf("%w: %q", ErrBackendAlreadyRegistered, name)
	}

	r.backends[name] = &backendEntry{
		creator: creator,
		info:    getBackendInfo(name),
	}

	return nil
}

// Get retrieves a backend creator by name.
func (r *Registry) Get(name string) (BackendCreator, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, exists := r.backends[name]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrUnknownBackend, name)
	}

	return entry.creator, nil
}

// GetInfo retrieves backend information by name.
func (r *Registry) GetInfo(name string) (BackendInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, exists := r.backends[name]
	if !exists {
		return BackendInfo{}, fmt.Errorf("%w: %q", ErrUnknownBackend, name)
	}

	return entry.info, nil
}

// List returns all registered backends.
func (r *Registry) List() []BackendInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]BackendInfo, 0, len(r.backends))
	for _, entry := range r.backends {
		result = append(result, entry.info)
	}

	return result
}

// Unregister removes a backend from the registry.
func (r *Registry) Unregister(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.backends[name]; !exists {
		return fmt.Errorf("%w: %q", ErrUnknownBackend, name)
	}

	delete(r.backends, name)
	return nil
}

// HasBackend checks if a backend is registered.
func (r *Registry) HasBackend(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.backends[name]
	return exists
}

// Count returns the number of registered backends.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.backends)
}

// getBackendInfo returns the default info for a backend.
func getBackendInfo(name string) BackendInfo {
	switch name {
	case BackendRedis:
		return BackendInfo{
			Name:        BackendRedis,
			Description: "Redis - In-memory data structure store",
			Protocol:    "redis",
			IsEmbedded:  false,
			Version:     "7.x",
			Features:    []string{"string", "hash", "list", "set", "sorted_set", "stream", "pub_sub", "transactions", "pipelining", "scripting", "cluster", "sentinel", "tls"},
		}
	case BackendDragonfly:
		return BackendInfo{
			Name:        BackendDragonfly,
			Description: "Dragonfly - Redis-compatible in-memory database",
			Protocol:    "redis",
			IsEmbedded:  false,
			Version:     "1.x",
			Features:    []string{"string", "hash", "list", "set", "sorted_set", "stream", "pub_sub", "multi_threading", "snapshot"},
		}
	case BackendKeyDB:
		return BackendInfo{
			Name:        BackendKeyDB,
			Description: "KeyDB - Redis-compatible high-performance fork",
			Protocol:    "redis",
			IsEmbedded:  false,
			Version:     "6.x",
			Features:    []string{"string", "hash", "list", "set", "sorted_set", "multi_threading", "cluster"},
		}
	case BackendGarnet:
		return BackendInfo{
			Name:        BackendGarnet,
			Description: "Garnet - Redis-compatible server with RocksDB",
			Protocol:    "redis",
			IsEmbedded:  false,
			Version:     "1.x",
			Features:    []string{"string", "hash", "list", "set", "sorted_set", "persistence", "cluster"},
		}
	case BackendBadger:
		return BackendInfo{
			Name:        BackendBadger,
			Description: "Badger - Embedded key-value database",
			Protocol:    "kv",
			IsEmbedded:  true,
			Version:     "4.x",
			Features:    []string{"key_value", "ttl", "compression", "transactions", "merge_operator"},
		}
	case BackendBBolt:
		return BackendInfo{
			Name:        BackendBBolt,
			Description: "BBolt - Embedded key-value database",
			Protocol:    "kv",
			IsEmbedded:  true,
			Version:     "1.4.x",
			Features:    []string{"key_value", "transactions", "buckets", "cursor"},
		}
	case BackendPebble:
		return BackendInfo{
			Name:        BackendPebble,
			Description: "Pebble - Embedded key-value database",
			Protocol:    "kv",
			IsEmbedded:  true,
			Version:     "1.x",
			Features:    []string{"key_value", "range_keys", "bloom_filter", "compaction", "ingestion"},
		}
	default:
		return BackendInfo{
			Name:        name,
			Description: "Unknown backend",
			Protocol:    "unknown",
			IsEmbedded:  false,
		}
	}
}
