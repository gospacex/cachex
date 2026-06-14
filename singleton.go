package cachex

import (
	"fmt"
	"os"
	"reflect"

	"gopkg.in/yaml.v3"

	"github.com/gospacex/cachex/utils"
)

// =============================================================================
// Global Functions - Convenient API
// =============================================================================

// DefaultFactory is the default factory instance.
// Backend packages register themselves into this factory via init().
var DefaultFactory = NewFactory()

// Open creates a cache client using the default factory.
// This is the simplest way to create a cache client.
func Open(backend string, cfg *Config) (Cache, error) {
	return DefaultFactory.Create(backend, cfg)
}

// OpenWithObservers creates a cache client with custom observers.
func OpenWithObservers(backend string, cfg *Config, observers ...Observer) (Cache, error) {
	factory := NewFactory()
	for _, obs := range observers {
		factory.AddObserver(obs)
	}
	return factory.Create(backend, cfg)
}

// C returns a singleton cache client for the specified backend.
// The first call creates the singleton, subsequent calls return the same instance.
func C(backend string, cfg *Config) (Cache, error) {
	singletonMu.Lock()
	defer singletonMu.Unlock()

	// Check if already initialized with error
	if err, ok := singletonErr[backend]; ok {
		return nil, err
	}

	// Check if already initialized successfully
	if client, ok := singletons[backend]; ok {
		return client.(Cache), nil
	}

	// Create new instance
	client, err := DefaultFactory.Create(backend, cfg)
	if err != nil {
		singletonErr[backend] = err
		return nil, err
	}

	singletons[backend] = client
	return client, nil
}

// Reset clears all singleton instances. Useful for testing.
func Reset() {
	singletonMu.Lock()
	defer singletonMu.Unlock()

	// Close all connections
	for backend, client := range singletons {
		if c, ok := client.(Cache); ok {
			c.Close()
		}
		delete(singletons, backend)
	}

	// Clear errors
	for backend := range singletonErr {
		delete(singletonErr, backend)
	}
}

// GetSingleton returns the singleton instance without creating one.
func GetSingleton(backend string) (Cache, bool) {
	singletonMu.RLock()
	defer singletonMu.RUnlock()

	client, ok := singletons[backend]
	if !ok {
		return nil, false
	}
	return client.(Cache), true
}

// =============================================================================
// Custom Creator - For user-defined backends
// =============================================================================

type customCreator struct {
	creator Creator
}

func (c *customCreator) Create(cfg *Config) (Cache, error) {
	return c.creator.Create(cfg)
}

// =============================================================================
// Magic Short Functions - Load config from YAML file
// =============================================================================

// LoadConfig loads configuration from a YAML file and returns *Config.
// It runs ExpandEnvVars over every string field, auto-promotes the
// legacy `tracing:` block to the new `trace:` block, and validates
// the trace.exporter value.
func LoadConfig(cfgPath string) (*Config, error) {
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Expand ${env:VAR} / ${env:VAR:-default} placeholders in every
	// string field. Must run before validation so default fallbacks
	// resolve to a known shape.
	expandEnvVarsInStruct(reflect.ValueOf(cfg).Elem())

	// Promote legacy tracing: block to the new trace: block. Trace wins
	// if both are present, matching the precedence in design.md §3.4.
	parseLegacyTracing(cfg)

	if _, err := cfg.Trace.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// parseLegacyTracing auto-promotes the legacy `tracing:` block to the
// new `trace:` block. The new block always wins when both are present.
// Short names "redis" / "kafka" are normalised to the long form
// "redis_stream" / "kafka_topic" so downstream code only deals with
// canonical values.
func parseLegacyTracing(cfg *Config) {
	if cfg == nil {
		return
	}
	if cfg.Trace != nil {
		return // new block wins
	}
	if cfg.Tracing == nil {
		return
	}

	trace := &TraceConfig{
		Enabled:     cfg.Tracing.Enabled,
		ServiceName: cfg.Tracing.ServiceName,
	}
	switch cfg.Tracing.ExporterType {
	case "otlp", "":
		trace.Exporter = "otlp"
	case "jaeger":
		trace.Exporter = "jaeger"
	case "redis":
		trace.Exporter = "redis_stream"
	case "kafka":
		trace.Exporter = "kafka_topic"
	}
	if cfg.Tracing.RedisConfig != nil {
		trace.Stream = cfg.Tracing.RedisConfig.Channel
	}
	if cfg.Tracing.KafkaConfig != nil {
		trace.Brokers = cfg.Tracing.KafkaConfig.Brokers
		trace.Topic = cfg.Tracing.KafkaConfig.Topic
	}
	cfg.Trace = trace
}

// expandEnvVarsInStruct walks every exported string, string slice, and
// string-map field reachable through pointer indirection and runs
// utils.ExpandEnvVars on each. Non-string scalar fields (int, bool, …)
// are left untouched. Cycles are avoided by tracking visited pointers.
func expandEnvVarsInStruct(v reflect.Value) {
	expandEnvVarsValue(v, map[uintptr]bool{})
}

func expandEnvVarsValue(v reflect.Value, visited map[uintptr]bool) {
	if !v.IsValid() {
		return
	}

	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		addr := v.Pointer()
		if visited[addr] {
			return
		}
		visited[addr] = true
		expandEnvVarsValue(v.Elem(), visited)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if !f.CanSet() {
				continue
			}
			expandEnvVarsValue(f, visited)
		}
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.String {
			for i := 0; i < v.Len(); i++ {
				elem := v.Index(i)
				if elem.CanSet() {
					elem.SetString(utils.ExpandEnvVars(elem.String()))
				}
			}
			return
		}
		for i := 0; i < v.Len(); i++ {
			expandEnvVarsValue(v.Index(i), visited)
		}
	case reflect.Map:
		if v.Type().Elem().Kind() == reflect.String {
			iter := v.MapRange()
			for iter.Next() {
				k := iter.Key()
				val := iter.Value()
				newVal := utils.ExpandEnvVars(val.String())
				if newVal != val.String() {
					v.SetMapIndex(k, reflect.ValueOf(newVal))
				}
			}
			return
		}
		iter := v.MapRange()
		for iter.Next() {
			expandEnvVarsValue(iter.Value(), visited)
		}
	case reflect.String:
		v.SetString(utils.ExpandEnvVars(v.String()))
	}
}

// SaveConfig saves configuration to a YAML file.
func SaveConfig(cfg *Config, cfgPath string) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	return os.WriteFile(cfgPath, data, 0644)
}
