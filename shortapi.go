package cachex

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
)

// =============================================================================
// Driver pool registry
//
// The root cachex package cannot directly import drivers/redisx or
// drivers/kafkax (which import cachex.Config) without creating an import
// cycle. Instead, each driver package registers its pool functions via
// init(), and the shortcut functions RP / KP delegate to the registered
// implementations. The observability InitTracing shortcut follows the
// same pattern.
// =============================================================================

var (
	redisPoolFn    func(path string) (*redis.Client, error)
	redisClusterFn func(path string) (*redis.ClusterClient, error)
	kafkaPoolFn    func(path string) (sarama.SyncProducer, error)
	tracingInitFn  func(ctx context.Context, trace *TraceConfig) (func(context.Context), error)
)

// RegisterRedisPool sets the driver-level pool function for RP / PPC.
// Called by drivers/redisx.init().
func RegisterRedisPool(fn func(path string) (*redis.Client, error)) {
	redisPoolFn = fn
}

// RegisterRedisClusterPool sets the driver-level cluster pool function for PPC.
// Called by drivers/redisx.init().
func RegisterRedisClusterPool(fn func(path string) (*redis.ClusterClient, error)) {
	redisClusterFn = fn
}

// RegisterKafkaPool sets the driver-level pool function for KP.
// Called by drivers/kafkax.init().
func RegisterKafkaPool(fn func(path string) (sarama.SyncProducer, error)) {
	kafkaPoolFn = fn
}

// RegisterTracingInit sets the observability InitTracing function for the
// top-level InitTracing shortcut. Called by observability.init().
func RegisterTracingInit(fn func(ctx context.Context, trace *TraceConfig) (func(context.Context), error)) {
	tracingInitFn = fn
}

// =============================================================================
// Short API: Generic (OP/OO) - auto-detect backend from config
// =============================================================================

// OP opens a cache from a config file path (auto-detect backend).
func OP(path string) (Cache, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return DefaultFactory.Create(cfg.Backend, cfg)
}

// OO opens a cache from a *Config object (auto-detect backend).
func OO(cfg *Config) (Cache, error) {
	return DefaultFactory.Create(cfg.Backend, cfg)
}

// =============================================================================
// Short API: Driver pool shortcuts (RP/KP)
// =============================================================================

// RP returns a pooled *redis.Client from the driver-level pool.
// The pool deduplicates clients that share the same config fingerprint.
// Use this when you need a raw Redis client (for observability injection,
// custom commands, etc.) rather than a Cache instance.
//
// Returns an error if drivers/redisx was not imported (blank-import it
// to register the pool function automatically).
func RP(path string) (*redis.Client, error) {
	if redisPoolFn == nil {
		return nil, ErrRedisPoolNotRegistered
	}
	return redisPoolFn(path)
}

// RCP returns a pooled *redis.ClusterClient from the driver-level pool.
func RCP(path string) (*redis.ClusterClient, error) {
	if redisClusterFn == nil {
		return nil, ErrRedisClusterPoolNotRegistered
	}
	return redisClusterFn(path)
}

// KP returns a pooled sarama.SyncProducer from the driver-level pool.
// The pool deduplicates producers that share the same config fingerprint.
//
// Returns an error if drivers/kafkax was not imported (blank-import it
// to register the pool function automatically).
func KP(path string) (sarama.SyncProducer, error) {
	if kafkaPoolFn == nil {
		return nil, ErrKafkaPoolNotRegistered
	}
	return kafkaPoolFn(path)
}

// =============================================================================
// Short API: Observability (InitTracing)
// =============================================================================

// InitTracing loads the trace config from a YAML file, expands environment
// variables, and initialises the global OpenTelemetry tracer provider.
// The returned cleanup function should be deferred: defer cleanup(ctx).
//
// When the config has Trace == nil or Trace.Enabled == false, a no-op
// cleanup function is returned with no error.
func InitTracing(path string) (func(context.Context), error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	if cfg.Trace == nil || !cfg.Trace.Enabled {
		return func(context.Context) {}, nil
	}
	if tracingInitFn == nil {
		return nil, ErrTracingNotRegistered
	}
	return tracingInitFn(context.Background(), cfg.Trace)
}

// =============================================================================
// Short API: Dragonfly (DP/DO)
// =============================================================================

// DP opens a Dragonfly cache from a config file path.
func DP(path string) (Cache, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return DefaultFactory.Create(BackendDragonfly, cfg)
}

// DO opens a Dragonfly cache from a *Config object.
func DO(cfg *Config) (Cache, error) {
	return DefaultFactory.Create(BackendDragonfly, cfg)
}

// =============================================================================
// Short API: KeyDB cache shortcut (KP is driver-level above)
// =============================================================================

// KPCache opens a KeyDB cache from a config file path.
// For the driver-level sarama.SyncProducer shortcut, use KP.
func KPCache(path string) (Cache, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return DefaultFactory.Create(BackendKeyDB, cfg)
}

// KO opens a KeyDB cache from a *Config object.
func KO(cfg *Config) (Cache, error) {
	return DefaultFactory.Create(BackendKeyDB, cfg)
}

// =============================================================================
// Short API: Garnet (GP/GO)
// =============================================================================

// GP opens a Garnet cache from a config file path.
func GP(path string) (Cache, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return DefaultFactory.Create(BackendGarnet, cfg)
}

// GO opens a Garnet cache from a *Config object.
func GO(cfg *Config) (Cache, error) {
	return DefaultFactory.Create(BackendGarnet, cfg)
}

// =============================================================================
// Short API: Badger (BP/BO)
// =============================================================================

// BP opens a Badger cache from a config file path.
func BP(path string) (Cache, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return DefaultFactory.Create(BackendBadger, cfg)
}

// BO opens a Badger cache from a *Config object.
func BO(cfg *Config) (Cache, error) {
	return DefaultFactory.Create(BackendBadger, cfg)
}

// =============================================================================
// Short API: BBolt (VP/VO)
// =============================================================================

// VP opens a BBolt cache from a config file path.
func VP(path string) (Cache, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return DefaultFactory.Create(BackendBBolt, cfg)
}

// VO opens a BBolt cache from a *Config object.
func VO(cfg *Config) (Cache, error) {
	return DefaultFactory.Create(BackendBBolt, cfg)
}

// =============================================================================
// Short API: Pebble (PP/PO)
// =============================================================================

// PP opens a Pebble cache from a config file path.
func PP(path string) (Cache, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return DefaultFactory.Create(BackendPebble, cfg)
}

// PO opens a Pebble cache from a *Config object.
func PO(cfg *Config) (Cache, error) {
	return DefaultFactory.Create(BackendPebble, cfg)
}

// =============================================================================
// Legacy Singleton Short Functions (R/D/K/G/B/BB/P)
// =============================================================================

// R returns a singleton Redis client (single instance).
func R(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return C(BackendRedis, cfg)
}

// RC returns a singleton Redis client (cluster mode).
func RC(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	cfg.ClusterMode = true
	return C(BackendRedis, cfg)
}

// D returns a singleton Dragonfly client (single instance).
func D(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return C(BackendDragonfly, cfg)
}

// DC returns a singleton Dragonfly client (cluster mode).
func DC(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	cfg.ClusterMode = true
	return C(BackendDragonfly, cfg)
}

// K returns a singleton KeyDB client (single instance).
func K(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return C(BackendKeyDB, cfg)
}

// KC returns a singleton KeyDB client (cluster mode).
func KC(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	cfg.ClusterMode = true
	return C(BackendKeyDB, cfg)
}

// G returns a singleton Garnet client (single instance).
func G(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return C(BackendGarnet, cfg)
}

// GC returns a singleton Garnet client (cluster mode).
func GC(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	cfg.ClusterMode = true
	return C(BackendGarnet, cfg)
}

// B returns a singleton Badger client.
func B(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return C(BackendBadger, cfg)
}

// BB returns a singleton BBolt client.
func BB(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return C(BackendBBolt, cfg)
}

// P returns a singleton Pebble client.
func P(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return C(BackendPebble, cfg)
}
