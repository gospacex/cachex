package cachex

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
)

type lazyClient struct {
	mu     sync.RWMutex
	once   sync.Once
	client Cache
	err    error
}

var clientRegistry sync.Map // key: config hash string → *lazyClient

func configKey(cfg *Config) string {
	if cfg == nil {
		return "nil"
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Sprintf("%p", cfg)
	}
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

// lazySingleton returns a singleton Cache for the given (backend, cfg) pair.
// Two calls with the same arguments return the same Cache instance. The
// first call constructs the client via DefaultFactory.Create; concurrent
// callers block on sync.Once until init completes.
//
// This is the universal "single instance per unique config" mechanism used
// by the 22 S/CS shortcuts and by RFromConfig. The legacy single-letter
// shortcuts (R/D/K/G/B/BB/P) and their cluster variants (RC/DC/KC/GC)
// continue to use the backend-keyed C() function, whose semantics are
// preserved for backward compatibility with the deprecation notice in
// spec/shortcut-functions.
func lazySingleton(backend string, cfg *Config) (Cache, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cachex: lazySingleton(%s): nil config", backend)
	}
	key := backend + "|" + configKey(cfg)

	// LoadOrStore avoids race between Load and Store across goroutines.
	lp, loaded := clientRegistry.LoadOrStore(key, &lazyClient{})
	if loaded {
		lp.(*lazyClient).mu.RLock()
		defer lp.(*lazyClient).mu.RUnlock()
		return lp.(*lazyClient).client, lp.(*lazyClient).err
	}

	// First goroutine to reach here initializes the underlying client.
	lp.(*lazyClient).once.Do(func() {
		lp.(*lazyClient).mu.Lock()
		defer lp.(*lazyClient).mu.Unlock()
		lp.(*lazyClient).client, lp.(*lazyClient).err = DefaultFactory.Create(backend, cfg)
	})

	lp.(*lazyClient).mu.RLock()
	defer lp.(*lazyClient).mu.RUnlock()
	return lp.(*lazyClient).client, lp.(*lazyClient).err
}

// =============================================================================
// Auto-detect Singleton (RFromConfig)
// =============================================================================

// RFromConfig returns a singleton client from *Config object.
// Auto-detects backend from cfg.Backend field. RFromConfig is the
// original Redis-family entry point; it is now a thin wrapper over
// lazySingleton, preserving its public API while routing through the
// same config-keyed registry used by RS/ROS/RCS/ROCS.
func RFromConfig(cfg *Config) (Cache, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nil config")
	}
	return lazySingleton(cfg.Backend, cfg)
}

// =============================================================================
// go-redis Series (Redis/Dragonfly/KeyDB/Garnet) - S/CS shortcuts
// =============================================================================

// RS returns a standalone Redis client from config path. Singleton-keyed
// by (BackendRedis, *Config), so repeated calls with the same config
// return the same instance.
func RS(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return lazySingleton(BackendRedis, cfg)
}

// ROS returns a standalone Redis client from *Config object. RS(path) and
// ROS(cfg) with the same cfg return the same instance.
func ROS(cfg *Config) (Cache, error) {
	return lazySingleton(BackendRedis, cfg)
}

// RCS returns a Redis cluster client from config path. Sets ClusterMode=true
// before the singleton lookup so the cluster variant and the standalone
// variant do not share a Cache.
func RCS(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	cfg.ClusterMode = true
	return lazySingleton(BackendRedis, cfg)
}

// ROCS returns a Redis cluster client from *Config object. Clones the
// caller's config to avoid mutating it, sets ClusterMode=true, then
// looks up the singleton.
func ROCS(cfg *Config) (Cache, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nil config")
	}
	cfg = cfg.Clone()
	cfg.ClusterMode = true
	return lazySingleton(BackendRedis, cfg)
}

// DS returns a standalone Dragonfly client from config path.
func DS(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return lazySingleton(BackendDragonfly, cfg)
}

// DOS returns a standalone Dragonfly client from *Config object.
func DOS(cfg *Config) (Cache, error) {
	return lazySingleton(BackendDragonfly, cfg)
}

// DCS returns a Dragonfly cluster client from config path.
func DCS(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	cfg.ClusterMode = true
	return lazySingleton(BackendDragonfly, cfg)
}

// DOCS returns a Dragonfly cluster client from *Config object.
func DOCS(cfg *Config) (Cache, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nil config")
	}
	cfg = cfg.Clone()
	cfg.ClusterMode = true
	return lazySingleton(BackendDragonfly, cfg)
}

// KS returns a standalone KeyDB client from config path.
func KS(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return lazySingleton(BackendKeyDB, cfg)
}

// KOS returns a standalone KeyDB client from *Config object.
func KOS(cfg *Config) (Cache, error) {
	return lazySingleton(BackendKeyDB, cfg)
}

// KCS returns a KeyDB cluster client from config path.
func KCS(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	cfg.ClusterMode = true
	return lazySingleton(BackendKeyDB, cfg)
}

// KOCS returns a KeyDB cluster client from *Config object.
func KOCS(cfg *Config) (Cache, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nil config")
	}
	cfg = cfg.Clone()
	cfg.ClusterMode = true
	return lazySingleton(BackendKeyDB, cfg)
}

// GS returns a standalone Garnet client from config path.
func GS(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return lazySingleton(BackendGarnet, cfg)
}

// GOS returns a standalone Garnet client from *Config object.
func GOS(cfg *Config) (Cache, error) {
	return lazySingleton(BackendGarnet, cfg)
}

// GCS returns a Garnet cluster client from config path.
func GCS(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	cfg.ClusterMode = true
	return lazySingleton(BackendGarnet, cfg)
}

// GOCS returns a Garnet cluster client from *Config object.
func GOCS(cfg *Config) (Cache, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nil config")
	}
	cfg = cfg.Clone()
	cfg.ClusterMode = true
	return lazySingleton(BackendGarnet, cfg)
}

// =============================================================================
// KV-Family Series (Badger/BBolt/Pebble) - S shortcuts only
// =============================================================================

// BS returns a standalone Badger client from config path.
func BS(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return lazySingleton(BackendBadger, cfg)
}

// BOS returns a standalone Badger client from *Config object.
func BOS(cfg *Config) (Cache, error) {
	return lazySingleton(BackendBadger, cfg)
}

// BBS returns a standalone BBolt client from config path.
func BBS(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return lazySingleton(BackendBBolt, cfg)
}

// BBOS returns a standalone BBolt client from *Config object.
func BBOS(cfg *Config) (Cache, error) {
	return lazySingleton(BackendBBolt, cfg)
}

// PS returns a standalone Pebble client from config path.
func PS(cfgPath string) (Cache, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return lazySingleton(BackendPebble, cfg)
}

// POS returns a standalone Pebble client from *Config object.
func POS(cfg *Config) (Cache, error) {
	return lazySingleton(BackendPebble, cfg)
}
