// Package cachex provides a unified, production-ready cache client factory.
//
// It offers a consistent interface across multiple cache backends including
// Redis, Dragonfly, KeyDB, Garnet, Badger, BBolt, and Pebble.
//
// # Features
//
//   - Unified interface across all backends
//   - Structured logging with context
//   - Prometheus metrics integration
//   - OpenTelemetry tracing support
//   - Circuit breaker for resilience
//   - Connection pool management
//   - Health checks
//   - Graceful shutdown
//
// # Backend auto-registration
//
// Backend packages register themselves into the global DefaultFactory via
// their init() functions. Simply blank-import the desired backend package:
//
//	import _ "github.com/gospacex/cachex/backends/network/redis" // registers redis, dragonfly, keydb, garnet
//	import _ "github.com/gospacex/cachex/backends/embedded/badger" // registers badger
//
// All Redis-protocol backends (Redis, Dragonfly, KeyDB, Garnet) share a
// single implementation in backends/network/redis that uses go-redis/v9.
package cachex

import "sync"

// Backend type constants.
const (
	BackendRedis     = "redis"
	BackendDragonfly = "dragonfly"
	BackendKeyDB     = "keydb"
	BackendGarnet    = "garnet"
	BackendBadger    = "badger"
	BackendBBolt     = "bbolt"
	BackendPebble    = "pebble"
)

// Global singleton instances.
var (
	singletons   = make(map[string]interface{})
	singletonMu  sync.RWMutex
	singletonErr = make(map[string]error)
)
