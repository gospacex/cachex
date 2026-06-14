package redisx

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// gaugeSnapshot is a per-cacheKey → metricName → value map. Writes are
// monotonic; the latest value wins. Observability can read Gauges() to
// snapshot this into OTel.
var gaugeSnapshot sync.Map // cacheKey → *sync.Map[metricName]float64

// poolStater is the minimal interface satisfied by *redis.Client and
// *redis.ClusterClient, so the monitor can run on either without coupling.
type poolStater interface {
	PoolStats() *redis.PoolStats
	Ping(ctx context.Context) *redis.StatusCmd
}

// startRedisPoolMonitor spawns a background goroutine that polls PoolStats
// every 5s and publishes the snapshot. It exits cleanly when ctx is
// cancelled (i.e. when the holder is closed).
func startRedisPoolMonitor(ctx context.Context, cacheKey string, c poolStater) {
	// Allocate the per-key inner map once.
	inner := &sync.Map{}
	gaugeSnapshot.Store(cacheKey, inner)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
				err := c.Ping(pingCtx).Err()
				cancel()
				if err != nil {
					if err.Error() == "redis: client is closed" {
						gaugeSnapshot.Delete(cacheKey)
						return
					}
					// transient ping failure: skip this tick
					continue
				}
				stats := c.PoolStats()
				if stats == nil {
					continue
				}
				inner.Store("total_conns", float64(stats.TotalConns))
				inner.Store("idle_conns", float64(stats.IdleConns))
				inner.Store("stale_conns", float64(stats.StaleConns))
				inner.Store("hits", float64(stats.Hits))
				inner.Store("misses", float64(stats.Misses))
				inner.Store("timeouts", float64(stats.Timeouts))
			}
		}
	}()
}

// Gauges returns a snapshot of every pool's last reported metrics. The outer
// map is keyed by cacheKey; the inner map by metric name. The inner map is
// allocated freshly on each call so callers can mutate it without affecting
// future snapshots.
func Gauges() map[string]map[string]float64 {
	out := make(map[string]map[string]float64)
	gaugeSnapshot.Range(func(key, value any) bool {
		cacheKey, ok1 := key.(string)
		inner, ok2 := value.(*sync.Map)
		if !ok1 || !ok2 {
			return true
		}
		snap := make(map[string]float64, 8)
		inner.Range(func(k, v any) bool {
			name, okN := k.(string)
			val, okV := v.(float64)
			if okN && okV {
				snap[name] = val
			}
			return true
		})
		out[cacheKey] = snap
		return true
	})
	return out
}

// logMonitorStart is a tiny helper kept for symmetry with the mqx reference
// and to allow future structured logging.
var _ = log.Printf
