package kafkax

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

// gaugeSnapshot is a per-cacheKey → metricName → value map.
var gaugeSnapshot sync.Map // cacheKey → *sync.Map[metricName]float64

// sendCounter tracks per-producer sent-message counts. We can't use sarama's
// built-in stats because the SyncProducer/AsyncProducer interfaces don't
// expose a getter. Callers can increment this via PPS/PPC if they want
// per-message counters surfaced; otherwise the connection-count gauges are
// the primary observability signal.
var sendCounter sync.Map // cacheKey → *int64

// startSaramaStatsMonitor spawns a background goroutine that polls the
// underlying sarama.Client every 5s and publishes connection-level gauges:
//   - broker_count     number of brokers known to the client
//   - open_connections number of currently-open connections (best-effort)
//   - topic_count      number of topics known to the client
//   - controller_alive 1 if Controller() returned nil error, 0 otherwise
//
// The goroutine exits cleanly when ctx is cancelled (i.e. when the holder
// is closed).
func startSaramaStatsMonitor(ctx context.Context, cacheKey string, client sarama.Client, prod any) {
	inner := &sync.Map{}
	gaugeSnapshot.Store(cacheKey, inner)
	var ctr int64
	sendCounter.Store(cacheKey, &ctr)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				gaugeSnapshot.Delete(cacheKey)
				sendCounter.Delete(cacheKey)
				return
			case <-ticker.C:
				brokers := client.Brokers()
				inner.Store("broker_count", float64(len(brokers)))

				open := 0
				for _, b := range brokers {
					if b == nil {
						continue
					}
					if ok, _ := b.Connected(); ok {
						open++
					}
				}
				inner.Store("open_connections", float64(open))

				topics, terr := client.Topics()
				if terr == nil {
					inner.Store("topic_count", float64(len(topics)))
				}

				_, cerr := client.Controller()
				if cerr == nil {
					inner.Store("controller_alive", 1.0)
				} else {
					inner.Store("controller_alive", 0.0)
				}

				inner.Store("send_count", float64(atomic.LoadInt64(&ctr)))
			}
		}
	}()
}

// Gauges returns a snapshot of every producer pool's last reported metrics.
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
