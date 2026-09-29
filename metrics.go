// cachex/metrics.go — Prometheus 指标注册表
package cachex

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	commandTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "cachex",
			Name:      "command_total",
			Help:      "Total number of cache commands executed",
		},
		[]string{"driver", "name", "command", "status"},
	)

	commandDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "cachex",
			Name:      "command_duration_seconds",
			Help:      "Cache command duration in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
		[]string{"driver", "name", "command"},
	)

	circuitBreakerState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "cachex",
			Name:      "circuit_breaker_state",
			Help:      "Circuit breaker state (0=closed, 1=open, 2=half-open)",
		},
		[]string{"driver", "name"},
	)

	poolActiveCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "cachex",
			Name:      "pool_active_count",
			Help:      "Number of active connections in the pool",
		},
		[]string{"driver", "name"},
	)

	poolIdleCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "cachex",
			Name:      "pool_idle_count",
			Help:      "Number of idle connections in the pool",
		},
		[]string{"driver", "name"},
	)
)

// init 把指标注册到 Prometheus 默认注册器。
// 注意：同进程内只会执行一次；若宿主应用另行注册同名指标会导致 panic，
// 这种情况请改用 promhttp + 自定义 Registry 拉取本包指标。
func init() {
	prometheus.MustRegister(commandTotal)
	prometheus.MustRegister(commandDuration)
	prometheus.MustRegister(circuitBreakerState)
	prometheus.MustRegister(poolActiveCount)
	prometheus.MustRegister(poolIdleCount)
}

// RecordCommand 记录一次命令执行（供驱动调用）
func RecordCommand(driver, name, command, status string, duration time.Duration) {
	commandTotal.WithLabelValues(driver, name, command, status).Inc()
	commandDuration.WithLabelValues(driver, name, command).Observe(duration.Seconds())
}

// UpdatePoolMetrics 更新连接池指标
func UpdatePoolMetrics(driver, name string, active, idle int) {
	poolActiveCount.WithLabelValues(driver, name).Set(float64(active))
	poolIdleCount.WithLabelValues(driver, name).Set(float64(idle))
}

// UpdateCircuitBreakerState 更新熔断器状态指标
func UpdateCircuitBreakerState(driver, name string, state int) {
	circuitBreakerState.WithLabelValues(driver, name).Set(float64(state))
}

// RegisterMetricsHTTPHandler 在 addr 上启动 Prometheus /metrics 端点。
//
// 端口在返回前完成绑定，因此端口被占用、地址非法等情况会立即以 error 返回，
// 而不是在后台 goroutine 里静默失败。HTTP 服务本身随进程生命周期运行，无需关闭。
func RegisterMetricsHTTPHandler(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("cachex: listen metrics addr %q: %w", addr, err)
	}

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "cachex: metrics server error: %v\n", err)
		}
	}()
	return nil
}
