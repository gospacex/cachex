// cachex/tracing.go — 全局调用链开关与 TracerProvider 初始化
package cachex

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	cachexotel "github.com/gospacex/cachex/otel"
	"github.com/gospacex/traceflag"
	oteltrace "go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TracingConfig 全局调用链开关及上报通道配置
//
// 同 InstanceConfig：yaml / json / mapstructure 三套 tag 齐备，
// 保证 configx 的本地 viper 通道与远程 JSON 通道读取结果一致。
type TracingConfig struct {
	Enabled        bool     `yaml:"enabled" json:"enabled" mapstructure:"enabled"`
	Exporter       string   `yaml:"exporter" json:"exporter" mapstructure:"exporter"` // "jaeger" | "jaeger-grpc" | "kafka"（默认 "jaeger"）
	JaegerEndpoint string   `yaml:"jaeger_endpoint" json:"jaeger_endpoint" mapstructure:"jaeger_endpoint"`
	JaegerGRPC     bool     `yaml:"jaeger_grpc" json:"jaeger_grpc" mapstructure:"jaeger_grpc"`
	KafkaBrokers   []string `yaml:"kafka_brokers" json:"kafka_brokers" mapstructure:"kafka_brokers"`
	KafkaTopic     string   `yaml:"kafka_topic" json:"kafka_topic" mapstructure:"kafka_topic"`
	ServiceName    string   `yaml:"service_name" json:"service_name" mapstructure:"service_name"`
	ServiceVersion string   `yaml:"service_version" json:"service_version" mapstructure:"service_version"`
	Insecure       bool     `yaml:"insecure" json:"insecure" mapstructure:"insecure"`
	TLSEnabled     bool     `yaml:"tls_enabled" json:"tls_enabled" mapstructure:"tls_enabled"`
	BatchSize      int      `yaml:"batch_size" json:"batch_size" mapstructure:"batch_size"`
	// Endpoint 通用导出端点（与 dbx 的 tracing.endpoint 对齐；jaeger_endpoint 为空时兜底）
	Endpoint string `yaml:"endpoint" json:"endpoint" mapstructure:"endpoint"`
	// SamplerRatio 采样率 0~1（与 dbx 的 tracing.sampler_ratio 对齐）
	SamplerRatio float64 `yaml:"sampler_ratio" json:"sampler_ratio" mapstructure:"sampler_ratio"`
	// HostProvider 表示「宿主已注册全局 TracerProvider，SDK 不要抢全局位」。
	//
	// 为什么需要：本方法默认会用上面的 exporter 配置自建一个 TracerProvider 并
	// otel.SetTracerProvider 注册到全局。独立运行（examples / 单测）时这是对的；
	// 但在 launchx / dbx 这类宿主里，全局 provider 由宿主的 telemetry 步骤注册
	// （含传播器与服务名资源），SDK 再抢一次会把宿主的整套链路打回 SDK 自带的
	// exporter —— 其余组件的 span 直接消失，且看不出是谁干的。
	// 置 true 时只开开关、不初始化任何 provider，span 走的仍是宿主的 provider。
	HostProvider bool `yaml:"host_provider" json:"host_provider" mapstructure:"host_provider"`
}

var (
	tracingCfg   TracingConfig
	tracingCfgMu sync.RWMutex
	// tpOnce 与 tpErr 由 tpMu 保护：初始化失败时重置 tpOnce，允许下一次调用重试。
	tpOnce sync.Once
	tpErr  error
	tpMu   sync.Mutex
)

// SetTracingConfig 设置全局调用链开关及上报通道配置（可由 Watch 回调调用）。
//
// 开关打开且 TracerProvider 尚未初始化时，自动初始化并注册到全局 otel。
// 初始化失败仅记录错误，不阻断 SDK——调用链降级为不上报，业务读写不受影响。
//
// 例外：HostProvider=true 时只登记开关、不初始化 provider（宿主已管起来了，
// 抢全局位会把宿主其余组件的 span 一并打没，详见 Config.HostProvider）。
func SetTracingConfig(cfg TracingConfig) {
	// 接入 hubx 统一调用链开关：组件自身配置为主开关，全局总闸
	// traceflag.SetEnabled(false) 可一键熔断 cachex。这里同时完成
	// "注册组件级开关" 与 "按总闸收敛"两件事，避免两处调用脱节。
	// 注意热更新路径（Watch 回调）也会走到这里，因此开关变化会实时同步。
	cfg.Enabled = traceflag.Apply("cachex", cfg.Enabled)

	tracingCfgMu.Lock()
	tracingCfg = cfg
	tracingCfgMu.Unlock()

	if cfg.HostProvider {
		// 宿主托管模式：只登记开关，绝不碰全局 provider（理由见 Config.HostProvider）。
		return
	}

	if !cfg.Enabled {
		return
	}

	tpMu.Lock()
	defer tpMu.Unlock()
	tpOnce.Do(func() {
		tpErr = initTracerProvider(cfg)
		if tpErr != nil {
			// 允许下一次调用（例如配置热更新 exporter 地址后）重试
			tpOnce = sync.Once{}
		}
	})
}

// TracingError 返回 TracerProvider 最近一次初始化的错误，nil 表示已就绪或未启用
func TracingError() error {
	tpMu.Lock()
	defer tpMu.Unlock()
	return tpErr
}

// IsTracingEnabled 返回当前全局调用链开关状态。
//
// 除组件自身配置外还要过一遍 traceflag.EnabledFor，这样运行期调用
// traceflag.SetEnabled(false) 能立刻关停 cachex 的埋点（配置热更新
// 之外的第二条熔断通路）。
func IsTracingEnabled() bool {
	if !traceflag.EnabledFor("cachex") {
		return false
	}
	tracingCfgMu.RLock()
	defer tracingCfgMu.RUnlock()
	return tracingCfg.Enabled
}

// initTracerProvider 根据 TracingConfig 创建 TracerProvider。
//
// 关键：创建完成后必须调用 otel.SetTracerProvider 注册到全局。
// 此前版本只把 provider 存进包内变量而未注册全局，导致 span 数据量产后
// 无人消费——开关形同虚设，这是本轮 review 修复的核心缺陷。
func initTracerProvider(cfg TracingConfig) error {
	setTP := func(p *sdktrace.TracerProvider) {
		oteltrace.SetTracerProvider(p)
	}

	endpoint := cfg.JaegerEndpoint
	if endpoint == "" {
		endpoint = cfg.Endpoint
	}

	switch cfg.Exporter {
	case "kafka":
		if len(cfg.KafkaBrokers) == 0 {
			return fmt.Errorf("cachex: kafka exporter requires at least one broker")
		}
		return cachexotel.InitKafka(&cachexotel.KafkaConfig{
			Brokers:        cfg.KafkaBrokers,
			Topic:          defaultKafkaTopic(cfg.KafkaTopic),
			ServiceName:    defaultServiceName(cfg.ServiceName),
			ServiceVersion: cfg.ServiceVersion,
			BatchSize:      cfg.BatchSize,
			SamplerRatio:   cfg.SamplerRatio,
		}, setTP)

	case "jaeger-grpc":
		if endpoint == "" {
			endpoint = "localhost:4317"
		}
		return cachexotel.InitJaegerGRPC(&cachexotel.JaegerConfig{
			Endpoint:       endpoint,
			Insecure:       cfg.Insecure,
			TLSEnabled:     cfg.TLSEnabled,
			ServiceName:    defaultServiceName(cfg.ServiceName),
			ServiceVersion: cfg.ServiceVersion,
			BatchSize:      cfg.BatchSize,
			SamplerRatio:   cfg.SamplerRatio,
		}, setTP)

	default: // "jaeger"，OTLP/HTTP
		if endpoint == "" {
			endpoint = "localhost:4318"
		}
		return cachexotel.InitJaeger(&cachexotel.JaegerConfig{
			Endpoint:       endpoint,
			Insecure:       cfg.Insecure,
			TLSEnabled:     cfg.TLSEnabled,
			ServiceName:    defaultServiceName(cfg.ServiceName),
			ServiceVersion: cfg.ServiceVersion,
			BatchSize:      cfg.BatchSize,
			SamplerRatio:   cfg.SamplerRatio,
		}, setTP)
	}
}

// shutdownTracing 冲刷并关闭全局 TracerProvider，由 CloseAll 调用
func shutdownTracing() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cachexotel.Shutdown(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "cachex: shutdown tracer provider: %v\n", err)
	}
}

func defaultKafkaTopic(t string) string {
	if t == "" {
		return "cachex-traces"
	}
	return t
}

func defaultServiceName(n string) string {
	if n == "" {
		return "cachex"
	}
	return n
}
