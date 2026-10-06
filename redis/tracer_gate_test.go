package redis

import (
	"context"
	"testing"

	"github.com/gospacex/cachex"
	"github.com/gospacex/traceflag"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// 本文件钉住 cachex 调用链开关的裁决顺序：
//
//	traceflag 全局总闸 AND traceflag 组件闸("cachex")
//	AND (实例级 Config.EnableTrace OR 全局 SetTracingConfig 打开)
//
// 背景（本文件存在的原因）：驱动原先只在 startSpan 里查
// cachex.IsTracingEnabled()（读全局 tracingCfg，只有 SetTracingConfig 能写）。
// configx / 独立用法下只配 instances.<name>.enable_trace 而不调
// SetTracingConfig 时，开关被解码进 Config.EnableTrace 却没有任何地方读它 ——
// span 恒为零且不打日志，表现为「明明开了埋点，Jaeger 上一个 redis span 都没有」。

// gateFixture 搭一个只受本用例控制的开关环境。
//
// 两个坑：
//  1. cachex.SetTracingConfig 内部会 traceflag.Apply("cachex", cfg.Enabled)，
//     即**顺带把组件闸也写成同一个值**。所以要模拟「全局关、实例开」，必须
//     先 SetTracingConfig(false) 再把组件闸单独拨回 true，顺序反了会被覆盖。
//  2. span 落到自建 provider（tracetest exporter），不依赖宿主是否注册过全局
//     provider，也不污染其它用例。
func gateFixture(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()

	traceflag.SetEnabled(true)
	cachex.SetTracingConfig(cachex.TracingConfig{Enabled: false, HostProvider: true})
	traceflag.SetComponent("cachex", true)
	t.Cleanup(func() {
		traceflag.SetComponent("cachex", true)
		traceflag.SetEnabled(true)
		cachex.SetTracingConfig(cachex.TracingConfig{Enabled: true, HostProvider: true})
	})
	return exp
}

// newGateClient 建一个连通真实 redis 的客户端（驱动构造会 ping，跳过会失败），
// 然后把 tracer 换成用例自己的 provider 以便断言 span。
func newGateClient(t *testing.T, tp *sdktrace.TracerProvider, enableTrace bool) *Client {
	t.Helper()
	c, err := New(&cachex.Config{
		Name:        "gate",
		Driver:      "redis",
		Addr:        testRedisAddr(),
		Password:    testRedisPassword(),
		DB:          15,
		EnableTrace: enableTrace,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	c.tracer = tp.Tracer("cachex-redis-gate")
	return c
}

// 实例级开关必须能**单独**打开 span（不依赖 SetTracingConfig）。
func TestStartSpan_InstanceSwitchAlone(t *testing.T) {
	exp := gateFixture(t)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))

	c := newGateClient(t, tp, true)
	_, span := c.startSpan(context.Background(), "GET", "gate:key")
	span.End()
	require.Len(t, exp.GetSpans(), 1, "实例级 enable_trace=true 且未调 SetTracingConfig 时也应出 span")
}

// 开关关闭时不得出 span（改判据不能把关闭路径一起打开）。
func TestStartSpan_InstanceSwitchOff(t *testing.T) {
	exp := gateFixture(t)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))

	c := newGateClient(t, tp, false)
	_, span := c.startSpan(context.Background(), "GET", "gate:key")
	span.End()
	require.Zero(t, len(exp.GetSpans()), "实例级开关关闭时不应产生任何 span")
}

// 全局 SetTracingConfig 打开时，即使实例级没写也要出 span（launchx 的 cache
// 组件走的就是这条兜底路径，不能因为加了实例级判据就把它弄丢）。
func TestStartSpan_GlobalConfigFallback(t *testing.T) {
	exp := gateFixture(t)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	cachex.SetTracingConfig(cachex.TracingConfig{Enabled: true, HostProvider: true})

	c := newGateClient(t, tp, false)
	_, span := c.startSpan(context.Background(), "SET", "gate:global")
	span.End()
	require.Len(t, exp.GetSpans(), 1, "全局开关打开时应兜住实例级未配置的场景")
}

// traceflag 组件闸关闭时，实例级开关不得翻盘。
func TestStartSpan_RespectsTraceflagComponent(t *testing.T) {
	exp := gateFixture(t)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	cachex.SetTracingConfig(cachex.TracingConfig{Enabled: true, HostProvider: true})
	traceflag.SetComponent("cachex", false) // 必须在 SetTracingConfig 之后

	c := newGateClient(t, tp, true)
	_, span := c.startSpan(context.Background(), "GET", "gate:off")
	span.End()
	require.Zero(t, len(exp.GetSpans()), "traceflag 组件闸关闭时实例级开关不得翻盘")
}

// traceflag 全局总闸关闭时同理，且优先级最高。
func TestStartSpan_RespectsGlobalKillSwitch(t *testing.T) {
	exp := gateFixture(t)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	cachex.SetTracingConfig(cachex.TracingConfig{Enabled: true, HostProvider: true})
	traceflag.SetEnabled(false)

	c := newGateClient(t, tp, true)
	_, span := c.startSpan(context.Background(), "GET", "gate:kill")
	span.End()
	require.Zero(t, len(exp.GetSpans()), "全局总闸关闭时任何实例级开关都不得翻盘")
}
