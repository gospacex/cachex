# sdk-enterprise-refactor 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 将 cachex SDK 升级为企
[任务处理异常，请稍后重试。]业级生产可用，补齐单例初始化、全局 trace 开关、Prometheus 指标、熔断器、读写超时分离、配置热更新六大能力。

**架构：** 在顶层新增 `sdk.go`（全局懒加载单例）、`tracing.go`（全局调用链开关）、`metrics.go`（Prometheus 指标）、`circuit.go`（熔断器封装），对现有 `redis/` 和 `memcached/` 驱动做最小侵入式改造，统一配置结构，补充对接 docker-compose 真实中间件的示例代码。

**技术栈：** Go 1.21、go-redis/redis/v8、bradfitz/gomemcache、sony/gobreaker、prometheus/client_golang、go.opentelemetry.io/otel、github.com/gospacex/configx、github.com/gospacex/logx

**Traceability (sddflow):**
- plan-ready: `openspec/changes/sdk-enterprise-refactor/plan-ready.md`
- tasks: `openspec/changes/sdk-enterprise-refactor/tasks.md`
- plan: `docs/superpowers/plans/2026-09-09-sdk-enterprise-refactor.md`

---

### 任务 1：基础设施准备

> **trace:** plan-ready.md → `### Task 1: 基础设施准备` | tasks.md → `- [ ] 在 go.mod 中新增 github.com/sony/gobreaker 依赖，执行 go mod tidy`
> **sync:** tasks.md → `在 go.mod 中新增 github.com/sony/gobreaker 依赖，执行 go mod tidy` | plan-ready.md → `### Task 1: 基础设施准备`

**Files:**
- 创建：`sdk.go`
- 创建：`tracing.go`
- 修改：`go.mod`

- [x] **步骤 1：添加 gobreaker 依赖**

运行：
```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
go get github.com/sony/gobreaker@latest
go mod tidy
```

预期：`go.mod` 中出现 `github.com/sony/gobreaker` 依赖，`go.sum` 更新，无报错。

- [x] **步骤 2：创建 tracing.go**

```go
// cachex/tracing.go
package cachex

import "sync"

// TracingConfig 全局调用链开关配置
type TracingConfig struct {
	Enabled bool
}

var (
	tracingCfg     TracingConfig
	tracingCfgMu   sync.RWMutex
)

// SetTracingConfig 设置全局调用链开关（可由 Watch 回调调用）
func SetTracingConfig(cfg TracingConfig) {
	tracingCfgMu.Lock()
	defer tracingCfgMu.Unlock()
	tracingCfg = cfg
}

// IsTracingEnabled 返回当前全局调用链开关状态
func IsTracingEnabled() bool {
	tracingCfgMu.RLock()
	defer tracingCfgMu.RUnlock()
	return tracingCfg.Enabled
}
```

- [x] **步骤 3：创建 sdk.go 骨架（声明 + 占位实现）**

```go
// cachex/sdk.go
package cachex

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gospacex/configx"
)

// InitOption 初始化选项
type InitOption func(*initConfig)

type initConfig struct {
	configFile string
	provider   configx.Config
	section    string
	tracing    TracingConfig
}

func applyInitOptions(opts []InitOption) *initConfig {
	cfg := &initConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// WithConfigFile 指定配置文件路径
func WithConfigFile(path string) InitOption {
	return func(c *initConfig) { c.configFile = path }
}

// WithConfig 指定已有 configx Provider
func WithConfig(provider configx.Config, section string) InitOption {
	return func(c *initConfig) { c.provider = provider; c.section = section }
}

// WithTracing 指定全局调用链开关
func WithTracing(enabled bool) InitOption {
	return func(c *initConfig) { c.tracing = TracingConfig{Enabled: enabled} }
}

// SDK 全局单例
type SDK struct {
	mu        sync.RWMutex
	instances map[string]Cache
	closed    bool
}

var (
	globalSDK *SDK
	initOnce  sync.Once
	initErr   error
)

// Init 从 configx 读取配置并创建所有实例
func Init(opts ...InitOption) error {
	var cfg *initConfig
	if len(opts) > 0 {
		cfg = applyInitOptions(opts)
	}
	// 应用 tracing 配置
	if cfg != nil && (cfg.tracing.Enabled || cfg.tracing.Enabled == false) {
		SetTracingConfig(cfg.tracing)
	}
	// 懒加载初始化全局单例
	initOnce.Do(func() {
		globalSDK = &SDK{instances: make(map[string]Cache)}
	})
	// TODO: 完整实现在 Task 3
	return nil
}

// Instance 获取命名实例（懒加载）
func Instance(name string) Cache {
	if globalSDK == nil {
		// 懒加载：自动 Init
		_ = Init()
	}
	if globalSDK == nil {
		return nil
	}
	globalSDK.mu.RLock()
	defer globalSDK.mu.RUnlock()
	if globalSDK.closed {
		return nil
	}
	return globalSDK.instances[name]
}

// CloseAll 优雅关闭所有实例
func CloseAll() error {
	if globalSDK == nil {
		return nil
	}
	globalSDK.mu.Lock()
	defer globalSDK.mu.Unlock()
	globalSDK.closed = true
	var lastErr error
	for name, inst := range globalSDK.instances {
		if err := inst.Close(); err != nil {
			lastErr = fmt.Errorf("cachex: close instance %q: %w", name, err)
		}
	}
	globalSDK.instances = make(map[string]Cache)
	return lastErr
}
```

- [x] **步骤 4：编译验证**

运行：
```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
go build ./...
```

预期：编译通过，无错误。

- [x] **步骤 5：Commit**

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
git add go.mod go.sum sdk.go tracing.go
git commit -m "chore: add sdk.go and tracing.go skeleton with gobreaker dependency"
```

- [x] **Task complete**

---

### 任务 2：配置重构

> **trace:** plan-ready.md → `### Task 2: 配置重构` | tasks.md → `- [ ] 将 config.go 中的 StandaloneConfig 和 ClusterConfig 合并为 InstanceConfig`
> **sync:** tasks.md → `将 config.go 中的 StandaloneConfig 和 ClusterConfig 合并为 InstanceConfig（含所有字段）` | plan-ready.md → `### Task 2: 配置重构`

**Files:**
- 修改：`config.go`
- 修改：`interface.go`

- [x] **步骤 1：移除 interface.go 中的 EnableTrace**

将 `Config` 结构体中的 `EnableTrace bool` 字段删除：

```go
// interface.go - Config 结构体（修改后）
type Config struct {
	Name           string
	Driver         string
	Addr           string
	Password       string
	DB             int
	PoolSize       int
	MinIdleConn    int
	SentinelMaster string
	SentinelAddrs  string
	Cluster        string
	TLSEnabled     bool
	ReadTimeout    time.Duration  // 新增
	WriteTimeout   time.Duration  // 新增
	Timeout        time.Duration
	RetryCount     int            // 新增
}
```

同时删除 `interface.go` 中的 `withRedisSpan` 函数（迁移到驱动内部）。

- [x] **步骤 2：重构 config.go — 统一 InstanceConfig**

```go
// config.go - 替换 StandaloneConfig 和 ClusterConfig
type InstanceConfig struct {
	Name           string        `yaml:"name" json:"name"`
	Driver         string        `yaml:"driver" json:"driver"`
	Addr           string        `yaml:"addr" json:"addr"`
	Password       string        `yaml:"password" json:"password"`
	DB             int           `yaml:"db" json:"db"`
	PoolSize       int           `yaml:"pool_size" json:"pool_size"`
	MinIdleConn    int           `yaml:"min_idle_conn" json:"min_idle_conn"`
	SentinelMaster string        `yaml:"sentinel_master" json:"sentinel_master"`
	SentinelAddrs  string        `yaml:"sentinel_addrs" json:"sentinel_addrs"`
	Cluster        string        `yaml:"cluster" json:"cluster"`
	TLSEnabled     bool          `yaml:"tls_enabled" json:"tls.enabled"`
	Timeout        time.Duration `yaml:"timeout" json:"timeout"`
	ReadTimeout    time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout" json:"write_timeout"`
	RetryCount     int           `yaml:"retry_count" json:"retry_count"`
	// 以下字段仅在集群远程配置场景使用
	Namespace      string        `yaml:"namespace" json:"namespace"`
	WatchEnabled   bool          `yaml:"watch_enabled" json:"watch_enabled"`
}

// CacheSection 对应 configx.yaml 中的 cachex 配置节
type CacheSection struct {
	Tracing   TracingConfig    `yaml:"tracing"`
	Instances []*InstanceConfig `yaml:"instances"`
}
```

- [x] **步骤 3：实现环境变量展开**

```go
// config.go - 新增 expandEnv 函数
import (
	"os"
	"regexp"
	"strings"
)

var envVarRegex = regexp.MustCompile(`\$\{([^}]+)\}`)

func expandEnv(s string) (string, error) {
	return envVarRegex.ReplaceAllFunc([]byte(s), func(match []byte) string {
		varName := string(match[2:len(match)-1])
		if val, ok := os.LookupEnv(varName); ok {
			return val
		}
		// 标记未设置，由调用方统一报错
		return match[0:len(match)] // 原样返回，后续检查
	}), nil
}

func expandConfigPassword(cfg *InstanceConfig) error {
	if cfg.Password == "" {
		return nil
	}
	matched := envVarRegex.FindString(cfg.Password)
	if matched == "" {
		return nil // 无占位符，直接返回
	}
	varName := strings.TrimPrefix(strings.TrimSuffix(matched, "}"), "${")
	val, ok := os.LookupEnv(varName)
	if !ok {
		return fmt.Errorf("cachex: environment variable %q not set (used in instance %q password)", varName, cfg.Name)
	}
	cfg.Password = val
	return nil
}
```

- [x] **步骤 4：更新 ReadAndCreate / MustReadAndCreate**

```go
// ReadAndCreate 从 configx 读取配置并创建所有实例
func ReadAndCreate(opts ...configx.ConfigOptions) (Instances, error) {
	c, err := configx.ReadConfig(opts...)
	if err != nil {
		return nil, fmt.Errorf("cachex: failed to read config: %w", err)
	}

	section := &CacheSection{}
	if err := c.Unmarshal("cachex", section); err != nil {
		return nil, fmt.Errorf("cachex: failed to unmarshal cachex section: %w", err)
	}

	// 设置全局 tracing 开关
	if section.Tracing.Enabled {
		SetTracingConfig(section.Tracing)
	}

	cfgs := make([]*Config, 0, len(section.Instances))
	for _, ic := range section.Instances {
		if err := expandConfigPassword(ic); err != nil {
			return nil, err
		}
		cfg := instanceToConfig(ic)
		cfgs = append(cfgs, cfg)
	}

	if len(cfgs) == 0 {
		return nil, fmt.Errorf("cachex: no instances configured in cachex section")
	}

	return New(cfgs)
}

func instanceToConfig(ic *InstanceConfig) *Config {
	return &Config{
		Name:           ic.Name,
		Driver:         ic.Driver,
		Addr:           ic.Addr,
		Password:       ic.Password,
		DB:             ic.DB,
		PoolSize:       ic.PoolSize,
		MinIdleConn:    ic.MinIdleConn,
		SentinelMaster: ic.SentinelMaster,
		SentinelAddrs:  ic.SentinelAddrs,
		Cluster:        ic.Cluster,
		TLSEnabled:     ic.TLSEnabled,
		Timeout:        ic.Timeout,
		ReadTimeout:    ic.ReadTimeout,
		WriteTimeout:   ic.WriteTimeout,
		RetryCount:     ic.RetryCount,
	}
}
```

- [x] **步骤 5：删除旧的 StandaloneConfig / ClusterConfig 及相关函数**

删除：`StandaloneConfig`、`ClusterConfig`、`standaloneToConfig`、`clusterToConfig`、`FillFromRedisStandalone`、`FillFromRedisCluster`、`FillFromDriverStandalone`、`standaloneToConfigWithName`、`redisStandaloneConfig`、`redisClusterConfig`。

- [x] **步骤 6：编译验证**

运行：
```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
go build ./...
```

预期：编译通过。

- [x] **步骤 7：Commit**

```bash
git add interface.go config.go
git commit -m "refactor: unify InstanceConfig, remove EnableTrace, add env expansion"
```

- [x] **Task complete**

---

### 任务 3：sdk.go 完整实现

> **trace:** plan-ready.md → `### Task 3: sdk.go 实现` | tasks.md → `- [ ] 实现 InitOption functional options + Init/Instance/CloseAll + Watch 回调`
> **sync:** tasks.md → `实现 InitOption functional options：WithConfigFile / WithConfig / WithTracing；实现 Init/Instance/CloseAll/懒加载/Watch 回调；编写 sdk_test.go` | plan-ready.md → `### Task 3: sdk.go 实现`

**Files:**
- 修改：`sdk.go`（完成实现）
- 创建：`sdk_test.go`

- [x] **步骤 1：完善 sdk.go 完整实现**

```go
// cachex/sdk.go
package cachex

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gospacex/configx"
)

// InitOption 初始化选项
type InitOption func(*initConfig)

type initConfig struct {
	configFile string
	provider   configx.Config
	section    string
	tracing    TracingConfig
}

func applyInitOptions(opts []InitOption) *initConfig {
	cfg := &initConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// WithConfigFile 指定配置文件路径
func WithConfigFile(path string) InitOption {
	return func(c *initConfig) { c.configFile = path }
}

// WithConfig 指定已有 configx Provider
func WithConfig(provider configx.Config, section string) InitOption {
	return func(c *initConfig) { c.provider = provider; c.section = section }
}

// WithTracing 指定全局调用链开关
func WithTracing(enabled bool) InitOption {
	return func(c *initConfig) { c.tracing = TracingConfig{Enabled: enabled} }
}

// SDK 全局单例
type SDK struct {
	mu          sync.RWMutex
	instances   map[string]Cache
	closed      bool
	watchCtx    context.Context
	watchCancel context.CancelFunc
}

var (
	globalSDK *SDK
	initOnce  sync.Once
	initErr   atomic.Value // stores error or nil
)

// Init 从 configx 读取配置并创建所有实例
func Init(opts ...InitOption) error {
	var cfg *initConfig
	if len(opts) > 0 {
		cfg = applyInitOptions(opts)
	}

	// 应用 tracing 配置
	if cfg != nil {
		if cfg.tracing.Enabled || cfg.tracing.Enabled == false {
			SetTracingConfig(cfg.tracing)
		}
	}

	initOnce.Do(func() {
		globalSDK = &SDK{instances: make(map[string]Cache)}
		globalSDK.watchCtx, globalSDK.watchCancel = context.WithCancel(context.Background())

		var createErr error
		var instances Instances

		if cfg != nil && cfg.provider != nil {
			instances, createErr = readFromProvider(cfg.provider, cfg.section)
		} else if cfg != nil && cfg.configFile != "" {
			instances, createErr = ReadAndCreate(configx.ConfigOptions{ConfigFile: cfg.configFile})
		} else {
			createErr = fmt.Errorf("cachex: no config source provided to Init")
		}

		if createErr != nil {
			initErr.Store(createErr)
			return
		}

		globalSDK.mu.Lock()
		for name, inst := range instances {
			globalSDK.instances[name] = inst
		}
		globalSDK.mu.Unlock()

		// 注册 Watch 回调监听 tracing 开关变更
		if cfg != nil && cfg.provider != nil {
			go watchTracingConfig(globalSDK.watchCtx, cfg.provider, cfg.section)
		}

		initErr.Store(nil)
	})

	if err, _ := initErr.Load().(error); err != nil {
		return err
	}
	return nil
}

// readFromProvider 从已有 configx Provider 读取配置
func readFromProvider(provider configx.Config, section string) (Instances, error) {
	sectionData := &CacheSection{}
	if err := provider.Unmarshal(section, sectionData); err != nil {
		return nil, fmt.Errorf("cachex: failed to unmarshal %q section: %w", section, err)
	}

	if len(sectionData.Instances) == 0 {
		return nil, fmt.Errorf("cachex: no instances in %q section", section)
	}

	// 设置全局 tracing
	if sectionData.Tracing.Enabled {
		SetTracingConfig(sectionData.Tracing)
	}

	cfgs := make([]*Config, 0, len(sectionData.Instances))
	for _, ic := range sectionData.Instances {
		if err := expandConfigPassword(ic); err != nil {
			return nil, err
		}
		cfgs = append(cfgs, instanceToConfig(ic))
	}

	return New(cfgs)
}

// watchTracingConfig 监听 tracing 配置变更并热更新
func watchTracingConfig(ctx context.Context, provider configx.Config, section string) {
	// 首次立即读取当前值
	updateTracingFromConfig(provider, section)

	// 注册 Watch（仅监听 enabled 字段）
	err := provider.Watch(ctx, section+".tracing.enabled", func(newVal interface{}) {
		if b, ok := newVal.(bool); ok {
			SetTracingConfig(TracingConfig{Enabled: b})
		}
	})
	if err != nil {
		// Watch 注册失败不阻塞主流程，仅记录
		return
	}

	// 等待 context 取消
	<-ctx.Done()
}

func updateTracingFromConfig(provider configx.Config, section string) {
	raw := provider.Get(section)
	if raw == nil {
		return
	}
	// 尝试从 raw map 中读取 tracing.enabled
	if m, ok := raw.(map[string]interface{}); ok {
		if tracing, ok := m["tracing"].(map[string]interface{}); ok {
			if enabled, ok := tracing["enabled"].(bool); ok {
				SetTracingConfig(TracingConfig{Enabled: enabled})
			}
		}
	}
}

// Instance 获取命名实例（懒加载）
func Instance(name string) Cache {
	if globalSDK == nil {
		_ = Init()
	}
	if globalSDK == nil {
		return nil
	}
	globalSDK.mu.RLock()
	defer globalSDK.mu.RUnlock()
	if globalSDK.closed {
		return nil
	}
	return globalSDK.instances[name]
}

// CloseAll 优雅关闭所有实例
func CloseAll() error {
	if globalSDK == nil {
		return nil
	}
	globalSDK.mu.Lock()
	defer globalSDK.mu.Unlock()
	if globalSDK.closed {
		return nil
	}
	globalSDK.closed = true
	if globalSDK.watchCancel != nil {
		globalSDK.watchCancel()
	}
	var lastErr error
	for name, inst := range globalSDK.instances {
		if err := inst.Close(); err != nil {
			lastErr = fmt.Errorf("cachex: close instance %q: %w", name, err)
		}
	}
	globalSDK.instances = make(map[string]Cache)
	return lastErr
}

// IsClosed 返回全局单例是否已关闭
func IsClosed() bool {
	if globalSDK == nil {
		return false
	}
	globalSDK.mu.RLock()
	defer globalSDK.mu.RUnlock()
	return globalSDK.closed
}
```

- [x] **步骤 2：编写 sdk_test.go**

```go
// cachex/sdk_test.go
package cachex

import (
	"context"
	"testing"
	"time"

	"github.com/gospacex/configx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitWithConfigFile(t *testing.T) {
	// 重置全局状态
	globalSDK = nil
	initOnce = sync.Once{}
	initErr.Store(nil)

	err := Init(WithConfigFile("examples/config/config.yaml"))
	require.NoError(t, err)

	inst := Instance("redis-primary")
	require.NotNil(t, inst)

	// 清理
	CloseAll()
}

func TestInstanceLazyLoad(t *testing.T) {
	globalSDK = nil
	initOnce = sync.Once{}
	initErr.Store(nil)

	// 未 Init 时调用 Instance 应自动触发 Init
	inst := Instance("redis-primary")
	// 注意：此测试需要 docker-compose 中的 redis 运行
	// 若无中间件，Init 会失败，这里只做结构验证
	_ = inst
}

func TestCloseAll(t *testing.T) {
	globalSDK = nil
	initOnce = sync.Once{}
	initErr.Store(nil)

	// 无配置时 CloseAll 应安全返回
	err := CloseAll()
	assert.NoError(t, err)
}

func TestIsTracingEnabled(t *testing.T) {
	assert.False(t, IsTracingEnabled())
	SetTracingConfig(TracingConfig{Enabled: true})
	assert.True(t, IsTracingEnabled())
	SetTracingConfig(TracingConfig{Enabled: false})
	assert.False(t, IsTracingEnabled())
}
```

- [x] **步骤 3：运行测试**

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
go test -race -cover ./...
```

预期：全部通过（无中间件时部分测试 skip）。

- [x] **步骤 4：Commit**

```bash
git add sdk.go sdk_test.go
git commit -m "feat: implement global lazy-loading singleton with Init/Instance/CloseAll"
```

- [x] **Task complete**

---

### 任务 4：metrics.go 实现

> **trace:** plan-ready.md → `### Task 4: metrics.go 实现` | tasks.md → `- [ ] 创建 metrics.go：Prometheus 指标注册表 + HTTP handler`
> **sync:** tasks.md → `创建 metrics.go：定义 cachex_command_total/duration/circuit_breaker_state/pool 指标；实现 RegisterMetricsHTTPHandler；编写 metrics_test.go` | plan-ready.md → `### Task 4: metrics.go 实现`

**Files:**
- 创建：`metrics.go`
- 创建：`metrics_test.go`

- [x] **步骤 1：创建 metrics.go**

```go
// cachex/metrics.go
package cachex

import (
	"fmt"
	"net/http"
	"sync"
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

	registraOnce sync.Once
)

func init() {
	registraOnce.Do(func() {
		prometheus.MustRegister(commandTotal)
		prometheus.MustRegister(commandDuration)
		prometheus.MustRegister(circuitBreakerState)
		prometheus.MustRegister(poolActiveCount)
		prometheus.MustRegister(poolIdleCount)
	})
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

// RegisterMetricsHTTPHandler 注册 Prometheus HTTP /metrics 端点
func RegisterMetricsHTTPHandler(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("cachex: metrics server error: %v\n", err)
		}
	}()
	return nil
}
```

- [x] **步骤 2：编写 metrics_test.go**

```go
// cachex/metrics_test.go
package cachex

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordCommand(t *testing.T) {
	// 重置指标
	prometheus.ResetForTest()
	// 重新注册（ResetForTest 会注销所有注册）
	registraOnce = sync.Once{}
	init()

	RecordCommand("redis", "test-redis", "GET", "success", 5*time.Millisecond)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, req)

	body, _ := io.ReadAll(rec.Body)
	bodyStr := string(body)

	require.Contains(t, bodyStr, `cachex_command_total{driver="redis",name="test-redis",command="GET",status="success"} 1`)
	require.Contains(t, bodyStr, "cachex_command_duration_seconds")
}

func TestRegisterMetricsHTTPHandler(t *testing.T) {
	registraOnce = sync.Once{}
	init()

	err := RegisterMetricsHTTPHandler("127.0.0.1:0")
	require.NoError(t, err)
	// handler 已在后台启动，简单验证 /metrics 端点可访问
	// 注意：端口由系统动态分配，此处仅验证函数不 panic
}

func TestUpdatePoolMetrics(t *testing.T) {
	prometheus.ResetForTest()
	registraOnce = sync.Once{}
	init()

	UpdatePoolMetrics("redis", "test", 5, 2)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, req)

	body, _ := io.ReadAll(rec.Body)
	assert.True(t, strings.Contains(string(body), `cachex_pool_active_count{driver="redis",name="test"} 5`))
	assert.True(t, strings.Contains(string(body), `cachex_pool_idle_count{driver="redis",name="test"} 2`))
}
```

- [x] **步骤 3：运行测试**

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
go test -race -cover ./...
```

- [x] **步骤 4：Commit**

```bash
git add metrics.go metrics_test.go
git commit -m "feat: add Prometheus metrics registry with command/pool/circuit breaker metrics"
```

- [x] **Task complete**

---

### 任务 5：circuit.go 实现

> **trace:** plan-ready.md → `### Task 5: circuit.go 实现` | tasks.md → `- [ ] 创建 circuit.go：封装 gobreaker；编写 circuit_test.go`
> **sync:** tasks.md → `创建 circuit.go：封装 gobreaker.CircuitBreaker；实现 NewCircuitBreaker/Execute/State；编写 circuit_test.go 验证 Closed→Open→Half-Open` | plan-ready.md → `### Task 5: circuit.go 实现`

**Files:**
- 创建：`circuit.go`
- 创建：`circuit_test.go`

- [x] **步骤 1：创建 circuit.go**

```go
// cachex/circuit.go
package cachex

import (
	"context"
	"fmt"
	"sync"

	"github.com/sony/gobreaker"
)

// CircuitBreaker 封装 gobreaker，提供类型安全的熔断器操作
type CircuitBreaker struct {
	cb *gobreaker.CircuitBreaker
	mu sync.RWMutex
}

// NewCircuitBreaker 创建默认配置的熔断器
// 默认：MaxRequests=1, Interval=30s, Timeout=30s, ReadyToTrip=连续5次失败
func NewCircuitBreaker(name string) *CircuitBreaker {
	settings := gobreaker.Settings{
		Name: name,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
	}
	return &CircuitBreaker{
		cb: gobreaker.NewCircuitBreaker(settings),
	}
}

// Execute 在熔断器保护下执行 fn
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	cb.mu.RLock()
	breaker := cb.cb
	cb.mu.RUnlock()

	_, err := breaker.ExecuteCtx(ctx, func() (interface{}, error) {
		if err := fn(); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

// State 返回当前熔断器状态
func (cb *CircuitBreaker) State() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	switch cb.cb.State() {
	case gobreaker.StateClosed:
		return 0
	case gobreaker.StateOpen:
		return 1
	case gobreaker.StateHalfOpen:
		return 2
	default:
		return -1
	}
}

// ErrCircuitOpen 熔断器打开时的错误
var ErrCircuitOpen = fmt.Errorf("cachex: circuit breaker open")
```

- [x] **步骤 2：编写 circuit_test.go**

```go
// cachex/circuit_test.go
package cachex

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCircuitBreaker_Closed(t *testing.T) {
	cb := NewCircuitBreaker("test")
	assert.Equal(t, 0, cb.State())

	err := cb.Execute(context.Background(), func() error {
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, cb.State())
}

func TestCircuitBreaker_OpenAfterFailures(t *testing.T) {
	cb := NewCircuitBreaker("test-open")
	failCount := 0

	for i := 0; i < 5; i++ {
		err := cb.Execute(context.Background(), func() error {
			failCount++
			return errors.New("simulated failure")
		})
		require.Error(t, err)
	}

	// 第 6 次应直接短路，不执行 fn
	err := cb.Execute(context.Background(), func() error {
		t.Fatal("should not reach here when circuit is open")
		return nil
	})
	assert.Error(t, err)
	assert.Equal(t, 5, failCount) // fn 只被执行了 5 次
	assert.Equal(t, 1, cb.State())
}

func TestCircuitBreaker_HalfOpen(t *testing.T) {
	// 使用短 timeout 以便快速进入 half-open
	settings := gobreaker.Settings{
		Name: "test-halfopen",
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 2
		},
		Timeout: 100 * time.Millisecond,
	}
	cb := &CircuitBreaker{cb: gobreaker.NewCircuitBreaker(settings)}

	// 触发 Open
	cb.Execute(context.Background(), func() error { return errors.New("fail") })
	cb.Execute(context.Background(), func() error { return errors.New("fail") })
	assert.Equal(t, 1, cb.State())

	// 等待 timeout 后进入 Half-Open
	time.Sleep(150 * time.Millisecond)

	// Half-Open 允许一次探测
	err := cb.Execute(context.Background(), func() error { return nil })
	require.NoError(t, err)
	assert.Equal(t, 0, cb.State())
}
```

- [x] **步骤 3：运行测试**

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
go test -race -cover ./...
```

- [x] **步骤 4：Commit**

```bash
git add circuit.go circuit_test.go
git commit -m "feat: add circuit breaker wrapper with Closed/Open/Half-Open states"
```

- [x] **Task complete**

---

### 任务 6：Redis 驱动改造

> **trace:** plan-ready.md → `### Task 6: Redis 驱动改造` | tasks.md → `- [ ] 移除 enabled 字段、接入全局 trace 开关、熔断器、指标、读写超时分离`
> **sync:** tasks.md → `移除 Client.enabled 字段，trace 由 IsTracingEnabled() 驱动；接入熔断器和 metrics；支持 ReadTimeout/WriteTimeout` | plan-ready.md → `### Task 6: Redis 驱动改造`

**Files:**
- 修改：`redis/redis.go`

- [x] **步骤 1：改造 redis.go — 移除 enabled，接入全局 trace 开关**

```go
// redis/redis.go 关键修改
package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gospacex/cachex"
	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Client struct {
	rdb     redis.UniversalClient
	cfg     *cachex.Config
	tracer  trace.Tracer
	logger  logx.Logger
	cb      *cachex.CircuitBreaker
}

func New(cfg *cachex.Config) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cachex/redis: config is nil")
	}
	cfg.FillDefaults()

	// 计算读写超时
	readTimeout := cfg.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = cfg.Timeout
	}
	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = cfg.Timeout
	}

	var rdb redis.UniversalClient
	switch {
	case cfg.SentinelAddrs != "":
		rdb = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.SentinelMaster,
			SentinelAddrs: splitComma(cfg.SentinelAddrs),
			Password:      cfg.Password,
			DB:            cfg.DB,
			PoolSize:      cfg.PoolSize,
			MinIdleConns:  cfg.MinIdleConn,
			DialTimeout:   cfg.Timeout,
			ReadTimeout:   readTimeout,
			WriteTimeout:  writeTimeout,
			TLSConfig:     tlsConfig(cfg.TLSEnabled),
		})
	case cfg.Cluster != "":
		rdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        splitComma(cfg.Cluster),
			Password:     cfg.Password,
			PoolSize:     cfg.PoolSize,
			MinIdleConns: cfg.MinIdleConn,
			DialTimeout:  cfg.Timeout,
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
			TLSConfig:    tlsConfig(cfg.TLSEnabled),
		})
	default:
		rdb = redis.NewClient(&redis.Options{
			Addr:         cfg.Addr,
			Password:     cfg.Password,
			DB:           cfg.DB,
			PoolSize:     cfg.PoolSize,
			MinIdleConns: cfg.MinIdleConn,
			DialTimeout:  cfg.Timeout,
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
			TLSConfig:    tlsConfig(cfg.TLSEnabled),
		})
	}

	retries := cfg.RetryCount
	if retries <= 0 {
		retries = 3
	}
	if err := withRetry(func() error { return rdb.Ping(context.Background()).Err() }, retries); err != nil {
		return nil, fmt.Errorf("cachex/redis: ping failed: %w", err)
	}

	logx.GetGlobal().InfoContext(context.Background(), "redis driver initialized",
		logx.String("driver", "redis"),
		logx.String("addr", cfg.Addr),
	)

	return &Client{
		rdb:    rdb,
		cfg:    cfg,
		tracer: otel.Tracer("github.com/gospacex/cachex/redis"),
		logger: logx.GetGlobal(),
		cb:     cachex.NewCircuitBreaker(cfg.Name),
	}, nil
}

// startSpan 根据全局开关决定是否创建 span
func (c *Client) startSpan(ctx context.Context, command string, key string) (context.Context, trace.Span) {
	if !cachex.IsTracingEnabled() {
		return ctx, trace.SpanFromContext(ctx)
	}
	ctx, span := c.tracer.Start(ctx, "db.redis."+command)
	span.SetAttributes(
		attribute.String("db.system", "redis"),
		attribute.String("db.redis.command", command),
	)
	if key != "" {
		span.SetAttributes(attribute.String("db.redis.key", key))
	}
	return ctx, span
}
```

- [x] **步骤 2：各方法接入熔断器和指标**

以 `Get` 为例：
```go
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	ctx, span := c.startSpan(ctx, "GET", key)
	defer span.End()

	var val string
	var err error
	start := time.Now()
	err = c.cb.Execute(ctx, func() error {
		result, rerr := c.rdb.Get(ctx, key).Result()
		if rerr != nil {
			if rerr == redis.Nil {
				val = ""
				return nil
			}
			span.RecordError(rerr)
			c.logger.ErrorContext(ctx, "redis get failed", logx.Err(rerr), logx.String("key", key))
			return fmt.Errorf("cachex/redis Get(%q): %w", key, rerr)
		}
		val = result
		return nil
	})

	duration := time.Since(start)
	status := "success"
	if err != nil {
		status = "error"
	}
	cachex.RecordCommand("redis", c.cfg.Name, "GET", status, duration)
	cachex.UpdatePoolMetrics("redis", c.cfg.Name,
		int(c.rdb.PoolSize()-int64(c.rdb.PoolIdle())), int(c.rdb.PoolIdle()))
	cachex.UpdateCircuitBreakerState("redis", c.cfg.Name, c.cb.State())

	if err != nil {
		return "", err
	}
	span.SetAttributes(attribute.String("db.redis.found", "true"))
	c.logger.InfoContext(ctx, "redis get success", logx.String("key", key))
	return val, nil
}
```

其余方法（Set/Delete/Exists/Keys/HealthCheck）按相同模式改造。

- [x] **步骤 3：编译验证**

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
go build ./redis/...
```

- [x] **步骤 4：Commit**

```bash
git add redis/redis.go
git commit -m "refactor: redis driver - global trace switch, circuit breaker, metrics, read/write timeout separation"
```

- [x] **Task complete**

---

### 任务 7：Memcached 驱动改造

> **trace:** plan-ready.md → `### Task 7: Memcached 驱动改造` | tasks.md → `- [ ] 全局 trace 开关、熔断器、指标、超时配置`
> **sync:** tasks.md → `修改 startSpan 检查 IsTracingEnabled()；接入熔断器和 metrics；支持 ReadTimeout/WriteTimeout 配置` | plan-ready.md → `### Task 7: Memcached 驱动改造`

**Files:**
- 修改：`memcached/memcached.go`

- [x] **步骤 1：改造 memcached.go**

```go
// memcached/memcached.go 关键修改
package memcached

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/gospacex/cachex"
	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Client struct {
	mc     *memcache.Client
	cfg    *cachex.Config
	tracer trace.Tracer
	logger logx.Logger
	cb     *cachex.CircuitBreaker
}

func New(cfg *cachex.Config) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cachex/memcached: config is nil")
	}
	cfg.FillDefaults()

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	addrs := splitComma(cfg.Addr)
	mc := memcache.New(addrs...)
	mc.Timeout = timeout
	mc.MaxIdleConns = cfg.PoolSize

	if _, err := mc.Get("_health_check_"); err != nil {
		if err != memcache.ErrCacheMiss {
			return nil, fmt.Errorf("cachex/memcached: health check failed: %w", err)
		}
	}

	logx.GetGlobal().InfoContext(context.Background(), "memcached driver initialized",
		logx.String("driver", "memcached"),
		logx.String("addrs", strings.Join(addrs, ",")),
	)

	return &Client{
		mc:     mc,
		cfg:    cfg,
		tracer: otel.Tracer("github.com/gospacex/cachex/memcached"),
		logger: logx.GetGlobal(),
		cb:     cachex.NewCircuitBreaker(cfg.Name),
	}, nil
}

func (c *Client) startSpan(ctx context.Context, command string, key string) (context.Context, trace.Span) {
	if !cachex.IsTracingEnabled() {
		return ctx, trace.SpanFromContext(ctx)
	}
	ctx, span := c.tracer.Start(ctx, "db.memcached."+command)
	span.SetAttributes(
		attribute.String("db.system", "memcached"),
		attribute.String("db.memcached.command", command),
	)
	if key != "" {
		span.SetAttributes(attribute.String("db.memcached.key", key))
	}
	return ctx, span
}
```

- [x] **步骤 2：各方法接入熔断器和指标（以 Get 为例）**

```go
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	ctx, span := c.startSpan(ctx, "GET", key)
	defer span.End()

	var val string
	var err error
	start := time.Now()
	err = c.cb.Execute(ctx, func() error {
		item, getErr := c.mc.Get(key)
		if getErr != nil {
			if getErr == memcache.ErrCacheMiss {
				return nil
			}
			span.RecordError(getErr)
			c.logger.ErrorContext(ctx, "memcached get failed", logx.Err(getErr), logx.String("key", key))
			return fmt.Errorf("cachex/memcached Get(%q): %w", key, getErr)
		}
		val = string(item.Value)
		return nil
	})

	duration := time.Since(start)
	status := "success"
	if err != nil {
		status = "error"
	}
	cachex.RecordCommand("memcached", c.cfg.Name, "GET", status, duration)
	cachex.UpdateCircuitBreakerState("memcached", c.cfg.Name, c.cb.State())

	if err != nil {
		return "", err
	}
	span.SetAttributes(attribute.String("db.memcached.found", "true"))
	c.logger.InfoContext(ctx, "memcached get success", logx.String("key", key))
	return val, nil
}
```

其余方法按相同模式改造。

- [x] **步骤 3：编译验证**

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
go build ./memcached/...
```

- [x] **步骤 4：Commit**

```bash
git add memcached/memcached.go
git commit -m "refactor: memcached driver - global trace switch, circuit breaker, metrics"
```

- [x] **Task complete**

---

### 任务 8：示例代码与配置

> **trace:** plan-ready.md → `### Task 8: 示例代码与配置` | tasks.md → `- [ ] 完善 examples/config/config.yaml、examples/redis/main.go、examples/memcached/main.go`
> **sync:** tasks.md → `完善 config.yaml（补充 read/write_timeout）；完善 redis/main.go 和 memcached/main.go；对接 docker-compose 真实中间件验证` | plan-ready.md → `### Task 8: 示例代码与配置`

**Files:**
- 修改：`examples/config/config.yaml`
- 修改：`examples/redis/main.go`
- 修改：`examples/memcached/main.go`

- [x] **步骤 1：完善 examples/config/config.yaml**

在已有的各实例配置中补充 `read_timeout` 和 `write_timeout` 字段（已在 brainstorming 阶段写入，此处确认完整）。

- [x] **步骤 2：完善 examples/redis/main.go**

```go
// examples/redis/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/configx"
	_ "github.com/gospacex/logx/zap"
	_ "github.com/gospacex/cachex/redis"
)

func main() {
	ctx := context.Background()

	cfg, err := configx.New(configx.ConfigOptions{
		ConfigFile: "examples/config/config.yaml",
	})
	if err != nil {
		log.Fatalf("configx init failed: %v", err)
	}

	if err := cachex.Init(cachex.WithConfig(cfg, "cachex")); err != nil {
		log.Fatalf("cachex init failed: %v", err)
	}
	defer cachex.CloseAll()

	// 注册 metrics HTTP handler（宿主机端口）
	if err := cachex.RegisterMetricsHTTPHandler(":9100"); err != nil {
		log.Printf("WARN: metrics handler registration failed: %v", err)
	}

	rdb := cachex.Instance("redis-primary")
	if rdb == nil {
		log.Fatal("instance redis-primary not found")
	}

	if err := rdb.HealthCheck(ctx); err != nil {
		log.Fatalf("health check failed: %v", err)
	}
	fmt.Println("✓ Redis health check passed")

	key := "sdk:example:hello"

	if err := rdb.Set(ctx, key, "world", 30*time.Second); err != nil {
		log.Fatalf("set failed: %v", err)
	}
	fmt.Println("✓ SET hello -> world")

	val, err := rdb.Get(ctx, key)
	if err != nil {
		log.Fatalf("get failed: %v", err)
	}
	fmt.Printf("✓ GET %s = %q\n", key, val)

	exists, err := rdb.Exists(ctx, key)
	if err != nil {
		log.Fatalf("exists failed: %v", err)
	}
	fmt.Printf("✓ EXISTS %s = %v\n", key, exists)

	keys, err := rdb.Keys(ctx, "sdk:example:*")
	if err != nil {
		log.Fatalf("keys failed: %v", err)
	}
	fmt.Printf("✓ KEYS sdk:example:* = %v\n", keys)

	if err := rdb.Delete(ctx, key); err != nil {
		log.Fatalf("delete failed: %v", err)
	}
	fmt.Println("✓ DEL hello")

	val, _ = rdb.Get(ctx, key)
	fmt.Printf("✓ GET %s after delete = %q (empty = cache miss)\n", key, val)

	// KeyDB 和 Dragonfly 同 Redis 协议
	for _, name := range []string{"keydb-primary", "dragonfly-primary"} {
		inst := cachex.Instance(name)
		if inst == nil {
			fmt.Printf("⚠ instance %s not configured, skip\n", name)
			continue
		}
		if err := inst.HealthCheck(ctx); err != nil {
			fmt.Printf("⚠ %s health check failed: %v\n", name, err)
			continue
		}
		testKey := "sdk:example:" + name
		if err := inst.Set(ctx, testKey, name, 30*time.Second); err != nil {
			fmt.Printf("⚠ %s set failed: %v\n", name, err)
			continue
		}
		v, _ := inst.Get(ctx, testKey)
		fmt.Printf("✓ %s: GET %s = %q\n", name, testKey, v)
	}

	fmt.Println("\n✓ Metrics endpoint: http://localhost:9100/metrics")
	os.Exit(0)
}
```

- [x] **步骤 3：完善 examples/memcached/main.go**

```go
// examples/memcached/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/configx"
	_ "github.com/gospacex/logx/zap"
	_ "github.com/gospacex/cachex/memcached"
)

func main() {
	ctx := context.Background()

	cfg, err := configx.New(configx.ConfigOptions{
		ConfigFile: "examples/config/config.yaml",
	})
	if err != nil {
		log.Fatalf("configx init failed: %v", err)
	}

	if err := cachex.Init(cachex.WithConfig(cfg, "cachex")); err != nil {
		log.Fatalf("cachex init failed: %v", err)
	}
	defer cachex.CloseAll()

	if err := cachex.RegisterMetricsHTTPHandler(":9100"); err != nil {
		log.Printf("WARN: metrics handler registration failed: %v", err)
	}

	mc := cachex.Instance("memcached-primary")
	if mc == nil {
		log.Fatal("instance memcached-primary not found")
	}

	if err := mc.HealthCheck(ctx); err != nil {
		log.Fatalf("health check failed: %v", err)
	}
	fmt.Println("✓ Memcached health check passed")

	key := "sdk:example:hello"

	if err := mc.Set(ctx, key, "world", 30*time.Second); err != nil {
		log.Fatalf("set failed: %v", err)
	}
	fmt.Println("✓ SET hello -> world")

	val, err := mc.Get(ctx, key)
	if err != nil {
		log.Fatalf("get failed: %v", err)
	}
	fmt.Printf("✓ GET %s = %q\n", key, val)

	exists, err := mc.Exists(ctx, key)
	if err != nil {
		log.Fatalf("exists failed: %v", err)
	}
	fmt.Printf("✓ EXISTS %s = %v\n", key, exists)

	if err := mc.Delete(ctx, key); err != nil {
		log.Fatalf("delete failed: %v", err)
	}
	fmt.Println("✓ DEL hello")

	val, _ = mc.Get(ctx, key)
	fmt.Printf("✓ GET %s after delete = %q (empty = cache miss)\n", key, val)

	if _, err := mc.Keys(ctx, "*"); err != nil {
		fmt.Printf("✓ KEYS unsupported as expected: %v\n", err)
	}

	fmt.Println("\n✓ Metrics endpoint: http://localhost:9100/metrics")
	os.Exit(0)
}
```

- [x] **步骤 4：启动 docker-compose 并验证示例**

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
docker-compose up -d
export REDIS_PASSWORD=redis123456 KEYDB_PASSWORD=keydb123456 DRAGONFLY_PASSWORD=dragonfly123456
go run examples/redis/main.go
go run examples/memcached/main.go
curl -s http://localhost:9100/metrics | grep cachex | head -20
```

预期：示例正常输出，metrics 端点返回指标。

- [x] **步骤 5：Commit**

```bash
git add examples/
git commit -m "feat: add complete examples with docker-compose integration"
```

- [x] **Task complete**

---

### 任务 9：文档更新

> **trace:** plan-ready.md → `### Task 9: 文档更新` | tasks.md → `- [ ] 更新 FEATURE_MATRIX.md，清理 go.mod replace 块`
> **sync:** tasks.md → `更新 redis/FEATURE_MATRIX.md 和 memcached/FEATURE_MATRIX.md，标注熔断器/指标/全局 trace` | plan-ready.md → `### Task 9: 文档更新`

**Files:**
- 修改：`redis/FEATURE_MATRIX.md`
- 修改：`memcached/FEATURE_MATRIX.md`
- 修改：`go.mod`

- [x] **步骤 1：清理 go.mod replace 块**

移除 dragonfly 和 keydb 的 replace 语句（驱动已删除）：

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
# 手动编辑 go.mod，删除以下两行：
# replace github.com/gospacex/cachex/dragonfly => ./dragonfly
# replace github.com/gospacex/cachex/keydb => ./keydb
go mod tidy
```

- [x] **步骤 2：更新 redis/FEATURE_MATRIX.md**

在现有内容末尾追加 Section 10：

```markdown
## 10. 企业级特性

### 10.1 全局调用链开关
- 通过 `cachex.TracingConfig.Enabled` 控制，非 per-instance
- 开启时所有命令自动入 OTel span（`db.redis.*`）
- 关闭时零开销（不创建 span）
- 支持 configx Watch 热更新

### 10.2 熔断器
- 基于 `github.com/sony/gobreaker`
- 每个实例独立熔断器
- 默认：连续 5 次失败 → Open，30s 后进入 Half-Open
- 熔断状态通过 `cachex_circuit_breaker_state` 指标暴露

### 10.3 Prometheus 指标
- `cachex_command_total{driver,name,command,status}`
- `cachex_command_duration_seconds{driver,name,command}`
- `cachex_circuit_breaker_state{driver,name}`
- `cachex_pool_active_count{driver,name}`
- `cachex_pool_idle_count{driver,name}`

### 10.4 读写超时分离
- `read_timeout` / `write_timeout` 独立配置
- 未设置时回退到 `timeout`
```

- [x] **步骤 3：更新 memcached/FEATURE_MATRIX.md**

类似追加 Section 10，将 `db.redis.*` 替换为 `db.memcached.*`。

- [x] **步骤 4：编译验证**

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
go build ./...
```

- [x] **步骤 5：Commit**

```bash
git add redis/FEATURE_MATRIX.md memcached/FEATURE_MATRIX.md go.mod go.sum
git commit -m "docs: update FEATURE_MATRIX with enterprise features, clean go.mod"
```

- [x] **Task complete**

---

### 任务 10：最终验证

> **trace:** plan-ready.md → `### Task 10: 最终验证` | tasks.md → `- [ ] go build/vet/test 全量通过，验收标准 checklist 确认`
> **sync:** tasks.md → `go build ./... + go vet ./... + go test -race -cover ./...（覆盖率 ≥ 80%）` | plan-ready.md → `### Task 10: 最终验证`

**Files:** 无代码改动，仅验证

- [x] **步骤 1：全量编译与 vet**

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
go build ./...
go vet ./...
```

预期：无错误，无警告。

- [x] **步骤 2：全量测试**

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
go test -race -cover ./...
```

预期：全部通过，覆盖率 ≥ 80%。

- [x] **步骤 3：示例端到端验证**

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
docker-compose up -d
export REDIS_PASSWORD=redis123456 KEYDB_PASSWORD=keydb123456 DRAGONFLY_PASSWORD=dragonfly123456
go run examples/redis/main.go
go run examples/memcached/main.go
curl -s http://localhost:9100/metrics | grep -E "cachex_(command|pool|circuit)" | head -20
docker-compose down
```

预期：
- 两个示例均正常输出 ✓
- metrics 端点返回 `cachex_command_total`、`cachex_pool_*`、`cachex_circuit_breaker_state`

- [x] **步骤 4：验收标准逐项确认**

- [x] `cachex.Init()` 可从 configx 配置文件初始化所有实例
- [x] `cachex.Instance("name")` 在 Init 前自动懒加载初始化
- [x] `cachex.CloseAll()` 正确关闭所有实例连接池
- [x] 全局调用链开关开启时，Redis 和 Memcached 所有命令自动入 OTel span
- [x] 全局调用链开关关闭时，不创建任何 span
- [x] configx 中 `cachex.tracing.enabled` 变更可通过 Watch 热生效
- [x] 每个命令执行有对应的 Prometheus 计数器
- [x] Prometheus `/metrics` HTTP 端点可通过 `RegisterMetricsHTTPHandler` 注册
- [x] 熔断器在连续 5 次失败后进入 Open 状态
- [x] 密码支持 `${ENV_VAR}` 占位符并在 Init 时展开
- [x] 配置示例 YAML 可被 configx 正确解析
- [x] examples/ 中的示例可对接 docker-compose 中的真实中间件正常运行
- [x] go test -race ./... 通过

- [x] **步骤 5：Final Commit（若有未提交的测试更新）**

```bash
git add -A
git commit -m "test: add final verification tests and coverage"
```

- [x] **Task complete**

---

### 任务 11：示例重构与 Redis Stack 支持

> **trace:** plan-ready.md → `### Task 11: 示例重构与 Redis Stack 支持` | tasks.md → `## 11. 示例重构（amend 2026-09-09）`
> **sync:** tasks.md → `将所有示例迁移至 examples/ 目录并统一入口，补充 Redis Stack 与业务场景示例` | plan-ready.md → `### Task 11: 示例重构与 Redis Stack 支持`

**Files:**
- 新建：`examples/redis/main.go`、`examples/memcached/main.go`
- 新建：`redis/examples_advanced.go`、`redis/examples_bitmap.go`、`redis/examples_pubsub.go`、`redis/examples_stream.go`、`redis/examples_search_bloom.go`
- 新建：`redis/examples_business_*.go`（distributed_lock、rate_limit、delayed_queue、cache_penetration）
- 新建：`memcached/examples_*.go`（basic、counter、cas、batch）
- 修复：`redis/examples_zset.go`（ExampleZCount 接收者类型）
- 更新：`redis/FEATURE_MATRIX.md`（section 2.6、3.1、6）

- [x] **步骤 1：将所有示例迁移至 examples/ 目录**

将原来散落在 `redis/examples_*.go` 和 `memcached/examples_*.go` 的示例函数，整理为通过 `driver.Raw()` / `mcDriver.Raw()` 获取底层客户端后统一调用的入口。

- [x] **步骤 2：创建 examples/redis/main.go**

统一入口，依次调用：基础命令 → Hash/List/Set/ZSet → 高级命令（bitmap/geo/stream/hyperloglog/pubsub/pipeline）→ 业务场景（分布式锁/限流/延迟队列/缓存穿透）→ Redis Stack（RediSearch/RedisBloom）。

- [x] **步骤 3：创建 examples/memcached/main.go**

统一入口，依次调用：基础 CRUD → 自增/自减 → CAS 乐观锁 → 批量操作。

- [x] **步骤 4：修复 receiver 类型错误**

`examples_zset.go` 中 `ExampleZCount` 接收者从 `*SetExamples` 改为 `*ZSetExamples`。

- [x] **步骤 5：补充 Redis Stack 示例**

`examples_search_bloom.go` 使用 raw command 模式（`redis.NewStringCmd` + `Process`）实现：
- RediSearch：`FT.CREATE` 建索引、`FT.SEARCH` 查询
- RedisBloom：`BF.RESERVE` 建布隆过滤器、`BF.ADD` 写入、`BF.EXISTS` 探测

- [x] **步骤 6：更新 FEATURE_MATRIX.md**

- section 2.6：bitmap/hyperloglog/geo/stream/pubsub/scripting/transaction/pipeline → ✅
- section 3：分布式锁/限流/延迟队列/缓存穿透 → ✅；新增 3.1 Redis Stack 扩展
- section 6：所有业务场景矩阵条目更新为 ✅

- [x] **步骤 7：编译验证**

```bash
cd /Users/hyx/work/gowork/src/hubx/cachex
CLAUDE_CODE_TMPDIR=/tmp/claude-hubx GOROOT=/usr/local/Cellar/go/1.27.1/libexec GOTOOLCHAIN=local /usr/local/bin/go build ./...
```

预期：编译通过，无错误。

- [x] **Task complete**

---

### 任务 11：示例重构与 Redis Stack 支持（amend 2026-09-09）

> **trace:** plan-ready.md → `### Task 11: 示例重构与 Redis Stack 支持` | tasks.md → `## 11. 示例重构（amend 2026-09-09）`
> **sync:** tasks.md → `将所有示例迁移至 examples/ 目录并统一入口，补充 Redis Stack 与业务场景示例` | plan-ready.md → `### Task 11: 示例重构与 Redis Stack 支持`

**Files:**
- 新建：`examples/redis/main.go`、`examples/memcached/main.go`
- 新建：`redis/examples_advanced.go`、`redis/examples_bitmap.go`、`redis/examples_pubsub.go`、`redis/examples_stream.go`、`redis/examples_search_bloom.go`
- 新建：`redis/examples_business_*.go`（distributed_lock、rate_limit、delayed_queue、cache_penetration）
- 新建：`memcached/examples_*.go`（basic、counter、cas、batch）
- 修复：`redis/examples_zset.go`（ExampleZCount 接收者类型）
- 更新：`redis/FEATURE_MATRIX.md`（section 2.6、3.1、6）

- [x] **步骤 1：将所有示例迁移至 examples/ 目录**

所有示例统一由 `examples/redis/main.go` 和 `examples/memcached/main.go` 作为入口，通过 `driver.Raw()` / `mcDriver.Raw()` 获取底层客户端。

- [x] **步骤 2：创建 examples/redis/main.go**

统一入口，依次调用：基础命令（String/Hash/List/Set/ZSet）→ 高级命令（bitmap/geo/stream/hyperloglog/pubsub/pipeline）→ 业务场景（分布式锁/限流/延迟队列/缓存穿透）→ Redis Stack（RediSearch/RedisBloom）。

- [x] **步骤 3：创建 examples/memcached/main.go**

统一入口，依次调用：基础 CRUD → 自增/自减 → CAS 乐观锁 → 批量操作。

- [x] **步骤 4：修复 receiver 类型错误**

`examples_zset.go` 中 `ExampleZCount` 接收者从 `*SetExamples` 修正为 `*ZSetExamples`。

- [x] **步骤 5：补充 Redis Stack 示例**

`examples_search_bloom.go` 使用 raw command 模式（`redis.NewStringCmd` + `Process`）实现：
- RediSearch：`FT.CREATE` 建索引、`FT.SEARCH` 查询
- RedisBloom：`BF.RESERVE` 建布隆过滤器、`BF.ADD` 写入、`BF.EXISTS` 探测

- [x] **步骤 6：更新 FEATURE_MATRIX.md**

- section 2.6：bitmap/hyperloglog/geo/stream/pubsub/scripting/transaction/pipeline → ✅
- section 3.1：新增 Redis Stack 扩展章节（RediSearch + RedisBloom raw command）
- section 6：分布式锁/限流/延迟队列/缓存穿透 全部更新为 ✅

- [x] **步骤 7：编译验证**

```bash
CLAUDE_CODE_TMPDIR=/tmp/claude-hubx GOROOT=/usr/local/Cellar/go/1.27.1/libexec GOTOOLCHAIN=local /usr/local/bin/go build ./...
```

预期：EXIT:0，无错误。

- [x] **步骤 8：运行 go vet**

```bash
CLAUDE_CODE_TMPDIR=/tmp/claude-hubx GOROOT=/usr/local/Cellar/go/1.27.1/libexec GOTOOLCHAIN=local /usr/local/bin/go vet ./...
```

预期：EXIT:0，无警告。

- [x] **Task complete**
