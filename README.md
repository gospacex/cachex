# cachex

**统一多后端缓存 SDK** — 一套 `Cache` 接口，Redis 与 Memcached 双驱动，内置连接池治理、熔断降级、OpenTelemetry 调用链与 Prometheus 指标。

> hubx x 系列基础设施组件之一 · Go 1.21+ · 依赖 [configx](https://github.com/gospacex/configx) 配置框架

---

## 目录

- [为什么用它](#为什么用它)
- [架构](#架构)
- [安装](#安装)
- [快速开始](#快速开始)
- [配置参考](#配置参考)
- [驱动能力矩阵](#驱动能力矩阵)
- [可观测性](#可观测性)
- [韧性设计](#韧性设计)
- [安全](#安全)
- [生产最佳实践](#生产最佳实践)
- [本地开发与示例](#本地开发与示例)
- [API 参考](#api-参考)
- [已知限制](#已知限制)

---

## 为什么用它

| 能力 | 说明 |
|---|---|
| **统一接口** | 业务代码面向 `Cache` 接口编程，切换 Redis / Memcached 只改配置中的 `driver` 字段 |
| **多实例命名访问** | 一个进程内同时持有若干命名实例（如 `redis-primary`、`memcached-session`），按业务维度隔离 |
| **零感知注册** | 核心包不 import 任何驱动，`_ "…/cachex/redis"` 即完成注册，未引入的驱动不进二进制 |
| **调用链内建** | 全局开关开启后，每条缓存命令自动产生携带 `db.system`、`command`、`key` 的 OTel span |
| **指标开箱可用** | 命令计数、耗时直方图、熔断器状态、连接池水位直接注册到 Prometheus |
| **故障隔离** | 每实例独立熔断器（连续 5 次失败跳闸），下游抖动不会拖垮调用方 |
| **配置热生效** | 通过 configx 的 Watch 机制，调用链开关可运行时切换而无需重启 |

---

## 架构

```
┌──────────────────────────────────────────────────────────────┐
│                        业务代码                               │
│        cachex.Instance("redis-primary").Get(ctx, key)        │
└───────────────────────────┬──────────────────────────────────┘
                            │  Cache 接口
┌───────────────────────────▼──────────────────────────────────┐
│                     core（cachex 根包）                       │
│  Init / Instance / CloseAll        全局单例生命周期            │
│  Config / InstanceConfig           配置装配与默认值           │
│  New / RegisterCreator             驱动注册表（无静态依赖）    │
│  tracing.go                        OTel 开关 + TracerProvider │
│  circuit.go                        gobreaker 熔断封装          │
│  metrics.go                        Prometheus 注册表           │
└───────┬─────────────────────────────────┬────────────────────┘
        │ blank import 注册               │ 同上
┌───────▼───────────┐            ┌────────▼──────────┐
│  driver: redis    │            │ driver: memcached │
│  独立 Go module   │            │  独立 Go module   │
│  go-redis/v8      │            │  gomemcache       │
│  单机/Sentinel/   │            │  客户端分片多节点  │
│  Cluster          │            │                   │
└───────┬───────────┘            └────────┬──────────┘
        │                                 │
┌───────▼───────────┐            ┌────────▼──────────┐
│  Redis / Sentinel │            │    Memcached      │
│  / Cluster        │            │                   │
└───────────────────┘            └───────────────────┘
```

**三条设计红线**

1. **核心包不依赖驱动**。`cachex` 根包只认 `RegisterCreator` 注册进来的工厂函数，未 blank import 的驱动不会进入编译产物。
2. **驱动是独立 Go module**。`cachex/redis`、`cachex/memcached` 各自持有 `go.mod`，引入 Redis 不会连带拉入 gomemcache。
3. **示例不进产品包**。所有命令用法示例位于 `examples/` 下的独立包，驱动包内只保留 SDK 代码。

---

## 安装

```bash
# 核心包
go get github.com/gospacex/cachex

# 按需引入驱动（用到哪个装哪个）
go get github.com/gospacex/cachex/redis
go get github.com/gospacex/cachex/memcached
```

本仓库为 monorepo，仓内模块间通过 `replace` 指向本地目录，开发时无需发布即可联调：

```go
replace (
    github.com/gospacex/cachex          => ./cachex
    github.com/gospacex/cachex/redis    => ./cachex/redis
    github.com/gospacex/configx         => ./configx
    github.com/gospacex/logx            => ./logx
)
```

---

## 快速开始

### 方式一：配置文件（推荐）

```yaml
# config.yaml
cachex:
  tracing:
    enabled: true
    exporter: jaeger
    jaeger_endpoint: "localhost:4318"
    insecure: true
    service_name: "order-service"
  instances:
    - name: redis-primary
      driver: redis
      addr: "localhost:46379"
      password: "${REDIS_PASSWORD}"
      db: 0
      pool_size: 20
      min_idle_conn: 5
      timeout: 5s
```

```go
package main

import (
    "context"
    "log"

    "github.com/gospacex/cachex"
    // blank import 完成驱动注册；未 import 则 Init 报 unknown driver
    _ "github.com/gospacex/cachex/redis"
    _ "github.com/gospacex/logx/zap"
)

func main() {
    if err := cachex.Init(cachex.WithConfigFile("config.yaml")); err != nil {
        log.Fatalf("cachex init: %v", err)
    }
    defer func() {
        if err := cachex.CloseAll(); err != nil {
            log.Printf("cachex close: %v", err)
        }
    }()

    c := cachex.Instance("redis-primary")
    if c == nil {
        log.Fatal("instance redis-primary not found")
    }

    ctx := context.Background()
    if err := c.Set(ctx, "user:1001", `{"name":"Alice"}`, 30*time.Second); err != nil {
        log.Fatalf("set: %v", err)
    }

    val, err := c.Get(ctx, "user:1001") // 缓存未命中返回 ("", nil)，不是错误
    if err != nil {
        log.Fatalf("get: %v", err)
    }
    log.Printf("value = %s", val)
}
```

### 方式二：复用已有 configx Provider

微服务通常已持有统一的 configx 句柄，直接复用即可，避免重复加载配置：

```go
cfg, err := configx.ReadConfig(configx.ConfigOptions{ConfigFile: "app.yaml"})
if err != nil {
    log.Fatal(err)
}

// section 为 cachex 配置节的路径，例如顶层 cachex 或 xmiddleware.cachex
if err := cachex.Init(cachex.WithConfig(cfg, "cachex")); err != nil {
    log.Fatal(err)
}
```

### 方式三：纯代码构造（不依赖配置文件）

```go
instances, err := cachex.New([]*cachex.Config{
    {
        Name:     "redis-primary",
        Driver:   "redis",
        Addr:     "localhost:46379",
        PoolSize: 20,
        Timeout:  5 * time.Second,
    },
})
if err != nil {
    log.Fatal(err)
}
c := instances["redis-primary"]
```

### 绕过接口使用底层能力

标准接口只覆盖字符串 KV。需要 Hash、ZSet、Stream、Lua 等原生能力时，从接口实例取底层句柄：

```go
inst := cachex.Instance("redis-primary")
rdb := inst.(*redisdriver.Client).Raw() // redis.UniversalClient
rdb.HSet(ctx, "user:1001", "name", "Alice")
```

> ⚠️ 通过 `Raw()` 执行的命令**不经过**熔断器、调用链和 Prometheus 埋点，仅用于确需原生命令的场景。

---

## 配置参考

### TracingConfig（`cachex.tracing.*`）

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `enabled` | bool | `false` | 全局调用链开关 |
| `exporter` | string | `jaeger` | `jaeger`（OTLP/HTTP，4318）、`jaeger-grpc`（4317）、`kafka` |
| `jaeger_endpoint` | string | `localhost:4318` | OTLP 端点，`endpoint` 为空时兜底使用本值 |
| `insecure` | bool | `false` | 关闭传输加密（仅本地开发用） |
| `service_name` | string | `cachex` | OTel resource 的 `service.name` |
| `service_version` | string | `0.1.0` | OTel resource 的 `service.version` |
| `sampler_ratio` | float | 全采 | `(0,1)` 按比例采样；高 QPS 生产环境建议 `0.01`~`0.1` |
| `batch_size` | int | `512` | 单批导出 span 上限 |

### InstanceConfig（`cachex.instances[]`）

| 字段 | 适用驱动 | 默认 | 说明 |
|---|---|---|---|
| `name` | 全部 | — | 实例名，`Instance(name)` 的键，**必填且唯一** |
| `driver` | 全部 | — | `redis` / `memcached`，**必填** |
| `addr` | 全部 | — | 逗号分隔地址；Redis 单机地址，Memcached 可为多节点 |
| `password` | 全部 | — | 支持 `${ENV_VAR}` 占位符，Init 时展开 |
| `db` | redis | `0` | Redis 库号 |
| `pool_size` | 全部 | `10` | 连接池上限（Memcached 为 `MaxIdleConns`） |
| `min_idle_conn` | redis | `0` | 最小空闲连接 |
| `sentinel_master` / `sentinel_addrs` | redis | — | 二者同时给出时走 Sentinel 模式 |
| `cluster` | redis | — | 给出时走 Cluster 模式，优先级低于 Sentinel |
| `tls_enabled` | redis | `false` | 启用 TLS（最低 TLS 1.2） |
| `timeout` | 全部 | `5s` | 建连超时 |
| `read_timeout` / `write_timeout` | 全部 | 取 `timeout` | 读写超时 |
| `retry_count` | redis | `3` | 建连阶段指数退避重试次数 |
| `dial_timeout` / `max_retries` | 全部 | — | configx 企业配置别名，未设时回落 `timeout` / `retry_count` |

**驱动模式判定顺序**（Redis）：`sentinel_addrs` 非空 → Sentinel；`cluster` 非空 → Cluster；否则单机。

---

## 驱动能力矩阵

### 统一接口行为

| 方法 | Redis | Memcached | 行为约定 |
|---|---|---|---|
| `Get` | ✅ `GET` | ✅ `get` | **未命中返回 `("", nil)`**，不是错误 |
| `Set` | ✅ `SET` | ✅ `set` | `ttl <= 0` 表示永不过期（两驱动语义一致） |
| `Delete` | ✅ `DEL` | ✅ `delete` | 删除不存在的 key 返回 `nil`（幂等） |
| `Exists` | ✅ `EXISTS` | ✅ `get` 模拟 | Memcached 无原生 EXISTS，用 GET 判定，多一次反序列化 |
| `Keys` | ✅ `KEYS` | ❌ 返回错误 | Memcached 无键遍历协议；**Redis 侧请勿在生产使用 `KEYS`** |
| `Close` | ✅ 关连接池 | ✅ no-op | Memcached 无显式关闭 |
| `HealthCheck` | ✅ `PING` | ✅ GET 探测 | Memcached 无 PING 命令，用 `_health_check_` key 的 GET 判定连通性 |

### Redis 特有

- 三种部署形态：Standalone / Sentinel / Cluster，由配置自动路由
- 协议兼容 **KeyDB、DragonflyDB、Valkey** —— 均使用 `driver: redis` 接入，无需独立驱动
- 连接池水位指标取自 `go-redis` 实时 `PoolStats`

### Memcached 特有

- 多节点客户端分片（`addr: "host1:11211,host2:11211"`）
- 超出接口的 `incr/decr`、`CAS`、`GetMulti` 能力通过 `Raw()` 使用
- **不产生连接池水位指标**（协议无对应原语）

> 完整命令矩阵见 [redis/FEATURE_MATRIX.md](redis/FEATURE_MATRIX.md) 与 [memcached/FEATURE_MATRIX.md](memcached/FEATURE_MATRIX.md)。

---

## 可观测性

### Prometheus 指标

指标注册到 Prometheus **默认注册器**，`curl localhost:9100/metrics` 即可抓取。

| 指标 | 类型 | 标签 | 说明 |
|---|---|---|---|
| `cachex_command_total` | Counter | `driver,name,command,status` | 命令执行次数，`status` 为 `success`/`error` |
| `cachex_command_duration_seconds` | Histogram | `driver,name,command` | 命令耗时，桶边界 1ms~1s |
| `cachex_circuit_breaker_state` | Gauge | `driver,name` | 熔断器状态：`0` closed、`1` open、`2` half-open |
| `cachex_pool_active_count` | Gauge | `driver,name` | 活跃连接数（**仅 Redis**） |
| `cachex_pool_idle_count` | Gauge | `driver,name` | 空闲连接数（**仅 Redis**） |

暴露 HTTP 端点：

```go
if err := cachex.RegisterMetricsHTTPHandler(":9100"); err != nil {
    log.Fatal(err) // 端口被占用会立即返回，而不是后台静默失败
}
```

推荐告警规则：

```yaml
# 某实例持续熔断
cachex_circuit_breaker_state == 1
# 命令错误率（5 分钟窗口）超过 5%
sum(rate(cachex_command_total{status="error"}[5m])) by (name)
  / sum(rate(cachex_command_total[5m])) by (name) > 0.05
# P99 延迟超过 100ms
histogram_quantile(0.99, sum(rate(cachex_command_duration_seconds_bucket[5m])) by (le, name)) > 0.1
```

### 调用链

开关打开后，每条命令产生一个 span：

| 属性 | 值 |
|---|---|
| span name | `db.redis.{COMMAND}` / `db.memcached.{COMMAND}` |
| `db.system` | `redis` / `memcached` |
| `db.redis.command` | `GET` / `SET` / `DEL` / `EXISTS` / `KEYS` / `PING` |
| `db.redis.key` | 请求的 key（非空时） |
| 附加属性 | `db.redis.found`、`db.redis.ttl_ms`、`db.redis.keys_count` |

实现要点：

- span 经 **BatchSpanProcessor** 异步批量导出，单批 512 条 / 5 秒，不会因导出阻塞业务请求
- 开关关闭时返回 noop span，**不会误关**调用方注入到 `ctx` 中的外部 span
- TracerProvider 首次构建后注册到全局 `otel`；若宿主应用已自建 TracerProvider，请将 `tracing.enabled` 保持为 `false` 交由宿主管理
- `Cache` 接口会自动透传 `ctx` 中的 parent span，天然形成完整调用树

诊断初始化是否成功：

```go
if err := cachex.TracingError(); err != nil {
    log.Printf("tracing unavailable: %v", err)
}
```

### 日志

经由 [logx](https://github.com/gospacex/logx) 输出结构化日志。错误日志记录 `key`、`driver` 与原始错误，**不记录 value**，避免敏感数据落盘。

---

## 韧性设计

### 熔断器

每个实例独享一个 `sony/gobreaker` 熔断器：

| 参数 | 值 | 说明 |
|---|---|---|
| `ReadyToTrip` | 连续 5 次失败 | 达到阈值立即跳闸 |
| `MaxRequests` | `1` | 半开状态只允许 1 个探测请求，成功即闭合 |
| `Interval` | `30s` | 统计按 30 秒滚动窗口重置，避免陈旧失败影响当前判定 |
| `Timeout` | `30s` | 跳闸后等待 30 秒转入半开，探测下游是否恢复 |

熔断器状态通过 `cachex_circuit_breaker_state` 实时暴露。处于 Open 状态时命令快速失败并返回错误，调用方应据此执行降级逻辑（查库、返回旧值等）。

`Execute` 会先检查 `ctx` 是否已取消，已取消的请求不再占用一次熔断器配额，避免无效失败污染统计。

### 建连重试

Redis 驱动在 `New` 阶段以**指数退避**（100ms、200ms、400ms…）重试至 `retry_count` 次，全部失败才返回错误；失败时会释放已创建的连接池，不泄漏文件描述符。

### 优雅关闭

`CloseAll()` 的关闭顺序为：

1. 标记 `closed`，后续 `Instance()` 一律返回 `nil`
2. 取消配置 Watch
3. 逐个关闭实例连接池，**聚合**所有错误（`errors.Join`），不因某个实例失败中断其余关闭
4. 冲刷并关闭 TracerProvider，保证尾部 span 不丢

---

## 安全

- **凭据不入配置**：`password` 支持 `${ENV_VAR}` 占位符，Init 阶段展开；变量缺失时直接报错而非静默使用字面值
- **传输加密**：`tls_enabled: true` 强制最低 TLS 1.2
- **日志脱敏**：只记录 key 与命令，不记录 value

---

## 生产最佳实践

1. **务必判空** `Instance(name)`：未初始化、实例不存在、已关闭三种情况都返回 `nil`。
2. **给每个实例设超时**：`read_timeout`/`write_timeout` 缺省会回落到 `timeout`，建议写命令与读命令分别设阈值。
3. **不要用 `Keys`**：Redis 的 `KEYS` 是 O(N) 阻塞命令，大 key 空间等同于故障；需要遍历请用 `Raw()` 走 `SCAN`。
4. **区分「未命中」与「错误」**：`Get` 返回 `("", nil)` 表示未命中，返回非 nil error 表示真故障，二者处理路径不同（前者查库回写，后者降级）。
5. **缓存雪崩**：`Set` 的 TTL 加随机抖动，避免同一批 key 同时失效。
6. **采样率调低**：全链路采样在万级 QPS 下会压垮 collector，`sampler_ratio` 建议 `0.01`~`0.1`。
7. **进程退出前调用** `CloseAll()`，否则最后一批 span 会丢失。
8. **按需引入驱动**：只 `get` 实际使用的驱动 module，保持二进制体积与依赖树精简。

---

## 本地开发与示例

```bash
# 起 Redis(46379)、Memcached(21121)、Jaeger(16686/4317/4318)
docker compose up -d

# 基础 CRUD + 全数据类型 + 业务场景
go run ./examples/redis
go run ./examples/memcached

# 调用链验证，完成后访问 http://localhost:16686
go run ./examples/tracing

# 单元测试（核心包与 otel 包不依赖外部中间件）
go test ./ ./otel/

# 集成测试需先起容器：Redis 46379 / Memcached 21121
go test ./redis/ ./memcached/
```

### 目录结构

```
cachex/
├── interface.go        Cache 接口、Config、驱动注册表
├── config.go           configx 配置绑定（InstanceConfig / CacheSection）
├── sdk.go              全局单例：Init / Instance / CloseAll
├── tracing.go          调用链开关与 TracerProvider 装配
├── circuit.go          熔断器封装
├── metrics.go          Prometheus 注册表与 HTTP 端点
├── otel/               Jaeger / Kafka 导出器实现
├── redis/              Redis 驱动（独立 module）
├── memcached/          Memcached 驱动（独立 module）
├── examples/           可运行示例与命令用法集
└── openspec/           变更归档（提案、规格、任务清单）
```

---

## API 参考

### 生命周期

| 函数 | 说明 |
|---|---|
| `Init(opts ...InitOption) error` | 初始化全局单例，幂等；需 `WithConfigFile` 或 `WithConfig` 之一 |
| `Instance(name string) Cache` | 取命名实例，未初始化/已关闭/不存在返回 `nil` |
| `CloseAll() error` | 关闭全部实例并冲刷调用链，幂等，错误聚合返回 |
| `IsClosed() bool` | 单例是否已关闭 |

### 配置装配

| 函数 | 说明 |
|---|---|
| `New(cfgs []*Config) (Instances, error)` | 由配置切片直接创建实例集合 |
| `ReadAndCreate(opts ...configx.ConfigOptions) (Instances, error)` | 从 configx 读取并创建 |
| `MustReadAndCreate(...)` | 同上，失败直接退出进程 |
| `RegisterCreator(name string, creator func(*Config) (Cache, error))` | 注册驱动工厂，由驱动的 `init()` 调用 |

### 可观测性

| 函数 | 说明 |
|---|---|
| `SetTracingConfig(cfg TracingConfig)` | 设置调用链开关，可被 Watch 回调调用 |
| `IsTracingEnabled() bool` | 当前开关状态 |
| `TracingError() error` | TracerProvider 最近一次初始化错误 |
| `RegisterMetricsHTTPHandler(addr string) error` | 启动 `/metrics` HTTP 端点 |
| `RecordCommand / UpdatePoolMetrics / UpdateCircuitBreakerState` | 供驱动埋点的导出函数 |

### 驱动

| 函数 | 说明 |
|---|---|
| `redis.New(cfg *cachex.Config) (*Client, error)` | 直接构造 Redis 客户端 |
| `(*redis.Client).Raw() redis.UniversalClient` | 取底层 go-redis 句柄 |
| `memcached.New(cfg *cachex.Config) (*Client, error)` | 直接构造 Memcached 客户端 |
| `(*memcached.Client).Raw() *memcache.Client` | 取底层 gomemcache 句柄 |

---

## 已知限制

- **`Keys` 在生产不可用**：Redis 走阻塞式 `KEYS`，Memcached 直接不支持。遍历需求请用 `Raw()` + `SCAN`。
- **接口仅覆盖字符串 KV**：Hash / List / ZSet / Stream 等类型不走 `Cache` 接口，统一通过 `Raw()` 使用底层句柄，也因此不享受熔断与埋点。
- **Memcached 的 `Exists` 有额外开销**：协议无 EXISTS 原语，用 GET 拉取值后丢弃。
- **熔断器参数为内置默认值**：暂未开放 `MaxRequests`、`Interval` 等自定义，如需调整请扩展 `NewCircuitBreaker`。
- **TracerProvider 由 cachex 托管**：宿主应用若自建全局 TracerProvider，请将 `tracing.enabled` 设为 `false` 以避免覆盖。

---

## 相关文档

- [Redis 驱动功能矩阵](redis/FEATURE_MATRIX.md)
- [Memcached 驱动功能矩阵](memcached/FEATURE_MATRIX.md)
- [企业级重构提案](openspec/changes/archive/2026-09-09-sdk-enterprise-refactor/proposal.md)
