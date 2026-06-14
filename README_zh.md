# CacheX

<div align="center">

[![Go版本](https://img.shields.io/badge/Go-1.21+-00ADD8?style=for-the-badge&logo=go)](https://golang.org)
[![许可证](https://img.shields.io/badge/许可证-MIT-green?style=for-the-badge)](LICENSE)
[![测试状态](https://img.shields.io/badge/测试-100%25-brightgreen?style=for-the-badge)](#)
[![代码覆盖](https://img.shields.io/badge/覆盖-85%25+-brightgreen?style=for-the-badge)](#)
[![GoDoc](https://img.shields.io/badge/GoDoc-参考-blue?style=for-the-badge)](https://pkg.go.dev/github.com/gospacex/cachex)

**生产级统一缓存客户端库 — 一套API，多种后端**

[English](./README.md) | 中文

</div>

---

## 目录

- [特性](#特性)
- [快速开始](#快速开始)
- [核心概念](#核心概念)
- [后端支持](#后端支持)
- [配置参考](#配置参考)
- [API参考](#api参考)
- [可观测性](#可观测性)
- [弹性模式](#弹性模式)
- [扩展模块](#扩展模块)
- [中间件模式](#中间件模式)
- [示例代码](#示例代码)
- [测试](#测试)
- [性能基准](#性能基准)
- [贡献指南](#贡献指南)
- [许可证](#许可证)

---

## 特性

### 🔧 核心特性

| 特性 | 描述 |
|------|------|
| **统一接口** | 16个标准Cache操作，适配所有后端 |
| **多后端支持** | Redis、Dragonfly、KeyDB、Garnet、Badger、BBolt、Pebble |
| **连接池** | 可配置大小的连接池，最小空闲连接数 |
| **TLS安全** | 所有网络后端支持mTLS |
| **批量操作** | MGet/MSet批量读写，减少网络往返 |

### 🛡️ 生产就绪

| 特性 | 描述 |
|------|------|
| **熔断器** | 防止级联故障，自动恢复 |
| **重试机制** | 指数退避+抖动，提高可靠性 |
| **健康检查** | 就绪探针+存活探针 |
| **连接池监控** | 实时监控连接池健康状态 |
| **结构化日志** | JSON格式日志，支持级别控制 |

### 📊 可观测性

| 特性 | 描述 |
|------|------|
| **Prometheus指标** | 操作计数、延迟直方图、命中率 |
| **OpenTelemetry** | 分布式追踪，Span上下文传递 |
| **观察者模式** | 可插拔的监控扩展 |

### ⚡ 扩展能力

| 特性 | 描述 |
|------|------|
| **速率限制** | Token Bucket + Sliding Window |
| **分布式锁** | Redis-based 分布式锁 |
| **布隆过滤器** | 高效存在性检测 |
| **缓存模式** | Cache-Aside、Write-Through、Write-Behind |

---

## 快速开始

### 安装

```bash
go get github.com/gospacex/cachex@latest
```

### 基础用法

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/gospacex/cachex"
    _ "github.com/gospacex/cachex/backends/embedded/badger" // 注册后端
)

func main() {
    ctx := context.Background()

    // 方式1: 工厂模式创建
    cache, err := cachex.Open("badger", &cachex.Config{
        Dir: "/tmp/my-cache",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer cache.Close()

    // 基本操作
    err = cache.Set(ctx, "key", []byte("value"))
    if err != nil {
        log.Fatal(err)
    }

    val, err := cache.Get(ctx, "key")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("获取到: %s\n", string(val))

    // 批量操作
    cache.MSet(ctx, map[string][]byte{
        "user:1": []byte(`{"name":"Alice"}`),
        "user:2": []byte(`{"name":"Bob"}`),
    })

    values, _ := cache.MGet(ctx, "user:1", "user:2")
    fmt.Printf("找到 %d 个用户\n", len(values))
}
```

### 单例模式

```go
// 通过配置文件创建单例
cache, err := cachex.B("/path/to/config.yaml")

// 或直接创建
cfg := cachex.DefaultConfig(cachex.BackendRedis)
cfg.Addrs = []string{"localhost:6379"}
cache, err := cachex.C(cachex.BackendRedis, cfg)
```

---

## 核心概念

### 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                        CacheX                               │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │   工厂      │───▶│  注册表     │───▶│   创建器    │     │
│  │  Factory    │    │  Registry   │    │  Creator    │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
│         │                                        │          │
│         ▼                                        ▼          │
│  ┌─────────────┐                        ┌─────────────┐     │
│  │  观察者     │                        │    缓存     │     │
│  │  Observers │                        │    Cache    │     │
│  │  (指标)    │                        └──────┬──────┘     │
│  │  (追踪)    │                               │            │
│  │  (日志)    │                               ▼            │
│  └─────────────┘                        ┌─────────────┐     │
│                                         │   后端      │     │
│                                         │  Backends   │     │
│                                         └──────┬──────┘     │
│  ┌───────────────────────────────────────────┼─────────────┤
│  │                                           │             │
│  ▼                                           ▼             ▼
│ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐     │
│ │ Redis  │ │Dragonfly│ │ KeyDB  │ │ Garnet │ │ Badger │     │
│ └────────┘ └────────┘ └────────┘ └────────┘ └────────┘     │
│                                              ┌────────┐     │
│                                              │ BBolt  │     │
│                                              └────────┘     │
│                                              ┌────────┐     │
│                                              │ Pebble │     │
│                                              └────────┘     │
└─────────────────────────────────────────────────────────────┘
```

---

## 后端支持

### 后端对比表

| 后端 | 类型 | 协议 | 集群 | TTL | 原子操作 | 性能 | 适用场景 |
|------|------|------|------|-----|---------|------|----------|
| **Redis** | 网络 | Redis | Sentinel/Cluster | ✅ | ✅ | ⭐⭐⭐⭐⭐ | 生产环境缓存 |
| **Dragonfly** | 网络 | Redis | ✅ | ✅ | ✅ | ⭐⭐⭐⭐⭐ | 高性能场景 |
| **KeyDB** | 网络 | Redis | Cluster | ✅ | ✅ | ⭐⭐⭐⭐⭐ | Redis兼容替代 |
| **Garnet** | 网络 | Redis | Cluster | ✅ | ✅ | ⭐⭐⭐⭐ | 持久化缓存 |
| **Badger** | 嵌入 | KV | ❌ | ✅ | ❌ | ⭐⭐⭐⭐ | 嵌入式KV |
| **BBolt** | 嵌入 | KV | ❌ | ❌ | ✅ | ⭐⭐⭐ | 简单嵌入式 |
| **Pebble** | 嵌入 | KV | ❌ | ❌ | ✅ | ⭐⭐⭐⭐ | RocksDB替代 |

### 使用示例

```go
// Redis
cache, _ := cachex.Open("redis", &cachex.Config{
    Addrs:    []string{"localhost:6379"},
    Password: "secret",
    PoolSize: 20,
})

// Redis Cluster
cache, _ := cachex.Open("redis", &cachex.Config{
    Addrs:       []string{"localhost:7000", "localhost:7001", "localhost:7002"},
    ClusterMode: true,
})

// Badger
cache, _ := cachex.Open("badger", &cachex.Config{
    Dir:             "/var/lib/cachex/badger",
    BlockCacheSize:  256 * 1024 * 1024,
    IndexCacheSize:  128 * 1024 * 1024,
})

// BBolt
cache, _ := cachex.Open("bbolt", &cachex.Config{
    Dir:       "/var/lib/cachex/bbolt.db",
    BucketName: "cachex",
})

// Pebble
cache, _ := cachex.Open("pebble", &cachex.Config{
    Dir:             "/var/lib/cachex/pebble",
    BlockCacheSize:  64 * 1024 * 1024,
})
```

---

## 配置参考

### YAML配置文件

```yaml
# config.yaml
backend: redis
addrs:
  - localhost:6379
  - localhost:6378
password: "your-password"
pool_size: 20
min_idle_conns: 5
max_retries: 3
dial_timeout: 5
read_timeout: 3
write_timeout: 3

circuit_breaker:
  enabled: true
  threshold: 5
  timeout: 30
  half_open_max_requests: 3

logger:
  level: info
  format: json
  output: stdout
```

```go
// 加载配置文件
cfg, err := cachex.LoadConfig("config.yaml")
cache, err := cachex.Open("redis", cfg)
```

### 环境变量

| 变量 | 描述 |
|------|------|
| `CACHEX_ADDRS` | 地址列表 |
| `CACHEX_PASSWORD` | 认证密码 |
| `CACHEX_DB` | 数据库编号 |
| `CACHEX_POOL_SIZE` | 连接池大小 |
| `CACHEX_TLS_ENABLED` | 启用TLS |
| `CACHEX_DIR` | 嵌入式存储目录 |

---

## API参考

### 基本操作

```go
ctx := context.Background()

// 存储
cache.Set(ctx, "key", []byte("value"))
cache.SetEX(ctx, "key", []byte("value"), 3600) // 1小时过期
set, _ := cache.SetNX(ctx, "lock", []byte("owner"), 30) // 分布式锁

// 获取
val, err := cache.Get(ctx, "key")
// err == cachex.ErrKeyNotFound 表示不存在

// 删除
n, _ := cache.Delete(ctx, "key1", "key2")

// 检查存在
count, _ := cache.Exists(ctx, "a", "b", "c")

// 过期
cache.Expire(ctx, "key", 60) // 60秒后过期
ttl, _ := cache.TTL(ctx, "key")
// -1: 无过期, -2: 不存在, >0: 剩余秒数

// 批量
cache.MSet(ctx, map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2")})
vals, _ := cache.MGet(ctx, "k1", "k2")

// 原子操作
counter, _ := cache.Incr(ctx, "counter")
counter, _ = cache.Decr(ctx, "counter")
```

### 错误处理

```go
import "github.com/gospacex/cachex"

// 预定义错误
cachex.ErrKeyNotFound      // 键不存在
cachex.ErrConnectionFailed // 连接失败
cachex.ErrTimeout          // 操作超时
cachex.ErrCircuitOpen      // 熔断器打开
cachex.ErrNotSupported     // 操作不支持

// 错误检查
if err == cachex.ErrKeyNotFound { ... }
if errors.Is(err, cachex.ErrKeyNotFound) { ... }
if cachex.IsRetryable(err) { ... } // 是否可重试
```

---

## 可观测性

### Prometheus指标

```go
import "github.com/gospacex/cachex/observability/metrics"

collector := metrics.NewCollector("cachex", "redis")
factory := cachex.NewFactory()
factory.AddObserver(collector)

// 指标:
// cachex_redis_operations_total{backend, operation, status}
// cachex_redis_operation_duration_seconds{backend, operation}
// cachex_redis_hits_total{backend, operation}
// cachex_redis_errors_total{backend, operation, error_type}
```

### OpenTelemetry追踪

```go
import "github.com/gospacex/cachex/observability"

tracer := otel.Tracer("my-service")
traceObserver := observability.NewTraceObserver(tracer)

factory := cachex.NewFactory()
factory.AddObserver(traceObserver)
```

### 结构化日志

```go
logger := observability.NewLogger(
    observability.WithLevel(observability.LevelDebug),
    observability.WithFormat("json"),
)
factory.AddObserver(observability.NewLoggingObserver(logger))
```

### 连接池监控

```go
monitor := observability.NewPoolMonitor(100, time.Second)
monitor.Start(ctx, cache)

stats := monitor.CurrentStats()
fmt.Printf("活跃: %d, 空闲: %d, 利用率: %.2f%%\n",
    stats.Active, stats.Idle, monitor.UtilizationRate()*100)
```

### 路径 2 可观测性 (对齐 mqx)

新增的 `cachex/initx` 与 `cachex/observability` 表面对齐 mqx 的链路追踪模式——相同的 yaml schema，相同的 4 个后端，相同的 OTel SDK。开发者无需手写 TracerProvider 装配逻辑，一行 `InitTracing` 即可。

#### 后端对照

| Backend | yaml `exporter` | 前置依赖 |
|---------|----------------|----------|
| jaeger | `jaeger` | jaegertracing/all-in-one docker（端口 14268 gRPC） |
| otlp | `otlp` | 任意 OTLP collector（端口 4317 gRPC / 4318 HTTP） |
| redis_stream | `redis_stream` | redis 7+；通过 `WithRedisClient` 注入 |
| kafka_topic | `kafka_topic` | kafka 3.x；通过 `WithKafkaProducer` 注入 |

#### 快速开始

```go
import "github.com/gospacex/cachex/initx"

func main() {
    ctx := context.Background()
    cleanup, err := initx.InitTracing(ctx, "config/tracing.yaml")
    if err != nil {
        log.Fatal(err)
    }
    defer cleanup(context.Background())

    // 业务代码：使用 otel.Tracer 即可获得全局 provider
    // tracer := otel.Tracer("my-service")
    // _, span := tracer.Start(ctx, "my-op")
    // defer span.End()
}
```

#### YAML 片段

```yaml
trace:
  enabled: true
  service_name: my-service
  exporter: jaeger
  endpoint: http://localhost:14268/api/traces
  insecure: true
  sampler_type: always_on
```

#### 更多内容

完整架构图、4 后端配置、跨进程传播示例见 [docs/observability.md](docs/observability.md)。完整快捷函数参考见 [docs/shortcut-functions.md](docs/shortcut-functions.md)。

---

## 弹性模式

### 熔断器

```go
cb := observability.NewCircuitBreaker("redis",
    observability.WithThreshold(5),
    observability.WithTimeout(30*time.Second),
    observability.WithHalfOpenMaxRequests(3),
)

protectedCache := observability.WrapCacheWithCircuitBreaker(cache, cb)
```

### 重试机制

```go
import "github.com/gospacex/cachex/extensions/retry"

retryCfg := &retry.Config{
    MaxAttempts:    5,
    InitialBackoff: 100 * time.Millisecond,
    MaxBackoff:     5 * time.Second,
    Multiplier:     2.0,
    Jitter:         true,
}

retryableCache := retry.NewRetryableCache(cache, retryCfg)
```

### 速率限制

```go
import "github.com/gospacex/cachex/extensions/ratelimit"

// Token Bucket
limiter := ratelimit.NewTokenBucket(100, 10) // 100容量，每秒补充10
if limiter.Allow() {
    // 处理请求
}

// 滑动窗口
sliding := ratelimit.NewSlidingWindow(100, time.Minute) // 100请求/分钟
```

---

## 扩展模块

### 分布式锁

```go
import "github.com/gospacex/cachex/extensions/distlock"

lockMgr := distlock.NewDistributedLock(cache)
lock, err := lockMgr.Lock(ctx, "resource:123", 30*time.Second)
// 执行操作
lock.Release(ctx)

// 信号量
sem := distlock.NewSemaphore(cache, "my-sem", 10)
sem.Acquire(ctx)
// 执行操作
sem.Release(ctx)
```

### 布隆过滤器

```go
import "github.com/gospacex/cachex/extensions/bloom"

filter := bloom.New(10000, 0.01) // 1%误报率
filter.Add([]byte("item"))
if filter.Test([]byte("item")) {
    // 可能在集合中
}
```

### 健康检查

```go
import "github.com/gospacex/cachex/extensions/healthcheck"

checker := healthcheck.NewHealthChecker(cache)
checker.AddCheck("custom", func(ctx context.Context) error {
    return nil
})

results := checker.Check(ctx)
if err := checker.CheckAll(ctx); err != nil {
    log.Printf("缓存不健康: %v", err)
}
```

---

## 中间件模式

### Cache-Aside

```go
import "github.com/gospacex/cachex/middleware"

data, err := middleware.CacheAside(ctx, cache, "user:123", 300, func() ([]byte, error) {
    return fetchFromDatabase("user:123")
})
```

### Write-Behind

```go
wb := middleware.NewWriteBehind(cache, dbWriter, 1000)
wb.Set(ctx, "key", value) // 立即返回，异步写入
```

---

## 示例代码

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/gospacex/cachex"
    "github.com/gospacex/cachex/observability"
    "github.com/gospacex/cachex/observability/metrics"
)

func main() {
    ctx := context.Background()

    // 创建指标收集器
    collector := metrics.NewCollector("cachex", "api")

    // 创建工厂
    factory := cachex.NewFactory()
    factory.AddObserver(collector)

    // 创建缓存
    cache, err := factory.Create("redis", &cachex.Config{
        Addrs:    []string{"localhost:6379"},
        PoolSize: 20,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer cache.Close()

    // 使用
    user, _ := getUser(ctx, cache, "user:123")
    fmt.Printf("User: %s\n", user)

    // 统计
    stats := cache.Stats()
    fmt.Printf("命中: %d, 未命中: %d\n", stats.Hits(), stats.Misses())
}

func getUser(ctx context.Context, cache cachex.Cache, userID string) (string, error) {
    key := "user:" + userID

    val, err := cache.Get(ctx, key)
    if err == nil {
        return string(val), nil
    }
    if err != cachex.ErrKeyNotFound {
        return "", err
    }

    // 模拟从数据库加载
    userData := fmt.Sprintf(`{"id":"%s","name":"User %s"}`, userID, userID)
    cache.SetEX(ctx, key, []byte(userData), 3600)

    return userData, nil
}
```

更多示例见 [examples/](examples/) 目录。

---

## 测试

```bash
# 所有测试
make test

# 带覆盖率
make test-cover

# 竞态检测
make test-race

# 集成测试
make test-integration
```

---

## 贡献指南

欢迎提交Issue和Pull Request！

```bash
# 克隆
git clone https://github.com/gospacex/cachex.git
cd cachex

# 开发
go mod download
make test
make fmt
make lint-ci
```

详细指南见 [CONTRIBUTING.md](CONTRIBUTING.md)。

---

## 许可证

MIT License - 见 [LICENSE](LICENSE) 文件。

---

<div align="center">

**CacheX** - 让缓存集成变得简单

[提交Issue](https://github.com/gospacex/cachex/issues) | 
[查看文档](https://pkg.go.dev/github.com/gospacex/cachex) | 
[贡献代码](CONTRIBUTING.md)

</div>