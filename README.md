# CacheX

<div align="center">

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=for-the-badge&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)
[![Test Status](https://img.shields.io/badge/Tests-100%25-brightgreen?style=for-the-badge)](#)
[![Coverage](https://img.shields.io/badge/Coverage-85%25+-brightgreen?style=for-the-badge)](#)
[![GoDoc](https://img.shields.io/badge/GoDoc-Reference-blue?style=for-the-badge)](https://pkg.go.dev/github.com/gospacex/cachex)
[![Build Status](https://img.shields.io/badge/Build-Passing-green?style=for-the-badge)]()

**生产级统一缓存客户端库 — 一套API，多种后端**

[English](./README.md) | [中文](./README_zh.md)

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
    fmt.Printf("Got: %s\n", string(val))

    // 批量操作
    cache.MSet(ctx, map[string][]byte{
        "user:1": []byte(`{"name":"Alice"}`),
        "user:2": []byte(`{"name":"Bob"}`),
    })

    values, _ := cache.MGet(ctx, "user:1", "user:2")
    fmt.Printf("Found %d users\n", len(values))
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
│  │   Factory   │───▶│  Registry   │───▶│   Creator   │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
│         │                                        │          │
│         ▼                                        ▼          │
│  ┌─────────────┐                        ┌─────────────┐     │
│  │  Observers  │                        │    Cache    │     │
│  │  (Metrics)  │                        │  Interface  │     │
│  │  (Tracing)  │                        └──────┬──────┘     │
│  │  (Logging)  │                               │            │
│  └─────────────┘                               ▼            │
│                                         ┌─────────────┐     │
│                                         │  Backends   │     │
│                                         ├─────────────┤     │
│  ┌──────────────────────────────────────┼─────────────┤     │
│  │                                      │             │     │
│  ▼                                      ▼             ▼     │
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

### 关键接口

```go
// 核心Cache接口
type Cache interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Set(ctx context.Context, key string, value []byte) error
    SetEX(ctx context.Context, key string, value []byte, ttlSeconds int64) error
    SetNX(ctx context.Context, key string, value []byte, ttlSeconds int64) (bool, error)
    Delete(ctx context.Context, keys ...string) (int64, error)
    Exists(ctx context.Context, keys ...string) (int64, error)
    Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error)
    TTL(ctx context.Context, key string) (int64, error)
    MGet(ctx context.Context, keys ...string) ([][]byte, error)
    MSet(ctx context.Context, kvs map[string][]byte) error
    Keys(ctx context.Context, pattern string) ([]string, error)
    Incr(ctx context.Context, key string) (int64, error)
    Decr(ctx context.Context, key string) (int64, error)
    Ping(ctx context.Context) error
    Close() error
    Stats() Stats
}

// 统计信息接口
type Stats interface {
    Hits() int64
    Misses() int64
    Errors() int64
    Latency() int64
}

// 观察者接口（用于可观测性）
type Observer interface {
    OnOperation(ctx context.Context, op Operation, backend string, err error, duration time.Duration)
    OnError(ctx context.Context, op Operation, backend string, err error)
}

// 工厂模式
type Factory struct {
    registry  *Registry
    observers []Observer
}

func (f *Factory) Create(backend string, cfg *Config) (Cache, error)
func (f *Factory) AddObserver(obs Observer)
func (f *Factory) ListBackends() []BackendInfo
```

---

## 后端支持

### 后端对比表

| 后端 | 类型 | 协议 | 集群 | TTL | 原子操作 | 性能 | 适用场景 |
|------|------|------|------|-----|---------|------|----------|
| **Redis** | Network | Redis | Sentinel/Cluster | ✅ | ✅ | ⭐⭐⭐⭐⭐ | 生产环境缓存 |
| **Dragonfly** | Network | Redis | ✅ | ✅ | ✅ | ⭐⭐⭐⭐⭐ | 高性能场景 |
| **KeyDB** | Network | Redis | Cluster | ✅ | ✅ | ⭐⭐⭐⭐⭐ | Redis兼容替代 |
| **Garnet** | Network | Redis | Cluster | ✅ | ✅ | ⭐⭐⭐⭐ | 持久化缓存 |
| **Badger** | Embedded | KV | ❌ | ✅ | ❌ | ⭐⭐⭐⭐ | 嵌入式KV |
| **BBolt** | Embedded | KV | ❌ | ❌ | ✅ | ⭐⭐⭐ | 简单嵌入式 |
| **Pebble** | Embedded | KV | ❌ | ❌ | ✅ | ⭐⭐⭐⭐ | RocksDB替代 |

### 网络后端

```go
// Redis 单机
cache, _ := cachex.Open("redis", &cachex.Config{
    Addrs:    []string{"localhost:6379"},
    Password: "secret",
    PoolSize: 20,
})

// Redis Cluster
cache, _ := cachex.Open("redis", &cachex.Config{
    Addrs:       []string{"localhost:7000", "localhost:7001", "localhost:7002"},
    ClusterMode: true,
    PoolSize:    50,
})

// Redis Sentinel
cache, _ := cachex.Open("redis", &cachex.Config{
    Addrs:       []string{"localhost:26379", "localhost:26380"},
    MasterName:  "mymaster",
    Password:    "sentinel-password",
})

// Dragonfly
cache, _ := cachex.Open("dragonfly", &cachex.Config{
    Addrs:    []string{"localhost:6380"},
    PoolSize: 30,
})

// KeyDB
cache, _ := cachex.Open("keydb", &cachex.Config{
    Addrs:    []string{"localhost:6379"},
    PoolSize: 30,
})

// Garnet
cache, _ := cachex.Open("garnet", &cachex.Config{
    Addrs:    []string{"localhost:6379"},
    PoolSize: 30,
})
```

### 嵌入式后端

```go
// Badger (推荐用于高性能嵌入式场景)
cache, _ := cachex.Open("badger", &cachex.Config{
    Dir:             "/var/lib/cachex/badger",
    BlockCacheSize:  256 * 1024 * 1024, // 256MB
    IndexCacheSize:  128 * 1024 * 1024, // 128MB
    SyncWrites:      false,
})

// Badger 内存模式
cache, _ := cachex.Open("badger", &cachex.Config{
    InMemory: true,
})

// BBolt (简单场景)
cache, _ := cachex.Open("bbolt", &cachex.Config{
    Dir:       "/var/lib/cachex/bbolt.db",
    BucketName: "cachex",
    SyncWrites: true,
})

// Pebble (RocksDB替代)
cache, _ := cachex.Open("pebble", &cachex.Config{
    Dir:             "/var/lib/cachex/pebble",
    BlockCacheSize:  64 * 1024 * 1024, // 64MB
    Compression:     true,
})
```

---

## 配置参考

### 配置结构

```go
type Config struct {
    // 后端类型
    Backend string
    
    // Redis驱动类型 (dragonfly, keydb, garnet)
    Driver string
    
    // 网络后端配置
    Addrs         []string
    Password      string
    DB            int
    PoolSize      int
    MinIdleConns  int
    MaxRetries    int
    DialTimeout   int // 秒
    ReadTimeout   int // 秒
    WriteTimeout  int // 秒
    IdleTimeout   int // 秒
    PoolTimeout   int // 秒
    
    // TLS配置
    TLS TLSConfig
    
    // Sentinel配置
    MasterName       string
    SentinelPassword string
    
    // Cluster配置
    ClusterMode bool
    
    // 嵌入式存储配置
    Dir         string
    ValueDir    string
    BucketName  string
    FileMode    int
    MmapSize    int64
    ReadOnly    bool
    SyncWrites  bool
    InMemory    bool
    
    // 缓存配置
    BlockCacheSize int64
    IndexCacheSize int64
    MemTableSize   int64
    
    // 熔断器配置
    CircuitBreaker *CircuitBreakerConfig
    
    // 可观测性配置
    Metrics       bool
    MetricsPrefix string
    Logger        *LoggerConfig
}
```

### YAML配置文件

```yaml
# examples/config.yaml
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

| 变量 | 类型 | 描述 |
|------|------|------|
| `CACHEX_ADDRS` | string | 地址列表，逗号分隔 |
| `CACHEX_PASSWORD` | string | 认证密码 |
| `CACHEX_DB` | int | 数据库编号 |
| `CACHEX_POOL_SIZE` | int | 连接池大小 |
| `CACHEX_TLS_ENABLED` | bool | 启用TLS |
| `CACHEX_TLS_CA_FILE` | string | CA证书路径 |
| `CACHEX_DIR` | string | 嵌入式存储目录 |

---

## API参考

### 工厂方法

```go
// 创建新缓存实例
cache, err := cachex.Open(backend string, cfg *Config)

// 创建带观察者的缓存
cache, err := cachex.OpenWithObservers(backend string, cfg *Config, observers ...Observer)

// 获取单例
cache, err := cachex.C(backend string, cfg *Config)

// 创建工厂
factory := cachex.NewFactory()
factory.AddObserver(metricsCollector)
cache, err := factory.Create(backend string, cfg *Config)
```

### 单例快捷方法

```go
// Redis
cache, err := cachex.R("/path/to/config.yaml")  // 单机
cache, err := cachex.RC("/path/to/config.yaml") // 集群

// Dragonfly
cache, err := cachex.D("/path/to/config.yaml")  // 单机
cache, err := cachex.DC("/path/to/config.yaml") // 集群

// KeyDB
cache, err := cachex.K("/path/to/config.yaml")  // 单机
cache, err := cachex.KC("/path/to/config.yaml") // 集群

// Garnet
cache, err := cachex.G("/path/to/config.yaml")  // 单机
cache, err := cachex.GC("/path/to/config.yaml") // 集群

// Badger
cache, err := cachex.B("/path/to/config.yaml")

// BBolt
cache, err := cachex.BB("/path/to/config.yaml")

// Pebble
cache, err := cachex.P("/path/to/config.yaml")
```

### 基本操作

```go
ctx := context.Background()

// Set - 存储键值对
cache.Set(ctx, "key", []byte("value"))

// Get - 获取值
val, err := cache.Get(ctx, "key")
// 错误: cachex.ErrKeyNotFound 表示键不存在

// SetEX - 带过期时间存储
cache.SetEX(ctx, "session:123", []byte("data"), 3600) // 1小时后过期

// SetNX - 不存在时设置
set, _ := cache.SetNX(ctx, "lock", []byte("owner"), 30) // 30秒锁

// Delete - 删除键
n, _ := cache.Delete(ctx, "key1", "key2", "key3")

// Exists - 检查键是否存在
count, _ := cache.Exists(ctx, "a", "b", "c")

// Expire - 设置过期时间
cache.Expire(ctx, "key", 60) // 60秒后过期

// TTL - 获取剩余生存时间
ttl, _ := cache.TTL(ctx, "key")
// -1: 无过期时间, -2: 键不存在, >0: 剩余秒数
```

### 批量操作

```go
// MSet - 批量设置
cache.MSet(ctx, map[string][]byte{
    "user:1": []byte(`{"id":1,"name":"Alice"}`),
    "user:2": []byte(`{"id":2,"name":"Bob"}`),
    "user:3": []byte(`{"id":3,"name":"Charlie"}`),
})

// MGet - 批量获取
values, _ := cache.MGet(ctx, "user:1", "user:2", "user:3")
// 注意: 返回的切片索引与keys对应，未找到的键对应nil

// Keys - 模式匹配
keys, _ := cache.Keys(ctx, "user:*")
```

### 原子操作

```go
// Incr - 原子递增
counter, _ := cache.Incr(ctx, "counter")
counter, _ = cache.Incr(ctx, "counter") // counter = 2

// Decr - 原子递减
counter, _ = cache.Decr(ctx, "counter") // counter = 1
```

### 工具方法

```go
// Ping - 健康检查
if err := cache.Ping(ctx); err != nil {
    log.Printf("Cache unhealthy: %v", err)
}

// Stats - 获取统计信息
stats := cache.Stats()
fmt.Printf("Hits: %d, Misses: %d, Errors: %d\n", 
    stats.Hits(), stats.Misses(), stats.Errors())

// Close - 关闭连接
cache.Close()
```

### 错误处理

```go
import "github.com/gospacex/cachex"

// 预定义错误
var ErrKeyNotFound = errors.New("key not found")
var ErrConnectionFailed = errors.New("connection failed")
var ErrTimeout = errors.New("operation timeout")
var ErrCircuitOpen = errors.New("circuit breaker open")
var ErrNotSupported = errors.New("operation not supported")

// 错误包装
err := cachex.NewCacheError("get", "redis", cachex.ErrKeyNotFound)
err = err.WithKey("my-key")
err = err.WithCause(underlyingErr)

// 错误检查
if err == cachex.ErrKeyNotFound { ... }
if errors.Is(err, cachex.ErrKeyNotFound) { ... }

// 错误分类
if cachex.IsRetryable(err) { ... }
```

---

## 可观测性

### Prometheus指标

```go
import "github.com/gospacex/cachex/observability/metrics"

collector := metrics.NewCollector("cachex", "redis")

factory := cachex.NewFactory()
factory.AddObserver(collector)

cache, _ := factory.Create("redis", cfg)

// 在 /metrics 端点暴露指标
// cachex_redis_operations_total{backend="redis",operation="get",status="success"}
// cachex_redis_operation_duration_seconds{backend="redis",operation="get"}
// cachex_redis_hits_total{backend="redis",operation="get"}
// cachex_redis_errors_total{backend="redis",operation="get",error_type="timeout"}
```

### OpenTelemetry追踪

```go
import (
    "github.com/gospacex/cachex/observability"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/trace"
)

tracer := otel.Tracer("my-service")
traceObserver := observability.NewTraceObserver(tracer)

factory := cachex.NewFactory()
factory.AddObserver(traceObserver)

// 或者直接包装缓存
tracedCache := observability.NewTracedCache(cache, tracer)

// Span会自动包含: cache.operation, cache.key, cache.backend, error信息
```

### 结构化日志

```go
logger := observability.NewLogger(
    observability.WithLevel(observability.LevelDebug),
    observability.WithFormat("json"),
    observability.WithFields(map[string]interface{}{
        "service": "cachex",
        "env":     "production",
    }),
)

factory.AddObserver(observability.NewLoggingObserver(logger))

// 输出示例:
// {"timestamp":"2024-01-15T10:30:00Z","level":"INFO","message":"cache operation completed","operation":"get","backend":"redis","duration_ms":5}
```

### 连接池监控

```go
monitor := observability.NewPoolMonitor(100, time.Second)
monitor.Start(ctx, cache)
defer monitor.Stop()

// 获取当前统计
stats := monitor.CurrentStats()
fmt.Printf("Active: %d, Idle: %d, Total: %d\n",
    stats.Active, stats.Idle, stats.Total)

// 获取平均统计
avg := monitor.AverageStats()

// 获取健康状态
fmt.Printf("Health: %s, Utilization: %.2f%%\n",
    monitor.HealthStatus(),
    monitor.UtilizationRate()*100)
```

### 路径 2 可观测性 (对齐 mqx)

新增的 `cachex/initx` 与 `cachex/observability` 表面对齐 mqx 的链路追踪模式——相同的 yaml schema，相同的 4 个后端，相同的 OTel SDK。开发者无需再手写 TracerProvider 装配逻辑。

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

    // ... rest of your app ...
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
    observability.WithThreshold(5),           // 5次失败后打开
    observability.WithTimeout(30*time.Second), // 30秒后半开
    observability.WithHalfOpenMaxRequests(3),  // 半开状态允许3个请求
    observability.WithOnStateChange(func(name string, from, to observability.State) {
        log.Printf("Circuit breaker %s: %s -> %s", name, from, to)
    }),
)

protectedCache := observability.WrapCacheWithCircuitBreaker(cache, cb)

// 检查熔断器状态
if cb.State() == observability.StateOpen {
    log.Println("Circuit is open, request rejected")
}
```

### 重试机制

```go
import "github.com/gospacex/cachex/extensions/retry"

retryCfg := &retry.Config{
    MaxAttempts:    5,
    InitialBackoff: 100 * time.Millisecond,
    MaxBackoff:     5 * time.Second,
    Multiplier:     2.0,
    Jitter:         true, // 添加随机抖动
}

retryableCache := retry.NewRetryableCache(cache, retryCfg)

// 失败会自动重试，指数退避
retryableCache.Set(ctx, "key", []byte("value"))
```

### 速率限制

```go
import "github.com/gospacex/cachex/extensions/ratelimit"

// Token Bucket (漏桶)
limiter := ratelimit.NewTokenBucket(100, 10) // 容量100，每秒补充10个token
if limiter.Allow() {
    // 处理请求
}

// Sliding Window (滑动窗口)
sliding := ratelimit.NewSlidingWindow(100, time.Minute) // 100请求/分钟
if sliding.Allow() {
    // 处理请求
}

// 包装缓存实现速率限制
rateLimitedCache := ratelimit.NewRateLimitedCache(cache, 200, 100) // 200容量，100/秒
```

---

## 扩展模块

### 分布式锁

```go
import "github.com/gospacex/cachex/extensions/distlock"

lockMgr := distlock.NewDistributedLock(cache)

// 获取锁
lock, err := lockMgr.Lock(ctx, "resource:123", 30*time.Second)
if err == distlock.ErrLockNotAcquired {
    log.Println("Failed to acquire lock")
    return
}

// 执行关键操作
doCriticalOperation()

// 释放锁
lock.Release(ctx)

// Semaphore (信号量)
sem := distlock.NewSemaphore(cache, "my-semaphore", 10) // 10个槽位
sem.Acquire(ctx)
// 执行操作
sem.Release(ctx)
```

### 布隆过滤器

```go
import "github.com/gospacex/cachex/extensions/bloom"

filter := bloom.New(10000, 0.01) // 10000元素，1%误报率

filter.Add([]byte("item1"))
filter.Add([]byte("item2"))

if filter.Test([]byte("item1")) {
    // 可能在集合中（可能有false positive）
}

if !filter.Test([]byte("nonexistent")) {
    // 一定不在集合中（无false negative）
}
```

### 健康检查

```go
import "github.com/gospacex/cachex/extensions/healthcheck"

checker := healthcheck.NewHealthChecker(cache)

// 添加自定义检查
checker.AddCheck("data_integrity", func(ctx context.Context) error {
    // 检查数据完整性
    return nil
})

// 执行所有检查
results := checker.Check(ctx)
for _, r := range results {
    fmt.Printf("[%s] %s: %s (latency: %v)\n", r.Status, r.Name, r.Message, r.Latency)
}

// 综合健康检查
if err := checker.CheckAll(ctx); err != nil {
    log.Printf("Cache unhealthy: %v", err)
}

// 就绪检查
ready := healthcheck.NewReadyChecker(cache, 5*time.Second)
if err := ready.Ready(ctx); err != nil {
    log.Println("Cache not ready")
}
```

---

## 中间件模式

### Cache-Aside (旁路缓存)

```go
import "github.com/gospacex/cachex/middleware"

data, err := middleware.CacheAside(ctx, cache, "user:123", 300, func() ([]byte, error) {
    // 缓存未命中，从数据库加载
    return fetchFromDatabase("user:123")
})
```

### Write-Through (穿透写)

```go
err := middleware.WriteThrough(ctx, cache, "key", value, func(v []byte) error {
    return db.Update("key", v)
})
```

### Write-Behind (回写)

```go
wb := middleware.NewWriteBehind(cache, 
    func(key string, value []byte) error {
        return db.Update(key, value)
    },
    1000, // 缓冲区大小
)

wb.Set(ctx, "key", value) // 立即返回，异步写入数据库
defer wb.Close()
```

### 超时包装

```go
timeoutCache := middleware.NewTimeoutCache(cache, 100*time.Millisecond)

// 所有操作都会在100ms内超时
timeoutCache.Get(ctx, "key")
```

---

## 示例代码

### Redis完整示例

```go
package main

import (
    "context"
    "fmt"
    "log"
    "net/http"
    _ "net/http/pprof"

    "github.com/gospacex/cachex"
    "github.com/gospacex/cachex/observability"
    "github.com/gospacex/cachex/observability/metrics"
)

func main() {
    // 启动pprof服务器
    go http.ListenAndServe(":6060", nil)

    ctx := context.Background()

    // 创建指标收集器
    collector := metrics.NewCollector("cachex", "api")

    // 创建熔断器
    cb := observability.NewCircuitBreaker("redis",
        observability.WithThreshold(5),
        observability.WithTimeout(30*time.Second),
    )

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

    // 包装熔断器
    cache = observability.WrapCacheWithCircuitBreaker(cache, cb)

    // 示例操作
    user, _ := getUser(ctx, cache, "user:123")
    fmt.Printf("User: %s\n", user)

    // 统计信息
    stats := cache.Stats()
    fmt.Printf("Hits: %d, Misses: %d\n", stats.Hits(), stats.Misses())
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
    
    // 缓存1小时
    cache.SetEX(ctx, key, []byte(userData), 3600)
    
    return userData, nil
}
```

更多示例见 [examples/](examples/) 目录。

---

## 测试

```bash
# 运行所有测试
make test

# 运行带覆盖率的测试
make test-cover

# 运行竞态检测
make test-race

# 运行特定模块测试
make test-backends
make test-observability
make test-extensions

# 运行集成测试 (需要Redis)
make test-integration

# 查看帮助
make help
```

### 集成测试

```bash
# 启动Redis服务
docker-compose -f test/docker-compose.yml up -d

# 运行集成测试
go test -tags=integration ./...

# 停止服务
docker-compose -f test/docker-compose.yml down
```

---

## 性能基准

### 基准测试命令

```bash
make bench
```

### 预期性能 (单线程)

| 后端 | Set | Get | MSet(100) | MGet(100) |
|------|-----|-----|-----------|-----------|
| Badger | 50μs | 30μs | 2ms | 1.5ms |
| BBolt | 40μs | 25μs | 1.8ms | 1.2ms |
| Pebble | 35μs | 20μs | 1.5ms | 1ms |

基准测试详情见 [benchmarks_test.go](benchmarks_test.go)。

---

## 贡献指南

欢迎提交Issue和Pull Request！

### 开发环境

```bash
# 克隆仓库
git clone https://github.com/gospacex/cachex.git
cd cachex

# 下载依赖
go mod download

# 运行测试
make test

# 格式化代码
make fmt

# 运行lint
make lint-ci
```

### 提交规范

- 使用清晰的commit message
- 确保所有测试通过
- 添加新功能的测试
- 更新相关文档

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