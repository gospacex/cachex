// cachex/redis - Redis 驱动
// 支持单机、Sentinel、Cluster 三种模式，含 OTel 调用链追踪、熔断器、Prometheus 指标
package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gospacex/cachex"
	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap"
	"github.com/gospacex/traceflag"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const driverName = "redis"

// Client Redis 缓存客户端
type Client struct {
	rdb    redis.UniversalClient
	cfg    *cachex.Config
	tracer trace.Tracer
	logger logx.Logger
	cb     *cachex.CircuitBreaker
}

// New 创建 Redis 客户端。
// 建连阶段按指数退避重试 RetryCount 次，全部失败才返回错误——避免瞬时抖动导致启动崩溃。
func New(cfg *cachex.Config) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cachex/redis: config is nil")
	}
	cfg.FillDefaults()

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
			SentinelAddrs: splitAddrs(cfg.SentinelAddrs),
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
			Addrs:        splitAddrs(cfg.Cluster),
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
		// 建连失败时必须释放底层连接池，否则泄漏文件描述符
		_ = rdb.Close()
		return nil, fmt.Errorf("cachex/redis: ping failed: %w", err)
	}

	logx.GetGlobal().InfoContext(context.Background(), "redis driver initialized",
		logx.String("driver", driverName),
		logx.String("addr", cfg.Addr),
		logx.String("name", cfg.Name),
	)

	return &Client{
		rdb:    rdb,
		cfg:    cfg,
		tracer: otel.Tracer("github.com/gospacex/cachex/redis"),
		logger: logx.GetGlobal(),
		cb:     cachex.NewCircuitBreaker(cfg.Name),
	}, nil
}

// ===== Cache 接口实现 =====

// Get 读取缓存。key 不存在时返回空字符串且 err 为 nil（缓存未命中不是错误）。
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	ctx, span := c.startSpan(ctx, "GET", key)
	defer span.End()

	var val string
	start := time.Now()
	err := c.cb.Execute(ctx, func() error {
		result, rerr := c.rdb.Get(ctx, key).Result()
		if rerr != nil {
			if rerr == redis.Nil {
				return nil
			}
			span.RecordError(rerr)
			c.logger.ErrorContext(ctx, "redis get failed", logx.Err(rerr), logx.String("key", key))
			return fmt.Errorf("cachex/redis Get(%q): %w", key, rerr)
		}
		val = result
		return nil
	})
	c.report("GET", start, err)

	if err != nil {
		return "", err
	}
	span.SetAttributes(attribute.Bool("db.redis.found", val != ""))
	return val, nil
}

// Set 写入缓存。ttl <= 0 表示不过期。
func (c *Client) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	ctx, span := c.startSpan(ctx, "SET", key)
	defer span.End()

	if ttl < 0 {
		ttl = 0
	}
	start := time.Now()
	err := c.cb.Execute(ctx, func() error {
		if err := c.rdb.Set(ctx, key, value, ttl).Err(); err != nil {
			span.RecordError(err)
			c.logger.ErrorContext(ctx, "redis set failed", logx.Err(err), logx.String("key", key))
			return fmt.Errorf("cachex/redis Set(%q): %w", key, err)
		}
		return nil
	})
	c.report("SET", start, err)

	if err != nil {
		return err
	}
	span.SetAttributes(attribute.Int64("db.redis.ttl_ms", ttl.Milliseconds()))
	return nil
}

// Delete 删除缓存。key 不存在时同样返回 nil（幂等）。
func (c *Client) Delete(ctx context.Context, key string) error {
	ctx, span := c.startSpan(ctx, "DEL", key)
	defer span.End()

	start := time.Now()
	err := c.cb.Execute(ctx, func() error {
		if err := c.rdb.Del(ctx, key).Err(); err != nil {
			span.RecordError(err)
			c.logger.ErrorContext(ctx, "redis delete failed", logx.Err(err), logx.String("key", key))
			return fmt.Errorf("cachex/redis Delete(%q): %w", key, err)
		}
		return nil
	})
	c.report("DEL", start, err)
	return err
}

// Exists 判断 key 是否存在
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	ctx, span := c.startSpan(ctx, "EXISTS", key)
	defer span.End()

	var count int64
	start := time.Now()
	err := c.cb.Execute(ctx, func() error {
		result, rerr := c.rdb.Exists(ctx, key).Result()
		if rerr != nil {
			span.RecordError(rerr)
			c.logger.ErrorContext(ctx, "redis exists failed", logx.Err(rerr), logx.String("key", key))
			return fmt.Errorf("cachex/redis Exists(%q): %w", key, rerr)
		}
		count = result
		return nil
	})
	c.report("EXISTS", start, err)

	if err != nil {
		return false, err
	}
	span.SetAttributes(attribute.Int64("db.redis.keys_count", count))
	return count > 0, nil
}

// Keys 按 pattern 枚举 key。
// 警告：生产环境请用 SCAN 替代——KEYS 会阻塞 Redis 单线程，大 key 空间下等同于故障。
func (c *Client) Keys(ctx context.Context, pattern string) ([]string, error) {
	ctx, span := c.startSpan(ctx, "KEYS", pattern)
	defer span.End()

	var keys []string
	start := time.Now()
	err := c.cb.Execute(ctx, func() error {
		result, rerr := c.rdb.Keys(ctx, pattern).Result()
		if rerr != nil {
			span.RecordError(rerr)
			c.logger.ErrorContext(ctx, "redis keys failed", logx.Err(rerr), logx.String("pattern", pattern))
			return fmt.Errorf("cachex/redis Keys(%q): %w", pattern, rerr)
		}
		keys = result
		return nil
	})
	c.report("KEYS", start, err)

	if err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.Int("db.redis.keys_count", len(keys)))
	return keys, nil
}

// Close 关闭底层连接池
func (c *Client) Close() error {
	return c.rdb.Close()
}

// HealthCheck 探活
func (c *Client) HealthCheck(ctx context.Context) error {
	ctx, span := c.startSpan(ctx, "PING", "")
	defer span.End()

	start := time.Now()
	err := c.cb.Execute(ctx, func() error {
		if err := c.rdb.Ping(ctx).Err(); err != nil {
			span.RecordError(err)
			c.logger.ErrorContext(ctx, "redis health check failed", logx.Err(err))
			return fmt.Errorf("cachex/redis health check failed: %w", err)
		}
		return nil
	})
	c.report("PING", start, err)
	return err
}

// Raw 返回底层 go-redis/v8 句柄，供需要高级命令的场景使用。
// 注意：通过此句柄执行的命令不会经过熔断器、追踪和 Prometheus 指标。
func (c *Client) Raw() redis.UniversalClient {
	return c.rdb
}

// ===== 可观测性与工具 =====

// report 统一上报一次命令的可观测数据：Prometheus 计数器/直方图、熔断器状态、连接池水位。
// 所有 Cache 方法共用，避免每个方法重复三段埋点样板。
func (c *Client) report(command string, start time.Time, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}
	cachex.RecordCommand(driverName, c.cfg.Name, command, status, time.Since(start))
	cachex.UpdateCircuitBreakerState(driverName, c.cfg.Name, c.cb.State())

	// 连接池水位取 go-redis 的实时统计，而非配置推算值：
	// 用 poolSize-minIdleConn 伪造指标会让监控面板永远显示"健康"。
	if st := c.rdb.PoolStats(); st != nil {
		idle := int(st.IdleConns)
		active := int(st.TotalConns) - idle
		if active < 0 {
			active = 0
		}
		cachex.UpdatePoolMetrics(driverName, c.cfg.Name, active, idle)
	}
}

// traceEnabled 判本实例是否入链。
//
// 为什么实例级开关必须参与裁决：cachex.IsTracingEnabled() 读的是**全局**
// tracingCfg（只有 SetTracingConfig 能写）。configx / 独立用法下只配
// instances.<name>.enable_trace 而不调 SetTracingConfig 的话，开关虽然被
// 解码进 Config.EnableTrace，却没有任何地方读它 —— span 永远为零且不打日志，
// 表现为「开了埋点但 Jaeger 上一个 redis span 都没有」。
//
// 裁决顺序（与其余 x 系列 SDK 对齐）：
//
//	traceflag 全局总闸 AND traceflag 组件闸("cachex")
//	AND (实例级 EnableTrace OR 全局 SetTracingConfig 打开)
//
// 最后一层的 OR 是兼容：既保留兜底路径，又让实例级开关真正能单独生效。
func (c *Client) traceEnabled() bool {
	if !traceflag.EnabledFor("cachex") {
		return false
	}
	if c.cfg != nil && c.cfg.EnableTrace {
		return true
	}
	return cachex.IsTracingEnabled()
}

// startSpan 开启命令 span。
//
// tracing 关闭时返回 noop span，绝不返回 ctx 中的外部 span——
// 否则 defer span.End() 会提前结束调用方的 span，破坏上层链路。
func (c *Client) startSpan(ctx context.Context, command string, key string) (context.Context, trace.Span) {
	if !c.traceEnabled() {
		return ctx, noop.Span{}
	}
	ctx, span := c.tracer.Start(ctx, "db.redis."+command)
	span.SetAttributes(
		attribute.String("db.system", driverName),
		attribute.String("db.redis.command", command),
	)
	if key != "" {
		span.SetAttributes(attribute.String("db.redis.key", key))
	}
	return ctx, span
}

// splitAddrs 拆分逗号分隔的地址列表，容忍空格与尾随逗号
func splitAddrs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func tlsConfig(enabled bool) *tls.Config {
	if !enabled {
		return nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

var _ cachex.Cache = (*Client)(nil)

// withRetry 以指数退避执行 fn，最多 maxRetries 次
func withRetry(fn func() error, maxRetries int) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(time.Duration(100<<uint(i-1)) * time.Millisecond)
		}
		if lastErr = fn(); lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("failed to connect after %d retries: %w", maxRetries, lastErr)
}
