// cachex/memcached - Memcached 驱动
// 使用 github.com/bradfitz/gomemcache/memcache，含 OTel 调用链追踪、熔断器、Prometheus 指标
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
	"go.opentelemetry.io/otel/trace/noop"
)

const driverName = "memcached"

// Client Memcached 缓存客户端
type Client struct {
	mc     *memcache.Client
	cfg    *cachex.Config
	tracer trace.Tracer
	logger logx.Logger
	cb     *cachex.CircuitBreaker
}

// New 创建 Memcached 客户端
func New(cfg *cachex.Config) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cachex/memcached: config is nil")
	}
	cfg.FillDefaults()

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	addrs := splitAddrs(cfg.Addr)
	if len(addrs) == 0 {
		return nil, fmt.Errorf("cachex/memcached: no server address configured")
	}
	mc := memcache.New(addrs...)
	mc.Timeout = timeout
	mc.MaxIdleConns = cfg.PoolSize

	if _, err := mc.Get("_health_check_"); err != nil && err != memcache.ErrCacheMiss {
		return nil, fmt.Errorf("cachex/memcached: connect failed: %w", err)
	}

	logx.GetGlobal().InfoContext(context.Background(), "memcached driver initialized",
		logx.String("driver", driverName),
		logx.String("addrs", strings.Join(addrs, ",")),
		logx.String("name", cfg.Name),
	)

	return &Client{
		mc:     mc,
		cfg:    cfg,
		tracer: otel.Tracer("github.com/gospacex/cachex/memcached"),
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
	c.report("GET", start, err)

	if err != nil {
		return "", err
	}
	span.SetAttributes(attribute.Bool("db.memcached.found", val != ""))
	return val, nil
}

// Set 写入缓存。
// ttl <= 0 表示永不过期（Memcached expiration=0）；
// ttl > 0 按秒下发，不足 1 秒的余量向上取整到 1 秒，避免出现"写入即失效"。
func (c *Client) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	ctx, span := c.startSpan(ctx, "SET", key)
	defer span.End()

	var expiration int32
	if ttl > 0 {
		if sec := int32(ttl.Seconds()); sec > 0 {
			expiration = sec
		} else {
			expiration = 1
		}
	}
	start := time.Now()
	err := c.cb.Execute(ctx, func() error {
		item := &memcache.Item{
			Key:        key,
			Value:      []byte(value),
			Expiration: expiration,
		}
		if setErr := c.mc.Set(item); setErr != nil {
			span.RecordError(setErr)
			c.logger.ErrorContext(ctx, "memcached set failed", logx.Err(setErr), logx.String("key", key))
			return fmt.Errorf("cachex/memcached Set(%q): %w", key, setErr)
		}
		return nil
	})
	c.report("SET", start, err)
	return err
}

// Delete 删除缓存。key 不存在时返回 nil（幂等）。
func (c *Client) Delete(ctx context.Context, key string) error {
	ctx, span := c.startSpan(ctx, "DELETE", key)
	defer span.End()

	start := time.Now()
	err := c.cb.Execute(ctx, func() error {
		if delErr := c.mc.Delete(key); delErr != nil && delErr != memcache.ErrCacheMiss {
			span.RecordError(delErr)
			c.logger.ErrorContext(ctx, "memcached delete failed", logx.Err(delErr), logx.String("key", key))
			return fmt.Errorf("cachex/memcached Delete(%q): %w", key, delErr)
		}
		return nil
	})
	c.report("DELETE", start, err)
	return err
}

// Exists 判断 key 是否存在
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	ctx, span := c.startSpan(ctx, "EXISTS", key)
	defer span.End()

	var found bool
	start := time.Now()
	err := c.cb.Execute(ctx, func() error {
		_, getErr := c.mc.Get(key)
		if getErr != nil {
			if getErr == memcache.ErrCacheMiss {
				found = false
				return nil
			}
			span.RecordError(getErr)
			c.logger.ErrorContext(ctx, "memcached exists failed", logx.Err(getErr), logx.String("key", key))
			return fmt.Errorf("cachex/memcached Exists(%q): %w", key, getErr)
		}
		found = true
		return nil
	})
	c.report("EXISTS", start, err)

	if err != nil {
		return false, err
	}
	return found, nil
}

// Keys 不支持：Memcached 协议没有 key 枚举原语
func (c *Client) Keys(ctx context.Context, pattern string) ([]string, error) {
	return nil, fmt.Errorf("cachex/memcached: Keys is not supported by memcached protocol")
}

// Close 关闭底层客户端。gomemcache 无显式 Close，连接按需由 GC 回收。
func (c *Client) Close() error {
	return nil
}

// HealthCheck 探活
func (c *Client) HealthCheck(ctx context.Context) error {
	ctx, span := c.startSpan(ctx, "PING", "")
	defer span.End()

	start := time.Now()
	err := c.cb.Execute(ctx, func() error {
		if _, getErr := c.mc.Get("_health_check_"); getErr != nil && getErr != memcache.ErrCacheMiss {
			span.RecordError(getErr)
			c.logger.ErrorContext(ctx, "memcached health check failed", logx.Err(getErr))
			return fmt.Errorf("cachex/memcached health check failed: %w", getErr)
		}
		return nil
	})
	c.report("PING", start, err)
	return err
}

// Raw 返回底层 gomemcache 句柄，供需要 incr/decr/cas/getMulti 等
// 超出标准 Cache 接口的场景使用。注意：通过此句柄的命令不经过
// 熔断器和 OTel span。
func (c *Client) Raw() *memcache.Client {
	return c.mc
}

// ===== 可观测性与工具 =====

// report 统一上报一次命令的可观测数据。
// Memcached 无连接池统计原语，因此只上报命令计数器与熔断器状态。
func (c *Client) report(command string, start time.Time, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}
	cachex.RecordCommand(driverName, c.cfg.Name, command, status, time.Since(start))
	cachex.UpdateCircuitBreakerState(driverName, c.cfg.Name, c.cb.State())
}

// startSpan 开启命令 span。tracing 关闭时返回 noop span，
// 避免 defer span.End() 误结束调用方注入到 ctx 中的 span。
func (c *Client) startSpan(ctx context.Context, command string, key string) (context.Context, trace.Span) {
	if !cachex.IsTracingEnabled() {
		return ctx, noop.Span{}
	}
	ctx, span := c.tracer.Start(ctx, "db.memcached."+command)
	span.SetAttributes(
		attribute.String("db.system", driverName),
		attribute.String("db.memcached.command", command),
	)
	if key != "" {
		span.SetAttributes(attribute.String("db.memcached.key", key))
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

var _ cachex.Cache = (*Client)(nil)
