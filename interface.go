// cachex - 多后端缓存 SDK
// 提供统一的 Cache 接口，支持 Redis、Memcached 驱动
package cachex

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ===== 公共接口 =====

// Cache 缓存客户端接口
type Cache interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	Keys(ctx context.Context, pattern string) ([]string, error)
	Close() error
	HealthCheck(ctx context.Context) error
}

// Instances 命名实例注册表
type Instances map[string]Cache

// ===== 配置 =====

// Config 缓存实例配置。
// tag 与 InstanceConfig 保持三通道一致（yaml 本地文件 / json 远程配置中心 / mapstructure viper）。
type Config struct {
	Name           string        `yaml:"name" json:"name" mapstructure:"name"`
	Driver         string        `yaml:"driver" json:"driver" mapstructure:"driver"`
	Addr           string        `yaml:"addr" json:"addr" mapstructure:"addr"`
	Password       string        `yaml:"password" json:"password" mapstructure:"password"`
	DB             int           `yaml:"db" json:"db" mapstructure:"db"`
	PoolSize       int           `yaml:"pool_size" json:"pool_size" mapstructure:"pool_size"`
	MinIdleConn    int           `yaml:"min_idle_conn" json:"min_idle_conn" mapstructure:"min_idle_conns"`
	SentinelMaster string        `yaml:"sentinel_master" json:"sentinel_master" mapstructure:"sentinel_master"`
	SentinelAddrs  string        `yaml:"sentinel_addrs" json:"sentinel_addrs" mapstructure:"sentinel_addrs"`
	Cluster        string        `yaml:"cluster" json:"cluster" mapstructure:"cluster"`
	TLSEnabled     bool          `yaml:"tls_enabled" json:"tls_enabled" mapstructure:"tls_enabled"`
	Timeout        time.Duration `yaml:"timeout" json:"timeout" mapstructure:"timeout"`
	ReadTimeout    time.Duration `yaml:"read_timeout" json:"read_timeout" mapstructure:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout" json:"write_timeout" mapstructure:"write_timeout"`
	RetryCount     int           `yaml:"retry_count" json:"retry_count" mapstructure:"retry_count"`
	// EnableTrace 实例级调用链开关（configx 统一键 enable_trace）。
	// 段内任一实例开启即启用本组件的调用链（同时受全局总闸
	// traceflag.enabled 约束，见 IsTracingEnabled）。
	EnableTrace bool `yaml:"enable_trace" json:"enable_trace" mapstructure:"enable_trace"`
}

// FillDefaults 填充零值默认字段
func (c *Config) FillDefaults() {
	if c.Timeout <= 0 {
		c.Timeout = 5 * time.Second
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = c.Timeout
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = c.Timeout
	}
	if c.PoolSize <= 0 {
		c.PoolSize = 10
	}
	if c.MinIdleConn < 0 {
		c.MinIdleConn = 0
	}
	if c.DB < 0 {
		c.DB = 0
	}
	if c.RetryCount <= 0 {
		c.RetryCount = 3
	}
}

// ===== 工厂函数 =====

// New 根据配置创建命名实例映射
func New(cfgs []*Config) (Instances, error) {
	if len(cfgs) == 0 {
		return nil, fmt.Errorf("cachex: no instances configured")
	}

	instances := make(Instances, len(cfgs))
	for _, cfg := range cfgs {
		cfg.FillDefaults()
		inst, err := newInstance(cfg)
		if err != nil {
			return nil, fmt.Errorf("cachex: create instance %q: %w", cfg.Name, err)
		}
		instances[cfg.Name] = inst
	}
	return instances, nil
}

// ===== 驱动注册表 =====
// 核心包不直接 import 任何驱动；各驱动在自己的包内通过 init() 调用
// RegisterCreator 注册（使用方 blank import 对应驱动包即可启用）。

var (
	registryMu sync.RWMutex
	creators   = map[string]func(*Config) (Cache, error){}
)

// RegisterCreator 注册缓存驱动创建函数
func RegisterCreator(name string, creator func(*Config) (Cache, error)) {
	registryMu.Lock()
	defer registryMu.Unlock()
	creators[name] = creator
}

// newInstance 按注册表创建驱动实例
func newInstance(cfg *Config) (Cache, error) {
	registryMu.RLock()
	creator, ok := creators[cfg.Driver]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("cachex: unknown driver %q (registered: import the driver package, e.g. _ \"github.com/gospacex/cachex/redis\")", cfg.Driver)
	}
	return creator(cfg)
}
