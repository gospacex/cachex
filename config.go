// cachex/config.go — 统一配置集成（configx + ReadAndCreate）
package cachex

import (
	"fmt"
	"os"
	"time"

	"github.com/gospacex/configx"
)

// InstanceConfig 统一实例配置（单机/集群通用）
//
// tag 说明：三套 tag 必须齐备，分别对应 configx 的三条读取通道——
//   - yaml：本地文件直读与文档示例
//   - json：远程配置中心（Nacos/Apollo）下发的 JSON
//   - mapstructure：configx 本地 viper 通道（viper.Sub(key).Unmarshal）
//
// 早期版本只写了 yaml/json，导致 configx 本地通道下多词字段（pool_size、
// min_idle_conns、read_timeout …）全部解码为零值——远程好用、本地静默失效。
// 因此这里补齐 mapstructure，字段名与 configx/configs/config.enterprise.yaml
// 的 xmiddleware.cachex.instances.<name> 段保持一致。
type InstanceConfig struct {
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
	TLSEnabled     bool          `yaml:"tls_enabled" json:"tls.enabled" mapstructure:"tls_enabled"`
	Timeout        time.Duration `yaml:"timeout" json:"timeout" mapstructure:"timeout"`
	ReadTimeout    time.Duration `yaml:"read_timeout" json:"read_timeout" mapstructure:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout" json:"write_timeout" mapstructure:"write_timeout"`
	RetryCount     int           `yaml:"retry_count" json:"retry_count" mapstructure:"retry_count"`
	// DialTimeout 建连超时（configx 企业配置的 dial_timeout，等价于 Timeout 的细分项）
	DialTimeout time.Duration `yaml:"dial_timeout" json:"dial_timeout" mapstructure:"dial_timeout"`
	// MaxRetries 最大重试次数（configx 企业配置的 max_retries，等价于 RetryCount）
	MaxRetries int `yaml:"max_retries" json:"max_retries" mapstructure:"max_retries"`
	// EnableTrace 实例级调用链开关（configx 统一键 enable_trace）。
	// 任一实例开启即启用本组件的段级调用链开关（同时受全局总闸
	// traceflag.enabled 约束）。
	EnableTrace bool `yaml:"enable_trace" json:"enable_trace" mapstructure:"enable_trace"`
}

// CacheSection 对应 configx.yaml 中的 cachex 配置节
type CacheSection struct {
	// EnableTrace 统一调用链开关（主键）。未设置时回退到 Tracing.Enabled
	//（旧键 tracing.enabled，保留为兼容别名）；两者都缺省时 tracing 不启用。
	EnableTrace bool `yaml:"enable_trace" json:"enable_trace" mapstructure:"enable_trace"`
	// Tracing 调用链细节（exporter/endpoint 等），其 enabled 字段是
	// EnableTrace 的回退别名
	Tracing   TracingConfig     `yaml:"tracing" json:"tracing" mapstructure:"tracing"`
	Instances []*InstanceConfig `yaml:"instances" json:"instances" mapstructure:"instances"`
}

// tracingEnabled 段级调用链开关判定：
// 段级 enable_trace 主键 → 实例级 enable_trace → tracing.enabled 回退别名。
func (s *CacheSection) tracingEnabled() bool {
	if s.EnableTrace {
		return true
	}
	for _, ic := range s.Instances {
		if ic.EnableTrace {
			return true
		}
	}
	return s.Tracing.Enabled
}

// applyTracing 按段级开关启用全局 tracing；开启时沿用 Tracing 段的细节
// （exporter 等），未显式配置细节时使用默认导出目标。
func (s *CacheSection) applyTracing() {
	if !s.tracingEnabled() {
		return
	}
	cfg := s.Tracing
	cfg.Enabled = true
	SetTracingConfig(cfg)
}

// sectionTracingEnabled 从原始段映射判定调用链开关：
// 段级 enable_trace 为主键，实例级 enable_trace 任一开启同样算启用，
// tracing.enabled 为回退别名（主键缺失时才看）。
// 第二个返回值表示是否有任何开关键被显式配置。
func sectionTracingEnabled(m map[string]interface{}) (enabled, explicit bool) {
	if v, ok := m["enable_trace"].(bool); ok {
		return v, true
	}
	if instancesEnabled(m["instances"]) {
		return true, true
	}
	if t, ok := m["tracing"].(map[string]interface{}); ok {
		if v, ok := t["enabled"].(bool); ok {
			return v, true
		}
	}
	return false, false
}

// instancesEnabled 段内任一实例的 enable_trace 为 true 即返回 true。
// instances 兼容两种形态：yaml/viper 的 map（name → 实例）与
// JSON 远程通道的 slice（实例列表）。
func instancesEnabled(raw interface{}) bool {
	switch insts := raw.(type) {
	case map[string]interface{}:
		for _, r := range insts {
			if m, ok := r.(map[string]interface{}); ok {
				if v, ok := m["enable_trace"].(bool); ok && v {
					return true
				}
			}
		}
	case []interface{}:
		for _, r := range insts {
			if m, ok := r.(map[string]interface{}); ok {
				if v, ok := m["enable_trace"].(bool); ok && v {
					return true
				}
			}
		}
	}
	return false
}

// ReadConfig 加载统一配置句柄（所有 x 系列 SDK 共享同一配置源）。
//
// 只解析配置、不建实例，用于参数预检或"先验证配置可用再决定是否初始化"的
// 场景；建实例用 ReadAndCreate。
func ReadConfig(opts ...configx.ConfigOptions) error {
	_, err := configx.ReadConfig(opts...)
	return err
}

// ReadAndCreate 从 configx 读取 cachex 配置并创建所有缓存实例
func ReadAndCreate(opts ...configx.ConfigOptions) (Instances, error) {
	c, err := configx.ReadConfig(opts...)
	if err != nil {
		return nil, fmt.Errorf("cachex: failed to read config: %w", err)
	}

	section := &CacheSection{}
	if err := c.Unmarshal("cachex", section); err != nil {
		return nil, fmt.Errorf("cachex: failed to unmarshal cachex section: %w", err)
	}

	if len(section.Instances) == 0 {
		return nil, fmt.Errorf("cachex: no instances configured in cachex section")
	}

	// 同步段级调用链开关（enable_trace 主键 / tracing.enabled 回退别名）
	section.applyTracing()

	cfgs := make([]*Config, 0, len(section.Instances))
	for _, ic := range section.Instances {
		if err := expandConfigPassword(ic); err != nil {
			return nil, err
		}
		cfgs = append(cfgs, instanceToConfig(ic))
	}

	return New(cfgs)
}

// MustReadAndCreate 类似 ReadAndCreate，失败时 panic
func MustReadAndCreate(opts ...configx.ConfigOptions) Instances {
	instances, err := ReadAndCreate(opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cachex: failed to initialize cache: %v\n", err)
		os.Exit(1)
	}
	return instances
}

// instanceToConfig 把统一实例配置转为运行时配置。
// configx 企业配置里 dial_timeout / max_retries 与 timeout / retry_count 语义重叠，
// 这里做一次兜底：细分项未填时回落到通用项，反之亦然。
func instanceToConfig(ic *InstanceConfig) *Config {
	if ic.Timeout <= 0 && ic.DialTimeout > 0 {
		ic.Timeout = ic.DialTimeout
	}
	if ic.RetryCount <= 0 && ic.MaxRetries > 0 {
		ic.RetryCount = ic.MaxRetries
	}
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
		EnableTrace:    ic.EnableTrace,
	}
}
