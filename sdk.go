// cachex/sdk.go — 全局单例生命周期管理
package cachex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gospacex/configx"
)

// InitOption 初始化选项
type InitOption func(*initConfig)

type initConfig struct {
	configFile string
	provider   configx.Config
	section    string
	tracing    TracingConfig
	tracingSet bool // 是否显式通过 WithTracing 指定
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

// WithTracing 指定全局调用链开关。
// 只有显式调用本选项时才会覆盖配置文件中的 tracing 配置。
func WithTracing(enabled bool) InitOption {
	return func(c *initConfig) {
		c.tracing = TracingConfig{Enabled: enabled}
		c.tracingSet = true
	}
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
	// globalSDK 用原子指针读写，避免 Init 与 Instance 并发时的数据竞争
	globalSDK atomic.Pointer[SDK]
	initOnce  sync.Once
	initMu    sync.Mutex
	initErr   error // protected by initMu
)

// Init 从 configx 读取配置并创建所有实例。
//
// 幂等：多次调用只有第一次真正初始化，后续调用返回首次的结果。
// 传入 WithConfigFile 或 WithConfig 之一指明配置来源，否则返回错误。
func Init(opts ...InitOption) error {
	cfg := applyInitOptions(opts)

	// 仅当显式指定时才覆盖配置源中的 tracing 配置，
	// 避免无参调用把配置文件里 tracing.enabled=true 静默改成关闭。
	if cfg.tracingSet {
		SetTracingConfig(cfg.tracing)
	}

	initOnce.Do(func() {
		sdk := &SDK{instances: make(map[string]Cache)}
		sdk.watchCtx, sdk.watchCancel = context.WithCancel(context.Background())

		var instances Instances
		var err error
		switch {
		case cfg.provider != nil:
			instances, err = readFromProvider(cfg.provider, cfg.section)
		case cfg.configFile != "":
			instances, err = ReadAndCreate(configx.ConfigOptions{ConfigFile: cfg.configFile})
		default:
			err = errors.New("cachex: Init requires WithConfigFile or WithConfig")
		}

		if err != nil {
			sdk.watchCancel()
			initMu.Lock()
			initErr = err
			initMu.Unlock()
			return
		}

		for name, inst := range instances {
			sdk.instances[name] = inst
		}
		globalSDK.Store(sdk)

		// 注册 Watch 回调监听 tracing 开关变更
		if cfg.provider != nil {
			go watchTracingConfig(sdk.watchCtx, cfg.provider, cfg.section)
		}
	})

	initMu.Lock()
	defer initMu.Unlock()
	return initErr
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

	// 设置段级调用链开关（enable_trace 主键 / tracing.enabled 回退别名）
	sectionData.applyTracing()

	cfgs := make([]*Config, 0, len(sectionData.Instances))
	for _, ic := range sectionData.Instances {
		if err := expandConfigPassword(ic); err != nil {
			return nil, err
		}
		cfgs = append(cfgs, instanceToConfig(ic))
	}

	return New(cfgs)
}

// watchTracingConfig 监听段级调用链开关键（enable_trace 主键 /
// tracing.enabled 回退别名）的热更新。
func watchTracingConfig(ctx context.Context, provider configx.Config, section string) {
	updateTracingFromConfig(provider, section)

	// 主键变更时同步热更新
	_ = provider.Watch(ctx, section+".enable_trace", func(newVal interface{}) {
		if b, ok := newVal.(bool); ok {
			applyWatchedTracingChange(provider, section, b)
		}
	})
	// 别名变更时同样热更新（主键未显式配置才生效，见 applyWatchedTracingChange）
	_ = provider.Watch(ctx, section+".tracing.enabled", func(newVal interface{}) {
		if b, ok := newVal.(bool); ok {
			applyWatchedTracingChange(provider, section, b)
		}
	})
}

// applyWatchedTracingChange 把开关键的热更新落回全局 tracing 配置。
// 主键 enable_trace 显式存在时以它为准，未配置才沿用别名的新值。
func applyWatchedTracingChange(provider configx.Config, section string, aliasValue bool) {
	raw := provider.Get(section)
	var m map[string]interface{}
	if r, ok := raw.(map[string]interface{}); ok {
		m = r
	}
	primary, explicit := sectionTracingEnabled(m)
	enabled := aliasValue
	if explicit {
		enabled = primary
	}
	SetTracingConfig(TracingConfig{Enabled: enabled})
}

// updateTracingFromConfig 读取段映射并按统一规则同步调用链开关。
func updateTracingFromConfig(provider configx.Config, section string) {
	raw := provider.Get(section)
	if raw == nil {
		return
	}
	m, ok := raw.(map[string]interface{})
	if !ok {
		return
	}
	enabled, explicit := sectionTracingEnabled(m)
	if !explicit {
		return
	}
	SetTracingConfig(TracingConfig{Enabled: enabled})
}

// Instance 获取命名实例。
//
// 必须在 Init 成功之后调用；未初始化、已关闭或实例不存在时返回 nil。
// 调用方应对返回值做判空，不要假设实例一定存在。
func Instance(name string) Cache {
	sdk := globalSDK.Load()
	if sdk == nil {
		return nil
	}
	sdk.mu.RLock()
	defer sdk.mu.RUnlock()
	if sdk.closed {
		return nil
	}
	return sdk.instances[name]
}

// CloseAll 优雅关闭所有实例并冲刷调用链缓冲区。
//
// 幂等：重复调用返回 nil。所有实例的关闭错误会被聚合返回（errors.Join），
// 不会因中途某个实例失败而中断其余实例的关闭。
func CloseAll() error {
	sdk := globalSDK.Load()
	if sdk == nil {
		return nil
	}

	sdk.mu.Lock()
	if sdk.closed {
		sdk.mu.Unlock()
		return nil
	}
	sdk.closed = true
	instances := sdk.instances
	sdk.instances = make(map[string]Cache)
	sdk.mu.Unlock()

	if sdk.watchCancel != nil {
		sdk.watchCancel()
	}

	var errs []error
	for name, inst := range instances {
		if err := inst.Close(); err != nil {
			errs = append(errs, fmt.Errorf("cachex: close instance %q: %w", name, err))
		}
	}
	// 先关闭实例产生最后一批 span，再冲刷 tracer，避免尾部调用链丢失
	shutdownTracing()

	return errors.Join(errs...)
}

// IsClosed 返回全局单例是否已关闭
func IsClosed() bool {
	sdk := globalSDK.Load()
	if sdk == nil {
		return false
	}
	sdk.mu.RLock()
	defer sdk.mu.RUnlock()
	return sdk.closed
}

// expandConfigPassword 展开密码中的环境变量占位符 ${VAR_NAME}
func expandConfigPassword(cfg *InstanceConfig) error {
	if cfg.Password == "" {
		return nil
	}
	s := cfg.Password
	var result strings.Builder
	var hasUnresolved bool
	for {
		start := strings.Index(s, "${")
		if start < 0 {
			result.WriteString(s)
			break
		}
		result.WriteString(s[:start])
		end := strings.Index(s[start:], "}")
		if end < 0 {
			result.WriteString(s)
			break
		}
		end += start
		varName := s[start+2 : end]
		val, ok := os.LookupEnv(varName)
		if !ok {
			hasUnresolved = true
			result.WriteString(s[:end+1])
		} else {
			result.WriteString(val)
		}
		s = s[end+1:]
	}
	if hasUnresolved {
		return fmt.Errorf("cachex: unresolved env var in instance %q password", cfg.Name)
	}
	cfg.Password = result.String()
	return nil
}
