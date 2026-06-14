package cachex

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds the configuration for cache clients.
// All fields have sensible defaults and are optional unless specified.
type Config struct {
	// Backend is the type of cache backend (redis, badger, etc.)
	Backend string `mapstructure:"backend" json:"backend" yaml:"backend"`

	// Driver specifies the concrete driver for Redis-protocol backends.
	// When Backend is "redis" and Driver is "dragonfly", the unified redis
	// package will label the connection accordingly while using the same
	// go-redis/v9 client. Valid values: redis, dragonfly, keydb, garnet.
	Driver string `mapstructure:"driver" json:"driver" yaml:"driver"`

	// Addrs is the list of addresses for network backends.
	// For single instance, use one address.
	// For cluster, use multiple addresses.
	Addrs []string `mapstructure:"addrs" json:"addrs" yaml:"addrs"`

	// Password is the authentication password.
	Password string `mapstructure:"password" json:"-" yaml:"password"`

	// DB is the database number (for Redis-like backends).
	DB int `mapstructure:"db" json:"db" yaml:"db"`

	// PoolSize is the maximum number of connections in the pool.
	PoolSize int `mapstructure:"pool_size" json:"poolSize" yaml:"pool_size"`

	// MinIdleConns is the minimum number of idle connections.
	MinIdleConns int `mapstructure:"min_idle_conns" json:"minIdleConns" yaml:"min_idle_conns"`

	// MaxRetries is the maximum number of retries for failed operations.
	MaxRetries int `mapstructure:"max_retries" json:"maxRetries" yaml:"max_retries"`

	// DialTimeout is the connection dial timeout in seconds.
	DialTimeout int `mapstructure:"dial_timeout" json:"dialTimeout" yaml:"dial_timeout"`

	// ReadTimeout is the read timeout in seconds.
	ReadTimeout int `mapstructure:"read_timeout" json:"readTimeout" yaml:"read_timeout"`

	// WriteTimeout is the write timeout in seconds.
	WriteTimeout int `mapstructure:"write_timeout" json:"writeTimeout" yaml:"write_timeout"`

	// IdleTimeout is the idle connection timeout in seconds.
	IdleTimeout int `mapstructure:"idle_timeout" json:"idleTimeout" yaml:"idle_timeout"`

	// PoolTimeout is the maximum time to wait for a connection from the pool.
	PoolTimeout int `mapstructure:"pool_timeout" json:"poolTimeout" yaml:"pool_timeout"`

	// TLS holds TLS configuration.
	TLS TLSConfig `mapstructure:"tls" json:"tls" yaml:"tls"`

	// MasterName is the master name for Sentinel mode (Redis only).
	MasterName string `mapstructure:"master_name" json:"masterName" yaml:"master_name"`

	// SentinelPassword is the password for Sentinel authentication.
	SentinelPassword string `mapstructure:"sentinel_password" json:"-" yaml:"sentinel_password"`

	// ClusterMode enables cluster mode when multiple addresses are provided.
	ClusterMode bool `mapstructure:"cluster_mode" json:"clusterMode" yaml:"cluster_mode"`

	// Options holds backend-specific options.
	Options map[string]interface{} `mapstructure:"options" json:"options,omitempty" yaml:"options"`

	// For embedded KV stores

	// Dir is the path to the data directory.
	Dir string `mapstructure:"dir" json:"dir" yaml:"dir"`

	// ValueDir is the path to the value directory (Badger).
	ValueDir string `mapstructure:"value_dir" json:"valueDir" yaml:"value_dir"`

	// BucketName is the bucket name for BBolt.
	BucketName string `mapstructure:"bucket_name" json:"bucketName" yaml:"bucket_name"`

	// FileMode is the file permission for database files.
	FileMode int `mapstructure:"file_mode" json:"fileMode" yaml:"file_mode"`

	// MmapSize is the memory-map size for BBolt/Pebble.
	MmapSize int64 `mapstructure:"mmap_size" json:"mmapSize" yaml:"mmap_size"`

	// ReadOnly opens the database in read-only mode.
	ReadOnly bool `mapstructure:"read_only" json:"readOnly" yaml:"read_only"`

	// SyncWrites enables synchronous writes.
	SyncWrites bool `mapstructure:"sync_writes" json:"syncWrites" yaml:"sync_writes"`

	// InMemory creates an in-memory database.
	InMemory bool `mapstructure:"in_memory" json:"inMemory" yaml:"in_memory"`

	// BlockCacheSize is the block cache size for Badger/Pebble.
	BlockCacheSize int64 `mapstructure:"block_cache_size" json:"blockCacheSize" yaml:"block_cache_size"`

	// IndexCacheSize is the index cache size for Badger.
	IndexCacheSize int64 `mapstructure:"index_cache_size" json:"indexCacheSize" yaml:"index_cache_size"`

	// MemTableSize is the memtable size.
	MemTableSize int64 `mapstructure:"mem_table_size" json:"memTableSize" yaml:"mem_table_size"`

	// Compression enables compression for embedded stores.
	Compression bool `mapstructure:"compression" json:"compression" yaml:"compression"`

	// ValueLogFileSize is the value log file size for Badger.
	ValueLogFileSize int64 `mapstructure:"value_log_file_size" json:"valueLogFileSize" yaml:"value_log_file_size"`

	// ValueThreshold is the threshold for values to be stored in value log.
	ValueThreshold int64 `mapstructure:"value_threshold" json:"valueThreshold" yaml:"value_threshold"`

	// BypassLockGuard disables lock guard for Badger.
	BypassLockGuard bool `mapstructure:"bypass_lock_guard" json:"bypassLockGuard" yaml:"bypass_lock_guard"`

	// CircuitBreaker configures circuit breaker settings.
	CircuitBreaker *CircuitBreakerConfig `mapstructure:"circuit_breaker" json:"circuitBreaker,omitempty" yaml:"circuit_breaker"`

	// Metrics enables Prometheus metrics collection.
	Metrics bool `mapstructure:"metrics" json:"metrics" yaml:"metrics"`

	// MetricsPrefix is the prefix for Prometheus metric names.
	MetricsPrefix string `mapstructure:"metrics_prefix" json:"metricsPrefix" yaml:"metrics_prefix"`

	// Logger configures logging.
	Logger *LoggerConfig `mapstructure:"logger" json:"logger,omitempty" yaml:"logger"`

	// Tracing configures OpenTelemetry tracing (legacy block).
	// Use Trace for new code; the legacy block is auto-promoted by LoadConfig.
	Tracing *TracingConfig `mapstructure:"tracing" json:"tracing,omitempty" yaml:"tracing"`

	// Trace configures trace export (new schema aligned with mqx).
	// When both Trace and Tracing are present, Trace wins.
	Trace *TraceConfig `mapstructure:"trace" json:"trace,omitempty" yaml:"trace"`
}

// TraceConfig holds the new-schema trace configuration aligned with mqx.
// It supersedes the legacy TracingConfig; LoadConfig auto-promotes
// `tracing:` blocks to this schema when `trace:` is absent.
type TraceConfig struct {
	// Enabled turns on trace export.
	Enabled bool `mapstructure:"enabled" json:"enabled" yaml:"enabled"`

	// ServiceName is the resource.service.name attribute.
	ServiceName string `mapstructure:"service_name" json:"serviceName" yaml:"service_name"`

	// Exporter selects the long-name backend: jaeger | otlp | redis_stream | kafka_topic.
	// Short names "redis" → "redis_stream" and "kafka" → "kafka_topic" are
	// accepted and normalised by Validate.
	Exporter string `mapstructure:"exporter" json:"exporter" yaml:"exporter"`

	// Endpoint is the OTLP/Jaeger collector endpoint.
	Endpoint string `mapstructure:"endpoint" json:"endpoint" yaml:"endpoint"`

	// Protocol is "grpc" or "http" (OTLP only).
	Protocol string `mapstructure:"protocol" json:"protocol" yaml:"protocol"`

	// SamplerType is one of: always_on, always_off, traceidratio,
	// parentbased_always_on, parentbased_always_off, parentbased_traceidratio.
	SamplerType string `mapstructure:"sampler_type" json:"samplerType" yaml:"sampler_type"`

	// SamplerRatio is the ratio for ratio-based samplers (0.0–1.0).
	SamplerRatio float64 `mapstructure:"sampler_ratio" json:"samplerRatio" yaml:"sampler_ratio"`

	// Insecure disables TLS for the collector endpoint.
	Insecure bool `mapstructure:"insecure" json:"insecure" yaml:"insecure"`

	// Headers are extra OTLP/HTTP headers (e.g. "api-key: …").
	Headers map[string]string `mapstructure:"headers" json:"headers,omitempty" yaml:"headers"`

	// Stream is the Redis stream / channel name (exporter=redis_stream only).
	Stream string `mapstructure:"stream" json:"stream" yaml:"stream"`

	// Topic is the Kafka topic name (exporter=kafka_topic only).
	Topic string `mapstructure:"topic" json:"topic" yaml:"topic"`

	// Brokers is the list of Kafka bootstrap servers (exporter=kafka_topic only).
	Brokers []string `mapstructure:"brokers" json:"brokers" yaml:"brokers"`
}

// longNameBackends is the canonical set of trace.exporter values accepted
// in the new schema. The legacy short names "redis" and "kafka" are also
// accepted and normalised by TraceConfig.Validate.
var longNameBackends = map[string]bool{
	"jaeger":       true,
	"otlp":         true,
	"redis_stream": true,
	"kafka_topic":  true,
}

// normaliseExporter maps legacy short names to the long form, returning
// the normalised value and whether the input was recognised.
func normaliseExporter(name string) (string, bool) {
	switch name {
	case "jaeger", "otlp", "redis_stream", "kafka_topic":
		return name, true
	case "redis":
		return "redis_stream", true
	case "kafka":
		return "kafka_topic", true
	default:
		return "", false
	}
}

// Validate normalises Exporter and rejects unknown long names.
// Returns the normalised exporter string on success.
func (t *TraceConfig) Validate() (string, error) {
	if t == nil {
		return "", nil
	}
	if t.Exporter == "" {
		return "", nil
	}
	normalised, ok := normaliseExporter(t.Exporter)
	if !ok {
		return "", fmt.Errorf("unknown trace.exporter: %q (valid: jaeger, otlp, redis_stream, kafka_topic; short names redis, kafka are also accepted)", t.Exporter)
	}
	t.Exporter = normalised
	return normalised, nil
}

// TLSConfig holds TLS configuration.
type TLSConfig struct {
	// Enabled enables TLS.
	Enabled bool `mapstructure:"enabled" json:"enabled" yaml:"enabled"`

	// CAFile is the path to the CA certificate file.
	CAFile string `mapstructure:"ca_file" json:"caFile" yaml:"ca_file"`

	// CertFile is the path to the client certificate file.
	CertFile string `mapstructure:"cert_file" json:"certFile" yaml:"cert_file"`

	// KeyFile is the path to the client key file.
	KeyFile string `mapstructure:"key_file" json:"keyFile" yaml:"key_file"`

	// InsecureSkipVerify skips certificate verification (not recommended for production).
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify" json:"insecureSkipVerify" yaml:"insecure_skip_verify"`
}

// CircuitBreakerConfig holds circuit breaker configuration.
type CircuitBreakerConfig struct {
	// Enabled enables the circuit breaker.
	Enabled bool `mapstructure:"enabled" json:"enabled" yaml:"enabled"`

	// Threshold is the number of consecutive failures before opening the circuit.
	Threshold int `mapstructure:"threshold" json:"threshold" yaml:"threshold"`

	// Timeout is the time to wait before attempting to close the circuit (seconds).
	Timeout int `mapstructure:"timeout" json:"timeout" yaml:"timeout"`

	// HalfOpenMaxRequests is the maximum requests in half-open state.
	HalfOpenMaxRequests int `mapstructure:"half_open_max_requests" json:"halfOpenMaxRequests" yaml:"half_open_max_requests"`
}

// LoggerConfig holds logger configuration.
type LoggerConfig struct {
	// Level is the log level (debug, info, warn, error).
	Level string `mapstructure:"level" json:"level" yaml:"level"`

	// Format is the log format (json, text).
	Format string `mapstructure:"format" json:"format" yaml:"format"`

	// Output is the log output (stdout, stderr, or file path).
	Output string `mapstructure:"output" json:"output" yaml:"output"`
}

// TracingConfig holds OpenTelemetry tracing configuration.
type TracingConfig struct {
	// Enabled enables OpenTelemetry tracing.
	Enabled bool `mapstructure:"enabled" json:"enabled" yaml:"enabled"`

	// ServiceName is the service name for tracing.
	ServiceName string `mapstructure:"service_name" json:"serviceName" yaml:"service_name"`

	// ExporterType specifies the exporter type (otlp, jaeger, redis, kafka).
	ExporterType string `mapstructure:"exporter_type" json:"exporterType" yaml:"exporter_type"`

	// Endpoint is the OTLP/Jaeger endpoint for sending traces.
	Endpoint string `mapstructure:"endpoint" json:"endpoint" yaml:"endpoint"`

	// Protocol is the OTLP protocol (http, grpc).
	Protocol string `mapstructure:"protocol" json:"protocol" yaml:"protocol"`

	// JaegerConfig holds Jaeger-specific configuration.
	JaegerConfig *JaegerConfig `mapstructure:"jaeger_config" json:"jaegerConfig,omitempty" yaml:"jaeger_config"`

	// RedisConfig holds Redis-specific configuration for trace export.
	RedisConfig *RedisConfig `mapstructure:"redis_config" json:"redisConfig,omitempty" yaml:"redis_config"`

	// KafkaConfig holds Kafka-specific configuration for trace export.
	KafkaConfig *KafkaConfig `mapstructure:"kafka_config" json:"kafkaConfig,omitempty" yaml:"kafka_config"`
}

// JaegerConfig holds Jaeger exporter configuration.
type JaegerConfig struct {
	// AgentHost is the Jaeger agent host.
	AgentHost string `mapstructure:"agent_host" json:"agentHost" yaml:"agent_host"`

	// AgentPort is the Jaeger agent port.
	AgentPort int `mapstructure:"agent_port" json:"agentPort" yaml:"agent_port"`

	// ServiceName is the service name for Jaeger.
	ServiceName string `mapstructure:"service_name" json:"serviceName" yaml:"service_name"`

	// FlushInterval is the flush interval for Jaeger traces.
	FlushInterval int `mapstructure:"flush_interval" json:"flushInterval" yaml:"flush_interval"`
}

// RedisConfig holds Redis-based trace exporter configuration.
type RedisConfig struct {
	// Addrs is the list of Redis addresses.
	Addrs []string `mapstructure:"addrs" json:"addrs" yaml:"addrs"`

	// Password is the Redis password.
	Password string `mapstructure:"password" json:"-" yaml:"password"`

	// DB is the Redis database number.
	DB int `mapstructure:"db" json:"db" yaml:"db"`

	// Channel is the Pub/Sub channel for trace export.
	Channel string `mapstructure:"channel" json:"channel" yaml:"channel"`

	// KeyPrefix is the key prefix for trace storage.
	KeyPrefix string `mapstructure:"key_prefix" json:"keyPrefix" yaml:"key_prefix"`
}

// KafkaConfig holds Kafka-based trace exporter configuration.
type KafkaConfig struct {
	// Brokers is the list of Kafka broker addresses.
	Brokers []string `mapstructure:"brokers" json:"brokers" yaml:"brokers"`

	// Topic is the Kafka topic for trace export.
	Topic string `mapstructure:"topic" json:"topic" yaml:"topic"`

	// ClientID is the Kafka client ID.
	ClientID string `mapstructure:"client_id" json:"clientID" yaml:"client_id"`
}

// IsEnabled returns true if tracing is enabled.
func (c *TracingConfig) IsEnabled() bool {
	return c != nil && c.Enabled
}

// DefaultConfig returns a default configuration for the specified backend.
func DefaultConfig(backend string) *Config {
	cfg := &Config{
		Backend:       backend,
		PoolSize:      10,
		DB:            0,
		MinIdleConns:  5,
		MaxRetries:    3,
		DialTimeout:   5,
		ReadTimeout:   3,
		WriteTimeout:  3,
		IdleTimeout:   60,
		PoolTimeout:   4,
		FileMode:      0600,
		MmapSize:      64 * 1024 * 1024, // 64MB
		Metrics:       false,
		MetricsPrefix: "cachex",
	}

	// Set backend-specific defaults
	switch backend {
	case BackendRedis, BackendDragonfly, BackendKeyDB, BackendGarnet:
		cfg.Addrs = []string{"localhost:6379"}
		cfg.Driver = backend
	case BackendBadger:
		cfg.Dir = "/tmp/cachex-badger-" + time.Now().Format("20060102150405")
		cfg.ValueDir = cfg.Dir
		cfg.BlockCacheSize = 256 * 1024 * 1024 // 256MB
		cfg.IndexCacheSize = 128 * 1024 * 1024 // 128MB
		cfg.Compression = false
		cfg.ValueThreshold = 0
		cfg.BypassLockGuard = true
	case BackendBBolt:
		cfg.Dir = "/tmp/cachex-bbolt.db"
		cfg.BucketName = "cachex"
		cfg.SyncWrites = true
	case BackendPebble:
		cfg.Dir = "/tmp/cachex-pebble"
		cfg.BlockCacheSize = 64 * 1024 * 1024 // 64MB
		cfg.Compression = true
	}

	// Set default circuit breaker
	cfg.CircuitBreaker = &CircuitBreakerConfig{
		Enabled:             true,
		Threshold:           5,
		Timeout:             30,
		HalfOpenMaxRequests: 3,
	}

	// Set default logger
	cfg.Logger = &LoggerConfig{
		Level:  "info",
		Format: "json",
		Output: "stdout",
	}

	return cfg
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	switch c.Backend {
	case BackendRedis, BackendDragonfly, BackendKeyDB, BackendGarnet:
		if len(c.Addrs) == 0 {
			return ErrAddrsRequired
		}
		if c.PoolSize <= 0 {
			c.PoolSize = 10
		}
		if c.MaxRetries < 0 {
			c.MaxRetries = 3
		}
	case BackendBadger:
		if c.Dir == "" && !c.InMemory {
			return ErrDirRequired
		}
		if c.BlockCacheSize <= 0 {
			c.BlockCacheSize = 256 * 1024 * 1024
		}
	case BackendBBolt:
		if c.Dir == "" {
			return ErrDirRequired
		}
		if c.BucketName == "" {
			c.BucketName = "cachex"
		}
	case BackendPebble:
		if c.Dir == "" {
			return ErrDirRequired
		}
	case "":
		return ErrUnknownBackend
	default:
		return ErrUnknownBackend
	}

	return nil
}

// ToTimeout converts seconds to time.Duration.
func (c *Config) ToTimeout(seconds int) time.Duration {
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

// MergeWithEnv applies environment variable overrides.
// Environment variables take precedence over config file values.
// Format: CACHEX_<SECTION>_<KEY> (e.g., CACHEX_REDIS_ADDRS, CACHEX_TLS_ENABLED)
func (c *Config) MergeWithEnv() *Config {
	// Network backend options
	if addr := os.Getenv("CACHEX_ADDRS"); addr != "" {
		c.Addrs = []string{addr}
	}
	if pwd := os.Getenv("CACHEX_PASSWORD"); pwd != "" {
		c.Password = pwd
	}
	if db := os.Getenv("CACHEX_DB"); db != "" {
		if v, err := strconv.Atoi(db); err == nil {
			c.DB = v
		}
	}
	if pool := os.Getenv("CACHEX_POOL_SIZE"); pool != "" {
		if v, err := strconv.Atoi(pool); err == nil {
			c.PoolSize = v
		}
	}

	// TLS options
	if enabled := os.Getenv("CACHEX_TLS_ENABLED"); enabled != "" {
		c.TLS.Enabled = enabled == "true" || enabled == "1"
	}
	if caFile := os.Getenv("CACHEX_TLS_CA_FILE"); caFile != "" {
		c.TLS.CAFile = caFile
	}
	if certFile := os.Getenv("CACHEX_TLS_CERT_FILE"); certFile != "" {
		c.TLS.CertFile = certFile
	}
	if keyFile := os.Getenv("CACHEX_TLS_KEY_FILE"); keyFile != "" {
		c.TLS.KeyFile = keyFile
	}

	// Embedded backend options
	if dir := os.Getenv("CACHEX_DIR"); dir != "" {
		c.Dir = dir
	}
	if valueDir := os.Getenv("CACHEX_VALUE_DIR"); valueDir != "" {
		c.ValueDir = valueDir
	}
	if bucket := os.Getenv("CACHEX_BUCKET_NAME"); bucket != "" {
		c.BucketName = bucket
	}

	return c
}

// Clone creates a deep copy of the configuration.
func (c *Config) Clone() *Config {
	clone := *c

	if c.Addrs != nil {
		clone.Addrs = make([]string, len(c.Addrs))
		copy(clone.Addrs, c.Addrs)
	}

	if c.Options != nil {
		clone.Options = make(map[string]interface{})
		for k, v := range c.Options {
			clone.Options[k] = v
		}
	}

	if c.TLS.CAFile != "" || c.TLS.CertFile != "" || c.TLS.KeyFile != "" {
		clone.TLS = c.TLS
	}

	if c.CircuitBreaker != nil {
		cb := *c.CircuitBreaker
		clone.CircuitBreaker = &cb
	}

	if c.Logger != nil {
		log := *c.Logger
		clone.Logger = &log
	}

	return &clone
}
