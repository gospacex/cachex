package kafkax

// Config is the kafka-specific configuration consumed by the kafkax driver.
// It is kept separate from cachex.Config because sarama exposes knobs that
// have no analogue in the cachex shared struct (Brokers, Topic, Group,
// LingerMs, MaxOpenRequests, SASL, TLS, ...).
//
// All fields are loaded from YAML; ${env:VAR} placeholders are resolved at
// load time via utils.ExpandEnvVars.
type Config struct {
	// Brokers is the bootstrap server list (host:port).
	Brokers []string `yaml:"brokers"`

	// Topic is the default topic used by PPS when none is provided by the
	// caller. Callers can still override per-send.
	Topic string `yaml:"topic"`

	// Group is the consumer group used by Consumer(); ignored by producers.
	Group string `yaml:"group"`

	// BatchSize controls producer batching in bytes (sarama Producer.Flush).
	BatchSize int `yaml:"batch_size"`

	// LingerMs is the maximum time the producer will wait before sending a
	// batch (milliseconds). Maps to sarama Producer.Flush.Frequency.
	LingerMs int `yaml:"linger_ms"`

	// RequiredAcks is "none", "leader" or "all" (or "" for sarama default).
	RequiredAcks string `yaml:"required_acks"`

	// Compression is "none", "gzip", "snappy", "lz4", "zstd".
	Compression string `yaml:"compression"`

	// MaxOpenRequests is the in-flight per-connection cap (sarama default 5).
	MaxOpenRequests int `yaml:"max_open_requests"`

	// MaxMessageBytes caps a single record; defaults to 1MB if zero.
	MaxMessageBytes int `yaml:"max_message_bytes"`

	// FlushTimeoutMs is how long to wait during shutdown for in-flight
	// messages to drain (milliseconds). Defaults to 15000 when zero.
	FlushTimeoutMs int `yaml:"flush_timeout_ms"`

	// SASL credentials (optional).
	SASL SASLConfig `yaml:"sasl"`

	// TLS configuration (optional).
	TLS TLSConfig `yaml:"tls"`

	// Metrics enables sarama's native statistics emitter (gauge snapshot).
	Metrics bool `yaml:"metrics"`
}

// SASLConfig is the SASL sub-config.
type SASLConfig struct {
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	Mechanism string `yaml:"mechanism"` // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
}

// TLSConfig is the TLS sub-config.
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	CAFile             string `yaml:"ca_file"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}
