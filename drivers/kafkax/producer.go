package kafkax

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Package-level pools and per-key locks implementing double-checked locking.
var (
	syncProducerCache  sync.Map // cacheKey → *syncProducerHolder
	asyncProducerCache sync.Map // cacheKey → *asyncProducerHolder
	producerLocks      sync.Map // cacheKey → *sync.Mutex
)

// syncProducerHolder wraps a sarama.SyncProducer with its underlying client
// (needed for metadata probes + connection-count monitoring) and a cancel
// for the stats monitor goroutine.
type syncProducerHolder struct {
	prod   sarama.SyncProducer
	client sarama.Client
	cancel context.CancelFunc
}

func (h *syncProducerHolder) Close(flushMs int) error {
	if h.cancel != nil {
		h.cancel()
	}
	prodErr := error(nil)
	if h.prod != nil {
		prodErr = h.prod.Close()
	}
	if h.client != nil {
		if err := h.client.Close(); err != nil && prodErr == nil {
			prodErr = err
		}
	}
	return prodErr
}

func (h *syncProducerHolder) ProbeMetadata(_ time.Duration) error {
	if h.client == nil {
		return fmt.Errorf("nil client")
	}
	return h.client.RefreshMetadata()
}

// asyncProducerHolder wraps a sarama.AsyncProducer with its underlying
// client for the same reasons as syncProducerHolder.
type asyncProducerHolder struct {
	prod   sarama.AsyncProducer
	client sarama.Client
	cancel context.CancelFunc
}

func (h *asyncProducerHolder) Close() error {
	if h.cancel != nil {
		h.cancel()
	}
	prodErr := error(nil)
	if h.prod != nil {
		prodErr = h.prod.Close()
	}
	if h.client != nil {
		if err := h.client.Close(); err != nil && prodErr == nil {
			prodErr = err
		}
	}
	return prodErr
}

func (h *asyncProducerHolder) ProbeMetadata(_ time.Duration) error {
	if h.client == nil {
		return fmt.Errorf("nil client")
	}
	return h.client.RefreshMetadata()
}

// PPS returns a shared *sarama.SyncProducer for the configuration described
// by path. Concurrent callers asking for the same configuration share the
// same underlying producer.
func PPS(path string) (sarama.SyncProducer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("kafkax.PPS: %w", err)
	}
	cacheKey := buildCacheKey("single", key, cfg)
	holder, err := getOrCreateSyncProducer(cacheKey, cfg)
	if err != nil {
		return nil, err
	}
	return holder.prod, nil
}

// PPC returns a shared *sarama.AsyncProducer for the configuration described
// by path.
func PPC(path string) (sarama.AsyncProducer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("kafkax.PPC: %w", err)
	}
	cacheKey := buildCacheKey("cluster", key, cfg)
	holder, err := getOrCreateAsyncProducer(cacheKey, cfg)
	if err != nil {
		return nil, err
	}
	return holder.prod, nil
}

// getOrCreateSyncProducer implements the double-checked locking idiom.
func getOrCreateSyncProducer(cacheKey string, cfg *Config) (*syncProducerHolder, error) {
	if val, ok := syncProducerCache.Load(cacheKey); ok {
		return val.(*syncProducerHolder), nil
	}

	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafkax: no brokers provided")
	}

	lockVal, _ := producerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := syncProducerCache.Load(cacheKey); ok {
		return val.(*syncProducerHolder), nil
	}

	sc, err := buildSaramaConfig(cfg)
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(cfg.Brokers, sc)
	if err != nil {
		return nil, fmt.Errorf("kafkax: create client: %w", err)
	}

	prod, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("kafkax: create sync producer: %w", err)
	}

	monCtx, monCancel := context.WithCancel(context.Background())
	holder := &syncProducerHolder{prod: prod, client: client, cancel: monCancel}
	startSaramaStatsMonitor(monCtx, cacheKey, client, prod)

	syncProducerCache.Store(cacheKey, holder)
	return holder, nil
}

// getOrCreateAsyncProducer implements the async equivalent.
func getOrCreateAsyncProducer(cacheKey string, cfg *Config) (*asyncProducerHolder, error) {
	if val, ok := asyncProducerCache.Load(cacheKey); ok {
		return val.(*asyncProducerHolder), nil
	}

	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafkax: no brokers provided")
	}

	lockVal, _ := producerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := asyncProducerCache.Load(cacheKey); ok {
		return val.(*asyncProducerHolder), nil
	}

	sc, err := buildSaramaConfig(cfg)
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(cfg.Brokers, sc)
	if err != nil {
		return nil, fmt.Errorf("kafkax: create client: %w", err)
	}

	prod, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("kafkax: create async producer: %w", err)
	}

	monCtx, monCancel := context.WithCancel(context.Background())
	holder := &asyncProducerHolder{prod: prod, client: client, cancel: monCancel}
	startSaramaStatsMonitor(monCtx, cacheKey+":async", client, prod)

	asyncProducerCache.Store(cacheKey, holder)
	return holder, nil
}

// buildSaramaConfig translates a kafkax Config into *sarama.Config.
func buildSaramaConfig(cfg *Config) (*sarama.Config, error) {
	sc := sarama.NewConfig()
	sc.Version = sarama.V2_8_0_0

	if cfg.RequiredAcks != "" {
		switch cfg.RequiredAcks {
		case "none":
			sc.Producer.RequiredAcks = sarama.NoResponse
		case "leader":
			sc.Producer.RequiredAcks = sarama.WaitForLocal
		case "all":
			sc.Producer.RequiredAcks = sarama.WaitForAll
		default:
			return nil, fmt.Errorf("kafkax: unknown required_acks %q", cfg.RequiredAcks)
		}
	} else {
		sc.Producer.RequiredAcks = sarama.WaitForAll
	}
	if cfg.MaxMessageBytes > 0 {
		sc.Producer.MaxMessageBytes = cfg.MaxMessageBytes
	}
	if cfg.Compression != "" {
		switch cfg.Compression {
		case "none":
			sc.Producer.Compression = sarama.CompressionNone
		case "gzip":
			sc.Producer.Compression = sarama.CompressionGZIP
		case "snappy":
			sc.Producer.Compression = sarama.CompressionSnappy
		case "lz4":
			sc.Producer.Compression = sarama.CompressionLZ4
		case "zstd":
			sc.Producer.Compression = sarama.CompressionZSTD
		default:
			return nil, fmt.Errorf("kafkax: unknown compression %q", cfg.Compression)
		}
	}
	if cfg.BatchSize > 0 {
		sc.Producer.Flush.MaxMessages = cfg.BatchSize
	}
	if cfg.LingerMs > 0 {
		sc.Producer.Flush.Frequency = time.Duration(cfg.LingerMs) * time.Millisecond
	}
	if cfg.MaxOpenRequests > 0 {
		sc.Net.MaxOpenRequests = cfg.MaxOpenRequests
	}
	sc.Producer.Return.Successes = true
	sc.Producer.Return.Errors = true

	// SASL
	if cfg.SASL.Username != "" {
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = cfg.SASL.Username
		sc.Net.SASL.Password = cfg.SASL.Password
		switch cfg.SASL.Mechanism {
		case "", "PLAIN":
			sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			return nil, fmt.Errorf("kafkax: unknown SASL mechanism %q", cfg.SASL.Mechanism)
		}
	}

	// TLS
	if cfg.TLS.Enabled {
		tc, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		sc.Net.TLS.Enable = true
		sc.Net.TLS.Config = tc
	}

	return sc, nil
}

// buildTLSConfig returns a *tls.Config derived from cfg.TLS or nil when
// TLS is not enabled.
func buildTLSConfig(cfg *Config) (*tls.Config, error) {
	if !cfg.TLS.Enabled {
		return nil, nil
	}
	tc := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
	}
	if cfg.TLS.CAFile != "" {
		caCert, err := os.ReadFile(cfg.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kafkax: read CA file %s: %w", cfg.TLS.CAFile, err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("kafkax: failed to append CA certificate from %s", cfg.TLS.CAFile)
		}
		tc.RootCAs = pool
	}
	if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kafkax: load client cert/key: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}
