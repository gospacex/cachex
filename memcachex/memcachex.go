// Package memcachex provides a minimal in-process memcache-style client.
//
// It is a stub driver intended for the hubx provider integration and unit
// tests. It does not actually dial a memcached server; the New constructor
// always succeeds for non-empty Addr and returns a client whose HealthCheck
// returns nil. A real memcached client would replace this implementation.
package memcachex

import (
	"context"
	"fmt"
)

// Config is the YAML-driven configuration for memcachex.
type Config struct {
	// Addr is the memcached "host:port" endpoint.
	Addr string `yaml:"addr" mapstructure:"addr"`
	// Timeout is the per-operation timeout in seconds.
	Timeout int `yaml:"timeout" mapstructure:"timeout"`
}

// Client is the in-process stub memcache client.
type Client struct {
	cfg Config
}

// New returns a Client for cfg. It validates that Addr is set; the stub does
// not actually dial the network.
func New(c Config) (*Client, error) {
	if c.Addr == "" {
		return nil, fmt.Errorf("memcachex: addr is required")
	}
	return &Client{cfg: c}, nil
}

// HealthCheck always succeeds for the stub.
func (c *Client) HealthCheck(ctx context.Context) error { return nil }

// Close releases the stub client.
func (c *Client) Close() error { return nil }
