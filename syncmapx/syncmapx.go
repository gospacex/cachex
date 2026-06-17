// Package syncmapx provides a minimal sync.Map-backed cache client.
//
// It is a stub driver for the hubx provider integration. The Config carries
// no required fields; New accepts an empty config.
package syncmapx

import (
	"context"
	"sync"
)

// Config is the YAML-driven configuration for syncmapx.
type Config struct {
	// Capacity is a soft hint. 0 means unbounded.
	Capacity int `yaml:"capacity" mapstructure:"capacity"`
}

// Client is the in-process sync.Map-backed cache stub.
type Client struct {
	cfg Config

	mu sync.RWMutex
	m  map[string]any
}

// New returns a Client for cfg. cfg may be the zero value.
func New(c Config) (*Client, error) {
	return &Client{cfg: c, m: make(map[string]any)}, nil
}

// HealthCheck always succeeds for the stub.
func (c *Client) HealthCheck(ctx context.Context) error { return nil }

// Close releases the stub client.
func (c *Client) Close() error { return nil }
