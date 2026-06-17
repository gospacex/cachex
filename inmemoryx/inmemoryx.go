// Package inmemoryx provides a minimal in-memory cache client.
//
// It is a stub driver for the hubx provider integration. The Config carries
// only an optional capacity hint; New accepts an empty config.
package inmemoryx

import (
	"context"
	"sync"
)

// Config is the YAML-driven configuration for inmemoryx.
type Config struct {
	// Capacity is a soft hint for the in-memory store. 0 means unlimited.
	Capacity int `yaml:"capacity" mapstructure:"capacity"`
}

// Client is the in-process memory cache stub.
type Client struct {
	cfg Config

	mu    sync.RWMutex
	items map[string]any
}

// New returns a Client for cfg. cfg may be the zero value.
func New(c Config) (*Client, error) {
	return &Client{cfg: c, items: make(map[string]any)}, nil
}

// HealthCheck always succeeds for the stub.
func (c *Client) HealthCheck(ctx context.Context) error { return nil }

// Close releases the stub client.
func (c *Client) Close() error { return nil }
