// Package freecachex provides a minimal in-process free-cache-style client.
//
// It is a stub driver for the hubx provider integration. The Size field
// defines the maximum cache size in bytes; New rejects zero sizes.
package freecachex

import (
	"context"
	"fmt"
)

// Config is the YAML-driven configuration for freecachex.
type Config struct {
	// Size is the maximum cache size in bytes.
	Size int `yaml:"size" mapstructure:"size"`
}

// Client is the in-process free-cache stub.
type Client struct {
	cfg Config
}

// New returns a Client for cfg, rejecting zero-size configurations.
func New(c Config) (*Client, error) {
	if c.Size <= 0 {
		return nil, fmt.Errorf("freecachex: size must be > 0, got %d", c.Size)
	}
	return &Client{cfg: c}, nil
}

// HealthCheck always succeeds for the stub.
func (c *Client) HealthCheck(ctx context.Context) error { return nil }

// Close releases the stub client.
func (c *Client) Close() error { return nil }
