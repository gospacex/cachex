// Package ristrettox provides a minimal in-process ristretto-style client.
//
// It is a stub driver for the hubx provider integration. Ristretto requires
// a MaxCost; New rejects zero MaxCost configurations.
package ristrettox

import (
	"context"
	"fmt"
)

// Config is the YAML-driven configuration for ristrettox.
type Config struct {
	// NumCounters is the number of counters in the cache (typically 10x MaxCost).
	NumCounters int64 `yaml:"num_counters" mapstructure:"num_counters"`
	// MaxCost is the maximum cost the cache may hold.
	MaxCost int64 `yaml:"max_cost" mapstructure:"max_cost"`
	// BufferItems controls the size of the GetBuffer.
	BufferItems int64 `yaml:"buffer_items" mapstructure:"buffer_items"`
}

// Client is the in-process ristretto stub.
type Client struct {
	cfg Config
}

// New returns a Client for cfg, rejecting zero MaxCost configurations.
func New(c Config) (*Client, error) {
	if c.MaxCost <= 0 {
		return nil, fmt.Errorf("ristrettox: max_cost must be > 0, got %d", c.MaxCost)
	}
	return &Client{cfg: c}, nil
}

// HealthCheck always succeeds for the stub.
func (c *Client) HealthCheck(ctx context.Context) error { return nil }

// Close releases the stub client.
func (c *Client) Close() error { return nil }
