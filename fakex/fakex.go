// Package fakex provides an in-process fake cache client for tests.
//
// It is a stub driver for the hubx provider integration. New always succeeds
// with an empty Config.
package fakex

import (
	"context"
)

// Config is the YAML-driven configuration for fakex. No fields are required.
type Config struct{}

// Client is the in-process fake cache stub.
type Client struct{}

// New returns a Client. An empty Config is valid.
func New(c Config) (*Client, error) { return &Client{}, nil }

// HealthCheck always succeeds for the stub.
func (c *Client) HealthCheck(ctx context.Context) error { return nil }

// Close releases the stub client.
func (c *Client) Close() error { return nil }
