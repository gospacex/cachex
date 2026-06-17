// Package redisx implements hubx.ClientProvider for "cachex.redis".
//
// It adapts the cachex/drivers/redisx pool to the hubx.ClientProvider
// contract: a name, a Build that decodes a map[string]any config block, and
// trivial provider-level HealthCheck / Close (the instance is responsible
// for its own lifecycle).
package redisx

import (
	"context"
	"fmt"

	"github.com/gospacex/cachex"
	redisdriver "github.com/gospacex/cachex/drivers/redisx"
	hubx "github.com/gospacex/hubx"
	"github.com/mitchellh/mapstructure"
	"github.com/redis/go-redis/v9"
)

// Config is the YAML-driven configuration block for cachex/hubx/redisx.
type Config struct {
	Backend  string   `yaml:"backend" mapstructure:"backend"`
	Driver   string   `yaml:"driver" mapstructure:"driver"`
	Addrs    []string `yaml:"addrs" mapstructure:"addrs"`
	DB       int      `yaml:"db" mapstructure:"db"`
	Password string   `yaml:"password" mapstructure:"password"`
	PoolSize int      `yaml:"pool_size" mapstructure:"pool_size"`
}

// Provider is the hubx.ClientProvider implementation for "cachex.redis".
type Provider struct{}

// New returns a fresh Provider.
func New() *Provider { return &Provider{} }

// Name is the registry key: "cachex.redis".
func (p *Provider) Name() string { return "cachex.redis" }

// Build decodes cfg["config"] into a cachex.Config and returns a client
// wrapping a *redis.Client from the redisx pool. The redisx pool dials the
// configured address on first use; if no redis is reachable, the underlying
// GetSingle returns an error wrapped with ErrBuildFailed.
func (p *Provider) Build(instanceName string, cfg map[string]any) (hubx.Client, error) {
	raw, ok := cfg["config"]
	if !ok {
		return nil, fmt.Errorf("%w: cachex.redis/%s: missing 'config' key", hubx.ErrConfigInvalid, instanceName)
	}
	var c cachex.Config
	dec, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName: "yaml", Result: &c,
	})
	if err := dec.Decode(raw); err != nil {
		return nil, fmt.Errorf("%w: cachex.redis/%s: %v", hubx.ErrConfigInvalid, instanceName, err)
	}
	if len(c.Addrs) == 0 {
		return nil, fmt.Errorf("%w: cachex.redis/%s: addrs is required", hubx.ErrConfigInvalid, instanceName)
	}
	cli, err := redisdriver.GetSingle(&c)
	if err != nil {
		return nil, fmt.Errorf("%w: cachex.redis/%s: %v", hubx.ErrBuildFailed, instanceName, err)
	}
	return &client{c: cli}, nil
}

// HealthCheck at provider level is a no-op; per-instance health lives on the
// returned client.
func (p *Provider) HealthCheck(context.Context) error { return nil }

// Close is a no-op because the Provider is stateless.
func (p *Provider) Close() error { return nil }

type client struct{ c *redis.Client }

func (c *client) HealthCheck(ctx context.Context) error { return c.c.Ping(ctx).Err() }
func (c *client) Close() error                          { return c.c.Close() }
