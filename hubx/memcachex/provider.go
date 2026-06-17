// Package memcachex implements hubx.ClientProvider for "cachex.memcache".
//
// It decodes a Config block and constructs a cachex/memcachex.Client.
package memcachex

import (
	"context"
	"fmt"

	hubx "github.com/gospacex/hubx"
	"github.com/mitchellh/mapstructure"

	"github.com/gospacex/cachex/memcachex"
)

// Provider is the hubx.ClientProvider implementation for "cachex.memcache".
type Provider struct{}

// New returns a fresh Provider.
func New() *Provider { return &Provider{} }

// Name is the registry key: "cachex.memcache".
func (p *Provider) Name() string { return "cachex.memcache" }

// Build decodes cfg["config"] and constructs a memcachex.Client.
//
// We decode directly into the underlying memcachex.Config so the type
// matches the constructor's signature. Adding a separate local Config
// would shadow the imported type and force an awkward translation.
func (p *Provider) Build(instanceName string, cfg map[string]any) (hubx.Client, error) {
	raw, ok := cfg["config"]
	if !ok {
		return nil, fmt.Errorf("%w: cachex.memcache/%s: missing 'config' key", hubx.ErrConfigInvalid, instanceName)
	}
	var c memcachex.Config
	dec, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName: "yaml", ErrorUnused: true, Result: &c,
	})
	if err := dec.Decode(raw); err != nil {
		return nil, fmt.Errorf("%w: cachex.memcache/%s: %v", hubx.ErrConfigInvalid, instanceName, err)
	}
	cli, err := memcachex.New(c)
	if err != nil {
		return nil, fmt.Errorf("%w: cachex.memcache/%s: %v", hubx.ErrBuildFailed, instanceName, err)
	}
	return &client{c: cli}, nil
}

func (p *Provider) HealthCheck(context.Context) error { return nil }
func (p *Provider) Close() error                      { return nil }

type client struct{ c *memcachex.Client }

func (c *client) HealthCheck(ctx context.Context) error { return c.c.HealthCheck(ctx) }
func (c *client) Close() error                          { return c.c.Close() }
