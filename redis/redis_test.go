package redis

import (
	"context"
	"testing"
	"time"

	"github.com/gospacex/cachex"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestClient(t *testing.T) *Client {
	t.Helper()
	cfg := &cachex.Config{
		Name:         "test",
		Driver:       "redis",
		Addr:         "127.0.0.1:46379",
		Password:     "redis123456",
		DB:           15,
		PoolSize:     5,
		Timeout:      2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	}
	c, err := New(cfg)
	require.NoError(t, err, "New should succeed")
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestGet_Set_Delete(t *testing.T) {
	c := newTestClient(t)

	err := c.Set(context.Background(), "test_key_1", "hello", 10*time.Second)
	require.NoError(t, err)

	val, err := c.Get(context.Background(), "test_key_1")
	require.NoError(t, err)
	assert.Equal(t, "hello", val)

	err = c.Delete(context.Background(), "test_key_1")
	require.NoError(t, err)

	val, err = c.Get(context.Background(), "test_key_1")
	require.NoError(t, err)
	assert.Equal(t, "", val)
}

func TestExists(t *testing.T) {
	c := newTestClient(t)

	exists, err := c.Exists(context.Background(), "nonexistent_key")
	require.NoError(t, err)
	assert.False(t, exists)

	err = c.Set(context.Background(), "exists_key", "val", 5*time.Second)
	require.NoError(t, err)

	exists, err = c.Exists(context.Background(), "exists_key")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestKeys(t *testing.T) {
	c := newTestClient(t)

	err := c.Set(context.Background(), "k1", "v1", 5*time.Second)
	require.NoError(t, err)
	err = c.Set(context.Background(), "k2", "v2", 5*time.Second)
	require.NoError(t, err)

	keys, err := c.Keys(context.Background(), "k*")
	require.NoError(t, err)
	assert.Contains(t, keys, "k1")
	assert.Contains(t, keys, "k2")
}

func TestHealthCheck(t *testing.T) {
	c := newTestClient(t)
	err := c.HealthCheck(context.Background())
	require.NoError(t, err)
}

func TestSetNoTTL(t *testing.T) {
	c := newTestClient(t)
	err := c.Set(context.Background(), "no_ttl_key", "val", 0)
	require.NoError(t, err)
	val, err := c.Get(context.Background(), "no_ttl_key")
	require.NoError(t, err)
	assert.Equal(t, "val", val)
}

func TestGetNonExistent(t *testing.T) {
	c := newTestClient(t)
	val, err := c.Get(context.Background(), "definitely_not_exist_xyz")
	require.NoError(t, err)
	assert.Equal(t, "", val)
}

func TestDeleteNonExistent(t *testing.T) {
	c := newTestClient(t)
	err := c.Delete(context.Background(), "definitely_not_exist_xyz")
	require.NoError(t, err)
}

func TestClose(t *testing.T) {
	c := newTestClient(t)
	err := c.Close()
	require.NoError(t, err)

	err = c.Set(context.Background(), "after_close", "val", 0)
	assert.Error(t, err)
}

func TestNewNilConfig(t *testing.T) {
	_, err := New(nil)
	assert.Error(t, err)
}
