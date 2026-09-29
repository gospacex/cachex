package memcached

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
		Driver:       "memcached",
		Addr:         "127.0.0.1:21121",
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

func TestKeysNotSupported(t *testing.T) {
	c := newTestClient(t)
	_, err := c.Keys(context.Background(), "*")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestNewNilConfig(t *testing.T) {
	_, err := New(nil)
	assert.Error(t, err)
}
