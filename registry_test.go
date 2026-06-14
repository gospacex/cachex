package cachex

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRegistry(t *testing.T) {
	r := NewRegistry()
	assert.NotNil(t, r)
	assert.Equal(t, 0, r.Count())
}

func TestRegistryRegisterBackend(t *testing.T) {
	r := NewRegistry()

	// Test valid registration
	creator := &testCreator{}
	err := r.RegisterBackend("test", creator)
	require.NoError(t, err)
	assert.Equal(t, 1, r.Count())

	// Test duplicate registration
	err = r.RegisterBackend("test", creator)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrBackendAlreadyRegistered)

	// Test empty name
	err = r.RegisterBackend("", creator)
	assert.Error(t, err)

	// Test nil creator
	err = r.RegisterBackend("test2", nil)
	assert.Error(t, err)
}

func TestRegistryGet(t *testing.T) {
	r := NewRegistry()

	creator := &testCreator{}
	err := r.RegisterBackend("test", creator)
	require.NoError(t, err)

	// Test get existing
	retrieved, err := r.Get("test")
	require.NoError(t, err)
	assert.Equal(t, creator, retrieved)

	// Test get non-existing
	_, err = r.Get("non-existing")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknownBackend)
}

func TestRegistryGetInfo(t *testing.T) {
	r := NewRegistry()

	creator := &testCreator{}
	err := r.RegisterBackend("test", creator)
	require.NoError(t, err)

	// Test get info
	info, err := r.GetInfo("test")
	require.NoError(t, err)
	assert.Equal(t, "test", info.Name)

	// Test get info for non-existing
	_, err = r.GetInfo("non-existing")
	assert.Error(t, err)
}

func TestRegistryList(t *testing.T) {
	r := NewRegistry()

	// Empty list initially
	list := r.List()
	assert.Empty(t, list)

	// Register some backends
	creator1 := &testCreator{}
	creator2 := &testCreator{}
	r.RegisterBackend("backend1", creator1)
	r.RegisterBackend("backend2", creator2)

	// List should have 2 items
	list = r.List()
	assert.Len(t, list, 2)

	// List should contain both backends
	names := make(map[string]bool)
	for _, info := range list {
		names[info.Name] = true
	}
	assert.True(t, names["backend1"])
	assert.True(t, names["backend2"])
}

func TestRegistryUnregister(t *testing.T) {
	r := NewRegistry()

	creator := &testCreator{}
	r.RegisterBackend("test", creator)

	// Unregister existing
	err := r.Unregister("test")
	require.NoError(t, err)
	assert.Equal(t, 0, r.Count())

	// Unregister non-existing
	err = r.Unregister("test")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknownBackend)
}

func TestRegistryHasBackend(t *testing.T) {
	r := NewRegistry()

	creator := &testCreator{}
	r.RegisterBackend("test", creator)

	assert.True(t, r.HasBackend("test"))
	assert.False(t, r.HasBackend("non-existing"))
}

func TestRegistryCount(t *testing.T) {
	r := NewRegistry()

	assert.Equal(t, 0, r.Count())

	r.RegisterBackend("test1", &testCreator{})
	assert.Equal(t, 1, r.Count())

	r.RegisterBackend("test2", &testCreator{})
	assert.Equal(t, 2, r.Count())

	r.Unregister("test1")
	assert.Equal(t, 1, r.Count())
}

func TestGetBackendInfo(t *testing.T) {
	tests := []struct {
		name         string
		wantName     string
		wantEmbedded bool
		wantProtocol string
	}{
		{BackendRedis, "redis", false, "redis"},
		{BackendDragonfly, "dragonfly", false, "redis"},
		{BackendKeyDB, "keydb", false, "redis"},
		{BackendGarnet, "garnet", false, "redis"},
		{BackendBadger, "badger", true, "kv"},
		{BackendBBolt, "bbolt", true, "kv"},
		{BackendPebble, "pebble", true, "kv"},
		{"unknown", "unknown", false, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := getBackendInfo(tt.name)

			assert.Equal(t, tt.wantName, info.Name)
			assert.Equal(t, tt.wantEmbedded, info.IsEmbedded)
			assert.Equal(t, tt.wantProtocol, info.Protocol)

			if tt.name != "unknown" {
				assert.NotEmpty(t, info.Description)
			}
		})
	}
}

func TestBackendInfoFeatures(t *testing.T) {
	info := getBackendInfo(BackendRedis)
	assert.NotEmpty(t, info.Features)

	info = getBackendInfo(BackendBadger)
	assert.NotEmpty(t, info.Features)
}

// testCreator is a simple creator implementation for testing.
type testCreator struct{}

func (c *testCreator) Create(cfg *Config) (Cache, error) {
	return &mockCache{}, nil
}
