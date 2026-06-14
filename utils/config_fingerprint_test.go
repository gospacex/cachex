package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigFingerprint_IdenticalStructs(t *testing.T) {
	type testConfig struct {
		Backend string   `json:"backend"`
		Addrs   []string `json:"addrs"`
		DB      int      `json:"db"`
	}

	cfg1 := &testConfig{Backend: "redis", Addrs: []string{"localhost:6379"}, DB: 0}
	cfg2 := &testConfig{Backend: "redis", Addrs: []string{"localhost:6379"}, DB: 0}

	fp1 := ConfigFingerprint(cfg1)
	fp2 := ConfigFingerprint(cfg2)

	assert.Equal(t, fp1, fp2, "identical struct values should have the same fingerprint")
	assert.Len(t, fp1, 64, "fingerprint should be 64 hex chars (SHA-256)")
}

func TestConfigFingerprint_Reordering(t *testing.T) {
	type testConfig struct {
		Backend  string `json:"backend"`
		DB       int    `json:"db"`
		PoolSize int    `json:"pool_size"`
	}

	cfg1 := &testConfig{Backend: "redis", DB: 0, PoolSize: 10}
	cfg2 := &testConfig{PoolSize: 10, Backend: "redis", DB: 0}

	fp1 := ConfigFingerprint(cfg1)
	fp2 := ConfigFingerprint(cfg2)

	assert.Equal(t, fp1, fp2, "field reordering should produce the same fingerprint since JSON marshaling is deterministic")
}

func TestConfigFingerprint_SingleFieldChange(t *testing.T) {
	type testConfig struct {
		Backend  string `json:"backend"`
		PoolSize int    `json:"pool_size"`
	}

	cfg1 := &testConfig{Backend: "redis", PoolSize: 10}
	cfg2 := &testConfig{Backend: "redis", PoolSize: 20}

	fp1 := ConfigFingerprint(cfg1)
	fp2 := ConfigFingerprint(cfg2)

	assert.NotEqual(t, fp1, fp2, "changing one field should produce a different fingerprint")
}

func TestConfigFingerprint_NilInput(t *testing.T) {
	fp := ConfigFingerprint(nil)
	assert.Equal(t, nilSentinel, fp, "nil input should return the nil sentinel")
}

func TestConfigFingerprint_TypedNilPointer(t *testing.T) {
	var cfg *testConfigNil
	fp := ConfigFingerprint(cfg)
	assert.Equal(t, nilSentinel, fp, "typed nil pointer should return the nil sentinel")
}

type testConfigNil struct {
	Backend string `json:"backend"`
}

func TestConfigFingerprint_EmptyStruct(t *testing.T) {
	type emptyConfig struct{}
	cfg := &emptyConfig{}
	fp := ConfigFingerprint(cfg)
	assert.NotEmpty(t, fp, "empty struct should still produce a valid fingerprint")
}

func TestConfigFingerprint_NestedStruct(t *testing.T) {
	type innerConfig struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}
	type outerConfig struct {
		Backend string      `json:"backend"`
		Inner   innerConfig `json:"inner"`
	}

	cfg1 := &outerConfig{Backend: "redis", Inner: innerConfig{Host: "localhost", Port: 6379}}
	cfg2 := &outerConfig{Backend: "redis", Inner: innerConfig{Host: "localhost", Port: 6379}}

	assert.Equal(t, ConfigFingerprint(cfg1), ConfigFingerprint(cfg2))
}

func TestConfigFingerprint_MapField(t *testing.T) {
	type cfgWithMap struct {
		Backend string                 `json:"backend"`
		Options map[string]interface{} `json:"options"`
	}

	cfg1 := &cfgWithMap{Backend: "redis", Options: map[string]interface{}{"key": "val"}}
	cfg2 := &cfgWithMap{Backend: "redis", Options: map[string]interface{}{"key": "val"}}

	assert.Equal(t, ConfigFingerprint(cfg1), ConfigFingerprint(cfg2))
}

func TestConfigFingerprint_SliceField(t *testing.T) {
	type cfgWithSlice struct {
		Backend string   `json:"backend"`
		Addrs   []string `json:"addrs"`
	}

	cfg1 := &cfgWithSlice{Backend: "redis", Addrs: []string{"a", "b"}}
	cfg2 := &cfgWithSlice{Backend: "redis", Addrs: []string{"a", "b"}}
	cfg3 := &cfgWithSlice{Backend: "redis", Addrs: []string{"b", "a"}}

	assert.Equal(t, ConfigFingerprint(cfg1), ConfigFingerprint(cfg2))
	assert.NotEqual(t, ConfigFingerprint(cfg1), ConfigFingerprint(cfg3), "slice order matters")
}

func TestExpandEnvVars_Set(t *testing.T) {
	os.Setenv("TEST_FOO", "bar")
	defer os.Unsetenv("TEST_FOO")

	result := ExpandEnvVars("hello ${env:TEST_FOO}")
	assert.Equal(t, "hello bar", result)
}

func TestExpandEnvVars_Unset(t *testing.T) {
	os.Unsetenv("TEST_MISSING_VAR")

	result := ExpandEnvVars("hello ${env:TEST_MISSING_VAR}")
	assert.Equal(t, "hello ", result, "unset env var with no default should produce empty string")
}

func TestExpandEnvVars_WithDefault(t *testing.T) {
	os.Unsetenv("TEST_MISSING_VAR")

	result := ExpandEnvVars("hello ${env:TEST_MISSING_VAR:-fallback}")
	assert.Equal(t, "hello fallback", result)
}

func TestExpandEnvVars_WithDefaultButSet(t *testing.T) {
	os.Setenv("TEST_OVERRIDE", "actual")
	defer os.Unsetenv("TEST_OVERRIDE")

	result := ExpandEnvVars("${env:TEST_OVERRIDE:-ignored}")
	assert.Equal(t, "actual", result, "set env var should take precedence over default")
}

func TestExpandEnvVars_EmptyDefault(t *testing.T) {
	os.Unsetenv("TEST_EMPTY_DEFAULT")

	result := ExpandEnvVars("${env:TEST_EMPTY_DEFAULT:-}")
	assert.Equal(t, "", result, "empty default should produce empty string")
}

func TestExpandEnvVars_NestedEnvVars(t *testing.T) {
	os.Setenv("TEST_HOST", "redis.example.com")
	os.Setenv("TEST_PORT", "6379")
	defer os.Unsetenv("TEST_HOST")
	defer os.Unsetenv("TEST_PORT")

	result := ExpandEnvVars("${env:TEST_HOST}:${env:TEST_PORT}")
	assert.Equal(t, "redis.example.com:6379", result)
}

func TestExpandEnvVars_UnknownPlaceholder(t *testing.T) {
	result := ExpandEnvVars("hello ${file:/etc/config}")
	assert.Equal(t, "hello ${file:/etc/config}", result, "non-env placeholders should be preserved")
}

func TestExpandEnvVars_Multiple(t *testing.T) {
	os.Setenv("TEST_BACKEND", "redis")
	os.Setenv("TEST_PORT", "6379")
	defer os.Unsetenv("TEST_BACKEND")
	defer os.Unsetenv("TEST_PORT")

	result := ExpandEnvVars("backend=${env:TEST_BACKEND},port=${env:TEST_PORT}")
	assert.Equal(t, "backend=redis,port=6379", result)
}

func TestExpandEnvVars_EmptyString(t *testing.T) {
	result := ExpandEnvVars("")
	assert.Equal(t, "", result)
}

func TestExpandEnvVars_NoPlaceholders(t *testing.T) {
	result := ExpandEnvVars("plain string")
	assert.Equal(t, "plain string", result)
}
