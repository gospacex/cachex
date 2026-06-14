package utils

import (
	"os"
	"testing"
)

// Helper: set env for the duration of one test.
func setEnv(t *testing.T, key, value string) {
	t.Helper()
	old, had := os.LookupEnv(key)
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("os.Setenv: %v", err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}

func unsetEnv(t *testing.T, key string) {
	t.Helper()
	old, had := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("os.Unsetenv: %v", err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(key, old)
		}
	})
}

func TestExpandEnvVars_Set_Var(t *testing.T) {
	setEnv(t, "CACHEX_TEST_HOME", "/root")
	got := ExpandEnvVars("${env:CACHEX_TEST_HOME}")
	if got != "/root" {
		t.Fatalf("expected /root, got %q", got)
	}
}

func TestExpandEnvVars_Unset_Var(t *testing.T) {
	unsetEnv(t, "CACHEX_TEST_UNSET_XYZ")
	got := ExpandEnvVars("${env:CACHEX_TEST_UNSET_XYZ}")
	if got != "" {
		t.Fatalf("expected empty string for unset var, got %q", got)
	}
}

func TestExpandEnvVars_Unset_With_Default(t *testing.T) {
	unsetEnv(t, "CACHEX_TEST_PORT")
	got := ExpandEnvVars("${env:CACHEX_TEST_PORT:-8080}")
	if got != "8080" {
		t.Fatalf("expected default 8080, got %q", got)
	}
}

func TestExpandEnvVars_Empty_With_Default(t *testing.T) {
	setEnv(t, "CACHEX_TEST_PORT", "")
	got := ExpandEnvVars("${env:CACHEX_TEST_PORT:-8080}")
	if got != "8080" {
		t.Fatalf("expected default 8080 for empty var, got %q", got)
	}
}

func TestExpandEnvVars_Set_With_Default(t *testing.T) {
	setEnv(t, "CACHEX_TEST_PORT", "9090")
	got := ExpandEnvVars("${env:CACHEX_TEST_PORT:-8080}")
	if got != "9090" {
		t.Fatalf("expected 9090, got %q", got)
	}
}

func TestExpandEnvVars_Multiple_In_One_String(t *testing.T) {
	setEnv(t, "CACHEX_TEST_HOST", "db.local")
	setEnv(t, "CACHEX_TEST_PORT", "5432")
	got := ExpandEnvVars("postgres://${env:CACHEX_TEST_HOST}:${env:CACHEX_TEST_PORT}/app")
	if got != "postgres://db.local:5432/app" {
		t.Fatalf("expected postgres URL, got %q", got)
	}
}

func TestExpandEnvVars_Unknown_Placeholder(t *testing.T) {
	got := ExpandEnvVars("file content: ${file:/etc/passwd}")
	if got != "file content: ${file:/etc/passwd}" {
		t.Fatalf("expected unknown placeholder preserved, got %q", got)
	}
}

func TestExpandEnvVars_Template_Brace(t *testing.T) {
	got := ExpandEnvVars("template: {{ .Values.foo }}")
	if got != "template: {{ .Values.foo }}" {
		t.Fatalf("expected template brace preserved, got %q", got)
	}
}

func TestExpandEnvVars_Nested_Expansion(t *testing.T) {
	// OUTER is unset, INNER is unset → fallback.
	unsetEnv(t, "CACHEX_TEST_OUTER")
	unsetEnv(t, "CACHEX_TEST_INNER")
	got := ExpandEnvVars("${env:CACHEX_TEST_OUTER:-${env:CACHEX_TEST_INNER:-fallback}}")
	if got != "fallback" {
		t.Fatalf("expected nested fallback to win, got %q", got)
	}
	// OUTER unset, INNER set to "hello" → INNER value.
	setEnv(t, "CACHEX_TEST_INNER", "hello")
	got = ExpandEnvVars("${env:CACHEX_TEST_OUTER:-${env:CACHEX_TEST_INNER:-fallback}}")
	if got != "hello" {
		t.Fatalf("expected nested INNER value, got %q", got)
	}
	// OUTER set to "world" → OUTER value.
	setEnv(t, "CACHEX_TEST_OUTER", "world")
	got = ExpandEnvVars("${env:CACHEX_TEST_OUTER:-${env:CACHEX_TEST_INNER:-fallback}}")
	if got != "world" {
		t.Fatalf("expected OUTER value, got %q", got)
	}
}

// No placeholders → input is returned unchanged.
func TestExpandEnvVars_No_Placeholder(t *testing.T) {
	in := "literal string with no placeholders"
	if got := ExpandEnvVars(in); got != in {
		t.Fatalf("expected no-op, got %q", got)
	}
}
