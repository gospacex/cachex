// Package utils provides shared helpers used across the cachex SDK.
//
// The two exported helpers are:
//
//   - ConfigFingerprint:  computes a stable, field-level SHA-256 fingerprint
//     of any Go value (including nil). Used by the drivers/redisx and
//     drivers/kafkax pools as the pool-map key, and emitted as the
//     trace.exporter config_fingerprint label for observability correlation.
//
//   - ExpandEnvVars:  expands the OpenTelemetry-confmap-style
//     ${env:VAR} and ${env:VAR:-default} placeholders inside configuration
//     strings, with recursive expansion of nested defaults. Other placeholder
//     dialects (${file:...}, {{template}}, etc.) are preserved verbatim so
//     that the loader can reject or pass them through unchanged.
//
// Both helpers are pure functions with no external state and no side effects.
package utils
