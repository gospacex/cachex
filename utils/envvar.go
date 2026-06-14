package utils

import (
	"os"
	"strings"
)

const envPrefix = "${env:"

// ExpandEnvVars replaces ${env:VAR} and ${env:VAR:-default} placeholders
// with environment variable values. Non-env placeholders are preserved as-is.
// This follows the OpenTelemetry confmap syntax convention.
//
// Behavior:
//   - ${env:VAR}           → value if set (even if empty), "" if unset
//   - ${env:VAR:-default}  → value if set and non-empty, else default
//
// Nested placeholders in the default (e.g. ${env:A:-${env:B:-x}}) are resolved
// recursively until stable. Other placeholder dialects
// (${file:...}, {{template}}, etc.) are preserved verbatim so the loader
// can reject or pass them through unchanged.
func ExpandEnvVars(s string) string {
	return expandEnvVars(s, 0)
}

func expandEnvVars(s string, depth int) string {
	if depth > 32 {
		return s
	}

	var sb strings.Builder
	sb.Grow(len(s))
	i := 0
	for i < len(s) {
		idx := strings.Index(s[i:], envPrefix)
		if idx < 0 {
			sb.WriteString(s[i:])
			break
		}
		sb.WriteString(s[i : i+idx])

		// Parse ${env:NAME ...}
		j := i + idx + len(envPrefix)
		nameStart := j
		for j < len(s) && s[j] != ':' && s[j] != '}' {
			j++
		}
		if j >= len(s) {
			sb.WriteString(s[i+idx:])
			break
		}

		if s[j] == '}' {
			// Simple form: ${env:NAME}
			name := s[nameStart:j]
			if value, ok := os.LookupEnv(name); ok {
				sb.WriteString(value)
			}
			i = j + 1
			continue
		}

		// s[j] == ':'; check for ":-"
		if j+1 >= len(s) || s[j+1] != '-' {
			// Not the default form — emit the placeholder literally
			sb.WriteString(s[i+idx : j+1])
			i = j + 1
			continue
		}
		j += 2 // skip ":-"

		// Read default, counting braces so nested placeholders survive.
		defStart := j
		braceDepth := 1
		for j < len(s) && braceDepth > 0 {
			switch s[j] {
			case '{':
				braceDepth++
			case '}':
				braceDepth--
			}
			if braceDepth == 0 {
				break
			}
			j++
		}
		if j >= len(s) {
			sb.WriteString(s[i+idx:])
			break
		}
		// s[j] is the matching '}'.

		colonRel := strings.IndexByte(s[nameStart:], ':')
		if colonRel < 0 {
			sb.WriteString(s[i+idx:])
			break
		}
		name := s[nameStart : nameStart+colonRel]
		defaultVal := s[defStart:j]
		expandedDefault := expandEnvVars(defaultVal, depth+1)

		if value, ok := os.LookupEnv(name); ok && value != "" {
			sb.WriteString(value)
		} else {
			sb.WriteString(expandedDefault)
		}
		i = j + 1
	}
	return sb.String()
}
