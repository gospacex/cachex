package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
)

const nilSentinel = "nil"

// ConfigFingerprint computes a SHA-256 fingerprint of a struct value.
// It JSON-marshals the input with sorted keys and returns a 64-char hex string.
// Identical config values produce identical fingerprints regardless of pointer identity.
// A nil input (untyped or typed-nil pointer/interface) returns the stable "nil" sentinel.
func ConfigFingerprint(v interface{}) string {
	if v == nil {
		return nilSentinel
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface:
		if rv.IsNil() {
			return nilSentinel
		}
	}

	data, err := json.Marshal(jsonSafeValue(v))
	if err != nil {
		h := sha256.Sum256([]byte{0})
		return hex.EncodeToString(h[:])
	}

	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// jsonSafeValue converts a Go value into a JSON-sortable representation.
// Structs become map[string]interface{} (keyed by json tag or field name),
// maps are flattened, and slices/arrays are recursed element-wise.
func jsonSafeValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	rv := reflect.ValueOf(v)

	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		return jsonSafeValue(rv.Elem().Interface())

	case reflect.Struct:
		result := make(map[string]interface{})
		t := rv.Type()
		for i := 0; i < rv.NumField(); i++ {
			field := t.Field(i)
			if !field.IsExported() {
				continue
			}
			key := field.Name
			if jsonTag := field.Tag.Get("json"); jsonTag != "" && jsonTag != "-" {
				key = jsonTag
			}
			result[key] = jsonSafeValue(rv.Field(i).Interface())
		}
		return result

	case reflect.Map:
		result := make(map[string]interface{})
		for _, key := range rv.MapKeys() {
			k := key.String()
			result[k] = jsonSafeValue(rv.MapIndex(key).Interface())
		}
		return result

	case reflect.Slice, reflect.Array:
		length := rv.Len()
		result := make([]interface{}, length)
		for i := 0; i < length; i++ {
			result[i] = jsonSafeValue(rv.Index(i).Interface())
		}
		return result

	default:
		return v
	}
}
