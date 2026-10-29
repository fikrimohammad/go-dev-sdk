// Package attributes provides helpers for normalizing developer-style
// attribute keys and converting values to OpenTelemetry attributes. Keys that
// already follow OpenTelemetry conventions are preserved.
package attributes

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
)

// ErrMissingServiceName indicates that an appinfo.Info has no service name.
var ErrMissingServiceName = errors.New("service name is required")

// maxInt64Value is the largest value representable as an int64 attribute.
const maxInt64Value = uint64(1<<63 - 1)

// correlationKeys are log and span correlation keys that must keep their
// standard spelling even though they contain an underscore.
var correlationKeys = map[string]struct{}{
	"trace_id":    {},
	"span_id":     {},
	"trace_flags": {},
}

// NormalizeKey converts a developer-supplied attribute key to the OTel
// semantic convention style: lowercase, with underscores replaced by dots.
// Runs of dots are collapsed and leading and trailing dots are trimmed.
//
// A key that already contains a dot is treated as OTel-style and keeps its
// underscores, so registered keys such as http.response.status_code and
// user_agent.original are preserved. The standard log correlation keys
// trace_id, span_id, and trace_flags are also preserved.
//
//	"order_id"                  → "order.id"
//	"HTTP_METHOD"               → "http.method"
//	"http.response.status_code" → "http.response.status_code"
//	"trace_id"                  → "trace_id"
func NormalizeKey(key string) string {
	key = strings.ToLower(key)
	key = collapseDots(key)
	key = strings.Trim(key, ".")
	if _, ok := correlationKeys[key]; ok {
		return key
	}
	if strings.Contains(key, ".") {
		return key
	}
	key = strings.ReplaceAll(key, "_", ".")
	key = collapseDots(key)
	return strings.Trim(key, ".")
}

// collapseDots replaces runs of consecutive dots with a single dot without
// allocating when the input contains no such run.
func collapseDots(key string) string {
	if !strings.Contains(key, "..") {
		return key
	}
	var b strings.Builder
	b.Grow(len(key))
	for i := 0; i < len(key); i++ {
		if key[i] == '.' && i > 0 && key[i-1] == '.' {
			continue
		}
		b.WriteByte(key[i])
	}
	return b.String()
}

// IsValidKey reports whether key is a non-empty attribute key containing only
// ASCII letters, digits, underscores, dots, and hyphens.
func IsValidKey(key string) bool {
	if key == "" {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '_', c == '.', c == '-':
		default:
			return false
		}
	}
	return true
}

// ValidateAppInfo reports whether info identifies a service. A non-empty
// service name is required; version and environment are optional.
func ValidateAppInfo(info appinfo.Info) error {
	if strings.TrimSpace(info.Name) == "" {
		return ErrMissingServiceName
	}
	return nil
}

// ConvertMapsToKVs merges maps and converts the result to OTel attributes.
// Developer-style keys are normalized with [NormalizeKey], while keys that
// already contain a dot and the standard correlation keys keep their spelling.
// Later maps override earlier ones when keys normalize to the same value, so
// identity attributes passed last always win.
//
// Values preserve their types: numeric types are widened to int64/float64;
// time.Duration is recorded as its nanosecond count; []string, []float64, and
// []bool map to the OTel slice types. Signed types up to 32 bits and unsigned
// types up to 32 bits fit int64 safely; uint and uint64 values that fit in an
// int64 are recorded as integers and larger values are recorded as decimal
// strings because OTel has no unsigned integer attribute type. error and
// fmt.Stringer values are recorded as their string forms; typed nil
// error/Stringer values and nil values are omitted. Other types are
// stringified with fmt.Sprint.
//
// An attribute.KeyValue value is appended verbatim, using the key embedded in
// the KeyValue; the containing map key is used only for merging and ordering.
// Empty and invalid attribute keys are omitted. The result is sorted by
// attribute key, so it is deterministic across maps. No maps, or only
// nil/empty maps, yield nil.
func ConvertMapsToKVs(ms ...map[string]any) []attribute.KeyValue {
	merged := make(map[string]any)
	for _, m := range ms {
		for _, k := range slices.Sorted(maps.Keys(m)) {
			merged[NormalizeKey(k)] = m[k]
		}
	}
	if len(merged) == 0 {
		return nil
	}

	attrs := make([]attribute.KeyValue, 0, len(merged))
	for _, k := range slices.Sorted(maps.Keys(merged)) {
		v := merged[k]
		if kv, ok := v.(attribute.KeyValue); ok {
			if IsValidKey(string(kv.Key)) {
				attrs = append(attrs, kv)
			}
			continue
		}
		if !IsValidKey(k) {
			continue
		}
		key := attribute.Key(k)
		switch v := v.(type) {
		case string:
			attrs = append(attrs, key.String(v))
		case int:
			attrs = append(attrs, key.Int(v))
		case int64:
			attrs = append(attrs, key.Int64(v))
		case int32:
			attrs = append(attrs, key.Int64(int64(v)))
		case int16:
			attrs = append(attrs, key.Int64(int64(v)))
		case int8:
			attrs = append(attrs, key.Int64(int64(v)))
		case uint32:
			attrs = append(attrs, key.Int64(int64(v)))
		case uint16:
			attrs = append(attrs, key.Int64(int64(v)))
		case uint8:
			attrs = append(attrs, key.Int64(int64(v)))
		case uint:
			if uint64(v) <= maxInt64Value {
				attrs = append(attrs, key.Int64(int64(v)))
			} else {
				attrs = append(attrs, key.String(strconv.FormatUint(uint64(v), 10)))
			}
		case uint64:
			if v <= maxInt64Value {
				attrs = append(attrs, key.Int64(int64(v)))
			} else {
				attrs = append(attrs, key.String(strconv.FormatUint(v, 10)))
			}
		case float64:
			attrs = append(attrs, key.Float64(v))
		case float32:
			attrs = append(attrs, key.Float64(float64(v)))
		case bool:
			attrs = append(attrs, key.Bool(v))
		case time.Duration:
			attrs = append(attrs, key.Int64(int64(v)))
		case []string:
			attrs = append(attrs, key.StringSlice(v))
		case []float64:
			attrs = append(attrs, key.Float64Slice(v))
		case []bool:
			attrs = append(attrs, key.BoolSlice(v))
		case error:
			if isNilValue(v) {
				continue
			}
			attrs = append(attrs, key.String(v.Error()))
		case fmt.Stringer:
			if isNilValue(v) {
				continue
			}
			attrs = append(attrs, key.String(v.String()))
		case nil:
			continue
		default:
			attrs = append(attrs, key.String(fmt.Sprint(v)))
		}
	}
	slices.SortStableFunc(attrs, func(a, b attribute.KeyValue) int {
		return strings.Compare(string(a.Key), string(b.Key))
	})
	return attrs
}

// ConvertAppInfoToMap returns the service identity as a map suitable for
// composition with [ConvertMapsToKVs]. The service name is always included;
// version and environment are included only when non-empty. Passing the
// result last gives the identity attributes precedence over colliding earlier
// keys.
func ConvertAppInfoToMap(info appinfo.Info) map[string]any {
	m := map[string]any{"service.name": info.Name}
	if info.Version != "" {
		m["service.version"] = info.Version
	}
	if info.Environment != "" {
		m["deployment.environment"] = info.Environment
	}
	return m
}

// isNilValue reports whether v is nil or a typed nil pointer, map, slice,
// channel, function, or interface.
func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
