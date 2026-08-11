// Package attributes provides helpers for normalizing attribute keys and
// values shared by the logs, metrics, and tracer packages.
package attributes

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
)

// multiDot collapses runs of dots in a normalized key.
var multiDot = regexp.MustCompile(`\.{2,}`)

// NormalizeKey converts a dev-supplied attribute key to the OTel semantic
// convention style: lowercase, with underscores replaced by dots. Runs of dots
// are collapsed and leading/trailing dots are trimmed.
//
//	"order_id" → "order.id"
//	"shop_id"  → "shop.id"
//	"service.name" → "service.name"
//	"__a__"    → "a"
func NormalizeKey(key string) string {
	key = strings.ToLower(key)
	key = strings.ReplaceAll(key, "_", ".")
	key = multiDot.ReplaceAllString(key, ".")
	return strings.Trim(key, ".")
}

// ConvertMapsToKVs merges maps and converts the result to OTel attributes,
// normalizing keys to the OTel semantic convention style ("order_id" →
// "order.id"). Later maps override earlier ones when keys normalize to the same
// value, so identity attributes passed last always win. Values preserve their
// types: numeric types are widened to int64/float64; time.Duration is recorded
// as its nanosecond count; []string, []float64, and []bool map to the OTel
// slice types. Signed types up to 32 bits and unsigned types up to 32 bits fit
// int64 safely; uint and uint64 are recorded as decimal strings because OTel
// has no unsigned integer attribute type. error and fmt.Stringer values are
// recorded as their string forms, nil values are omitted, and any other type
// is stringified with fmt.Sprint. The result is sorted by key, so it is
// deterministic across maps. No maps, or only nil/empty maps, yield nil.
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
		key := attribute.Key(k)
		switch v := merged[k].(type) {
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
			attrs = append(attrs, key.String(strconv.FormatUint(uint64(v), 10)))
		case uint64:
			attrs = append(attrs, key.String(strconv.FormatUint(v, 10)))
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
			attrs = append(attrs, key.String(v.Error()))
		case fmt.Stringer:
			attrs = append(attrs, key.String(v.String()))
		case nil:
			continue
		default:
			attrs = append(attrs, key.String(fmt.Sprint(v)))
		}
	}
	return attrs
}

// ConvertAppInfoToMap returns the service identity as a map suitable for
// composition with ConvertMapsToKVs. Passing it last gives the identity
// attributes (service.name, service.version, deployment.environment)
// precedence over any colliding earlier keys.
func ConvertAppInfoToMap(info appinfo.Info) map[string]any {
	return map[string]any{
		"service.name":           info.Name,
		"service.version":        info.Version,
		"deployment.environment": info.Environment,
	}
}
