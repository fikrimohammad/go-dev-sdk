package attributes

import (
	"errors"
	"math"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/fikrimohammad/go-servicekit/identity"
)

type myStringer string

func (s myStringer) String() string { return string(s) }

type nilStringer struct{ s string }

func (s *nilStringer) String() string { return s.s }

type nilError struct{ s string }

func (e *nilError) Error() string { return e.s }

// kvsByKey indexes kvs by attribute key.
func kvsByKey(kvs []attribute.KeyValue) map[attribute.Key]attribute.Value {
	m := make(map[attribute.Key]attribute.Value, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value
	}
	return m
}

func TestNormalizeKey(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// NormalizeKey maps developer keys to OTel style.
		{name: "developer key order_id", in: "order_id", want: "order.id"},
		{name: "developer key shop_id", in: "shop_id", want: "shop.id"},
		{name: "developer key HTTP_METHOD", in: "HTTP_METHOD", want: "http.method"},
		{name: "developer key request_id", in: "request_id", want: "request.id"},

		// NormalizeKey preserves keys that already contain a dot.
		{name: "dotted key service.name", in: "service.name", want: "service.name"},
		{name: "dotted key http.response.status_code", in: "http.response.status_code", want: "http.response.status_code"},
		{name: "dotted key http.request.body.size", in: "http.request.body.size", want: "http.request.body.size"},
		{name: "dotted key user_agent.original", in: "user_agent.original", want: "user_agent.original"},
		{name: "dotted key exception.stacktrace", in: "exception.stacktrace", want: "exception.stacktrace"},

		// NormalizeKey preserves standard correlation keys.
		{name: "correlation key trace_id", in: "trace_id", want: "trace_id"},
		{name: "correlation key span_id", in: "span_id", want: "span_id"},
		{name: "correlation key trace_flags", in: "trace_flags", want: "trace_flags"},

		// NormalizeKey trims stray underscores and dots.
		{name: "stray underscores __a__", in: "__a__", want: "a"},
		{name: "stray leading underscore _order_id", in: "_order_id", want: "order.id"},
		{name: "stray repeated dots a..b", in: "a..b", want: "a.b"},
		{name: "stray repeated underscores a__b", in: "a__b", want: "a.b"},
		{name: "only separators ...", in: "...", want: ""},
		{name: "stray surrounding dots .http.route.", in: ".http.route.", want: "http.route"},

		// NormalizeKey leaves invalid characters for validation to reject.
		{name: "invalid space foo bar", in: "foo bar", want: "foo bar"},
		{name: "invalid character slash", in: "foo/bar", want: "foo/bar"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeKey(tt.in); got != tt.want {
				t.Errorf("NormalizeKey(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestIsValidKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want bool
	}{
		// IsValidKey accepts OTel-style keys.
		{name: "accepts single letter", key: "a", want: true},
		{name: "accepts dotted key", key: "order.id", want: true},
		{name: "accepts semantic convention key", key: "http.response.status_code", want: true},
		{name: "accepts underscore key", key: "user_agent.original", want: true},
		{name: "accepts hyphen underscore dot key", key: "a-b_c.d", want: true},
		{name: "accepts leading digit key", key: "9lives", want: true},

		// IsValidKey rejects invalid keys.
		{name: "rejects empty key", key: "", want: false},
		{name: "rejects space", key: "foo bar", want: false},
		{name: "rejects slash", key: "foo/bar", want: false},
		{name: "rejects percent", key: "foo%bar", want: false},
		{name: "rejects colon", key: "foo:bar", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidKey(tt.key); got != tt.want {
				t.Errorf("IsValidKey(%q) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}

func TestValidateAppInfo(t *testing.T) {
	tests := []struct {
		name    string
		info    identity.Info
		wantErr error
	}{
		{name: "accepts simple name", info: identity.Info{Name: "svc"}, wantErr: nil},
		{name: "accepts name with space", info: identity.Info{Name: "my service"}, wantErr: nil},
		{name: "rejects empty name", info: identity.Info{}, wantErr: ErrMissingServiceName},
		{name: "rejects blank name", info: identity.Info{Name: "   "}, wantErr: ErrMissingServiceName},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// errors.Is(err, nil) holds only when err is nil.
			if err := ValidateAppInfo(tt.info); !errors.Is(err, tt.wantErr) {
				t.Errorf("ValidateAppInfo(%+v) = %v, want %v", tt.info, err, tt.wantErr)
			}
		})
	}
}

func TestConvertMapsToKVs(t *testing.T) {
	t.Run("no maps, nil, and empty maps yield nil", func(t *testing.T) {
		if got := ConvertMapsToKVs(); got != nil {
			t.Errorf("ConvertMapsToKVs() = %v, want nil", got)
		}
		if got := ConvertMapsToKVs(nil); got != nil {
			t.Errorf("ConvertMapsToKVs(nil) = %v, want nil", got)
		}
		if got := ConvertMapsToKVs(map[string]any{}); got != nil {
			t.Errorf("ConvertMapsToKVs(map{}) = %v, want nil", got)
		}
	})

	t.Run("developer keys are normalized and values typed", func(t *testing.T) {
		m := kvsByKey(ConvertMapsToKVs(map[string]any{
			"user_id": "u1",
			"count":   int(1),
			"big":     int64(2),
			"ratio":   0.5,
			"enabled": true,
			"other":   struct{ name string }{name: "x"},
		}))
		if got := m["user.id"].AsString(); got != "u1" {
			t.Errorf(`ConvertMapsToKVs(user_id=u1)["user.id"].AsString() = %q, want "u1"`, got)
		}
		if got := m["count"].AsInt64(); got != 1 {
			t.Errorf(`ConvertMapsToKVs(count=1)["count"].AsInt64() = %d, want 1`, got)
		}
		if got := m["big"].AsInt64(); got != 2 {
			t.Errorf(`ConvertMapsToKVs(big=int64(2))["big"].AsInt64() = %d, want 2`, got)
		}
		if got := m["ratio"].AsFloat64(); got != 0.5 {
			t.Errorf(`ConvertMapsToKVs(ratio)["ratio"].AsFloat64() = %v, want 0.5`, got)
		}
		if got := m["enabled"].AsBool(); !got {
			t.Errorf(`ConvertMapsToKVs(enabled=true)["enabled"].AsBool() = %v, want true`, got)
		}
		if got := m["other"].AsString(); got != "{x}" {
			t.Errorf(`ConvertMapsToKVs(other=struct)["other"].AsString() = %q, want "{x}"`, got)
		}
	})

	t.Run("semantic-convention and correlation keys are preserved", func(t *testing.T) {
		m := kvsByKey(ConvertMapsToKVs(map[string]any{
			"http.response.status_code": 200,
			"user_agent.original":       "ua",
			"trace_id":                  "trace",
			"span_id":                   "span",
		}))
		if got := m["http.response.status_code"].AsInt64(); got != 200 {
			t.Errorf(`ConvertMapsToKVs(http.response.status_code=200)["http.response.status_code"].AsInt64() = %d, want 200`, got)
		}
		if got := m["user_agent.original"].AsString(); got != "ua" {
			t.Errorf(`ConvertMapsToKVs(user_agent.original=ua)["user_agent.original"].AsString() = %q, want "ua"`, got)
		}
		if got := m["trace_id"].AsString(); got != "trace" {
			t.Errorf(`ConvertMapsToKVs(trace_id=trace)["trace_id"].AsString() = %q, want "trace"`, got)
		}
		if got := m["span_id"].AsString(); got != "span" {
			t.Errorf(`ConvertMapsToKVs(span_id=span)["span_id"].AsString() = %q, want "span"`, got)
		}
	})

	t.Run("narrow integers widen to int64", func(t *testing.T) {
		m := kvsByKey(ConvertMapsToKVs(map[string]any{
			"i32": int32(32),
			"i16": int16(16),
			"i8":  int8(8),
			"u32": uint32(32),
			"u16": uint16(16),
			"u8":  uint8(8),
		}))
		tests := []struct {
			name string
			key  string
			want int64
		}{
			{name: "int32", key: "i32", want: 32},
			{name: "int16", key: "i16", want: 16},
			{name: "int8", key: "i8", want: 8},
			{name: "uint32", key: "u32", want: 32},
			{name: "uint16", key: "u16", want: 16},
			{name: "uint8", key: "u8", want: 8},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := m[attribute.Key(tt.key)].AsInt64(); got != tt.want {
					t.Errorf("ConvertMapsToKVs(%s=...)[%q].AsInt64() = %d, want %d", tt.key, tt.key, got, tt.want)
				}
			})
		}
	})

	t.Run("float32 widens to float64", func(t *testing.T) {
		kvs := ConvertMapsToKVs(map[string]any{"ratio": float32(0.5)})
		if len(kvs) != 1 {
			t.Fatalf("len(ConvertMapsToKVs(ratio=float32(0.5))) = %d, want 1", len(kvs))
		}
		if got := kvs[0].Value.AsFloat64(); math.Abs(got-0.5) > 1e-10 {
			t.Errorf(`ConvertMapsToKVs(ratio)["ratio"].AsFloat64() = %v, want 0.5`, got)
		}
	})

	t.Run("time.Duration is recorded as nanoseconds", func(t *testing.T) {
		kvs := ConvertMapsToKVs(map[string]any{"latency": 250 * time.Millisecond})
		if len(kvs) != 1 {
			t.Fatalf("len(ConvertMapsToKVs(latency=250ms)) = %d, want 1", len(kvs))
		}
		if got, want := kvs[0].Value.AsInt64(), int64(250*time.Millisecond); got != want {
			t.Errorf("ConvertMapsToKVs(latency=250ms)[0].Value.AsInt64() = %d, want %d", got, want)
		}
	})

	t.Run("uint values that fit int64 stay numeric and larger values are strings", func(t *testing.T) {
		m := kvsByKey(ConvertMapsToKVs(map[string]any{
			"u":     uint(1),
			"u64":   uint64(2),
			"large": uint64(1) << 63,
		}))
		if got := m["u"].AsInt64(); got != 1 {
			t.Errorf(`ConvertMapsToKVs(u=uint(1))["u"].AsInt64() = %d, want 1`, got)
		}
		if got := m["u64"].AsInt64(); got != 2 {
			t.Errorf(`ConvertMapsToKVs(u64=uint64(2))["u64"].AsInt64() = %d, want 2`, got)
		}
		if got := m["large"].AsString(); got != "9223372036854775808" {
			t.Errorf(`ConvertMapsToKVs(large=2^63)["large"].AsString() = %q, want "9223372036854775808"`, got)
		}
	})

	t.Run("slices map to OTel slice types", func(t *testing.T) {
		m := kvsByKey(ConvertMapsToKVs(map[string]any{
			"tags":  []string{"a", "b"},
			"nums":  []float64{1.5},
			"flags": []bool{true},
		}))
		if got, want := m["tags"].AsStringSlice(), []string{"a", "b"}; !slices.Equal(got, want) {
			t.Errorf("ConvertMapsToKVs(tags=[a b])[\"tags\"].AsStringSlice() = %q, want %q", got, want)
		}
		if got, want := m["nums"].AsFloat64Slice(), []float64{1.5}; !slices.Equal(got, want) {
			t.Errorf("ConvertMapsToKVs(nums=[1.5])[\"nums\"].AsFloat64Slice() = %v, want %v", got, want)
		}
		if got, want := m["flags"].AsBoolSlice(), []bool{true}; !slices.Equal(got, want) {
			t.Errorf("ConvertMapsToKVs(flags=[true])[\"flags\"].AsBoolSlice() = %v, want %v", got, want)
		}
	})

	t.Run("error and fmt.Stringer values are stringified", func(t *testing.T) {
		m := kvsByKey(ConvertMapsToKVs(map[string]any{
			"err":  errors.New("boom"),
			"name": myStringer("x"),
		}))
		if got := m["err"].AsString(); got != "boom" {
			t.Errorf(`ConvertMapsToKVs(err=boom)["err"].AsString() = %q, want "boom"`, got)
		}
		if got := m["name"].AsString(); got != "x" {
			t.Errorf(`ConvertMapsToKVs(name=myStringer(x))["name"].AsString() = %q, want "x"`, got)
		}
	})

	t.Run("typed nil error and Stringer values are omitted", func(t *testing.T) {
		var nilErr *nilError
		var nilStr *nilStringer
		kvs := ConvertMapsToKVs(map[string]any{
			"err":  nilErr,
			"name": nilStr,
			"kept": "v",
		})
		if len(kvs) != 1 {
			t.Fatalf("len(ConvertMapsToKVs(typed nils, kept)) = %d, want 1", len(kvs))
		}
		if got, want := kvs[0].Key, attribute.Key("kept"); got != want {
			t.Errorf("ConvertMapsToKVs(...)[0].Key = %q, want %q", got, want)
		}
	})

	t.Run("nil values are omitted", func(t *testing.T) {
		kvs := ConvertMapsToKVs(map[string]any{"empty": nil, "kept": "v"})
		if len(kvs) != 1 {
			t.Fatalf("len(ConvertMapsToKVs(nil value, kept)) = %d, want 1", len(kvs))
		}
		if got, want := kvs[0].Key, attribute.Key("kept"); got != want {
			t.Errorf("ConvertMapsToKVs(...)[0].Key = %q, want %q", got, want)
		}
	})

	t.Run("empty and invalid attribute keys are omitted", func(t *testing.T) {
		kvs := ConvertMapsToKVs(map[string]any{
			"":        1,
			"...":     2,
			"foo bar": 3,
			"foo/bar": 4,
			"ok":      5,
		})
		if len(kvs) != 1 {
			t.Fatalf("len(ConvertMapsToKVs(invalid keys, ok)) = %d, want 1", len(kvs))
		}
		if got, want := kvs[0].Key, attribute.Key("ok"); got != want {
			t.Errorf("ConvertMapsToKVs(...)[0].Key = %q, want %q", got, want)
		}
	})

	t.Run("attribute.KeyValue values are used verbatim", func(t *testing.T) {
		kvs := ConvertMapsToKVs(map[string]any{
			"status": attribute.Int("http.response.status_code", 200),
			"invalid": attribute.KeyValue{
				Key:   attribute.Key("foo bar"),
				Value: attribute.StringValue("x"),
			},
		})
		if len(kvs) != 1 {
			t.Fatalf("len(ConvertMapsToKVs(KeyValue values)) = %d, want 1", len(kvs))
		}
		if got, want := kvs[0].Key, attribute.Key("http.response.status_code"); got != want {
			t.Errorf("ConvertMapsToKVs(...)[0].Key = %q, want %q", got, want)
		}
		if got := kvs[0].Value.AsInt64(); got != 200 {
			t.Errorf("ConvertMapsToKVs(status=attribute.Int(200))[0].Value.AsInt64() = %d, want 200", got)
		}
	})

	t.Run("the result is sorted by attribute key", func(t *testing.T) {
		kvs := ConvertMapsToKVs(map[string]any{"b": 1, "a": 2})
		if len(kvs) != 2 {
			t.Fatalf("len(ConvertMapsToKVs(b, a)) = %d, want 2", len(kvs))
		}
		if got, want := kvs[0].Key, attribute.Key("a"); got != want {
			t.Errorf("ConvertMapsToKVs(b, a)[0].Key = %q, want %q", got, want)
		}
		if got, want := kvs[1].Key, attribute.Key("b"); got != want {
			t.Errorf("ConvertMapsToKVs(b, a)[1].Key = %q, want %q", got, want)
		}
	})
}

func TestConvertMapsToKVs_Merge(t *testing.T) {
	t.Run("later maps override earlier ones", func(t *testing.T) {
		kvs := ConvertMapsToKVs(
			map[string]any{"order_id": "1", "env": "custom"},
			ConvertAppInfoToMap(identity.Info{Name: "svc", Version: "1.0", Environment: "prod"}),
		)
		m := make(map[string]string)
		for _, kv := range kvs {
			m[string(kv.Key)] = kv.Value.AsString()
		}
		if got := m["order.id"]; got != "1" {
			t.Errorf(`ConvertMapsToKVs(order_id=1, env=custom, ...)["order.id"] = %q, want "1"`, got)
		}
		if got := m["env"]; got != "custom" {
			t.Errorf(`ConvertMapsToKVs(env=custom, ...)["env"] = %q, want "custom"`, got)
		}
		if got := m["service.name"]; got != "svc" {
			t.Errorf(`ConvertMapsToKVs(...)["service.name"] = %q, want "svc"`, got)
		}
		if got := m["service.version"]; got != "1.0" {
			t.Errorf(`ConvertMapsToKVs(...)["service.version"] = %q, want "1.0"`, got)
		}
		if got := m["deployment.environment.name"]; got != "prod" {
			t.Errorf(`ConvertMapsToKVs(...)["deployment.environment.name"] = %q, want "prod"`, got)
		}
	})

	t.Run("a later map's value overrides an earlier colliding key", func(t *testing.T) {
		kvs := ConvertMapsToKVs(
			map[string]any{"service.name": "spoofed"},
			ConvertAppInfoToMap(identity.Info{Name: "svc", Version: "1.0", Environment: "prod"}),
		)
		m := make(map[string]string)
		for _, kv := range kvs {
			m[string(kv.Key)] = kv.Value.AsString()
		}
		if got := m["service.name"]; got != "svc" {
			t.Errorf(`ConvertMapsToKVs(...)["service.name"] = %q, want "svc"`, got)
		}
		if got := m["service.version"]; got != "1.0" {
			t.Errorf(`ConvertMapsToKVs(...)["service.version"] = %q, want "1.0"`, got)
		}
		if got := m["deployment.environment.name"]; got != "prod" {
			t.Errorf(`ConvertMapsToKVs(...)["deployment.environment.name"] = %q, want "prod"`, got)
		}
	})

	t.Run("keys normalizing to the same value collapse deterministically", func(t *testing.T) {
		kvs := ConvertMapsToKVs(map[string]any{"order_id": "under", "order.id": "dot"})
		m := make(map[string]string)
		for _, kv := range kvs {
			m[string(kv.Key)] = kv.Value.AsString()
		}
		if got := m["order.id"]; got != "under" {
			t.Errorf(`ConvertMapsToKVs(order_id=under, order.id=dot)["order.id"] = %q, want "under"`, got)
		}
		if len(kvs) != 1 {
			t.Errorf("ConvertMapsToKVs(order_id=under, order.id=dot) len = %d, want 1", len(kvs))
		}
	})
}

func TestConvertAppInfoToMap(t *testing.T) {
	t.Run("returns the full service identity", func(t *testing.T) {
		m := ConvertAppInfoToMap(identity.Info{Name: "svc", Version: "1.0", Environment: "prod"})
		if got := m["service.name"]; got != "svc" {
			t.Errorf(`ConvertAppInfoToMap(...)["service.name"] = %v, want "svc"`, got)
		}
		if got := m["service.version"]; got != "1.0" {
			t.Errorf(`ConvertAppInfoToMap(...)["service.version"] = %v, want "1.0"`, got)
		}
		if got := m["deployment.environment.name"]; got != "prod" {
			t.Errorf(`ConvertAppInfoToMap(...)["deployment.environment.name"] = %v, want "prod"`, got)
		}
	})

	t.Run("omits empty optional identity fields", func(t *testing.T) {
		m := ConvertAppInfoToMap(identity.Info{Name: "svc"})
		if got := m["service.name"]; got != "svc" {
			t.Errorf(`ConvertAppInfoToMap(...)["service.name"] = %v, want "svc"`, got)
		}
		if _, ok := m["service.version"]; ok {
			t.Error(`ConvertAppInfoToMap(Name only) has "service.version", want it omitted`)
		}
		if _, ok := m["deployment.environment.name"]; ok {
			t.Error(`ConvertAppInfoToMap(Name only) has "deployment.environment.name", want it omitted`)
		}
	})
}

func BenchmarkNormalizeKey(b *testing.B) {
	for b.Loop() {
		_ = NormalizeKey("http.response.status_code")
	}
}

func BenchmarkNormalizeKeyDeveloperStyle(b *testing.B) {
	for b.Loop() {
		_ = NormalizeKey("order_id")
	}
}

func BenchmarkConvertMapsToKVs(b *testing.B) {
	attrs := map[string]any{
		"http.request.method":       "GET",
		"http.route":                "/users/:id",
		"http.response.status_code": 200,
		"network.protocol.name":     "http",
		"network.protocol.version":  "1.1",
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = ConvertMapsToKVs(attrs)
	}
}
