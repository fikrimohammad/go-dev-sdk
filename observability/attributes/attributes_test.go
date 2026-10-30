package attributes

import (
	"errors"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"go.opentelemetry.io/otel/attribute"

	"github.com/fikrimohammad/go-dev-sdk/identity"
)

type myStringer string

func (s myStringer) String() string { return string(s) }

type nilStringer struct{ s string }

func (s *nilStringer) String() string { return s.s }

type nilError struct{ s string }

func (e *nilError) Error() string { return e.s }

func TestNormalizeKey(t *testing.T) {
	Convey("NormalizeKey maps developer keys to OTel style", t, func() {
		So(NormalizeKey("order_id"), ShouldEqual, "order.id")
		So(NormalizeKey("shop_id"), ShouldEqual, "shop.id")
		So(NormalizeKey("HTTP_METHOD"), ShouldEqual, "http.method")
		So(NormalizeKey("request_id"), ShouldEqual, "request.id")
	})

	Convey("NormalizeKey preserves keys that already contain a dot", t, func() {
		So(NormalizeKey("service.name"), ShouldEqual, "service.name")
		So(NormalizeKey("http.response.status_code"), ShouldEqual, "http.response.status_code")
		So(NormalizeKey("http.request.body.size"), ShouldEqual, "http.request.body.size")
		So(NormalizeKey("user_agent.original"), ShouldEqual, "user_agent.original")
		So(NormalizeKey("exception.stacktrace"), ShouldEqual, "exception.stacktrace")
	})

	Convey("NormalizeKey preserves standard correlation keys", t, func() {
		So(NormalizeKey("trace_id"), ShouldEqual, "trace_id")
		So(NormalizeKey("span_id"), ShouldEqual, "span_id")
		So(NormalizeKey("trace_flags"), ShouldEqual, "trace_flags")
	})

	Convey("NormalizeKey trims stray underscores and dots", t, func() {
		So(NormalizeKey("__a__"), ShouldEqual, "a")
		So(NormalizeKey("_order_id"), ShouldEqual, "order.id")
		So(NormalizeKey("a..b"), ShouldEqual, "a.b")
		So(NormalizeKey("a__b"), ShouldEqual, "a.b")
		So(NormalizeKey("..."), ShouldEqual, "")
		So(NormalizeKey(".http.route."), ShouldEqual, "http.route")
	})

	Convey("NormalizeKey leaves invalid characters for validation to reject", t, func() {
		So(NormalizeKey("foo bar"), ShouldEqual, "foo bar")
		So(NormalizeKey("foo/bar"), ShouldEqual, "foo/bar")
	})
}

func TestIsValidKey(t *testing.T) {
	Convey("IsValidKey accepts OTel-style keys and rejects invalid ones", t, func() {
		for _, key := range []string{
			"a",
			"order.id",
			"http.response.status_code",
			"user_agent.original",
			"a-b_c.d",
			"9lives",
		} {
			So(IsValidKey(key), ShouldBeTrue)
		}
		for _, key := range []string{
			"",
			"foo bar",
			"foo/bar",
			"foo%bar",
			"foo:bar",
		} {
			So(IsValidKey(key), ShouldBeFalse)
		}
	})
}

func TestValidateAppInfo(t *testing.T) {
	Convey("ValidateAppInfo requires a non-empty service name", t, func() {
		So(ValidateAppInfo(identity.Info{Name: "svc"}), ShouldBeNil)
		So(ValidateAppInfo(identity.Info{Name: "my service"}), ShouldBeNil)
		So(errors.Is(ValidateAppInfo(identity.Info{}), ErrMissingServiceName), ShouldBeTrue)
		So(errors.Is(ValidateAppInfo(identity.Info{Name: "   "}), ErrMissingServiceName), ShouldBeTrue)
	})
}

func TestConvertMapsToKVs(t *testing.T) {
	Convey("no maps, nil, and empty maps yield nil", t, func() {
		So(ConvertMapsToKVs(), ShouldBeNil)
		So(ConvertMapsToKVs(nil), ShouldBeNil)
		So(ConvertMapsToKVs(map[string]any{}), ShouldBeNil)
	})

	Convey("developer keys are normalized and values typed", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{
			"user_id": "u1",
			"count":   int(1),
			"big":     int64(2),
			"ratio":   0.5,
			"enabled": true,
			"other":   struct{ name string }{name: "x"},
		})
		m := make(map[attribute.Key]attribute.Value)
		for _, kv := range kvs {
			m[kv.Key] = kv.Value
		}
		So(m["user.id"].AsString(), ShouldEqual, "u1")
		So(m["count"].AsInt64(), ShouldEqual, int64(1))
		So(m["big"].AsInt64(), ShouldEqual, int64(2))
		So(m["ratio"].AsFloat64(), ShouldEqual, 0.5)
		So(m["enabled"].AsBool(), ShouldBeTrue)
		So(m["other"].AsString(), ShouldEqual, "{x}")
	})

	Convey("semantic-convention and correlation keys are preserved", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{
			"http.response.status_code": 200,
			"user_agent.original":       "ua",
			"trace_id":                  "trace",
			"span_id":                   "span",
		})
		m := make(map[attribute.Key]attribute.Value)
		for _, kv := range kvs {
			m[kv.Key] = kv.Value
		}
		So(m["http.response.status_code"].AsInt64(), ShouldEqual, int64(200))
		So(m["user_agent.original"].AsString(), ShouldEqual, "ua")
		So(m["trace_id"].AsString(), ShouldEqual, "trace")
		So(m["span_id"].AsString(), ShouldEqual, "span")
	})

	Convey("narrow integers widen to int64", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{
			"i32": int32(32),
			"i16": int16(16),
			"i8":  int8(8),
			"u32": uint32(32),
			"u16": uint16(16),
			"u8":  uint8(8),
		})
		m := make(map[attribute.Key]attribute.Value)
		for _, kv := range kvs {
			m[kv.Key] = kv.Value
		}
		So(m["i32"].AsInt64(), ShouldEqual, int64(32))
		So(m["i16"].AsInt64(), ShouldEqual, int64(16))
		So(m["i8"].AsInt64(), ShouldEqual, int64(8))
		So(m["u32"].AsInt64(), ShouldEqual, int64(32))
		So(m["u16"].AsInt64(), ShouldEqual, int64(16))
		So(m["u8"].AsInt64(), ShouldEqual, int64(8))
	})

	Convey("float32 widens to float64", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{"ratio": float32(0.5)})
		So(kvs[0].Value.AsFloat64(), ShouldAlmostEqual, 0.5)
	})

	Convey("time.Duration is recorded as nanoseconds", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{"latency": 250 * time.Millisecond})
		So(kvs[0].Value.AsInt64(), ShouldEqual, int64(250*time.Millisecond))
	})

	Convey("uint values that fit int64 stay numeric and larger values are strings", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{
			"u":     uint(1),
			"u64":   uint64(2),
			"large": uint64(1) << 63,
		})
		m := make(map[attribute.Key]attribute.Value)
		for _, kv := range kvs {
			m[kv.Key] = kv.Value
		}
		So(m["u"].AsInt64(), ShouldEqual, int64(1))
		So(m["u64"].AsInt64(), ShouldEqual, int64(2))
		So(m["large"].AsString(), ShouldEqual, "9223372036854775808")
	})

	Convey("slices map to OTel slice types", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{
			"tags":  []string{"a", "b"},
			"nums":  []float64{1.5},
			"flags": []bool{true},
		})
		m := make(map[attribute.Key]attribute.Value)
		for _, kv := range kvs {
			m[kv.Key] = kv.Value
		}
		So(m["tags"].AsStringSlice(), ShouldResemble, []string{"a", "b"})
		So(m["nums"].AsFloat64Slice(), ShouldResemble, []float64{1.5})
		So(m["flags"].AsBoolSlice(), ShouldResemble, []bool{true})
	})

	Convey("error and fmt.Stringer values are stringified", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{
			"err":  errors.New("boom"),
			"name": myStringer("x"),
		})
		m := make(map[attribute.Key]attribute.Value)
		for _, kv := range kvs {
			m[kv.Key] = kv.Value
		}
		So(m["err"].AsString(), ShouldEqual, "boom")
		So(m["name"].AsString(), ShouldEqual, "x")
	})

	Convey("typed nil error and Stringer values are omitted", t, func() {
		var nilErr *nilError
		var nilStr *nilStringer
		kvs := ConvertMapsToKVs(map[string]any{
			"err":  nilErr,
			"name": nilStr,
			"kept": "v",
		})
		So(len(kvs), ShouldEqual, 1)
		So(kvs[0].Key, ShouldEqual, attribute.Key("kept"))
	})

	Convey("nil values are omitted", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{"empty": nil, "kept": "v"})
		So(len(kvs), ShouldEqual, 1)
		So(kvs[0].Key, ShouldEqual, attribute.Key("kept"))
	})

	Convey("empty and invalid attribute keys are omitted", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{
			"":        1,
			"...":     2,
			"foo bar": 3,
			"foo/bar": 4,
			"ok":      5,
		})
		So(len(kvs), ShouldEqual, 1)
		So(kvs[0].Key, ShouldEqual, attribute.Key("ok"))
	})

	Convey("attribute.KeyValue values are used verbatim", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{
			"status": attribute.Int("http.response.status_code", 200),
			"invalid": attribute.KeyValue{
				Key:   attribute.Key("foo bar"),
				Value: attribute.StringValue("x"),
			},
		})
		So(len(kvs), ShouldEqual, 1)
		So(kvs[0].Key, ShouldEqual, attribute.Key("http.response.status_code"))
		So(kvs[0].Value.AsInt64(), ShouldEqual, int64(200))
	})

	Convey("the result is sorted by attribute key", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{"b": 1, "a": 2})
		So(len(kvs), ShouldEqual, 2)
		So(kvs[0].Key, ShouldEqual, attribute.Key("a"))
		So(kvs[1].Key, ShouldEqual, attribute.Key("b"))
	})
}

func TestConvertMapsToKVs_Merge(t *testing.T) {
	Convey("later maps override earlier ones", t, func() {
		kvs := ConvertMapsToKVs(
			map[string]any{"order_id": "1", "env": "custom"},
			ConvertAppInfoToMap(identity.Info{Name: "svc", Version: "1.0", Environment: "prod"}),
		)
		m := make(map[string]string)
		for _, kv := range kvs {
			m[string(kv.Key)] = kv.Value.AsString()
		}
		So(m["order.id"], ShouldEqual, "1")
		So(m["env"], ShouldEqual, "custom")
		So(m["service.name"], ShouldEqual, "svc")
		So(m["service.version"], ShouldEqual, "1.0")
		So(m["deployment.environment.name"], ShouldEqual, "prod")
	})

	Convey("a later map's value overrides an earlier colliding key", t, func() {
		kvs := ConvertMapsToKVs(
			map[string]any{"service.name": "spoofed"},
			ConvertAppInfoToMap(identity.Info{Name: "svc", Version: "1.0", Environment: "prod"}),
		)
		m := make(map[string]string)
		for _, kv := range kvs {
			m[string(kv.Key)] = kv.Value.AsString()
		}
		So(m["service.name"], ShouldEqual, "svc")
		So(m["service.version"], ShouldEqual, "1.0")
		So(m["deployment.environment.name"], ShouldEqual, "prod")
	})

	Convey("keys normalizing to the same value collapse deterministically", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{"order_id": "under", "order.id": "dot"})
		m := make(map[string]string)
		for _, kv := range kvs {
			m[string(kv.Key)] = kv.Value.AsString()
		}
		So(m["order.id"], ShouldEqual, "under")
		So(len(kvs), ShouldEqual, 1)
	})
}

func TestConvertAppInfoToMap(t *testing.T) {
	Convey("returns the full service identity", t, func() {
		m := ConvertAppInfoToMap(identity.Info{Name: "svc", Version: "1.0", Environment: "prod"})
		So(m["service.name"], ShouldEqual, "svc")
		So(m["service.version"], ShouldEqual, "1.0")
		So(m["deployment.environment.name"], ShouldEqual, "prod")
	})

	Convey("omits empty optional identity fields", t, func() {
		m := ConvertAppInfoToMap(identity.Info{Name: "svc"})
		So(m["service.name"], ShouldEqual, "svc")
		_, hasVersion := m["service.version"]
		_, hasEnvironment := m["deployment.environment.name"]
		So(hasVersion, ShouldBeFalse)
		So(hasEnvironment, ShouldBeFalse)
	})
}

func BenchmarkNormalizeKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NormalizeKey("http.response.status_code")
	}
}

func BenchmarkNormalizeKeyDeveloperStyle(b *testing.B) {
	for i := 0; i < b.N; i++ {
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
	for i := 0; i < b.N; i++ {
		_ = ConvertMapsToKVs(attrs)
	}
}
