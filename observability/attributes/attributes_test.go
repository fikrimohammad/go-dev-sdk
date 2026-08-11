package attributes

import (
	"errors"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"go.opentelemetry.io/otel/attribute"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
)

type myStringer string

func (s myStringer) String() string { return string(s) }

func TestNormalizeKey(t *testing.T) {
	Convey("NormalizeKey maps dev keys to OTel semantic conventions", t, func() {
		So(NormalizeKey("order_id"), ShouldEqual, "order.id")
		So(NormalizeKey("shop_id"), ShouldEqual, "shop.id")
		So(NormalizeKey("service.name"), ShouldEqual, "service.name")
		So(NormalizeKey("HTTP_METHOD"), ShouldEqual, "http.method")
	})

	Convey("NormalizeKey trims stray underscores and dots", t, func() {
		So(NormalizeKey("__a__"), ShouldEqual, "a")
		So(NormalizeKey("_order_id"), ShouldEqual, "order.id")
		So(NormalizeKey("a..b"), ShouldEqual, "a.b")
		So(NormalizeKey("a__b"), ShouldEqual, "a.b")
		So(NormalizeKey("..."), ShouldEqual, "")
	})
}

func TestConvertMapsToKVs(t *testing.T) {
	Convey("no maps, nil, and empty maps yield nil", t, func() {
		So(ConvertMapsToKVs(), ShouldBeNil)
		So(ConvertMapsToKVs(nil), ShouldBeNil)
		So(ConvertMapsToKVs(map[string]any{}), ShouldBeNil)
	})

	Convey("keys are normalized and values typed", t, func() {
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

	Convey("unsigned int and uint64 are recorded as decimal strings", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{"u": uint(1), "u64": uint64(2)})
		m := make(map[attribute.Key]attribute.Value)
		for _, kv := range kvs {
			m[kv.Key] = kv.Value
		}
		So(m["u"].AsString(), ShouldEqual, "1")
		So(m["u64"].AsString(), ShouldEqual, "2")
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

	Convey("nil values are omitted", t, func() {
		kvs := ConvertMapsToKVs(map[string]any{"empty": nil, "kept": "v"})
		So(len(kvs), ShouldEqual, 1)
		So(kvs[0].Key, ShouldEqual, attribute.Key("kept"))
	})

	Convey("the result is sorted by key", t, func() {
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
			ConvertAppInfoToMap(appinfo.Info{Name: "svc", Version: "1.0", Environment: "prod"}),
		)
		m := make(map[string]string)
		for _, kv := range kvs {
			m[string(kv.Key)] = kv.Value.AsString()
		}
		So(m["order.id"], ShouldEqual, "1")
		So(m["env"], ShouldEqual, "custom")
		So(m["service.name"], ShouldEqual, "svc")
		So(m["service.version"], ShouldEqual, "1.0")
		So(m["deployment.environment"], ShouldEqual, "prod")
	})

	Convey("a later map's value overrides an earlier colliding key", t, func() {
		kvs := ConvertMapsToKVs(
			map[string]any{"service.name": "spoofed"},
			ConvertAppInfoToMap(appinfo.Info{Name: "svc", Version: "1.0", Environment: "prod"}),
		)
		m := make(map[string]string)
		for _, kv := range kvs {
			m[string(kv.Key)] = kv.Value.AsString()
		}
		So(m["service.name"], ShouldEqual, "svc")
		So(m["service.version"], ShouldEqual, "1.0")
		So(m["deployment.environment"], ShouldEqual, "prod")
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
	Convey("returns the service identity as a map", t, func() {
		m := ConvertAppInfoToMap(appinfo.Info{Name: "svc", Version: "1.0", Environment: "prod"})
		So(m["service.name"], ShouldEqual, "svc")
		So(m["service.version"], ShouldEqual, "1.0")
		So(m["deployment.environment"], ShouldEqual, "prod")
	})
}
