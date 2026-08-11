package metrics

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func boolPtr(v bool) *bool { return &v }

func TestConfig_Defaults(t *testing.T) {
	Convey("an empty Config gets sane defaults", t, func() {
		cfg := Config{}
		cfg.setDefaults()

		So(cfg.Endpoint, ShouldEqual, "localhost:4317")
		So(cfg.Timeout, ShouldEqual, 10*time.Second)
		So(cfg.ExportInterval, ShouldEqual, 10*time.Second)
		So(cfg.HistogramMaxBuckets, ShouldEqual, int32(160))
		So(cfg.HistogramMaxScale, ShouldEqual, int32(20))
	})

	Convey("explicit values are preserved", t, func() {
		cfg := Config{Endpoint: "collector:4318", Timeout: 2 * time.Second, ExportInterval: 1 * time.Minute}
		cfg.setDefaults()
		So(cfg.Endpoint, ShouldEqual, "collector:4318")
		So(cfg.Timeout, ShouldEqual, 2*time.Second)
		So(cfg.ExportInterval, ShouldEqual, 1*time.Minute)
	})

	Convey("histogram tuning values are preserved", t, func() {
		cfg := Config{HistogramMaxBuckets: 256, HistogramMaxScale: 15}
		cfg.setDefaults()
		So(cfg.HistogramMaxBuckets, ShouldEqual, int32(256))
		So(cfg.HistogramMaxScale, ShouldEqual, int32(15))
	})
}

func TestConfig_Validate(t *testing.T) {
	Convey("a defaulted Config validates", t, func() {
		cfg := Config{}
		cfg.setDefaults()
		So(cfg.validate(), ShouldBeNil)
	})

	Convey("non-positive timeout is rejected", t, func() {
		cfg := Config{Timeout: -time.Second}
		cfg.setDefaults()
		So(cfg.validate(), ShouldNotBeNil)
	})

	Convey("non-positive export interval is rejected", t, func() {
		cfg := Config{ExportInterval: -time.Second}
		cfg.setDefaults()
		So(cfg.validate(), ShouldNotBeNil)
	})

	Convey("negative histogram max buckets is rejected", t, func() {
		cfg := Config{HistogramMaxBuckets: -1}
		cfg.setDefaults()
		So(cfg.validate(), ShouldNotBeNil)
	})

	Convey("histogram max buckets over the cap is rejected", t, func() {
		cfg := Config{HistogramMaxBuckets: maxHistogramMaxBuckets + 1}
		cfg.setDefaults()
		So(cfg.validate(), ShouldNotBeNil)

		cfg = Config{HistogramMaxBuckets: maxHistogramMaxBuckets}
		cfg.setDefaults()
		So(cfg.validate(), ShouldBeNil)
	})

	Convey("out-of-range histogram max scale is rejected", t, func() {
		cfg := Config{HistogramMaxScale: 21}
		cfg.setDefaults()
		So(cfg.validate(), ShouldNotBeNil)

		cfg = Config{HistogramMaxScale: -11}
		cfg.setDefaults()
		So(cfg.validate(), ShouldNotBeNil)
	})

	Convey("multiple invalid fields are aggregated", t, func() {
		cfg := Config{Timeout: -time.Second, ExportInterval: -time.Second, HistogramMaxBuckets: -1}
		cfg.setDefaults()
		err := cfg.validate()
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "timeout")
		So(err.Error(), ShouldContainSubstring, "export_interval")
		So(err.Error(), ShouldContainSubstring, "histogram_max_buckets")
	})
}

func TestConfig_IsInsecure(t *testing.T) {
	Convey("isInsecure defaults to insecure", t, func() {
		So(Config{}.isInsecure(), ShouldBeTrue)
	})

	Convey("isInsecure honors an explicit value", t, func() {
		So(Config{Insecure: boolPtr(false)}.isInsecure(), ShouldBeFalse)
		So(Config{Insecure: boolPtr(true)}.isInsecure(), ShouldBeTrue)
	})
}
