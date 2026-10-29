package tracer

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestConfig_Defaults(t *testing.T) {
	Convey("an empty Config gets sane defaults", t, func() {
		cfg := Config{}
		cfg.setDefaults()

		So(cfg.Endpoint, ShouldEqual, "localhost:4317")
		So(cfg.ExportTimeout, ShouldEqual, 5*time.Second)
		So(cfg.BatchTimeout, ShouldEqual, 5*time.Second)
		So(cfg.MaxQueueSize, ShouldEqual, 2048)
		So(cfg.MaxExportBatchSize, ShouldEqual, 512)
		So(cfg.Sampler, ShouldBeNil)
	})

	Convey("explicit values are preserved", t, func() {
		sampler := sdktrace.NeverSample()
		cfg := Config{
			Endpoint:           "collector:4318",
			ExportTimeout:      3 * time.Second,
			Headers:            map[string]string{"auth": "x"},
			GlobalKV:           map[string]any{"cloud.region": "eu"},
			Sampler:            sampler,
			BatchTimeout:       2 * time.Second,
			MaxQueueSize:       100,
			MaxExportBatchSize: 10,
		}
		cfg.setDefaults()
		So(cfg.Endpoint, ShouldEqual, "collector:4318")
		So(cfg.ExportTimeout, ShouldEqual, 3*time.Second)
		So(cfg.Headers, ShouldEqual, map[string]string{"auth": "x"})
		So(cfg.GlobalKV, ShouldEqual, map[string]any{"cloud.region": "eu"})
		So(cfg.Sampler, ShouldEqual, sampler)
		So(cfg.BatchTimeout, ShouldEqual, 2*time.Second)
		So(cfg.MaxQueueSize, ShouldEqual, 100)
		So(cfg.MaxExportBatchSize, ShouldEqual, 10)
	})
}

func TestConfig_Validate(t *testing.T) {
	Convey("a defaulted Config validates", t, func() {
		cfg := Config{}
		cfg.setDefaults()
		So(cfg.validate(), ShouldBeNil)
	})

	Convey("non-positive export timeout is rejected", t, func() {
		cfg := Config{ExportTimeout: -time.Second}
		cfg.setDefaults()
		So(cfg.validate(), ShouldNotBeNil)
	})

	Convey("non-positive batch settings are rejected", t, func() {
		cfg := Config{BatchTimeout: -time.Second, MaxQueueSize: -1, MaxExportBatchSize: -1}
		cfg.setDefaults()
		So(cfg.validate(), ShouldNotBeNil)
	})

	Convey("max export batch size over the queue size is rejected", t, func() {
		cfg := Config{MaxQueueSize: 10, MaxExportBatchSize: 11}
		cfg.setDefaults()
		So(cfg.validate(), ShouldNotBeNil)
	})
}

func TestConfig_IsInsecure(t *testing.T) {
	Convey("isInsecure defaults to true", t, func() {
		So(Config{}.isInsecure(), ShouldBeTrue)
	})

	Convey("isInsecure honors an explicit flag", t, func() {
		f := false
		So(Config{Insecure: &f}.isInsecure(), ShouldBeFalse)
		t := true
		So(Config{Insecure: &t}.isInsecure(), ShouldBeTrue)
	})
}
