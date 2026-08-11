package tracer

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestConfig_Defaults(t *testing.T) {
	Convey("an empty Config gets sane defaults", t, func() {
		cfg := Config{}
		cfg.setDefaults()

		So(cfg.Endpoint, ShouldEqual, "localhost:4317")
		So(cfg.ExportTimeout, ShouldEqual, 5*time.Second)
	})

	Convey("explicit values are preserved", t, func() {
		cfg := Config{Endpoint: "collector:4318", ExportTimeout: 3 * time.Second, Headers: map[string]string{"auth": "x"}}
		cfg.setDefaults()
		So(cfg.Endpoint, ShouldEqual, "collector:4318")
		So(cfg.ExportTimeout, ShouldEqual, 3*time.Second)
		So(cfg.Headers, ShouldEqual, map[string]string{"auth": "x"})
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
