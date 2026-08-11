package logs

import (
	"errors"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestConfig_Defaults(t *testing.T) {
	Convey("an empty Config gets sane defaults", t, func() {
		cfg := Config{}
		cfg.setDefaults()

		So(cfg.Format, ShouldEqual, FormatText)
		So(cfg.Level, ShouldEqual, "debug")
	})

	Convey("explicit values are preserved", t, func() {
		cfg := Config{Format: FormatJSON, Level: "error", GlobalKV: map[string]any{"env": "prod"}}
		cfg.setDefaults()
		So(cfg.Format, ShouldEqual, FormatJSON)
		So(cfg.Level, ShouldEqual, "error")
		So(cfg.GlobalKV, ShouldEqual, map[string]any{"env": "prod"})
	})
}

func TestConfig_Validate(t *testing.T) {
	Convey("a defaulted Config validates", t, func() {
		cfg := Config{}
		cfg.setDefaults()
		So(cfg.validate(), ShouldBeNil)
	})

	Convey("invalid format is rejected", t, func() {
		cfg := Config{Format: "xml"}
		cfg.setDefaults()
		err := cfg.validate()
		So(err, ShouldNotBeNil)
		So(errors.Is(err, ErrInvalidLogFormat), ShouldBeTrue)
	})

	Convey("invalid level is rejected", t, func() {
		cfg := Config{Level: "verbose"}
		cfg.setDefaults()
		err := cfg.validate()
		So(err, ShouldNotBeNil)
		So(errors.Is(err, ErrInvalidLogLevel), ShouldBeTrue)
	})

	Convey("multiple invalid fields are aggregated", t, func() {
		cfg := Config{Format: "xml", Level: "verbose"}
		cfg.setDefaults()
		err := cfg.validate()
		So(errors.Is(err, ErrInvalidLogFormat), ShouldBeTrue)
		So(errors.Is(err, ErrInvalidLogLevel), ShouldBeTrue)
	})
}
