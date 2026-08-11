package apiserver

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestConfig_Defaults(t *testing.T) {
	Convey("an empty Config gets sane defaults", t, func() {
		cfg := Config{}
		cfg.setDefaults()

		So(cfg.Addr, ShouldEqual, ":3000")
		So(cfg.ReadTimeout, ShouldEqual, 30*time.Second)
		So(cfg.WriteTimeout, ShouldEqual, 30*time.Second)
		So(cfg.ShutdownTimeout, ShouldEqual, 10*time.Second)
	})

	Convey("explicit values are preserved", t, func() {
		cfg := Config{Addr: "127.0.0.1:8080", ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, ShutdownTimeout: 3 * time.Second}
		cfg.setDefaults()
		So(cfg.Addr, ShouldEqual, "127.0.0.1:8080")
		So(cfg.ReadTimeout, ShouldEqual, 5*time.Second)
		So(cfg.WriteTimeout, ShouldEqual, 10*time.Second)
		So(cfg.ShutdownTimeout, ShouldEqual, 3*time.Second)
	})
}

func TestConfig_Validate(t *testing.T) {
	Convey("a defaulted Config validates", t, func() {
		cfg := Config{}
		cfg.setDefaults()
		So(cfg.validate(), ShouldBeNil)
	})

	Convey("empty addr is rejected", t, func() {
		cfg := Config{Addr: ""}
		So(cfg.validate(), ShouldNotBeNil)
	})

	Convey("invalid addr host:port is rejected", t, func() {
		cfg := Config{Addr: "not a host:port"}
		So(cfg.validate(), ShouldNotBeNil)
	})

	Convey("non-positive shutdown timeout is rejected", t, func() {
		cfg := Config{Addr: ":3000", ShutdownTimeout: 0}
		So(cfg.validate(), ShouldNotBeNil)
	})

	Convey("negative read timeout is rejected", t, func() {
		cfg := Config{Addr: ":3000", ReadTimeout: -time.Second}
		So(cfg.validate(), ShouldNotBeNil)
	})

	Convey("negative write timeout is rejected", t, func() {
		cfg := Config{Addr: ":3000", WriteTimeout: -time.Second}
		So(cfg.validate(), ShouldNotBeNil)
	})
}
