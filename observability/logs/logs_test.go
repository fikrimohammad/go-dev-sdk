package logs

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/bytedance/mockey"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
)

func testInfo() appinfo.Info {
	return appinfo.Info{Name: "testapp", Version: "1.0.0", Environment: "test"}
}

// fakeLogger implements Logger and records nothing.
type fakeLogger struct{}

func (*fakeLogger) Debug(context.Context, string, ...any) {}
func (*fakeLogger) Info(context.Context, string, ...any)  {}
func (*fakeLogger) Warn(context.Context, string, ...any)  {}
func (*fakeLogger) Error(context.Context, string, ...any) {}

// mustLogger builds a Logger, panicking on error (test configs are valid).
func mustLogger(info appinfo.Info, cfg Config) Logger {
	l, err := New(info, cfg)
	if err != nil {
		panic(err)
	}
	return l
}

// withDefault restores the stderr default logger after the test, isolating the
// package facade from other tests.
func withDefault(t *testing.T) {
	t.Helper()
	SetDefault(newDefaultLogger())
}

func TestLogger_LevelRouting_Text(t *testing.T) {
	Convey("text logger routes by severity", t, func() {
		var outBuf, errBuf bytes.Buffer
		mockey.PatchConvey("redirect streams", func() {
			mockey.MockValue(&stdout).To(&outBuf)
			mockey.MockValue(&stderr).To(&errBuf)

			l := mustLogger(testInfo(), Config{Format: FormatText})
			l.Debug(context.Background(), "debug_msg", "key", "val")
			l.Info(context.Background(), "info_msg")
			l.Warn(context.Background(), "warn_msg")
			l.Error(context.Background(), "error_msg")

			So(outBuf.String(), ShouldContainSubstring, "debug_msg")
			So(outBuf.String(), ShouldContainSubstring, "info_msg")
			So(outBuf.String(), ShouldContainSubstring, "key=val")
			So(outBuf.String(), ShouldNotContainSubstring, "warn_msg")
			So(outBuf.String(), ShouldNotContainSubstring, "error_msg")

			So(errBuf.String(), ShouldContainSubstring, "warn_msg")
			So(errBuf.String(), ShouldContainSubstring, "error_msg")
		})
	})
}

func TestLogger_LevelRouting_JSON(t *testing.T) {
	Convey("json logger routes by severity", t, func() {
		var outBuf, errBuf bytes.Buffer
		mockey.PatchConvey("redirect streams", func() {
			mockey.MockValue(&stdout).To(&outBuf)
			mockey.MockValue(&stderr).To(&errBuf)

			l := mustLogger(testInfo(), Config{Format: FormatJSON})
			l.Info(context.Background(), "info_json")
			l.Error(context.Background(), "error_json")

			So(outBuf.String(), ShouldContainSubstring, `"level":"INFO"`)
			So(outBuf.String(), ShouldContainSubstring, `"msg":"info_json"`)
			So(errBuf.String(), ShouldContainSubstring, `"level":"ERROR"`)
			So(errBuf.String(), ShouldContainSubstring, `"msg":"error_json"`)
		})
	})
}

func TestLogger_LevelFilter(t *testing.T) {
	Convey("records below the configured level are dropped", t, func() {
		var outBuf, errBuf bytes.Buffer
		mockey.PatchConvey("redirect streams", func() {
			mockey.MockValue(&stdout).To(&outBuf)
			mockey.MockValue(&stderr).To(&errBuf)

			l := mustLogger(testInfo(), Config{Level: "warn"})
			l.Debug(context.Background(), "filtered debug")
			l.Info(context.Background(), "filtered info")
			l.Warn(context.Background(), "kept warn")
			l.Error(context.Background(), "kept error")

			So(outBuf.String()+errBuf.String(), ShouldNotContainSubstring, "filtered")
			So(errBuf.String(), ShouldContainSubstring, "kept warn")
			So(errBuf.String(), ShouldContainSubstring, "kept error")
		})
	})
}

func TestLogger_NormalizeArgs(t *testing.T) {
	Convey("normalizeArgs", t, func() {
		Convey("attribute keys are normalized in log args", func() {
			var outBuf bytes.Buffer
			mockey.PatchConvey("redirect stdout", func() {
				mockey.MockValue(&stdout).To(&outBuf)

				l := mustLogger(testInfo(), Config{})
				l.Info(context.Background(), "with_args",
					"order_id", 123, "shop_id", 456, slog.String("user_id", "u1"))

				text := outBuf.String()
				So(text, ShouldContainSubstring, "order.id=123")
				So(text, ShouldContainSubstring, "shop.id=456")
				So(text, ShouldContainSubstring, "user.id=u1")
				So(text, ShouldNotContainSubstring, "order_id")
				So(text, ShouldNotContainSubstring, "user_id")
			})
		})

		Convey("handles mixed pairs and attrs", func() {
			out := normalizeArgs([]any{"order_id", 1, slog.String("user_id", "u1")})
			So(out[0], ShouldEqual, "order.id")
			So(out[1], ShouldEqual, 1)
			attr, ok := out[2].(slog.Attr)
			So(ok, ShouldBeTrue)
			So(attr.Key, ShouldEqual, "user.id")
		})
	})
}

func TestLogger_GlobalKV(t *testing.T) {
	Convey("GlobalKV and identity attrs are attached and normalized", t, func() {
		var outBuf, errBuf bytes.Buffer
		mockey.PatchConvey("redirect streams", func() {
			mockey.MockValue(&stdout).To(&outBuf)
			mockey.MockValue(&stderr).To(&errBuf)

			l := mustLogger(appinfo.Info{Name: "svc", Version: "1.0", Environment: "prod"},
				Config{GlobalKV: map[string]any{"order_id": "x", "env": "test"}})
			l.Info(context.Background(), "with_global")
			l.Warn(context.Background(), "warn_global")

			for _, buf := range []string{outBuf.String(), errBuf.String()} {
				So(buf, ShouldContainSubstring, "order.id=x")
				So(buf, ShouldContainSubstring, "env=test")
				So(buf, ShouldContainSubstring, "service.name=svc")
				So(buf, ShouldContainSubstring, "service.version=1.0")
				So(buf, ShouldContainSubstring, "deployment.environment=prod")
			}
		})
	})

	Convey("identity wins over colliding GlobalKV keys", t, func() {
		var outBuf bytes.Buffer
		mockey.PatchConvey("redirect stdout", func() {
			mockey.MockValue(&stdout).To(&outBuf)

			l := mustLogger(appinfo.Info{Name: "svc", Version: "1.0", Environment: "prod"},
				Config{GlobalKV: map[string]any{"service.name": "spoofed", "service.version": "0.0.0"}})
			l.Info(context.Background(), "identity_wins")

			text := outBuf.String()
			So(text, ShouldNotContainSubstring, "spoofed")
			So(text, ShouldNotContainSubstring, "0.0.0")
			So(text, ShouldContainSubstring, "service.name=svc")
			So(text, ShouldContainSubstring, "service.version=1.0")
		})
	})
}

func TestLogger_NilContext(t *testing.T) {
	Convey("a nil context is tolerated", t, func() {
		var outBuf bytes.Buffer
		mockey.PatchConvey("redirect stdout", func() {
			mockey.MockValue(&stdout).To(&outBuf)

			l := mustLogger(testInfo(), Config{})
			l.Info(nil, "nil_ctx_ok") //nolint:staticcheck // intentionally verifying nil handling
			So(outBuf.String(), ShouldContainSubstring, "nil_ctx_ok")
		})
	})
}

func TestLogger_DefaultLogger(t *testing.T) {
	Convey("the default logger writes text to stderr", t, func() {
		var errBuf bytes.Buffer
		mockey.PatchConvey("redirect stderr", func() {
			mockey.MockValue(&stderr).To(&errBuf)

			l := newDefaultLogger()
			l.Info(context.Background(), "default_msg", "k", "v")
			So(errBuf.String(), ShouldContainSubstring, "default_msg")
			So(errBuf.String(), ShouldContainSubstring, "level=INFO")
		})
	})
}

func TestDefault_NeverNil(t *testing.T) {
	Convey("Default returns a non-nil logger before any SetDefault", t, func() {
		So(Default(), ShouldNotBeNil)
	})
}

func TestDefault_BeforeSetDefault(t *testing.T) {
	Convey("before SetDefault the package funcs write to the default stderr logger", t, func() {
		var errBuf bytes.Buffer
		mockey.PatchConvey("redirect stderr", func() {
			mockey.MockValue(&stderr).To(&errBuf)
			Info(context.Background(), "pre_set_msg", "k", "v")
			So(errBuf.String(), ShouldContainSubstring, "pre_set_msg")
		})
	})
}

func TestFacade_SetDefault(t *testing.T) {
	Convey("SetDefault routes the package funcs through the installed logger", t, func() {
		withDefault(t)
		fakeLog := &fakeLogger{}
		SetDefault(fakeLog)
		So(Default(), ShouldEqual, fakeLog)

		Debug(context.Background(), "x")
		Info(context.Background(), "x")
		Warn(context.Background(), "x")
		Error(context.Background(), "x")
		So(true, ShouldBeTrue)
	})
}

func TestFacade_LastWins(t *testing.T) {
	Convey("repeated SetDefault calls replace the default", t, func() {
		withDefault(t)
		first, second := &fakeLogger{}, &fakeLogger{}
		SetDefault(first)
		So(Default(), ShouldEqual, first)
		SetDefault(second)
		So(Default(), ShouldEqual, second)
	})
}

func TestFacade_NewAndSetDefault(t *testing.T) {
	Convey("a configured logger can be installed via New + SetDefault", t, func() {
		withDefault(t)
		var outBuf bytes.Buffer
		mockey.PatchConvey("redirect stdout", func() {
			mockey.MockValue(&stdout).To(&outBuf)

			SetDefault(mustLogger(testInfo(), Config{}))
			Info(context.Background(), "installed_msg")
			So(outBuf.String(), ShouldContainSubstring, "installed_msg")
		})
	})
}
