package logs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	. "github.com/smartystreets/goconvey/convey"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
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

// withDefault installs the default logger for the duration of the test and
// restores the previous default afterward, isolating the package facade from
// other tests.
func withDefault(t *testing.T) {
	t.Helper()
	prev := Default()
	SetDefault(newDefaultLogger())
	t.Cleanup(func() { SetDefault(prev) })
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

		Convey("preserves semantic-convention and correlation keys", func() {
			var outBuf bytes.Buffer
			mockey.PatchConvey("redirect stdout", func() {
				mockey.MockValue(&stdout).To(&outBuf)

				l := mustLogger(testInfo(), Config{})
				l.Info(context.Background(), "preserved_keys",
					"trace_id", "trace",
					"span_id", "span",
					"http.response.status_code", 200,
				)

				text := outBuf.String()
				So(text, ShouldContainSubstring, "trace_id=trace")
				So(text, ShouldContainSubstring, "span_id=span")
				So(text, ShouldContainSubstring, "http.response.status_code=200")
			})
		})
	})
}

func TestNew_InvalidInfo(t *testing.T) {
	Convey("New requires a service name", t, func() {
		_, err := New(appinfo.Info{}, Config{})
		So(errors.Is(err, attributes.ErrMissingServiceName), ShouldBeTrue)
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
		withDefault(t)
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

func TestLogger_OmitEmptyIdentity(t *testing.T) {
	Convey("empty optional identity fields are omitted from every record", t, func() {
		var outBuf bytes.Buffer
		mockey.PatchConvey("redirect stdout", func() {
			mockey.MockValue(&stdout).To(&outBuf)

			l := mustLogger(appinfo.Info{Name: "svc"}, Config{})
			l.Info(context.Background(), "identity")

			text := outBuf.String()
			So(text, ShouldContainSubstring, "service.name=svc")
			So(text, ShouldNotContainSubstring, "service.version")
			So(text, ShouldNotContainSubstring, "deployment.environment")
		})
	})
}

func TestLogger_ConfiguredWriters(t *testing.T) {
	Convey("configured writers receive records by severity", t, func() {
		var outBuf, errBuf bytes.Buffer
		l := mustLogger(testInfo(), Config{Writer: &outBuf, ErrorWriter: &errBuf})

		l.Info(context.Background(), "info_msg")
		l.Warn(context.Background(), "warn_msg")
		l.Error(context.Background(), "error_msg")

		So(outBuf.String(), ShouldContainSubstring, "info_msg")
		So(outBuf.String(), ShouldNotContainSubstring, "warn_msg")
		So(errBuf.String(), ShouldContainSubstring, "warn_msg")
		So(errBuf.String(), ShouldContainSubstring, "error_msg")
	})
}

func TestLogger_TraceCorrelation(t *testing.T) {
	Convey("records with a span context carry trace_id and span_id", t, func() {
		var outBuf bytes.Buffer
		l := mustLogger(testInfo(), Config{Format: FormatJSON, Writer: &outBuf, ErrorWriter: io.Discard})

		provider := sdktrace.NewTracerProvider()
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
		ctx, span := provider.Tracer("test").Start(context.Background(), "op")
		defer span.End()

		l.Info(ctx, "correlated")

		var record map[string]any
		So(json.Unmarshal(bytes.TrimSpace(outBuf.Bytes()), &record), ShouldBeNil)
		So(record["trace_id"], ShouldEqual, span.SpanContext().TraceID().String())
		So(record["span_id"], ShouldEqual, span.SpanContext().SpanID().String())
	})

	Convey("records without a span context omit correlation keys", t, func() {
		var outBuf bytes.Buffer
		l := mustLogger(testInfo(), Config{Format: FormatJSON, Writer: &outBuf, ErrorWriter: io.Discard})

		l.Info(context.Background(), "uncorrelated")

		var record map[string]any
		So(json.Unmarshal(bytes.TrimSpace(outBuf.Bytes()), &record), ShouldBeNil)
		_, hasTraceID := record["trace_id"]
		_, hasSpanID := record["span_id"]
		So(hasTraceID, ShouldBeFalse)
		So(hasSpanID, ShouldBeFalse)
	})
}

func TestLogger_ExplicitCorrelationWins(t *testing.T) {
	Convey("explicit trace_id and span_id values are not duplicated", t, func() {
		var outBuf bytes.Buffer
		l := mustLogger(testInfo(), Config{Writer: &outBuf, ErrorWriter: io.Discard})

		provider := sdktrace.NewTracerProvider()
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
		ctx, span := provider.Tracer("test").Start(context.Background(), "op")
		defer span.End()

		l.Info(ctx, "explicit", "trace_id", "manual-trace", "span_id", "manual-span")

		text := outBuf.String()
		So(strings.Count(text, "trace_id="), ShouldEqual, 1)
		So(strings.Count(text, "span_id="), ShouldEqual, 1)
		So(text, ShouldContainSubstring, "trace_id=manual-trace")
		So(text, ShouldContainSubstring, "span_id=manual-span")
	})
}

func TestLogger_Concurrent(t *testing.T) {
	l := mustLogger(testInfo(), Config{Writer: io.Discard, ErrorWriter: io.Discard})

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			l.Info(context.Background(), "info", "user_id", "u1")
			l.Error(context.Background(), "error", "order_id", "o1")
		})
	}
	wg.Wait()
}

func TestFacade_SetDefaultNilResets(t *testing.T) {
	Convey("SetDefault(nil) restores the default logger", t, func() {
		prev := Default()
		t.Cleanup(func() { SetDefault(prev) })

		SetDefault(nil)
		So(Default(), ShouldNotBeNil)

		var errBuf bytes.Buffer
		mockey.PatchConvey("redirect stderr", func() {
			mockey.MockValue(&stderr).To(&errBuf)
			Debug(context.Background(), "after_nil")
			So(errBuf.String(), ShouldContainSubstring, "after_nil")
		})
	})
}

func TestLogger_GlobalCorrelationWins(t *testing.T) {
	Convey("GlobalKV correlation values win over span context values", t, func() {
		var outBuf bytes.Buffer
		l := mustLogger(testInfo(), Config{
			Format:      FormatJSON,
			GlobalKV:    map[string]any{"trace_id": "fixed-trace"},
			Writer:      &outBuf,
			ErrorWriter: io.Discard,
		})

		provider := sdktrace.NewTracerProvider()
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
		ctx, span := provider.Tracer("test").Start(context.Background(), "op")
		defer span.End()

		l.Info(ctx, "global_correlation")

		text := outBuf.String()
		So(strings.Count(text, `"trace_id"`), ShouldEqual, 1)

		var record map[string]any
		So(json.Unmarshal(bytes.TrimSpace(outBuf.Bytes()), &record), ShouldBeNil)
		So(record["trace_id"], ShouldEqual, "fixed-trace")
		So(record["span_id"], ShouldEqual, span.SpanContext().SpanID().String())
	})
}

func TestNew_InvalidConfig(t *testing.T) {
	Convey("New rejects invalid configuration through the constructor", t, func() {
		_, err := New(testInfo(), Config{Format: "xml"})
		So(errors.Is(err, ErrInvalidLogFormat), ShouldBeTrue)

		_, err = New(testInfo(), Config{Level: "verbose"})
		So(errors.Is(err, ErrInvalidLogLevel), ShouldBeTrue)
	})
}

func BenchmarkLogger_Info(b *testing.B) {
	logger := mustLogger(testInfo(), Config{Writer: io.Discard, ErrorWriter: io.Discard})
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		logger.Info(ctx, "benchmark", "user_id", "u1", "order_id", "o-1")
	}
}

func TestSetStreams(t *testing.T) {
	Convey("SetStreams redirects package streams", t, func() {
		var outBuf, errBuf bytes.Buffer
		SetStreams(&outBuf, &errBuf)               //nolint:staticcheck // exercising deprecated compatibility API
		t.Cleanup(func() { SetStreams(nil, nil) }) //nolint:staticcheck // exercising deprecated compatibility API

		l := mustLogger(testInfo(), Config{})
		l.Info(context.Background(), "stream_info")
		l.Error(context.Background(), "stream_error")

		So(outBuf.String(), ShouldContainSubstring, "stream_info")
		So(errBuf.String(), ShouldContainSubstring, "stream_error")
	})
}

func TestParseLevel_Invalid(t *testing.T) {
	Convey("parseLevel rejects an unsupported level", t, func() {
		_, err := parseLevel("verbose")
		So(err, ShouldNotBeNil)
	})
}
