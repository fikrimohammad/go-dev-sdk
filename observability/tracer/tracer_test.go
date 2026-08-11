package tracer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"go.opentelemetry.io/otel/attribute"
	noop "go.opentelemetry.io/otel/trace/noop"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
)

func testInfo() appinfo.Info {
	return appinfo.Info{Name: "testapp", Version: "1.0.0", Environment: "test"}
}

// fakeClient builds a *client backed by a noop provider and a configurable
// shutdown hook.
func fakeClient(shutdown func(context.Context) error) *client {
	return &client{
		provider: noop.NewTracerProvider(),
		shutdown: shutdown,
	}
}

// withDefault restores the noop default after the test, isolating the package
// facade from other tests.
func withDefault(t *testing.T) {
	t.Helper()
	SetDefault(Noop())
}

func TestClient_StartSpan(t *testing.T) {
	Convey("client starts spans through its provider", t, func() {
		provider := sdktrace.NewTracerProvider()
		defer func() { _ = provider.Shutdown(context.Background()) }()

		c := &client{provider: provider}
		_, span := c.Tracer("test").Start(context.Background(), "op")
		So(span.SpanContext().IsValid(), ShouldBeTrue)
		So(span.SpanContext().IsSampled(), ShouldBeTrue)
		span.End()
	})
}

func TestClient_Stop(t *testing.T) {
	Convey("Stop shuts down the pipeline exactly once and returns its error", t, func() {
		var shut int32
		c := fakeClient(func(context.Context) error { atomic.AddInt32(&shut, 1); return nil })

		So(c.Stop(context.Background()), ShouldBeNil)
		So(c.Stop(context.Background()), ShouldBeNil)
		So(atomic.LoadInt32(&shut), ShouldEqual, 1)
	})

	Convey("Stop propagates the shutdown error", t, func() {
		boom := errors.New("shutdown failed")
		c := fakeClient(func(context.Context) error { return boom })
		So(errors.Is(c.Stop(context.Background()), boom), ShouldBeTrue)
	})
}

func TestTraceIDFrom(t *testing.T) {
	Convey("extracts the trace ID from a valid span context", t, func() {
		provider := sdktrace.NewTracerProvider()
		defer func() { _ = provider.Shutdown(context.Background()) }()

		ctx, span := provider.Tracer("test").Start(context.Background(), "op")
		got := traceIDFrom(ctx)
		So(got, ShouldEqual, span.SpanContext().TraceID().String())
		So(got, ShouldNotEqual, "")
		span.End()
	})

	Convey("returns empty for a background context", t, func() {
		So(traceIDFrom(context.Background()), ShouldEqual, "")
	})

	Convey("returns empty for a nil context", t, func() {
		So(traceIDFrom(nil), ShouldEqual, "") //nolint:staticcheck // intentionally verifying nil handling
	})
}

func TestTracer_TraceIDFromPackageFunc(t *testing.T) {
	Convey("TraceIDFrom works through the noop tracer", t, func() {
		provider := sdktrace.NewTracerProvider()
		defer func() { _ = provider.Shutdown(context.Background()) }()

		ctx, span := provider.Tracer("test").Start(context.Background(), "op")
		So(TraceIDFrom(ctx), ShouldEqual, span.SpanContext().TraceID().String())
		So(TraceIDFrom(context.Background()), ShouldEqual, "")
		span.End()
	})
}

func TestNoop_InvalidSpan(t *testing.T) {
	Convey("Noop produces invalid spans", t, func() {
		_, span := Noop().Tracer("scope").Start(context.Background(), "op")
		So(span.SpanContext().IsValid(), ShouldBeFalse)
		span.End()
	})
}

func TestTracer_Attrs(t *testing.T) {
	Convey("Attrs normalizes and types attribute values", t, func() {
		kvs := Attrs(map[string]any{"order_id": 1, "ok": true, "user_id": "u1"})
		m := make(map[attribute.Key]attribute.Value)
		for _, kv := range kvs {
			m[kv.Key] = kv.Value
		}
		So(m["order.id"].AsInt64(), ShouldEqual, int64(1))
		So(m["ok"].AsBool(), ShouldBeTrue)
		So(m["user.id"].AsString(), ShouldEqual, "u1")
	})

	Convey("Attrs is a pure helper and does not require a default", t, func() {
		kvs := Attrs(map[string]any{"user_id": "u1"})
		So(len(kvs), ShouldEqual, 1)
		So(kvs[0].Key, ShouldEqual, attribute.Key("user.id"))
	})
}

func TestNew_Validation(t *testing.T) {
	Convey("New rejects an invalid config", t, func() {
		_, err := New(context.Background(), testInfo(), Config{ExportTimeout: -1})
		So(err, ShouldNotBeNil)
	})
}

func TestDefault_NeverNil(t *testing.T) {
	Convey("Default returns a non-nil client before any SetDefault", t, func() {
		So(Default(), ShouldNotBeNil)
	})
}

func TestDefault_BeforeSetDefault(t *testing.T) {
	Convey("before SetDefault the package funcs are noop", t, func() {
		withDefault(t)
		_, span := Tracer("scope").Start(context.Background(), "op")
		So(span.SpanContext().IsValid(), ShouldBeFalse)
		span.End()
		So(Attrs(map[string]any{"user_id": "u1"}), ShouldNotBeNil)
		So(TraceIDFrom(context.Background()), ShouldEqual, "")
		So(Stop(context.Background()), ShouldBeNil)
	})
}

func TestFacade_SetDefault(t *testing.T) {
	Convey("SetDefault routes the package funcs through the installed client", t, func() {
		withDefault(t)
		fakeC := fakeClient(func(context.Context) error { return nil })
		SetDefault(fakeC)
		So(Default(), ShouldEqual, fakeC)
		So(Stop(context.Background()), ShouldBeNil)
	})
}

func TestFacade_LastWins(t *testing.T) {
	Convey("repeated SetDefault calls replace the default", t, func() {
		withDefault(t)
		first := fakeClient(func(context.Context) error { return nil })
		second := fakeClient(func(context.Context) error { return nil })
		SetDefault(first)
		So(Default(), ShouldEqual, first)
		SetDefault(second)
		So(Default(), ShouldEqual, second)
	})
}

func TestFacade_SetDefaultNilResets(t *testing.T) {
	Convey("SetDefault(nil) resets to the noop client", t, func() {
		SetDefault(nil)
		So(Default(), ShouldNotBeNil)
		_, span := Tracer("scope").Start(context.Background(), "op")
		So(span.SpanContext().IsValid(), ShouldBeFalse)
		span.End()
	})
}

func TestFacade_StopDelegates(t *testing.T) {
	Convey("Stop delegates to the installed client", t, func() {
		withDefault(t)
		var shut int32
		SetDefault(fakeClient(func(context.Context) error {
			atomic.AddInt32(&shut, 1)
			return nil
		}))
		So(Stop(context.Background()), ShouldBeNil)
		So(atomic.LoadInt32(&shut), ShouldEqual, 1)
	})

	Convey("Stop propagates the installed client's error", t, func() {
		withDefault(t)
		boom := errors.New("shutdown failed")
		SetDefault(fakeClient(func(context.Context) error { return boom }))
		So(errors.Is(Stop(context.Background()), boom), ShouldBeTrue)
	})
}
