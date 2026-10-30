package tracer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	noop "go.opentelemetry.io/otel/trace/noop"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
)

func testInfo() identity.Info {
	return identity.Info{Name: "testapp", Version: "1.0.0", Environment: "test"}
}

// fakeClient builds a *client backed by a noop provider and a configurable
// shutdown hook.
func fakeClient(shutdown func(context.Context) error) *client {
	return &client{
		provider: noop.NewTracerProvider(),
		shutdown: shutdown,
	}
}

// withDefault installs the noop default for the duration of the test and
// restores the previous default afterward, isolating the package facade from
// other tests.
func withDefault(t *testing.T) {
	t.Helper()
	prev := Default()
	SetDefault(Noop())
	t.Cleanup(func() { SetDefault(prev) })
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
		var shut atomic.Int32
		c := fakeClient(func(context.Context) error { shut.Add(1); return nil })

		So(c.Stop(context.Background()), ShouldBeNil)
		So(c.Stop(context.Background()), ShouldBeNil)
		So(shut.Load(), ShouldEqual, 1)
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
	Convey("New rejects an empty service name", t, func() {
		_, err := New(context.Background(), identity.Info{}, Config{})
		So(errors.Is(err, attributes.ErrMissingServiceName), ShouldBeTrue)
	})

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
		var shut atomic.Int32
		SetDefault(fakeClient(func(context.Context) error {
			shut.Add(1)
			return nil
		}))
		So(Stop(context.Background()), ShouldBeNil)
		So(shut.Load(), ShouldEqual, 1)
	})

	Convey("Stop propagates the installed client's error", t, func() {
		withDefault(t)
		boom := errors.New("shutdown failed")
		SetDefault(fakeClient(func(context.Context) error { return boom }))
		So(errors.Is(Stop(context.Background()), boom), ShouldBeTrue)
	})
}

func TestWrap_NilProvider(t *testing.T) {
	Convey("Wrap(nil) returns a noop-backed client", t, func() {
		c := Wrap(nil)
		So(c, ShouldNotBeNil)
		_, span := c.Tracer("scope").Start(context.Background(), "op")
		So(span.SpanContext().IsValid(), ShouldBeFalse)
		span.End()
	})
}

func TestSpanIDFrom(t *testing.T) {
	Convey("extracts the span ID from a valid span context", t, func() {
		provider := sdktrace.NewTracerProvider()
		defer func() { _ = provider.Shutdown(context.Background()) }()

		ctx, span := provider.Tracer("test").Start(context.Background(), "op")
		defer span.End()

		So(SpanIDFrom(ctx), ShouldEqual, span.SpanContext().SpanID().String())
		So(SpanIDFrom(context.Background()), ShouldEqual, "")
		So(SpanIDFrom(nil), ShouldEqual, "") //nolint:staticcheck // intentionally verifying nil handling
	})
}

// fakeSpanExporter captures exported spans for client construction tests.
type fakeSpanExporter struct {
	mu       sync.Mutex
	spans    []sdktrace.ReadOnlySpan
	shutdown atomic.Int32
}

func (f *fakeSpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.spans = append(f.spans, spans...)
	return nil
}

func (f *fakeSpanExporter) Shutdown(context.Context) error {
	f.shutdown.Add(1)
	return nil
}

func (f *fakeSpanExporter) snapshot() []sdktrace.ReadOnlySpan {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]sdktrace.ReadOnlySpan, len(f.spans))
	copy(out, f.spans)
	return out
}

func TestNewClientWithExporter_Resource(t *testing.T) {
	Convey("builds a provider with resource identity, scope, and exporter shutdown", t, func() {
		cfg := Config{GlobalKV: map[string]any{"deployment.environment.name": "staging"}}
		cfg.setDefaults()
		exporter := &fakeSpanExporter{}
		info := identity.Info{Name: "svc", Version: "1.0.0", Environment: "production"}

		c, err := newClientWithExporter(context.Background(), info, cfg, exporter)
		So(err, ShouldBeNil)
		So(c.Provider(), ShouldNotBeNil)

		ctx, span := c.Tracer("custom.scope").Start(context.Background(), "op")
		span.End()

		provider := c.Provider().(*sdktrace.TracerProvider)
		So(provider.ForceFlush(context.Background()), ShouldBeNil)

		spans := exporter.snapshot()
		So(len(spans), ShouldEqual, 1)
		So(spans[0].InstrumentationScope().Name, ShouldEqual, "custom.scope")

		attrs := map[string]string{}
		for _, kv := range spans[0].Resource().Attributes() {
			attrs[string(kv.Key)] = kv.Value.AsString()
		}
		So(attrs["service.name"], ShouldEqual, "svc")
		So(attrs["service.version"], ShouldEqual, "1.0.0")
		So(attrs["deployment.environment.name"], ShouldEqual, "production")

		So(c.Stop(ctx), ShouldBeNil)
		So(exporter.shutdown.Load(), ShouldEqual, 1)
	})
}

func TestNewClientWithExporter_Sampler(t *testing.T) {
	Convey("honors the configured sampler", t, func() {
		cfg := Config{Sampler: sdktrace.NeverSample()}
		cfg.setDefaults()
		exporter := &fakeSpanExporter{}

		c, err := newClientWithExporter(context.Background(), testInfo(), cfg, exporter)
		So(err, ShouldBeNil)

		_, span := c.Tracer("test").Start(context.Background(), "op")
		So(span.SpanContext().IsSampled(), ShouldBeFalse)
		span.End()

		provider := c.Provider().(*sdktrace.TracerProvider)
		So(provider.ForceFlush(context.Background()), ShouldBeNil)
		So(len(exporter.snapshot()), ShouldEqual, 0)

		So(c.Stop(context.Background()), ShouldBeNil)
		So(exporter.shutdown.Load(), ShouldEqual, 1)
	})
}

func TestSetGlobal(t *testing.T) {
	Convey("installs the provider and W3C propagator", t, func() {
		prevProvider := otel.GetTracerProvider()
		prevPropagator := otel.GetTextMapPropagator()
		t.Cleanup(func() {
			otel.SetTracerProvider(prevProvider)
			otel.SetTextMapPropagator(prevPropagator)
		})

		provider := sdktrace.NewTracerProvider()
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

		SetGlobal(Wrap(provider))
		So(otel.GetTracerProvider(), ShouldEqual, provider)

		ctx, span := provider.Tracer("test").Start(context.Background(), "op")
		defer span.End()

		carrier := propagation.MapCarrier{}
		otel.GetTextMapPropagator().Inject(ctx, carrier)
		So(carrier.Get("traceparent"), ShouldContainSubstring, span.SpanContext().TraceID().String())
	})
}

func TestSetGlobal_Nil(t *testing.T) {
	Convey("SetGlobal(nil) installs the noop provider and W3C propagator", t, func() {
		prevProvider := otel.GetTracerProvider()
		prevPropagator := otel.GetTextMapPropagator()
		t.Cleanup(func() {
			otel.SetTracerProvider(prevProvider)
			otel.SetTextMapPropagator(prevPropagator)
		})

		SetGlobal(nil)
		So(otel.GetTracerProvider(), ShouldNotBeNil)
		So(otel.GetTextMapPropagator().Fields(), ShouldContain, "traceparent")
	})
}
