package tracer

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

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

// withDefault restores the OTel global tracer provider after the test if the
// test changed it, isolating the package-level funcs from other tests.
func withDefault(t *testing.T) {
	t.Helper()
	prev := otel.GetTracerProvider()
	t.Cleanup(func() {
		if otel.GetTracerProvider() != prev {
			otel.SetTracerProvider(prev)
		}
	})
}

func TestClient_StartSpan(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer func() { _ = provider.Shutdown(context.Background()) }()

	c := &client{provider: provider}
	_, span := c.Tracer("test").Start(context.Background(), "op")
	if got := span.SpanContext().IsValid(); !got {
		t.Errorf("client.Tracer(test).Start(op).IsValid() = %v, want true", got)
	}
	if got := span.SpanContext().IsSampled(); !got {
		t.Errorf("client.Tracer(test).Start(op).IsSampled() = %v, want true", got)
	}
	span.End()
}

func TestClient_Stop(t *testing.T) {
	t.Run("Stop shuts down the pipeline exactly once and returns its error", func(t *testing.T) {
		var shut atomic.Int32
		c := fakeClient(func(context.Context) error { shut.Add(1); return nil })

		if err := c.Stop(context.Background()); err != nil {
			t.Errorf("Stop() first call = %v, want nil", err)
		}
		if err := c.Stop(context.Background()); err != nil {
			t.Errorf("Stop() second call = %v, want nil", err)
		}
		if got := shut.Load(); got != 1 {
			t.Errorf("shutdown calls = %d, want 1", got)
		}
	})

	t.Run("Stop propagates the shutdown error", func(t *testing.T) {
		boom := errors.New("shutdown failed")
		c := fakeClient(func(context.Context) error { return boom })
		if err := c.Stop(context.Background()); !errors.Is(err, boom) {
			t.Errorf("Stop() = %v, want %v", err, boom)
		}
	})
}

func TestTraceIDFrom(t *testing.T) {
	t.Run("extracts the trace ID from a valid span context", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider()
		defer func() { _ = provider.Shutdown(context.Background()) }()

		ctx, span := provider.Tracer("test").Start(context.Background(), "op")
		got := traceIDFrom(ctx)
		if want := span.SpanContext().TraceID().String(); got != want {
			t.Errorf("traceIDFrom(ctx) = %q, want %q", got, want)
		}
		if got == "" {
			t.Errorf("traceIDFrom(ctx) = %q, want non-empty", got)
		}
		span.End()
	})

	t.Run("returns empty for a background context", func(t *testing.T) {
		if got := traceIDFrom(context.Background()); got != "" {
			t.Errorf("traceIDFrom(background) = %q, want empty", got)
		}
	})

	t.Run("returns empty for a nil context", func(t *testing.T) {
		if got := traceIDFrom(nil); got != "" { //nolint:staticcheck // intentionally verifying nil handling
			t.Errorf("traceIDFrom(nil) = %q, want empty", got)
		}
	})
}

func TestTracer_TraceIDFromPackageFunc(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer func() { _ = provider.Shutdown(context.Background()) }()

	ctx, span := provider.Tracer("test").Start(context.Background(), "op")
	if want, got := span.SpanContext().TraceID().String(), TraceIDFrom(ctx); got != want {
		t.Errorf("TraceIDFrom(ctx) = %q, want %q", got, want)
	}
	if got := TraceIDFrom(context.Background()); got != "" {
		t.Errorf("TraceIDFrom(background) = %q, want empty", got)
	}
	span.End()
}

func TestNoop_InvalidSpan(t *testing.T) {
	_, span := Noop().Tracer("scope").Start(context.Background(), "op")
	if got := span.SpanContext().IsValid(); got {
		t.Errorf("Noop() span IsValid() = %v, want false", got)
	}
	span.End()
}

func TestTracer_Attrs(t *testing.T) {
	t.Run("Attrs normalizes and types attribute values", func(t *testing.T) {
		kvs := Attrs(map[string]any{"order_id": 1, "ok": true, "user_id": "u1"})
		m := make(map[attribute.Key]attribute.Value)
		for _, kv := range kvs {
			m[kv.Key] = kv.Value
		}
		if got := m["order.id"].AsInt64(); got != 1 {
			t.Errorf("Attrs(order_id=1) order.id = %d, want 1", got)
		}
		if got := m["ok"].AsBool(); !got {
			t.Errorf("Attrs(ok=true) ok = %v, want true", got)
		}
		if got := m["user.id"].AsString(); got != "u1" {
			t.Errorf("Attrs(user_id=u1) user.id = %q, want %q", got, "u1")
		}
	})

	t.Run("Attrs is a pure helper and does not require a default", func(t *testing.T) {
		kvs := Attrs(map[string]any{"user_id": "u1"})
		if got := len(kvs); got != 1 {
			t.Fatalf("Attrs(user_id=u1) len = %d, want 1", got)
		}
		if got := kvs[0].Key; got != attribute.Key("user.id") {
			t.Errorf("Attrs(user_id=u1)[0].Key = %q, want %q", got, "user.id")
		}
	})
}

func TestNew_Validation(t *testing.T) {
	t.Run("New rejects an empty service name", func(t *testing.T) {
		_, err := New(context.Background(), identity.Info{}, Config{})
		if !errors.Is(err, attributes.ErrMissingServiceName) {
			t.Errorf("New(empty identity) error = %v, want %v", err, attributes.ErrMissingServiceName)
		}
	})

	t.Run("New rejects an invalid config", func(t *testing.T) {
		_, err := New(context.Background(), testInfo(), Config{ExportTimeout: -1})
		if err == nil {
			t.Error("New(ExportTimeout=-1) error = nil, want non-nil")
		}
	})
}

// valueTP is a tracer provider whose dynamic type is an uncomparable struct
// value, so comparing it with == panics.
type valueTP struct {
	noop.TracerProvider
	tags []string
}

func TestDefault_UncomparableProvider(t *testing.T) {
	withDefault(t)
	otel.SetTracerProvider(valueTP{tags: []string{"value-typed"}})

	for range 2 {
		_, span := Default().Tracer("x").Start(context.Background(), "op")
		span.End()
	}
}

func TestDefault_NeverNil(t *testing.T) {
	if Default() == nil {
		t.Error("Default() before any install = nil, want non-nil")
	}
}

func TestDefault_FollowsOTelGlobal(t *testing.T) {
	withDefault(t)

	first := tracetest.NewSpanRecorder()
	p1 := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(first))
	t.Cleanup(func() { _ = p1.Shutdown(context.Background()) })
	otel.SetTracerProvider(p1)

	_, span := Tracer("x").Start(context.Background(), "op")
	span.End()
	if got := len(first.Ended()); got != 1 {
		t.Errorf("after otel.SetTracerProvider(first), Tracer(x) ended spans in first = %d, want 1", got)
	}

	second := tracetest.NewSpanRecorder()
	p2 := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(second))
	t.Cleanup(func() { _ = p2.Shutdown(context.Background()) })
	otel.SetTracerProvider(p2)

	_, span = Tracer("x").Start(context.Background(), "op")
	span.End()
	if got := len(second.Ended()); got != 1 {
		t.Errorf("after second otel.SetTracerProvider, Tracer(x) ended spans in second = %d, want 1", got)
	}
	if got := len(first.Ended()); got != 1 {
		t.Errorf("after second otel.SetTracerProvider, first recorder ended spans = %d, want still 1", got)
	}
}

func TestDefault_BeforeInstall(t *testing.T) {
	withDefault(t)
	_, span := Tracer("scope").Start(context.Background(), "op")
	if got := span.SpanContext().IsValid(); got {
		t.Errorf("Tracer(scope).Start(op) IsValid() = %v, want false", got)
	}
	span.End()
	if got := Attrs(map[string]any{"user_id": "u1"}); got == nil {
		t.Errorf("Attrs(user_id=u1) = nil, want non-nil")
	}
	if got := TraceIDFrom(context.Background()); got != "" {
		t.Errorf("TraceIDFrom(background) = %q, want empty", got)
	}
}

func TestWrap_NilProvider(t *testing.T) {
	c := Wrap(nil)
	if c == nil {
		t.Fatal("Wrap(nil) = nil, want non-nil")
	}
	_, span := c.Tracer("scope").Start(context.Background(), "op")
	if got := span.SpanContext().IsValid(); got {
		t.Errorf("Wrap(nil).Tracer(scope).Start(op) IsValid() = %v, want false", got)
	}
	span.End()
}

func TestSpanIDFrom(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer func() { _ = provider.Shutdown(context.Background()) }()

	ctx, span := provider.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	if want, got := span.SpanContext().SpanID().String(), SpanIDFrom(ctx); got != want {
		t.Errorf("SpanIDFrom(ctx) = %q, want %q", got, want)
	}
	if got := SpanIDFrom(context.Background()); got != "" {
		t.Errorf("SpanIDFrom(background) = %q, want empty", got)
	}
	if got := SpanIDFrom(nil); got != "" { //nolint:staticcheck // intentionally verifying nil handling
		t.Errorf("SpanIDFrom(nil) = %q, want empty", got)
	}
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
	cfg := Config{GlobalKV: map[string]any{"deployment.environment.name": "staging"}}
	cfg.setDefaults()
	exporter := &fakeSpanExporter{}
	info := identity.Info{Name: "svc", Version: "1.0.0", Environment: "production"}

	c, err := newClientWithExporter(context.Background(), info, cfg, exporter)
	if err != nil {
		t.Fatalf("newClientWithExporter(svc) error = %v, want nil", err)
	}
	if c.Provider() == nil {
		t.Fatal("Provider() = nil, want non-nil")
	}

	ctx, span := c.Tracer("custom.scope").Start(context.Background(), "op")
	span.End()

	provider, ok := c.Provider().(*sdktrace.TracerProvider)
	if !ok {
		t.Fatalf("Provider() = %T, want *sdktrace.TracerProvider", c.Provider())
	}
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Errorf("ForceFlush() = %v, want nil", err)
	}

	spans := exporter.snapshot()
	if got := len(spans); got != 1 {
		t.Fatalf("exported spans = %d, want 1", got)
	}
	if got := spans[0].InstrumentationScope().Name; got != "custom.scope" {
		t.Errorf("newClientWithExporter(svc) span scope name = %q, want %q", got, "custom.scope")
	}

	attrs := map[string]string{}
	for _, kv := range spans[0].Resource().Attributes() {
		attrs[string(kv.Key)] = kv.Value.AsString()
	}
	wantAttrs := map[string]string{
		"service.name":                "svc",
		"service.version":             "1.0.0",
		"deployment.environment.name": "production",
	}
	for k, want := range wantAttrs {
		if got := attrs[k]; got != want {
			t.Errorf("newClientWithExporter(svc) resource attribute %s = %q, want %q", k, got, want)
		}
	}

	if err := c.Stop(ctx); err != nil {
		t.Errorf("Stop() = %v, want nil", err)
	}
	if got := exporter.shutdown.Load(); got != 1 {
		t.Errorf("exporter shutdown calls = %d, want 1", got)
	}
}

func TestNewClientWithExporter_Sampler(t *testing.T) {
	cfg := Config{Sampler: sdktrace.NeverSample()}
	cfg.setDefaults()
	exporter := &fakeSpanExporter{}

	c, err := newClientWithExporter(context.Background(), testInfo(), cfg, exporter)
	if err != nil {
		t.Fatalf("newClientWithExporter(NeverSample) error = %v, want nil", err)
	}

	_, span := c.Tracer("test").Start(context.Background(), "op")
	if got := span.SpanContext().IsSampled(); got {
		t.Errorf("Tracer(test).Start(op) with NeverSample IsSampled() = %v, want false", got)
	}
	span.End()

	provider, ok := c.Provider().(*sdktrace.TracerProvider)
	if !ok {
		t.Fatalf("Provider() = %T, want *sdktrace.TracerProvider", c.Provider())
	}
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Errorf("ForceFlush() = %v, want nil", err)
	}
	if got := len(exporter.snapshot()); got != 0 {
		t.Errorf("exported spans with NeverSample = %d, want 0", got)
	}

	if err := c.Stop(context.Background()); err != nil {
		t.Errorf("Stop() = %v, want nil", err)
	}
	if got := exporter.shutdown.Load(); got != 1 {
		t.Errorf("exporter shutdown calls = %d, want 1", got)
	}
}

func TestSetGlobal(t *testing.T) {
	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		if otel.GetTracerProvider() != prevProvider {
			otel.SetTracerProvider(prevProvider)
		}
		otel.SetTextMapPropagator(prevPropagator)
	})

	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	SetGlobal(Wrap(provider))
	if got := otel.GetTracerProvider(); got != provider {
		t.Errorf("SetGlobal(Wrap(provider)) then otel.GetTracerProvider() = %v, want %v", got, provider)
	}

	ctx, span := provider.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	want := span.SpanContext().TraceID().String()
	if got := carrier.Get("traceparent"); !strings.Contains(got, want) {
		t.Errorf("SetGlobal(Wrap(provider)) traceparent = %q, want it to contain trace ID %q", got, want)
	}
}

func TestSetGlobal_Nil(t *testing.T) {
	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		if otel.GetTracerProvider() != prevProvider {
			otel.SetTracerProvider(prevProvider)
		}
		otel.SetTextMapPropagator(prevPropagator)
	})

	SetGlobal(nil)
	if otel.GetTracerProvider() == nil {
		t.Error("SetGlobal(nil) then otel.GetTracerProvider() = nil, want non-nil")
	}
	if fields := otel.GetTextMapPropagator().Fields(); !slices.Contains(fields, "traceparent") {
		t.Errorf("SetGlobal(nil) propagator Fields() = %v, want it to contain %q", fields, "traceparent")
	}
}
