package metrics

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
)

func testInfo() identity.Info {
	return identity.Info{Name: "testapp", Version: "1.0.0", Environment: "test"}
}

// fakeMetrics implements Client for facade tests.
type fakeMetrics struct {
	onStop func(context.Context) error
}

func (*fakeMetrics) Add(context.Context, Instrument, int64, map[string]any) error      { return nil }
func (*fakeMetrics) Record(context.Context, Instrument, float64, map[string]any) error { return nil }
func (*fakeMetrics) AddUpDown(context.Context, Instrument, int64, map[string]any) error {
	return nil
}
func (*fakeMetrics) RecordGauge(context.Context, Instrument, float64, map[string]any) error {
	return nil
}
func (*fakeMetrics) Provider() metric.MeterProvider { return nil }

func (f *fakeMetrics) Stop(ctx context.Context) error {
	if f.onStop != nil {
		return f.onStop(ctx)
	}
	return nil
}

// newTestMetrics creates a Client backed by a manual reader so
// recorded metrics can be inspected.
func newTestMetrics(t *testing.T) (*otelMetrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return newInstrumentClient(provider, "myapp", provider.Shutdown), reader
}

// withDefault clears any SetDefault override for the duration of the test and
// restores the OTel global meter provider afterward, isolating the package
// facade from other tests.
func withDefault(t *testing.T) {
	t.Helper()
	prev := otel.GetMeterProvider()
	SetDefault(nil)
	t.Cleanup(func() {
		SetDefault(nil)
		if otel.GetMeterProvider() != prev {
			otel.SetMeterProvider(prev)
		}
	})
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	rm := metricdata.ResourceMetrics{}
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return rm
}

func TestOtelMetrics_Add_Basic(t *testing.T) {
	c, reader := newTestMetrics(t)
	ctx := context.Background()

	if err := c.Add(ctx, Instrument{Name: "requests"}, 1, map[string]any{"order_id": "1"}); err != nil {
		t.Fatalf("Add(requests, 1, order_id=1) = %v, want nil", err)
	}

	rm := collect(t, reader)
	m := findMetric(rm, "requests")
	if m == nil {
		t.Fatal(`findMetric(requests) = nil, want metric`)
	}

	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("requests data = %T, want metricdata.Sum[int64]", m.Data)
	}
	if got := len(sum.DataPoints); got != 1 {
		t.Fatalf("requests data points = %d, want 1", got)
	}
	if got := sum.DataPoints[0].Value; got != 1 {
		t.Errorf("Add(requests, 1, order_id=1) value = %d, want 1", got)
	}

	seen := map[string]string{}
	for _, kv := range sum.DataPoints[0].Attributes.ToSlice() {
		seen[string(kv.Key)] = kv.Value.AsString()
	}
	if got := seen["order.id"]; got != "1" {
		t.Errorf("Add(requests, 1, order_id=1) attribute order.id = %q, want %q", got, "1")
	}
}

func TestOtelMetrics_Add_Accumulates(t *testing.T) {
	c, reader := newTestMetrics(t)
	ctx := context.Background()

	if err := c.Add(ctx, Instrument{Name: "requests"}, 3, nil); err != nil {
		t.Fatalf("Add(requests, 3) = %v, want nil", err)
	}
	if err := c.Add(ctx, Instrument{Name: "requests"}, 2, nil); err != nil {
		t.Fatalf("Add(requests, 2) = %v, want nil", err)
	}

	rm := collect(t, reader)
	m := findMetric(rm, "requests")
	if m == nil {
		t.Fatal(`findMetric(requests) = nil, want metric`)
	}

	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("requests data = %T, want metricdata.Sum[int64]", m.Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("requests data points = %d, want 1", len(sum.DataPoints))
	}
	if got := sum.DataPoints[0].Value; got != 5 {
		t.Errorf("Add(requests, 3) then Add(requests, 2) value = %d, want 5", got)
	}
}

func TestOtelMetrics_Record(t *testing.T) {
	c, reader := newTestMetrics(t)
	ctx := context.Background()

	if err := c.Record(ctx, Instrument{Name: "latency"}, 0.1, map[string]any{"path": "/api"}); err != nil {
		t.Fatalf("Record(latency, 0.1) = %v, want nil", err)
	}
	if err := c.Record(ctx, Instrument{Name: "latency"}, 0.3, map[string]any{"path": "/api"}); err != nil {
		t.Fatalf("Record(latency, 0.3) = %v, want nil", err)
	}

	rm := collect(t, reader)
	m := findMetric(rm, "latency")
	if m == nil {
		t.Fatal(`findMetric(latency) = nil, want metric`)
	}

	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("latency data = %T, want metricdata.Histogram[float64]", m.Data)
	}
	if got := len(hist.DataPoints); got != 1 {
		t.Fatalf("latency data points = %d, want 1", got)
	}
	if got := hist.DataPoints[0].Count; got != 2 {
		t.Errorf("Record(latency, 0.1) and Record(latency, 0.3) count = %d, want 2", got)
	}
}

func TestOtelMetrics_InstrumentMetadata(t *testing.T) {
	c, reader := newTestMetrics(t)
	ctx := context.Background()

	if err := c.Record(ctx,
		Instrument{Name: "http.server.request.duration", Unit: "s", Description: "request duration"},
		0.25,
		map[string]any{"http.route": "/users/:id"},
	); err != nil {
		t.Fatalf("Record(http.server.request.duration, 0.25) = %v, want nil", err)
	}

	rm := collect(t, reader)
	m := findMetric(rm, "http.server.request.duration")
	if m == nil {
		t.Fatal(`findMetric(http.server.request.duration) = nil, want metric`)
	}
	if m.Unit != "s" {
		t.Errorf("Record(http.server.request.duration, 0.25) unit = %q, want %q", m.Unit, "s")
	}
	if m.Description != "request duration" {
		t.Errorf("Record(http.server.request.duration, 0.25) description = %q, want %q", m.Description, "request duration")
	}
	if len(rm.ScopeMetrics) != 1 {
		t.Fatalf("collected ScopeMetrics = %d, want 1", len(rm.ScopeMetrics))
	}
	if got := rm.ScopeMetrics[0].Scope.Name; got != "myapp" {
		t.Errorf("Record(http.server.request.duration, 0.25) scope name = %q, want %q", got, "myapp")
	}
}

func TestOtelMetrics_GaugeAndUpDown(t *testing.T) {
	c, reader := newTestMetrics(t)
	ctx := context.Background()

	if err := c.AddUpDown(ctx, Instrument{Name: "queue.depth", Unit: "{item}"}, 5, nil); err != nil {
		t.Fatalf("AddUpDown(queue.depth, 5) = %v, want nil", err)
	}
	if err := c.RecordGauge(ctx, Instrument{Name: "pool.utilization", Unit: "1"}, 0.5, nil); err != nil {
		t.Fatalf("RecordGauge(pool.utilization, 0.5) = %v, want nil", err)
	}

	rm := collect(t, reader)
	upDown := findMetric(rm, "queue.depth")
	if upDown == nil {
		t.Fatal(`findMetric(queue.depth) = nil, want metric`)
	}
	sum, ok := upDown.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("queue.depth data = %T, want metricdata.Sum[int64]", upDown.Data)
	}
	if sum.IsMonotonic {
		t.Error("AddUpDown(queue.depth, 5) IsMonotonic = true, want false")
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("queue.depth data points = %d, want 1", len(sum.DataPoints))
	}
	if got := sum.DataPoints[0].Value; got != 5 {
		t.Errorf("AddUpDown(queue.depth, 5) value = %d, want 5", got)
	}

	gaugeMetric := findMetric(rm, "pool.utilization")
	if gaugeMetric == nil {
		t.Fatal(`findMetric(pool.utilization) = nil, want metric`)
	}
	gauge, ok := gaugeMetric.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("pool.utilization data = %T, want metricdata.Gauge[float64]", gaugeMetric.Data)
	}
	if len(gauge.DataPoints) != 1 {
		t.Fatalf("pool.utilization data points = %d, want 1", len(gauge.DataPoints))
	}
	if got := gauge.DataPoints[0].Value; got != 0.5 {
		t.Errorf("RecordGauge(pool.utilization, 0.5) value = %v, want 0.5", got)
	}
}

func TestOtelMetrics_InvalidName(t *testing.T) {
	c, _ := newTestMetrics(t)
	ctx := context.Background()

	for _, name := range []string{"", "!!!", "1bad", "bad name", "bad/name", strings.Repeat("a", 256)} {
		if err := c.Add(ctx, Instrument{Name: name}, 1, nil); !errors.Is(err, ErrInvalidMetricName) {
			t.Errorf("Add(%q) = %v, want ErrInvalidMetricName", name, err)
		}
		if err := c.Record(ctx, Instrument{Name: name}, 0.5, nil); !errors.Is(err, ErrInvalidMetricName) {
			t.Errorf("Record(%q) = %v, want ErrInvalidMetricName", name, err)
		}
	}

	count := 0
	c.instruments.Range(func(_, _ any) bool { count++; return true })
	if count != 0 {
		t.Errorf("Add/Record with invalid names cached instruments = %d, want 0", count)
	}
}

func TestInstrumentNameValidation(t *testing.T) {
	t.Run("validateInstrument accepts OTel names", func(t *testing.T) {
		for _, name := range []string{
			"simple",
			"with-dash",
			"with.dot",
			"a",
			"http.server.request.duration",
			"db.response.status_code",
		} {
			got, err := validateInstrument(Instrument{Name: name})
			if err != nil {
				t.Errorf("validateInstrument(%q) error = %v, want nil", name, err)
				continue
			}
			if got != name {
				t.Errorf("validateInstrument(%q) = %q, want %q", name, got, name)
			}
		}
	})

	t.Run("validateInstrument rejects invalid names", func(t *testing.T) {
		for _, name := range []string{
			"",
			"1bad",
			"with space",
			"special!@#$chars",
			"bad/name",
			"CamelCase",
			"_leading",
		} {
			if _, err := validateInstrument(Instrument{Name: name}); !errors.Is(err, ErrInvalidMetricName) {
				t.Errorf("validateInstrument(%q) error = %v, want ErrInvalidMetricName", name, err)
			}
		}
	})
}

func TestOtelMetrics_Caching(t *testing.T) {
	c, _ := newTestMetrics(t)
	ctx := context.Background()

	if err := c.Add(ctx, Instrument{Name: "requests"}, 1, nil); err != nil {
		t.Fatalf("Add(requests, 1) = %v, want nil", err)
	}
	if err := c.Add(ctx, Instrument{Name: "requests"}, 1, nil); err != nil {
		t.Fatalf("Add(requests, 1) = %v, want nil", err)
	}
	if err := c.Record(ctx, Instrument{Name: "latency"}, 0.1, nil); err != nil {
		t.Fatalf("Record(latency, 0.1) = %v, want nil", err)
	}
	if err := c.Record(ctx, Instrument{Name: "latency"}, 0.2, nil); err != nil {
		t.Fatalf("Record(latency, 0.2) = %v, want nil", err)
	}

	v, ok := c.instruments.Load("requests")
	if !ok {
		t.Fatal(`Add(requests, 1) cache entry = missing, want present`)
	}
	entry := v.(*instrumentEntry)
	if entry.kind != kindCounter {
		t.Errorf("Add(requests, 1) cached kind = %v, want %v", entry.kind, kindCounter)
	}
	if entry.counter == nil {
		t.Error("Add(requests, 1) cached counter = nil, want non-nil")
	}

	v, ok = c.instruments.Load("latency")
	if !ok {
		t.Fatal(`Record(latency, 0.1) cache entry = missing, want present`)
	}
	entry = v.(*instrumentEntry)
	if entry.kind != kindHistogram {
		t.Errorf("Record(latency, 0.1) cached kind = %v, want %v", entry.kind, kindHistogram)
	}
	if entry.histogram == nil {
		t.Error("Record(latency, 0.1) cached histogram = nil, want non-nil")
	}
}

func TestOtelMetrics_InstrumentConflict(t *testing.T) {
	c, _ := newTestMetrics(t)
	ctx := context.Background()

	if err := c.Add(ctx, Instrument{Name: "requests", Unit: "1"}, 1, nil); err != nil {
		t.Fatalf("Add(requests, unit=1) = %v, want nil", err)
	}
	if err := c.Add(ctx, Instrument{Name: "requests", Unit: "s"}, 1, nil); !errors.Is(err, ErrInstrumentConflict) {
		t.Errorf("Add(requests, unit=s) = %v, want ErrInstrumentConflict", err)
	}
	if err := c.Record(ctx, Instrument{Name: "requests"}, 0.5, nil); !errors.Is(err, ErrInstrumentConflict) {
		t.Errorf("Record(requests) = %v, want ErrInstrumentConflict", err)
	}
}

func TestOtelMetrics_DistinctNames(t *testing.T) {
	c, reader := newTestMetrics(t)
	ctx := context.Background()

	if err := c.Add(ctx, Instrument{Name: "foo-bar"}, 1, nil); err != nil {
		t.Fatalf("Add(foo-bar, 1) = %v, want nil", err)
	}
	if err := c.Add(ctx, Instrument{Name: "foo_bar"}, 2, nil); err != nil {
		t.Fatalf("Add(foo_bar, 2) = %v, want nil", err)
	}
	if err := c.Add(ctx, Instrument{Name: "foo.bar"}, 3, nil); err != nil {
		t.Fatalf("Add(foo.bar, 3) = %v, want nil", err)
	}

	rm := collect(t, reader)
	for _, name := range []string{"foo-bar", "foo_bar", "foo.bar"} {
		if findMetric(rm, name) == nil {
			t.Errorf("Add(%q, ...) metric = nil, want metric", name)
		}
	}
}

func TestOtelMetrics_Provider(t *testing.T) {
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	c := newInstrumentClient(provider, "scope", provider.Shutdown)
	if got := c.Provider(); got != provider {
		t.Errorf("Provider() = %v, want %v", got, provider)
	}
}

func TestWrap(t *testing.T) {
	t.Run("Wrap uses a custom provider and the default scope", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

		c := Wrap(provider)
		if got := c.Provider(); got != provider {
			t.Errorf("Wrap(provider).Provider() = %v, want %v", got, provider)
		}
		if err := c.Add(context.Background(), Instrument{Name: "wrapped.count"}, 1, nil); err != nil {
			t.Fatalf("Add(wrapped.count, 1) = %v, want nil", err)
		}

		rm := collect(t, reader)
		if findMetric(rm, "wrapped.count") == nil {
			t.Error(`findMetric(wrapped.count) = nil, want metric`)
		}
		if len(rm.ScopeMetrics) != 1 {
			t.Fatalf("collected ScopeMetrics = %d, want 1", len(rm.ScopeMetrics))
		}
		if got := rm.ScopeMetrics[0].Scope.Name; got != DefaultInstrumentationScope {
			t.Errorf("Wrap(provider) scope name = %q, want %q", got, DefaultInstrumentationScope)
		}
		if err := c.Stop(context.Background()); err != nil {
			t.Errorf("Stop() = %v, want nil", err)
		}
	})

	t.Run("WrapWithScope uses the requested scope", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

		c := WrapWithScope(provider, "custom.scope")
		if err := c.Record(context.Background(), Instrument{Name: "wrapped.histogram"}, 0.5, nil); err != nil {
			t.Fatalf("Record(wrapped.histogram, 0.5) = %v, want nil", err)
		}

		rm := collect(t, reader)
		if len(rm.ScopeMetrics) != 1 {
			t.Fatalf("collected ScopeMetrics = %d, want 1", len(rm.ScopeMetrics))
		}
		if got := rm.ScopeMetrics[0].Scope.Name; got != "custom.scope" {
			t.Errorf("WrapWithScope(provider, custom.scope) scope name = %q, want %q", got, "custom.scope")
		}
	})

	t.Run("Wrap(nil) is safe", func(t *testing.T) {
		c := Wrap(nil)
		if c == nil {
			t.Fatal("Wrap(nil) = nil, want non-nil")
		}
		if c.Provider() == nil {
			t.Error("Wrap(nil).Provider() = nil, want non-nil")
		}
		if err := c.Add(context.Background(), Instrument{Name: "noop.count"}, 1, nil); err != nil {
			t.Errorf("Add(noop.count, 1) = %v, want nil", err)
		}
		if err := c.Stop(context.Background()); err != nil {
			t.Errorf("Stop() = %v, want nil", err)
		}
	})
}

func TestNewMeterProvider_Resource(t *testing.T) {
	cfg := Config{Scope: "custom.scope", GlobalKV: map[string]any{"deployment.environment.name": "staging"}}
	cfg.setDefaults()
	info := identity.Info{Name: "testapp", Version: "1.0.0", Environment: "production"}

	reader := sdkmetric.NewManualReader()
	provider, err := newMeterProvider(context.Background(), info, cfg, reader)
	if err != nil {
		t.Fatalf("newMeterProvider() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	c := newInstrumentClient(provider, cfg.Scope, provider.Shutdown)
	if err := c.Add(context.Background(), Instrument{Name: "resource.count", Unit: "{request}"}, 1, nil); err != nil {
		t.Fatalf("Add(resource.count, 1) = %v, want nil", err)
	}

	rm := collect(t, reader)
	if rm.Resource == nil {
		t.Fatal("newMeterProvider(...) collected Resource = nil, want non-nil")
	}
	attrs := map[string]string{}
	for _, kv := range rm.Resource.Attributes() {
		attrs[string(kv.Key)] = kv.Value.AsString()
	}
	wantAttrs := map[string]string{
		"service.name":                "testapp",
		"service.version":             "1.0.0",
		"deployment.environment.name": "production",
	}
	for key, want := range wantAttrs {
		if got := attrs[key]; got != want {
			t.Errorf("newMeterProvider(info) resource attribute %s = %q, want %q", key, got, want)
		}
	}
	if len(rm.ScopeMetrics) != 1 {
		t.Fatalf("collected ScopeMetrics = %d, want 1", len(rm.ScopeMetrics))
	}
	if got := rm.ScopeMetrics[0].Scope.Name; got != "custom.scope" {
		t.Errorf("newMeterProvider(Scope=custom.scope) scope name = %q, want %q", got, "custom.scope")
	}
}

func TestOtelMetrics_Concurrent(t *testing.T) {
	c, _ := newTestMetrics(t)
	ctx := context.Background()

	// Passing means no panic or data race under -race; there is no value to assert.
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			_ = c.Add(ctx, Instrument{Name: "requests"}, 1, nil)
			_ = c.Record(ctx, Instrument{Name: "latency"}, 0.1, nil)
			_ = c.AddUpDown(ctx, Instrument{Name: "queue.depth"}, 1, nil)
			_ = c.RecordGauge(ctx, Instrument{Name: "pool.utilization"}, 0.5, nil)
		})
	}
	wg.Wait()
}

func TestOtelMetrics_Stop(t *testing.T) {
	t.Run("Stop is idempotent and shuts the provider exactly once", func(t *testing.T) {
		c, _ := newTestMetrics(t)
		var shut atomic.Int32
		orig := c.shutdown
		c.shutdown = func(ctx context.Context) error { shut.Add(1); return orig(ctx) }

		if err := c.Stop(context.Background()); err != nil {
			t.Errorf("first Stop() = %v, want nil", err)
		}
		if err := c.Stop(context.Background()); err != nil {
			t.Errorf("second Stop() = %v, want nil", err)
		}
		if got := shut.Load(); got != 1 {
			t.Errorf("Stop() x2 shutdown calls = %d, want 1", got)
		}
	})

	t.Run("Stop rejects subsequent measurements with ErrStopped", func(t *testing.T) {
		c, _ := newTestMetrics(t)
		if err := c.Stop(context.Background()); err != nil {
			t.Fatalf("Stop() = %v, want nil", err)
		}
		if err := c.Add(context.Background(), Instrument{Name: "counter"}, 1, nil); !errors.Is(err, ErrStopped) {
			t.Errorf("Stop() then Add(counter, 1) = %v, want ErrStopped", err)
		}
		if err := c.Record(context.Background(), Instrument{Name: "hist"}, 1, nil); !errors.Is(err, ErrStopped) {
			t.Errorf("Stop() then Record(hist, 1) = %v, want ErrStopped", err)
		}
		if err := c.AddUpDown(context.Background(), Instrument{Name: "updown"}, 1, nil); !errors.Is(err, ErrStopped) {
			t.Errorf("Stop() then AddUpDown(updown, 1) = %v, want ErrStopped", err)
		}
		if err := c.RecordGauge(context.Background(), Instrument{Name: "gauge"}, 1, nil); !errors.Is(err, ErrStopped) {
			t.Errorf("Stop() then RecordGauge(gauge, 1) = %v, want ErrStopped", err)
		}
	})

	t.Run("Stop returns the same shutdown error on repeated calls", func(t *testing.T) {
		c, _ := newTestMetrics(t)
		boom := errors.New("shutdown failed")
		c.shutdown = func(context.Context) error { return boom }

		if err := c.Stop(context.Background()); !errors.Is(err, boom) {
			t.Errorf("first Stop() = %v, want %v", err, boom)
		}
		if err := c.Stop(context.Background()); !errors.Is(err, boom) {
			t.Errorf("second Stop() = %v, want %v", err, boom)
		}
	})
}

func TestNoopMetrics(t *testing.T) {
	m := noopMetrics{}
	if err := m.Add(context.Background(), Instrument{Name: "counter"}, 1, nil); err != nil {
		t.Errorf("noopMetrics.Add(counter, 1) = %v, want nil", err)
	}
	if err := m.Record(context.Background(), Instrument{Name: "hist"}, 0.5, nil); err != nil {
		t.Errorf("noopMetrics.Record(hist, 0.5) = %v, want nil", err)
	}
	if err := m.AddUpDown(context.Background(), Instrument{Name: "updown"}, 1, nil); err != nil {
		t.Errorf("noopMetrics.AddUpDown(updown, 1) = %v, want nil", err)
	}
	if err := m.RecordGauge(context.Background(), Instrument{Name: "gauge"}, 0.5, nil); err != nil {
		t.Errorf("noopMetrics.RecordGauge(gauge, 0.5) = %v, want nil", err)
	}
	if m.Provider() == nil {
		t.Error("noopMetrics.Provider() = nil, want non-nil")
	}
	if err := m.Stop(context.Background()); err != nil {
		t.Errorf("noopMetrics.Stop() = %v, want nil", err)
	}
}

func TestNew_Validation(t *testing.T) {
	t.Run("New rejects an empty app name", func(t *testing.T) {
		_, err := New(context.Background(), identity.Info{}, Config{})
		if !errors.Is(err, attributes.ErrMissingServiceName) {
			t.Errorf("New(empty info) = %v, want ErrMissingServiceName", err)
		}
	})

	t.Run("New rejects an invalid config", func(t *testing.T) {
		_, err := New(context.Background(), testInfo(), Config{Timeout: -1})
		if err == nil {
			t.Error("New(negative timeout) = nil, want error")
		}
	})
}

func TestDefault_FollowsOTelGlobal(t *testing.T) {
	prev := otel.GetMeterProvider()
	SetDefault(nil)
	t.Cleanup(func() {
		SetDefault(nil)
		if otel.GetMeterProvider() != prev {
			otel.SetMeterProvider(prev)
		}
	})
	ctx := context.Background()

	// Measured before any install: the OTel global is a no-op, so this is dropped.
	if err := Add(ctx, Instrument{Name: "early.counter"}, 1, nil); err != nil {
		t.Fatalf("Add(early.counter, 1) before install = %v, want nil", err)
	}

	r1 := sdkmetric.NewManualReader()
	p1 := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r1))
	t.Cleanup(func() { _ = p1.Shutdown(context.Background()) })
	otel.SetMeterProvider(p1)
	if err := Add(ctx, Instrument{Name: "early.counter"}, 1, nil); err != nil {
		t.Fatalf("Add(early.counter, 1) after first install = %v, want nil", err)
	}
	if err := Add(ctx, Instrument{Name: "late.counter"}, 1, nil); err != nil {
		t.Fatalf("Add(late.counter, 1) after first install = %v, want nil", err)
	}
	rm1 := collect(t, r1)
	for _, name := range []string{"early.counter", "late.counter"} {
		if findMetric(rm1, name) == nil {
			t.Errorf("Add(%q) after first otel.SetMeterProvider: metric not collected, want it", name)
		}
	}

	r2 := sdkmetric.NewManualReader()
	p2 := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r2))
	t.Cleanup(func() { _ = p2.Shutdown(context.Background()) })
	otel.SetMeterProvider(p2)
	if err := Add(ctx, Instrument{Name: "second.counter"}, 1, nil); err != nil {
		t.Fatalf("Add(second.counter, 1) after second install = %v, want nil", err)
	}
	rm2 := collect(t, r2)
	if findMetric(rm2, "second.counter") == nil {
		t.Error("Add(second.counter) after second otel.SetMeterProvider: metric not collected by second provider, want it")
	}
}

// valueMP is a meter provider whose dynamic type is an uncomparable struct
// value, so comparing it with == panics.
type valueMP struct {
	noop.MeterProvider
	tags []string
}

func TestDefault_UncomparableProvider(t *testing.T) {
	prev := otel.GetMeterProvider()
	SetDefault(nil)
	t.Cleanup(func() {
		SetDefault(nil)
		otel.SetMeterProvider(prev)
	})
	otel.SetMeterProvider(valueMP{tags: []string{"value-typed"}})

	ctx := context.Background()
	for range 2 {
		if err := Default().Add(ctx, Instrument{Name: "uncomparable.counter"}, 1, nil); err != nil {
			t.Errorf("Default().Add(uncomparable.counter, 1) with value-typed provider = %v, want nil", err)
		}
	}
}

func TestDefault_NeverNil(t *testing.T) {
	withDefault(t)
	if Default() == nil {
		t.Error("Default() before any SetDefault = nil, want non-nil")
	}
}

func TestDefault_BeforeSetDefault(t *testing.T) {
	withDefault(t)
	if Default() == nil {
		t.Fatal("Default() before SetDefault = nil, want non-nil")
	}
	// Measurements are dropped without error while the OTel global is a no-op.
	if err := Add(context.Background(), Instrument{Name: "counter"}, 1, nil); err != nil {
		t.Errorf("Add(counter, 1) before SetDefault = %v, want nil", err)
	}
	if err := Record(context.Background(), Instrument{Name: "hist"}, 0.5, nil); err != nil {
		t.Errorf("Record(hist, 0.5) before SetDefault = %v, want nil", err)
	}
	if err := AddUpDown(context.Background(), Instrument{Name: "updown"}, 1, nil); err != nil {
		t.Errorf("AddUpDown(updown, 1) before SetDefault = %v, want nil", err)
	}
	if err := RecordGauge(context.Background(), Instrument{Name: "gauge"}, 0.5, nil); err != nil {
		t.Errorf("RecordGauge(gauge, 0.5) before SetDefault = %v, want nil", err)
	}
	if err := Stop(context.Background()); err != nil {
		t.Errorf("Stop() before SetDefault = %v, want nil", err)
	}
}

func TestFacade_SetDefault(t *testing.T) {
	withDefault(t)
	c, _ := newTestMetrics(t)
	SetDefault(c)
	if got := Default(); got != c {
		t.Fatalf("SetDefault(c) then Default() = %v, want %v", got, c)
	}

	if err := Add(context.Background(), Instrument{Name: "instrument.counter", Unit: "1"}, 1, nil); err != nil {
		t.Errorf("Add(instrument.counter, 1) = %v, want nil", err)
	}
	if err := Record(context.Background(), Instrument{Name: "instrument.hist", Unit: "s"}, 0.5, nil); err != nil {
		t.Errorf("Record(instrument.hist, 0.5) = %v, want nil", err)
	}
	if err := AddUpDown(context.Background(), Instrument{Name: "instrument.updown"}, 1, nil); err != nil {
		t.Errorf("AddUpDown(instrument.updown, 1) = %v, want nil", err)
	}
	if err := RecordGauge(context.Background(), Instrument{Name: "instrument.gauge"}, 0.5, nil); err != nil {
		t.Errorf("RecordGauge(instrument.gauge, 0.5) = %v, want nil", err)
	}
}

func TestFacade_LastWins(t *testing.T) {
	withDefault(t)
	first, second := &fakeMetrics{}, &fakeMetrics{}
	SetDefault(first)
	if got := Default(); got != first {
		t.Errorf("SetDefault(first) then Default() = %v, want first", got)
	}
	SetDefault(second)
	if got := Default(); got != second {
		t.Errorf("SetDefault(second) then Default() = %v, want second", got)
	}
}

func TestFacade_StopDelegates(t *testing.T) {
	t.Run("Stop delegates to the installed client", func(t *testing.T) {
		withDefault(t)
		var shut atomic.Int32
		fakeMe := &fakeMetrics{onStop: func(context.Context) error {
			shut.Add(1)
			return nil
		}}
		SetDefault(fakeMe)

		if err := Stop(context.Background()); err != nil {
			t.Errorf("Stop() = %v, want nil", err)
		}
		if got := shut.Load(); got != 1 {
			t.Errorf("Stop() delegated shutdown calls = %d, want 1", got)
		}
	})

	t.Run("Stop propagates the installed client's error", func(t *testing.T) {
		withDefault(t)
		boom := errors.New("shutdown failed")
		SetDefault(&fakeMetrics{onStop: func(context.Context) error { return boom }})
		if err := Stop(context.Background()); !errors.Is(err, boom) {
			t.Errorf("Stop() = %v, want %v", err, boom)
		}
	})
}

func TestFacade_SetDefaultNilResets(t *testing.T) {
	withDefault(t)
	override := &fakeMetrics{}
	SetDefault(override)

	SetDefault(nil)
	if Default() == nil {
		t.Fatal("SetDefault(nil) then Default() = nil, want non-nil")
	}
	if Default() == Client(override) {
		t.Error("SetDefault(nil) then Default() still returns the cleared override, want the OTel global client")
	}
	if err := Add(context.Background(), Instrument{Name: "counter"}, 1, nil); err != nil {
		t.Errorf("Add(counter, 1) after SetDefault(nil) = %v, want nil", err)
	}
}

// fakeExporter captures shutdown calls for newClientWithExporter tests.
type fakeExporter struct {
	shutdown atomic.Int32
}

func (*fakeExporter) Temporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

func (*fakeExporter) Aggregation(sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.AggregationDefault{}
}

func (*fakeExporter) Export(context.Context, *metricdata.ResourceMetrics) error { return nil }
func (*fakeExporter) ForceFlush(context.Context) error                          { return nil }

func (f *fakeExporter) Shutdown(context.Context) error {
	f.shutdown.Add(1)
	return nil
}

func TestNewClientWithExporter(t *testing.T) {
	cfg := Config{}
	cfg.setDefaults()
	exporter := &fakeExporter{}

	c, err := newClientWithExporter(context.Background(), testInfo(), cfg, exporter)
	if err != nil {
		t.Fatalf("newClientWithExporter() error = %v, want nil", err)
	}
	if err := c.Add(context.Background(), Instrument{Name: "exporter.count"}, 1, nil); err != nil {
		t.Errorf("Add(exporter.count, 1) = %v, want nil", err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Errorf("Stop() = %v, want nil", err)
	}
	if got := exporter.shutdown.Load(); got != 1 {
		t.Errorf("newClientWithExporter(...).Stop() exporter shutdown calls = %d, want 1", got)
	}
}

func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		for i, m := range sm.Metrics {
			if m.Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

func newBenchMetrics(b *testing.B) *otelMetrics {
	b.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	b.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return newInstrumentClient(provider, "bench", provider.Shutdown)
}

func BenchmarkOtelMetrics_Add(b *testing.B) {
	client := newBenchMetrics(b)
	ctx := context.Background()
	instrument := Instrument{Name: "bench.counter", Unit: "1"}
	attrs := map[string]any{"http.response.status_code": 200, "http.request.method": "GET"}
	b.ReportAllocs()
	for b.Loop() {
		_ = client.Add(ctx, instrument, 1, attrs)
	}
}

func BenchmarkOtelMetrics_Record(b *testing.B) {
	client := newBenchMetrics(b)
	ctx := context.Background()
	instrument := Instrument{Name: "bench.duration", Unit: "s"}
	attrs := map[string]any{"http.response.status_code": 200, "http.request.method": "GET"}
	b.ReportAllocs()
	for b.Loop() {
		_ = client.Record(ctx, instrument, 0.05, attrs)
	}
}
