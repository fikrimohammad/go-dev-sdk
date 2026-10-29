package metrics

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
)

func testInfo() appinfo.Info {
	return appinfo.Info{Name: "testapp", Version: "1.0.0", Environment: "test"}
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

// withDefault installs the noop default for the duration of the test and
// restores the previous default afterward, isolating the package facade from
// other tests.
func withDefault(t *testing.T) {
	t.Helper()
	prev := Default()
	SetDefault(Noop())
	t.Cleanup(func() { SetDefault(prev) })
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
	Convey("Add records a counter with normalized attributes", t, func() {
		c, reader := newTestMetrics(t)
		ctx := context.Background()

		So(c.Add(ctx, Instrument{Name: "requests"}, 1, map[string]any{"order_id": "1"}), ShouldBeNil)

		rm := collect(t, reader)
		m := findMetric(rm, "requests")
		So(m, ShouldNotBeNil)

		sum, ok := m.Data.(metricdata.Sum[int64])
		So(ok, ShouldBeTrue)
		So(len(sum.DataPoints), ShouldEqual, 1)
		So(sum.DataPoints[0].Value, ShouldEqual, int64(1))

		seen := map[string]string{}
		for _, kv := range sum.DataPoints[0].Attributes.ToSlice() {
			seen[string(kv.Key)] = kv.Value.AsString()
		}
		So(seen["order.id"], ShouldEqual, "1")
	})
}

func TestOtelMetrics_Add_Accumulates(t *testing.T) {
	Convey("repeated Add calls accumulate with identical attributes", t, func() {
		c, reader := newTestMetrics(t)
		ctx := context.Background()

		So(c.Add(ctx, Instrument{Name: "requests"}, 3, nil), ShouldBeNil)
		So(c.Add(ctx, Instrument{Name: "requests"}, 2, nil), ShouldBeNil)

		rm := collect(t, reader)
		m := findMetric(rm, "requests")
		So(m, ShouldNotBeNil)

		sum, ok := m.Data.(metricdata.Sum[int64])
		So(ok, ShouldBeTrue)
		So(sum.DataPoints[0].Value, ShouldEqual, int64(5))
	})
}

func TestOtelMetrics_Record(t *testing.T) {
	Convey("Record records histogram values", t, func() {
		c, reader := newTestMetrics(t)
		ctx := context.Background()

		So(c.Record(ctx, Instrument{Name: "latency"}, 0.1, map[string]any{"path": "/api"}), ShouldBeNil)
		So(c.Record(ctx, Instrument{Name: "latency"}, 0.3, map[string]any{"path": "/api"}), ShouldBeNil)

		rm := collect(t, reader)
		m := findMetric(rm, "latency")
		So(m, ShouldNotBeNil)

		hist, ok := m.Data.(metricdata.Histogram[float64])
		So(ok, ShouldBeTrue)
		So(len(hist.DataPoints), ShouldEqual, 1)
		So(hist.DataPoints[0].Count, ShouldEqual, 2)
	})
}

func TestOtelMetrics_InstrumentMetadata(t *testing.T) {
	Convey("instrument unit and description are exported", t, func() {
		c, reader := newTestMetrics(t)
		ctx := context.Background()

		So(c.Record(ctx,
			Instrument{Name: "http.server.request.duration", Unit: "s", Description: "request duration"},
			0.25,
			map[string]any{"http.route": "/users/:id"},
		), ShouldBeNil)

		rm := collect(t, reader)
		m := findMetric(rm, "http.server.request.duration")
		So(m, ShouldNotBeNil)
		So(m.Unit, ShouldEqual, "s")
		So(m.Description, ShouldEqual, "request duration")
		So(rm.ScopeMetrics[0].Scope.Name, ShouldEqual, "myapp")
	})
}

func TestOtelMetrics_GaugeAndUpDown(t *testing.T) {
	Convey("gauge and up-down counter instruments use the correct data model", t, func() {
		c, reader := newTestMetrics(t)
		ctx := context.Background()

		So(c.AddUpDown(ctx, Instrument{Name: "queue.depth", Unit: "{item}"}, 5, nil), ShouldBeNil)
		So(c.RecordGauge(ctx, Instrument{Name: "pool.utilization", Unit: "1"}, 0.5, nil), ShouldBeNil)

		rm := collect(t, reader)
		upDown := findMetric(rm, "queue.depth")
		So(upDown, ShouldNotBeNil)
		sum, ok := upDown.Data.(metricdata.Sum[int64])
		So(ok, ShouldBeTrue)
		So(sum.IsMonotonic, ShouldBeFalse)
		So(sum.DataPoints[0].Value, ShouldEqual, int64(5))

		gaugeMetric := findMetric(rm, "pool.utilization")
		So(gaugeMetric, ShouldNotBeNil)
		gauge, ok := gaugeMetric.Data.(metricdata.Gauge[float64])
		So(ok, ShouldBeTrue)
		So(gauge.DataPoints[0].Value, ShouldEqual, 0.5)
	})
}

func TestOtelMetrics_InvalidName(t *testing.T) {
	Convey("invalid metric names return ErrInvalidMetricName and record nothing", t, func() {
		c, _ := newTestMetrics(t)
		ctx := context.Background()

		for _, name := range []string{"", "!!!", "1bad", "bad name", "bad/name", strings.Repeat("a", 256)} {
			So(errors.Is(c.Add(ctx, Instrument{Name: name}, 1, nil), ErrInvalidMetricName), ShouldBeTrue)
			So(errors.Is(c.Record(ctx, Instrument{Name: name}, 0.5, nil), ErrInvalidMetricName), ShouldBeTrue)
		}

		count := 0
		c.instruments.Range(func(_, _ any) bool { count++; return true })
		So(count, ShouldEqual, 0)
	})
}

func TestInstrumentNameValidation(t *testing.T) {
	Convey("validateInstrument accepts OTel names and rejects invalid ones", t, func() {
		for _, name := range []string{
			"simple",
			"with-dash",
			"with.dot",
			"a",
			"http.server.request.duration",
			"db.response.status_code",
		} {
			got, err := validateInstrument(Instrument{Name: name})
			So(err, ShouldBeNil)
			So(got, ShouldEqual, name)
		}
		for _, name := range []string{
			"",
			"1bad",
			"with space",
			"special!@#$chars",
			"bad/name",
			"CamelCase",
			"_leading",
		} {
			_, err := validateInstrument(Instrument{Name: name})
			So(errors.Is(err, ErrInvalidMetricName), ShouldBeTrue)
		}
	})
}

func TestOtelMetrics_Caching(t *testing.T) {
	Convey("instruments are cached after first use", t, func() {
		c, _ := newTestMetrics(t)
		ctx := context.Background()

		So(c.Add(ctx, Instrument{Name: "requests"}, 1, nil), ShouldBeNil)
		So(c.Add(ctx, Instrument{Name: "requests"}, 1, nil), ShouldBeNil)
		So(c.Record(ctx, Instrument{Name: "latency"}, 0.1, nil), ShouldBeNil)
		So(c.Record(ctx, Instrument{Name: "latency"}, 0.2, nil), ShouldBeNil)

		v, ok := c.instruments.Load("requests")
		So(ok, ShouldBeTrue)
		entry := v.(*instrumentEntry)
		So(entry.kind, ShouldEqual, kindCounter)
		So(entry.counter, ShouldNotBeNil)

		v, ok = c.instruments.Load("latency")
		So(ok, ShouldBeTrue)
		entry = v.(*instrumentEntry)
		So(entry.kind, ShouldEqual, kindHistogram)
		So(entry.histogram, ShouldNotBeNil)
	})
}

func TestOtelMetrics_InstrumentConflict(t *testing.T) {
	Convey("reusing a name with different metadata is rejected", t, func() {
		c, _ := newTestMetrics(t)
		ctx := context.Background()

		So(c.Add(ctx, Instrument{Name: "requests", Unit: "1"}, 1, nil), ShouldBeNil)
		So(errors.Is(c.Add(ctx, Instrument{Name: "requests", Unit: "s"}, 1, nil), ErrInstrumentConflict), ShouldBeTrue)
		So(errors.Is(c.Record(ctx, Instrument{Name: "requests"}, 0.5, nil), ErrInstrumentConflict), ShouldBeTrue)
	})

}

func TestOtelMetrics_DistinctNames(t *testing.T) {
	Convey("names that were previously sanitized remain distinct", t, func() {
		c, reader := newTestMetrics(t)
		ctx := context.Background()

		So(c.Add(ctx, Instrument{Name: "foo-bar"}, 1, nil), ShouldBeNil)
		So(c.Add(ctx, Instrument{Name: "foo_bar"}, 2, nil), ShouldBeNil)
		So(c.Add(ctx, Instrument{Name: "foo.bar"}, 3, nil), ShouldBeNil)

		rm := collect(t, reader)
		for _, name := range []string{"foo-bar", "foo_bar", "foo.bar"} {
			So(findMetric(rm, name), ShouldNotBeNil)
		}
	})
}

func TestOtelMetrics_Provider(t *testing.T) {
	Convey("Provider exposes the backing meter provider", t, func() {
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

		c := newInstrumentClient(provider, "scope", provider.Shutdown)
		So(c.Provider(), ShouldEqual, provider)
	})
}

func TestWrap(t *testing.T) {
	Convey("Wrap uses a custom provider and the default scope", t, func() {
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

		c := Wrap(provider)
		So(c.Provider(), ShouldEqual, provider)
		So(c.Add(context.Background(), Instrument{Name: "wrapped.count"}, 1, nil), ShouldBeNil)

		rm := collect(t, reader)
		So(findMetric(rm, "wrapped.count"), ShouldNotBeNil)
		So(rm.ScopeMetrics[0].Scope.Name, ShouldEqual, DefaultInstrumentationScope)
		So(c.Stop(context.Background()), ShouldBeNil)
	})

	Convey("WrapWithScope uses the requested scope", t, func() {
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

		c := WrapWithScope(provider, "custom.scope")
		So(c.Record(context.Background(), Instrument{Name: "wrapped.histogram"}, 0.5, nil), ShouldBeNil)

		rm := collect(t, reader)
		So(rm.ScopeMetrics[0].Scope.Name, ShouldEqual, "custom.scope")
	})

	Convey("Wrap(nil) is safe", t, func() {
		c := Wrap(nil)
		So(c, ShouldNotBeNil)
		So(c.Provider(), ShouldNotBeNil)
		So(c.Add(context.Background(), Instrument{Name: "noop.count"}, 1, nil), ShouldBeNil)
		So(c.Stop(context.Background()), ShouldBeNil)
	})
}

func TestNewMeterProvider_Resource(t *testing.T) {
	Convey("newMeterProvider attaches identity and honors scope", t, func() {
		cfg := Config{Scope: "custom.scope", GlobalKV: map[string]any{"deployment.environment": "staging"}}
		cfg.setDefaults()
		info := appinfo.Info{Name: "testapp", Version: "1.0.0"}

		reader := sdkmetric.NewManualReader()
		provider, err := newMeterProvider(context.Background(), info, cfg, reader)
		So(err, ShouldBeNil)
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

		c := newInstrumentClient(provider, cfg.Scope, provider.Shutdown)
		So(c.Add(context.Background(), Instrument{Name: "resource.count", Unit: "{request}"}, 1, nil), ShouldBeNil)

		rm := collect(t, reader)
		So(rm.Resource, ShouldNotBeNil)
		attrs := map[string]string{}
		for _, kv := range rm.Resource.Attributes() {
			attrs[string(kv.Key)] = kv.Value.AsString()
		}
		So(attrs["service.name"], ShouldEqual, "testapp")
		So(attrs["service.version"], ShouldEqual, "1.0.0")
		So(attrs["deployment.environment"], ShouldEqual, "staging")
		So(rm.ScopeMetrics[0].Scope.Name, ShouldEqual, "custom.scope")
	})
}

func TestOtelMetrics_Concurrent(t *testing.T) {
	Convey("all instrument methods are concurrent-safe", t, func() {
		c, _ := newTestMetrics(t)
		ctx := context.Background()

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
		So(true, ShouldBeTrue)
	})
}

func TestOtelMetrics_Stop(t *testing.T) {
	Convey("Stop is idempotent and shuts the provider exactly once", t, func() {
		c, _ := newTestMetrics(t)
		var shut atomic.Int32
		orig := c.shutdown
		c.shutdown = func(ctx context.Context) error { shut.Add(1); return orig(ctx) }

		So(c.Stop(context.Background()), ShouldBeNil)
		So(c.Stop(context.Background()), ShouldBeNil)
		So(shut.Load(), ShouldEqual, 1)
	})

	Convey("Stop rejects subsequent measurements with ErrStopped", t, func() {
		c, _ := newTestMetrics(t)
		So(c.Stop(context.Background()), ShouldBeNil)
		So(errors.Is(c.Add(context.Background(), Instrument{Name: "counter"}, 1, nil), ErrStopped), ShouldBeTrue)
		So(errors.Is(c.Record(context.Background(), Instrument{Name: "hist"}, 1, nil), ErrStopped), ShouldBeTrue)
		So(errors.Is(c.AddUpDown(context.Background(), Instrument{Name: "updown"}, 1, nil), ErrStopped), ShouldBeTrue)
		So(errors.Is(c.RecordGauge(context.Background(), Instrument{Name: "gauge"}, 1, nil), ErrStopped), ShouldBeTrue)
	})

	Convey("Stop returns the same shutdown error on repeated calls", t, func() {
		c, _ := newTestMetrics(t)
		boom := errors.New("shutdown failed")
		c.shutdown = func(context.Context) error { return boom }

		So(errors.Is(c.Stop(context.Background()), boom), ShouldBeTrue)
		So(errors.Is(c.Stop(context.Background()), boom), ShouldBeTrue)
	})
}

func TestOtelMetrics_NilContext(t *testing.T) {
	Convey("nil contexts are tolerated by instrument methods", t, func() {
		c, _ := newTestMetrics(t)

		//nolint:staticcheck // intentionally verifying nil-context handling
		So(c.Add(nil, Instrument{Name: "nil.counter"}, 1, nil), ShouldBeNil)
		//nolint:staticcheck // intentionally verifying nil-context handling
		So(c.Record(nil, Instrument{Name: "nil.hist"}, 0.5, nil), ShouldBeNil)
		//nolint:staticcheck // intentionally verifying nil-context handling
		So(c.AddUpDown(nil, Instrument{Name: "nil.updown"}, 1, nil), ShouldBeNil)
		//nolint:staticcheck // intentionally verifying nil-context handling
		So(c.RecordGauge(nil, Instrument{Name: "nil.gauge"}, 0.5, nil), ShouldBeNil)
	})
}

func TestNoopMetrics(t *testing.T) {
	Convey("noop metrics discard silently", t, func() {
		m := noopMetrics{}
		So(m.Add(context.Background(), Instrument{Name: "counter"}, 1, nil), ShouldBeNil)
		So(m.Record(context.Background(), Instrument{Name: "hist"}, 0.5, nil), ShouldBeNil)
		So(m.AddUpDown(context.Background(), Instrument{Name: "updown"}, 1, nil), ShouldBeNil)
		So(m.RecordGauge(context.Background(), Instrument{Name: "gauge"}, 0.5, nil), ShouldBeNil)
		So(m.Provider(), ShouldNotBeNil)
		So(m.Stop(context.Background()), ShouldBeNil)
	})
}

func TestNew_Validation(t *testing.T) {
	Convey("New rejects an empty app name", t, func() {
		_, err := New(context.Background(), appinfo.Info{}, Config{})
		So(errors.Is(err, attributes.ErrMissingServiceName), ShouldBeTrue)
	})

	Convey("New rejects an invalid config", t, func() {
		_, err := New(context.Background(), testInfo(), Config{Timeout: -1})
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
		So(Default(), ShouldHaveSameTypeAs, noopMetrics{})
		So(Add(context.Background(), Instrument{Name: "counter"}, 1, nil), ShouldBeNil)
		So(Record(context.Background(), Instrument{Name: "hist"}, 0.5, nil), ShouldBeNil)
		So(AddUpDown(context.Background(), Instrument{Name: "updown"}, 1, nil), ShouldBeNil)
		So(RecordGauge(context.Background(), Instrument{Name: "gauge"}, 0.5, nil), ShouldBeNil)
		So(Stop(context.Background()), ShouldBeNil)
	})
}

func TestFacade_SetDefault(t *testing.T) {
	Convey("SetDefault routes the package funcs through the installed client", t, func() {
		withDefault(t)
		c, _ := newTestMetrics(t)
		SetDefault(c)
		So(Default(), ShouldEqual, c)

		So(Add(context.Background(), Instrument{Name: "instrument.counter", Unit: "1"}, 1, nil), ShouldBeNil)
		So(Record(context.Background(), Instrument{Name: "instrument.hist", Unit: "s"}, 0.5, nil), ShouldBeNil)
		So(AddUpDown(context.Background(), Instrument{Name: "instrument.updown"}, 1, nil), ShouldBeNil)
		So(RecordGauge(context.Background(), Instrument{Name: "instrument.gauge"}, 0.5, nil), ShouldBeNil)
	})
}

func TestFacade_LastWins(t *testing.T) {
	Convey("repeated SetDefault calls replace the default", t, func() {
		withDefault(t)
		first, second := &fakeMetrics{}, &fakeMetrics{}
		SetDefault(first)
		So(Default(), ShouldEqual, first)
		SetDefault(second)
		So(Default(), ShouldEqual, second)
	})
}

func TestFacade_StopDelegates(t *testing.T) {
	Convey("Stop delegates to the installed client", t, func() {
		withDefault(t)
		var shut atomic.Int32
		fakeMe := &fakeMetrics{onStop: func(context.Context) error {
			shut.Add(1)
			return nil
		}}
		SetDefault(fakeMe)

		So(Stop(context.Background()), ShouldBeNil)
		So(shut.Load(), ShouldEqual, 1)
	})

	Convey("Stop propagates the installed client's error", t, func() {
		withDefault(t)
		boom := errors.New("shutdown failed")
		SetDefault(&fakeMetrics{onStop: func(context.Context) error { return boom }})
		So(errors.Is(Stop(context.Background()), boom), ShouldBeTrue)
	})
}

func TestFacade_SetDefaultNilResets(t *testing.T) {
	Convey("SetDefault(nil) restores the noop client", t, func() {
		prev := Default()
		t.Cleanup(func() { SetDefault(prev) })

		SetDefault(nil)
		So(Default(), ShouldNotBeNil)
		So(Default(), ShouldHaveSameTypeAs, noopMetrics{})
		So(Add(context.Background(), Instrument{Name: "counter"}, 1, nil), ShouldBeNil)
	})
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
	Convey("builds a provider and shuts down its exporter", t, func() {
		cfg := Config{}
		cfg.setDefaults()
		exporter := &fakeExporter{}

		c, err := newClientWithExporter(context.Background(), testInfo(), cfg, exporter)
		So(err, ShouldBeNil)
		So(c.Add(context.Background(), Instrument{Name: "exporter.count"}, 1, nil), ShouldBeNil)
		So(c.Stop(context.Background()), ShouldBeNil)
		So(exporter.shutdown.Load(), ShouldEqual, 1)
	})
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
