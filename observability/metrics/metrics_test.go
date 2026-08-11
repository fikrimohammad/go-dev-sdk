package metrics

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
)

func testInfo() appinfo.Info {
	return appinfo.Info{Name: "testapp", Version: "1.0.0", Environment: "test"}
}

// fakeMetrics implements Metrics.
type fakeMetrics struct {
	onStop func(context.Context) error
}

func (*fakeMetrics) Count(context.Context, string, int64, map[string]any) error       { return nil }
func (*fakeMetrics) Histogram(context.Context, string, float64, map[string]any) error { return nil }

func (f *fakeMetrics) Stop(ctx context.Context) error {
	if f.onStop != nil {
		return f.onStop(ctx)
	}
	return nil
}

// newTestMetrics creates an otelMetrics backed by a manual reader so recorded
// metrics can be inspected.
func newTestMetrics(t *testing.T) (*otelMetrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return &otelMetrics{meter: provider.Meter("myapp"), shutdown: provider.Shutdown}, reader
}

// withDefault restores the noop default after the test, isolating the package
// facade from other tests.
func withDefault(t *testing.T) {
	t.Helper()
	SetDefault(Noop())
}

func TestOtelMetrics_Count_Basic(t *testing.T) {
	Convey("Count records a counter with normalized attributes", t, func() {
		c, reader := newTestMetrics(t)
		ctx := context.Background()

		So(c.Count(ctx, "requests", 1, map[string]any{"order_id": "1"}), ShouldBeNil)

		rm := metricdata.ResourceMetrics{}
		So(reader.Collect(ctx, &rm), ShouldBeNil)
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

func TestOtelMetrics_Count_Accumulates(t *testing.T) {
	Convey("repeated Count calls accumulate with identical attributes", t, func() {
		c, reader := newTestMetrics(t)
		ctx := context.Background()

		So(c.Count(ctx, "requests", 3, nil), ShouldBeNil)
		So(c.Count(ctx, "requests", 2, nil), ShouldBeNil)

		rm := metricdata.ResourceMetrics{}
		So(reader.Collect(ctx, &rm), ShouldBeNil)
		m := findMetric(rm, "requests")
		So(m, ShouldNotBeNil)

		sum, ok := m.Data.(metricdata.Sum[int64])
		So(ok, ShouldBeTrue)
		So(sum.DataPoints[0].Value, ShouldEqual, int64(5))
	})
}

func TestOtelMetrics_Histogram(t *testing.T) {
	Convey("Histogram records values", t, func() {
		c, reader := newTestMetrics(t)
		ctx := context.Background()

		So(c.Histogram(ctx, "latency", 0.1, map[string]any{"path": "/api"}), ShouldBeNil)
		So(c.Histogram(ctx, "latency", 0.3, map[string]any{"path": "/api"}), ShouldBeNil)

		rm := metricdata.ResourceMetrics{}
		So(reader.Collect(ctx, &rm), ShouldBeNil)
		m := findMetric(rm, "latency")
		So(m, ShouldNotBeNil)

		hist, ok := m.Data.(metricdata.Histogram[float64])
		So(ok, ShouldBeTrue)
		So(len(hist.DataPoints), ShouldEqual, 1)
		So(hist.DataPoints[0].Count, ShouldEqual, 2)
	})
}

func TestOtelMetrics_InvalidName(t *testing.T) {
	Convey("invalid metric names return ErrInvalidMetricName and record nothing", t, func() {
		c, _ := newTestMetrics(t)
		ctx := context.Background()

		So(errors.Is(c.Count(ctx, "!!!", 1, nil), ErrInvalidMetricName), ShouldBeTrue)
		So(errors.Is(c.Histogram(ctx, "", 0.5, nil), ErrInvalidMetricName), ShouldBeTrue)

		count := 0
		c.counters.Range(func(_, _ any) bool { count++; return true })
		c.histograms.Range(func(_, _ any) bool { count++; return true })
		So(count, ShouldEqual, 0)
	})
}

func TestOtelMetrics_Caching(t *testing.T) {
	Convey("instruments are cached after first use", t, func() {
		c, _ := newTestMetrics(t)
		ctx := context.Background()

		So(c.Count(ctx, "requests", 1, nil), ShouldBeNil)
		So(c.Count(ctx, "requests", 1, nil), ShouldBeNil)
		So(c.Histogram(ctx, "latency", 0.1, nil), ShouldBeNil)
		So(c.Histogram(ctx, "latency", 0.2, nil), ShouldBeNil)

		_, ok := c.counters.Load("requests")
		So(ok, ShouldBeTrue)
		_, ok = c.histograms.Load("latency")
		So(ok, ShouldBeTrue)
	})
}

func TestOtelMetrics_Concurrent(t *testing.T) {
	Convey("Count and Histogram are concurrent-safe", t, func() {
		c, _ := newTestMetrics(t)
		ctx := context.Background()

		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = c.Count(ctx, "requests", 1, nil)
				_ = c.Histogram(ctx, "latency", 0.1, nil)
			}()
		}
		wg.Wait()
		So(true, ShouldBeTrue)
	})
}

func TestOtelMetrics_Stop(t *testing.T) {
	Convey("Stop is idempotent and shuts the provider exactly once", t, func() {
		c, _ := newTestMetrics(t)
		var shut int32
		orig := c.shutdown
		c.shutdown = func(ctx context.Context) error { atomic.AddInt32(&shut, 1); return orig(ctx) }

		So(c.Stop(context.Background()), ShouldBeNil)
		So(c.Stop(context.Background()), ShouldBeNil)
		So(atomic.LoadInt32(&shut), ShouldEqual, 1)
	})

	Convey("Stop rejects subsequent measurements with ErrStopped", t, func() {
		c, _ := newTestMetrics(t)
		So(c.Stop(context.Background()), ShouldBeNil)
		So(errors.Is(c.Count(context.Background(), "requests", 1, nil), ErrStopped), ShouldBeTrue)
		So(errors.Is(c.Histogram(context.Background(), "latency", 0.1, nil), ErrStopped), ShouldBeTrue)
	})
}

func TestNoopMetrics(t *testing.T) {
	Convey("noop metrics discard silently", t, func() {
		m := noopMetrics{}
		So(m.Count(context.Background(), "requests", 1, nil), ShouldBeNil)
		So(m.Histogram(context.Background(), "latency", 0.5, nil), ShouldBeNil)
		So(m.Stop(context.Background()), ShouldBeNil)
	})
}

func TestSanitizeName(t *testing.T) {
	Convey("sanitizeName follows the expected rules", t, func() {
		tests := map[string]string{
			"simple":                "simple",
			"CamelCase":             "camelcase",
			"with-dash":             "with_dash",
			"with.dot":              "with.dot",
			"with space":            "with_space",
			"__double__under__":     "double_under",
			"_leading_trailing_":    "leading_trailing",
			"special!@#$chars":      "special_chars",
			"ALREADY_UPPER":         "already_upper",
			"MiXeD_CaSe-123":        "mixed_case_123",
			"a":                     "a",
			"":                      "",
			"http.server.req.count": "http.server.req.count",
			"api-request":           "api_request",
		}
		for input, want := range tests {
			So(sanitizeName(input), ShouldEqual, want)
		}
	})
}

func TestCanonicalMetricName(t *testing.T) {
	Convey("canonicalMetricName rejects names that sanitize to empty", t, func() {
		c := &otelMetrics{}
		_, err := c.canonicalMetricName("!!!")
		So(errors.Is(err, ErrInvalidMetricName), ShouldBeTrue)
		name, err := c.canonicalMetricName("requests")
		So(err, ShouldBeNil)
		So(name, ShouldEqual, "requests")
	})
}

func TestNew_Validation(t *testing.T) {
	Convey("New rejects an empty app name", t, func() {
		_, err := New(context.Background(), appinfo.Info{}, Config{})
		So(errors.Is(err, ErrInvalidMetricName), ShouldBeTrue)
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
		So(Count(context.Background(), "requests", 1, nil), ShouldBeNil)
		So(Histogram(context.Background(), "latency", 0.5, nil), ShouldBeNil)
		So(Stop(context.Background()), ShouldBeNil)
	})
}

func TestFacade_SetDefault(t *testing.T) {
	Convey("SetDefault routes the package funcs through the installed client", t, func() {
		withDefault(t)
		fakeMe := &fakeMetrics{}
		SetDefault(fakeMe)
		So(Default(), ShouldEqual, fakeMe)

		So(Count(context.Background(), "requests", 1, nil), ShouldBeNil)
		So(Histogram(context.Background(), "latency", 0.5, nil), ShouldBeNil)
		So(Stop(context.Background()), ShouldBeNil)
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
		var shut int32
		fakeMe := &fakeMetrics{onStop: func(context.Context) error {
			atomic.AddInt32(&shut, 1)
			return nil
		}}
		SetDefault(fakeMe)

		So(Stop(context.Background()), ShouldBeNil)
		So(atomic.LoadInt32(&shut), ShouldEqual, 1)
	})

	Convey("Stop propagates the installed client's error", t, func() {
		withDefault(t)
		boom := errors.New("shutdown failed")
		SetDefault(&fakeMetrics{onStop: func(context.Context) error { return boom }})
		So(errors.Is(Stop(context.Background()), boom), ShouldBeTrue)
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
