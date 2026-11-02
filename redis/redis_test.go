package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.43.0/dbconv"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-servicekit/observability/attributes"
	"github.com/fikrimohammad/go-servicekit/observability/metrics"
	"github.com/fikrimohammad/go-servicekit/observability/tracer"
)

// --- fake metrics client -----------------------------------------------------------

type metricRec struct {
	name  string
	value float64
	attrs map[string]any
}

type fakeMetrics struct {
	mu     sync.Mutex
	counts []metricRec
	hists  []metricRec
}

func (f *fakeMetrics) Add(_ context.Context, instrument metrics.Instrument, value int64, attrs map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts = append(f.counts, metricRec{instrument.Name, float64(value), normalizeAttrs(attrs)})
	return nil
}

func (f *fakeMetrics) Record(_ context.Context, instrument metrics.Instrument, value float64, attrs map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hists = append(f.hists, metricRec{instrument.Name, value, normalizeAttrs(attrs)})
	return nil
}

func (f *fakeMetrics) AddUpDown(context.Context, metrics.Instrument, int64, map[string]any) error {
	return nil
}

func (f *fakeMetrics) RecordGauge(context.Context, metrics.Instrument, float64, map[string]any) error {
	return nil
}

func (f *fakeMetrics) Provider() metric.MeterProvider { return nil }

func (f *fakeMetrics) Stop(context.Context) error { return nil }

func (f *fakeMetrics) lastHist() (metricRec, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.hists) == 0 {
		return metricRec{}, false
	}
	return f.hists[len(f.hists)-1], true
}

// srvError is an error reply from the server, the way go-redis returns one: it
// has a RedisError method, unlike a plain error that merely looks like one.
type srvError string

func (e srvError) Error() string { return string(e) }

func (srvError) RedisError() {}

// netError is a net.Error whose Timeout result is set by the test.
type netError struct{ timeout bool }

func (e netError) Error() string { return "i/o failure" }
func (e netError) Timeout() bool { return e.timeout }
func (netError) Temporary() bool { return false }

// --- fake: span capture ------------------------------------------------------------

type recordingExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (e *recordingExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, spans...)
	return nil
}

func (e *recordingExporter) Shutdown(context.Context) error { return nil }

func (e *recordingExporter) last() (sdktrace.ReadOnlySpan, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.spans) == 0 {
		return nil, false
	}
	return e.spans[len(e.spans)-1], true
}

func (e *recordingExporter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.spans)
}

func spanAttrs(s sdktrace.ReadOnlySpan) map[string]any {
	out := make(map[string]any)
	for _, kv := range s.Attributes() {
		out[string(kv.Key)] = kv.Value.String()
	}
	return out
}

// normalizeAttrs mirrors the OTel key normalization applied by the real metrics
// and tracer clients (attributes.NormalizeKey), so the fake records the keys as
// they are emitted.
func normalizeAttrs(attrs map[string]any) map[string]any {
	out := make(map[string]any, len(attrs))
	for k, v := range attrs {
		out[attributes.NormalizeKey(k)] = v
	}
	return out
}

// --- setup -------------------------------------------------------------------------

// setupTelemetry returns capturing metrics and tracer clients, and the exporter
// that receives the spans the tracer client produces.
func setupTelemetry() (*fakeMetrics, tracer.Client, *recordingExporter) {
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	return &fakeMetrics{}, tracer.Wrap(tp), ex
}

// setupInstrument builds an instrumentHook with injected capturing clients.
func setupInstrument() (*instrumentHook, *fakeMetrics, *recordingExporter) {
	fm, tc, ex := setupTelemetry()
	return &instrumentHook{
		meta: meta{
			serverAddr: "cache.example.com",
			serverPort: 6380,
			namespace:  "2",
			metrics:    fm,
			tracer:     tc,
		},
	}, fm, ex
}

// setOTelProviders installs tp and mp as the OTel global providers for the
// duration of the test. The previous globals are restored in t.Cleanup.
func setOTelProviders(t *testing.T, tp trace.TracerProvider, mp metric.MeterProvider) {
	t.Helper()
	prevTP, prevMP := otel.GetTracerProvider(), otel.GetMeterProvider()
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		if otel.GetTracerProvider() != prevTP {
			otel.SetTracerProvider(prevTP)
		}
		if otel.GetMeterProvider() != prevMP {
			otel.SetMeterProvider(prevMP)
		}
	})
}

// collectedMetrics returns the names of the metrics r has collected so far.
func collectedMetrics(t *testing.T, r *sdkmetric.ManualReader) []string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
		}
	}
	return names
}

// --- tests -------------------------------------------------------------------------

func TestProcessHook_Get_Success(t *testing.T) {
	h, fm, ex := setupInstrument()
	cmd := redis.NewStringCmd(context.Background(), "get", "report:1")
	var called bool
	hook := h.ProcessHook(func(_ context.Context, _ redis.Cmder) error {
		called = true
		return nil
	})

	if err := hook(context.Background(), cmd); err != nil {
		t.Fatalf("ProcessHook: %v", err)
	}
	if !called {
		t.Fatal("next hook was not called")
	}

	if n := len(fm.counts); n != 0 {
		t.Fatalf("count records = %d, want 0", n)
	}
	hist, ok := fm.lastHist()
	if !ok || hist.name != metricDuration {
		t.Fatalf("hist = %+v", hist)
	}

	wantAttrs := map[string]any{
		"server.address":    "cache.example.com",
		"server.port":       6380,
		"db.system.name":    "redis",
		"db.operation.name": "GET",
		"db.namespace":      "2",
	}
	assertAttrs(t, hist.attrs, wantAttrs)
	if _, ok := hist.attrs["db.query.text"]; ok {
		t.Fatal("metric carries db.query.text, want span only")
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Name() != "GET" || sp.SpanKind() != trace.SpanKindClient {
		t.Fatalf("span name=%q kind=%v", sp.Name(), sp.SpanKind())
	}
	spanWant := map[string]any{
		"server.address":    "cache.example.com",
		"server.port":       "6380",
		"db.system.name":    "redis",
		"db.operation.name": "GET",
		"db.namespace":      "2",
		"db.query.text":     "GET ?",
	}
	assertAttrs(t, spanAttrs(sp), spanWant)
	if sp.Status().Code != codes.Unset {
		t.Fatalf("status = %v, want unset", sp.Status().Code)
	}
}

func TestProcessHook_Set_QueryText(t *testing.T) {
	h, fm, ex := setupInstrument()
	cmd := redis.NewStatusCmd(context.Background(), "set", "lock:1", true, "px", "1000")
	hook := h.ProcessHook(func(_ context.Context, _ redis.Cmder) error { return nil })

	if err := hook(context.Background(), cmd); err != nil {
		t.Fatalf("ProcessHook: %v", err)
	}
	hist, ok := fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	if hist.attrs["db.operation.name"] != "SET" {
		t.Fatalf("db.operation.name = %v, want SET", hist.attrs["db.operation.name"])
	}
	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if got := spanAttrs(sp)["db.query.text"]; got != "SET ? ? ? ?" {
		t.Fatalf("db.query.text = %v, want redacted args", got)
	}
}

func TestProcessHook_Error(t *testing.T) {
	h, fm, ex := setupInstrument()
	cmd := redis.NewStringCmd(context.Background(), "get", "report:1")
	boom := srvError("WRONGTYPE Operation against a key holding the wrong kind of value")
	hook := h.ProcessHook(func(_ context.Context, _ redis.Cmder) error { return boom })

	if err := hook(context.Background(), cmd); !errors.Is(err, boom) {
		t.Fatalf("ProcessHook err = %v, want %v", err, boom)
	}

	hist, ok := fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	if hist.attrs["db.response.status_code"] != "WRONGTYPE" {
		t.Fatalf("db.response.status_code = %v, want WRONGTYPE", hist.attrs["db.response.status_code"])
	}
	if hist.attrs["error.type"] != "WRONGTYPE" {
		t.Fatalf("error.type = %v, want WRONGTYPE", hist.attrs["error.type"])
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
	attrs := spanAttrs(sp)
	if attrs["db.response.status_code"] != "WRONGTYPE" || attrs["error.type"] != "WRONGTYPE" {
		t.Fatalf("span attrs = %v", attrs)
	}
}

func TestProcessHook_RedisNil_NotError(t *testing.T) {
	h, fm, ex := setupInstrument()
	cmd := redis.NewStringCmd(context.Background(), "get", "missing")
	hook := h.ProcessHook(func(_ context.Context, _ redis.Cmder) error { return redis.Nil })

	if err := hook(context.Background(), cmd); !errors.Is(err, redis.Nil) {
		t.Fatalf("ProcessHook err = %v, want redis.Nil", err)
	}
	hist, ok := fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	for _, k := range []string{"db.response.status_code", "error.type"} {
		if _, ok := hist.attrs[k]; ok {
			t.Fatalf("redis.Nil must not set %s, got %v", k, hist.attrs)
		}
	}
	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Unset {
		t.Fatalf("status = %v, want unset", sp.Status().Code)
	}
}

func TestProcessPipelineHook(t *testing.T) {
	h, fm, ex := setupInstrument()
	cmds := []redis.Cmder{
		redis.NewStringCmd(context.Background(), "get", "a"),
		redis.NewStatusCmd(context.Background(), "set", "b", "1"),
	}
	hook := h.ProcessPipelineHook(func(_ context.Context, _ []redis.Cmder) error { return nil })

	if err := hook(context.Background(), cmds); err != nil {
		t.Fatalf("ProcessPipelineHook: %v", err)
	}

	hist, ok := fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	if hist.attrs["db.operation.name"] != "PIPELINE" {
		t.Fatalf("db.operation.name = %v, want PIPELINE", hist.attrs["db.operation.name"])
	}
	if _, ok := hist.attrs["db.operation.batch.size"]; ok {
		t.Fatal("metric carries db.operation.batch.size, want span only")
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Name() != "PIPELINE" {
		t.Fatalf("span name = %q, want PIPELINE", sp.Name())
	}
	attrs := spanAttrs(sp)
	if attrs["db.operation.batch.size"] != "2" {
		t.Fatalf("db.operation.batch.size = %v, want 2", attrs["db.operation.batch.size"])
	}
	if attrs["db.query.text"] != "GET ?; SET ? ?" {
		t.Fatalf("db.query.text = %v", attrs["db.query.text"])
	}
}

func TestProcessPipelineHook_BatchSize(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		cmds      []redis.Cmder
		wantBatch string // empty when db.operation.batch.size must be absent
		wantText  string
	}{
		{
			name:     "single command is not a batch",
			cmds:     []redis.Cmder{redis.NewStringCmd(ctx, "get", "a")},
			wantText: "GET ?",
		},
		{
			name: "two commands",
			cmds: []redis.Cmder{
				redis.NewStringCmd(ctx, "get", "a"),
				redis.NewStatusCmd(ctx, "set", "b", "1"),
			},
			wantBatch: "2",
			wantText:  "GET ?; SET ? ?",
		},
		{
			name: "transaction: MULTI and EXEC are not counted",
			cmds: []redis.Cmder{
				redis.NewStatusCmd(ctx, "multi"),
				redis.NewStringCmd(ctx, "get", "a"),
				redis.NewStatusCmd(ctx, "set", "b", "1"),
				redis.NewStatusCmd(ctx, "exec"),
			},
			wantBatch: "2",
			wantText:  "GET ?; SET ? ?",
		},
		{
			name: "transaction with one command is not a batch",
			cmds: []redis.Cmder{
				redis.NewStatusCmd(ctx, "multi"),
				redis.NewStringCmd(ctx, "get", "a"),
				redis.NewStatusCmd(ctx, "exec"),
			},
			wantText: "GET ?",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _, ex := setupInstrument()
			hook := h.ProcessPipelineHook(func(_ context.Context, _ []redis.Cmder) error { return nil })
			if err := hook(ctx, tc.cmds); err != nil {
				t.Fatalf("ProcessPipelineHook: %v", err)
			}
			sp, ok := ex.last()
			if !ok {
				t.Fatal("no span recorded")
			}
			attrs := spanAttrs(sp)
			got, present := attrs["db.operation.batch.size"]
			switch {
			case tc.wantBatch == "" && present:
				t.Errorf("db.operation.batch.size = %v, want absent", got)
			case tc.wantBatch != "" && got != tc.wantBatch:
				t.Errorf("db.operation.batch.size = %v, want %v", got, tc.wantBatch)
			}
			if got := attrs["db.query.text"]; got != tc.wantText {
				t.Errorf("db.query.text = %v, want %v", got, tc.wantText)
			}
		})
	}
}

func TestInstrumentHook_FallsBackToOTelGlobal(t *testing.T) {
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	setOTelProviders(t, tp, mp)

	// No injected clients: the hook must fall back to the OTel globals.
	h := &instrumentHook{meta: meta{serverAddr: "h", serverPort: 6379, namespace: "0"}}
	cmd := redis.NewStringCmd(context.Background(), "get", "k")
	hook := h.ProcessHook(func(_ context.Context, _ redis.Cmder) error { return nil })

	if err := hook(context.Background(), cmd); err != nil {
		t.Fatalf("ProcessHook: %v", err)
	}
	if n := ex.count(); n != 1 {
		t.Fatalf("OTel global tracer spans = %d, want 1", n)
	}
	sp, _ := ex.last()
	if sp.Name() != "GET" {
		t.Fatalf("span name = %q, want GET", sp.Name())
	}
	names := collectedMetrics(t, reader)
	if want := []string{metricDuration}; !slices.Equal(names, want) {
		t.Errorf("OTel global meter provider collected %v, want %v", names, want)
	}
}

func TestInstrumentHook_MetricInstrument(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	h := &instrumentHook{meta: meta{
		metrics: metrics.Wrap(mp),
		tracer:  tracer.Wrap(sdktrace.NewTracerProvider()),
	}}
	hook := h.ProcessHook(func(_ context.Context, _ redis.Cmder) error { return nil })
	if err := hook(context.Background(), redis.NewStringCmd(context.Background(), "get", "k")); err != nil {
		t.Fatalf("ProcessHook: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	var found []metricdata.Metrics
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == metricDuration {
				found = append(found, m)
			}
		}
	}
	if len(found) != 1 {
		t.Fatalf("metrics named %q = %d, want 1", metricDuration, len(found))
	}
	m := found[0]
	if m.Unit != "s" {
		t.Errorf("unit = %q, want %q", m.Unit, "s")
	}
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("data type = %T, want metricdata.Histogram[float64]", m.Data)
	}
	if len(hist.DataPoints) != 1 || hist.DataPoints[0].Count != 1 {
		t.Errorf("data points = %+v, want one point with count 1", hist.DataPoints)
	}
}

func TestMetricDurationInstrument_MatchesDBConv(t *testing.T) {
	want := dbconv.ClientOperationDuration{}
	if got := metricDurationInstrument.Name; got != want.Name() {
		t.Errorf("Name = %q, want %q", got, want.Name())
	}
	if got := metricDurationInstrument.Unit; got != want.Unit() {
		t.Errorf("Unit = %q, want %q", got, want.Unit())
	}
	if got := metricDurationInstrument.Description; got != want.Description() {
		t.Errorf("Description = %q, want %q", got, want.Description())
	}
}

func TestQueryText(t *testing.T) {
	cases := []struct {
		name string
		cmd  redis.Cmder
		want string
	}{
		{"get", redis.NewStringCmd(context.Background(), "get", "k"), "GET ?"},
		{"set with value", redis.NewStatusCmd(context.Background(), "set", "k", "v"), "SET ? ?"},
		{"no args", redis.NewStringCmd(context.Background(), "ping"), "PING"},
		{"multi args", redis.NewIntCmd(context.Background(), "del", "a", "b"), "DEL ? ?"},
		{"unlink", redis.NewIntCmd(context.Background(), "unlink", "a", "b"), "UNLINK ? ?"},
		{"hset", redis.NewIntCmd(context.Background(), "hset", "h", "f", "v"), "HSET ? ? ?"},
		{"hincrbyfloat", redis.NewFloatCmd(context.Background(), "hincrbyfloat", "h", "f", 1.5), "HINCRBYFLOAT ? ? ?"},
		{"incrbyfloat", redis.NewFloatCmd(context.Background(), "incrbyfloat", "k", 2.5), "INCRBYFLOAT ? ?"},
		{"lpush", redis.NewIntCmd(context.Background(), "lpush", "l", "v1", "v2"), "LPUSH ? ? ?"},
		{"rpush", redis.NewIntCmd(context.Background(), "rpush", "l", "v1", "v2"), "RPUSH ? ? ?"},
		{"lpop", redis.NewStringCmd(context.Background(), "lpop", "l"), "LPOP ?"},
		{"rpop", redis.NewStringCmd(context.Background(), "rpop", "l"), "RPOP ?"},
		{"ltrim", redis.NewStatusCmd(context.Background(), "ltrim", "l", 0, 10), "LTRIM ? ? ?"},
		{"llen", redis.NewIntCmd(context.Background(), "llen", "l"), "LLEN ?"},
		{"lrange", redis.NewStringSliceCmd(context.Background(), "lrange", "l", 0, -1), "LRANGE ? ? ?"},
		{"publish", redis.NewIntCmd(context.Background(), "publish", "ch", "msg"), "PUBLISH ? ?"},
		{"zadd", redis.NewIntCmd(context.Background(), "zadd", "z", 10.0, "m"), "ZADD ? ? ?"},
		{"sadd", redis.NewIntCmd(context.Background(), "sadd", "s", "m1", "m2"), "SADD ? ? ?"},
		{"sinter", redis.NewStringSliceCmd(context.Background(), "sinter", "s1", "s2"), "SINTER ? ?"},
		{"sunion", redis.NewStringSliceCmd(context.Background(), "sunion", "s1", "s2"), "SUNION ? ?"},
		{"sdiff", redis.NewStringSliceCmd(context.Background(), "sdiff", "s1", "s2"), "SDIFF ? ?"},
		{"eval", redis.NewCmd(context.Background(), "eval", "return 1", 0), "EVAL ? ?"},
		{"scan", redis.NewScanCmd(context.Background(), nil, "scan", 0, "match", "*", "count", 10), "SCAN ? ? ? ? ?"},
		{"getdel", redis.NewStringCmd(context.Background(), "getdel", "k"), "GETDEL ?"},
		{"getex", redis.NewStringCmd(context.Background(), "getex", "k", "ex", 10), "GETEX ? ? ?"},
		{"setxx", redis.NewBoolCmd(context.Background(), "set", "k", "v", "xx"), "SET ? ? ?"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := queryText(tc.cmd); got != tc.want {
				t.Fatalf("queryText = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestProcessPipelineHook_RedisNilFirst_WithErrorLater(t *testing.T) {
	h, fm, ex := setupInstrument()
	cmd1 := redis.NewStringCmd(context.Background(), "get", "missing")
	cmd1.SetErr(redis.Nil)
	cmd2 := redis.NewStatusCmd(context.Background(), "set", "k", "v")
	cmd2.SetErr(srvError("ERR database is read-only"))

	cmds := []redis.Cmder{cmd1, cmd2}
	hook := h.ProcessPipelineHook(func(_ context.Context, _ []redis.Cmder) error {
		// In go-redis, pipe.Exec returns the first error encountered, which is redis.Nil
		return redis.Nil
	})

	if err := hook(context.Background(), cmds); !errors.Is(err, redis.Nil) {
		t.Fatalf("ProcessPipelineHook err = %v, want redis.Nil", err)
	}

	hist, ok := fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	if hist.attrs["db.response.status_code"] != "ERR" {
		t.Fatalf("db.response.status_code = %v, want ERR", hist.attrs["db.response.status_code"])
	}
	if hist.attrs["error.type"] != "ERR" {
		t.Fatalf("error.type = %v, want ERR", hist.attrs["error.type"])
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
}

func TestRedisErrorCode(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"wrongtype", srvError("WRONGTYPE Operation against key"), "WRONGTYPE"},
		{"err", srvError("ERR unknown command"), "ERR"},
		{"oom", srvError("OOM command not allowed"), "OOM"},
		{"busykey", srvError("BUSYKEY Target key name already exists"), "BUSYKEY"},
		{"noscript", srvError("NOSCRIPT No matching script"), "NOSCRIPT"},
		{"io.EOF", io.EOF, ""},
		{"dial failure", errors.New("dial tcp 127.0.0.1:6379: connect: connection refused"), ""},
		{"context deadline", errors.New("context deadline exceeded"), ""},
		{"io timeout", errors.New("read tcp 127.0.0.1:6379: i/o timeout"), ""},
		{"client closed", errors.New("redis: client is closed"), ""},
		{"plain error", errors.New("plain error"), ""},
		{"uppercase text, not a server reply", errors.New("WRONGTYPE Operation against key"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := redisErrorCode(tc.err); got != tc.want {
				t.Errorf("redisErrorCode(%q) = %q, want %q", tc.err.Error(), got, tc.want)
			}
		})
	}
}

func TestErrorType(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"deadline exceeded", context.DeadlineExceeded, "timeout"},
		{"wrapped deadline exceeded", fmt.Errorf("op: %w", context.DeadlineExceeded), "timeout"},
		{"canceled", context.Canceled, "canceled"},
		{"net timeout", netError{timeout: true}, "timeout"},
		{"net error, not a timeout", netError{timeout: false}, "_OTHER"},
		{"pool timeout", redis.ErrPoolTimeout, "timeout"},
		{"server error", srvError("WRONGTYPE Operation against key"), "WRONGTYPE"},
		{"io.EOF", io.EOF, "_OTHER"},
		{"plain error", errors.New("boom"), "_OTHER"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := errorType(tc.err); got != tc.want {
				t.Errorf("errorType(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestPipelineText_Truncation(t *testing.T) {
	cmds := make([]redis.Cmder, 60)
	for i := range cmds {
		cmds[i] = redis.NewStringCmd(context.Background(), "get", "k")
	}
	txt := pipelineText(cmds)
	if !strings.Contains(txt, "(10 more commands)") {
		t.Fatalf("pipelineText = %q, want truncation notice", txt)
	}
}

func TestBuildTLSConfig(t *testing.T) {
	// Disabled
	tlsCfg, err := buildTLSConfig(Config{TLSEnabled: false})
	if err != nil || tlsCfg != nil {
		t.Fatalf("buildTLSConfig(disabled) = %v, %v, want nil, nil", tlsCfg, err)
	}

	// Enabled defaults
	tlsCfg, err = buildTLSConfig(Config{
		Host:                  "redis.example.com",
		TLSEnabled:            true,
		TLSInsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if tlsCfg.ServerName != "redis.example.com" {
		t.Fatalf("ServerName = %q, want redis.example.com", tlsCfg.ServerName)
	}
	if !tlsCfg.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify = false, want true")
	}

	// Invalid CA
	_, err = buildTLSConfig(Config{
		TLSEnabled: true,
		TLSCACert:  "not-a-valid-ca-cert",
	})
	if err == nil {
		t.Fatal("buildTLSConfig(missing CA) err = nil, want error")
	}
}

func TestOptions(t *testing.T) {
	fm := &fakeMetrics{}
	tc := tracer.Wrap(sdktrace.NewTracerProvider())

	var o options
	for _, opt := range []Option{WithMetrics(fm), WithTracer(tc), WithPoolMetrics(5 * time.Second)} {
		opt(&o)
	}

	if o.metrics != fm {
		t.Errorf("WithMetrics: metrics = %v, want %v", o.metrics, fm)
	}
	if o.tracer != tc {
		t.Errorf("WithTracer: tracer = %v, want %v", o.tracer, tc)
	}
	if o.poolMetricsInterval != 5*time.Second {
		t.Errorf("WithPoolMetrics: interval = %v, want 5s", o.poolMetricsInterval)
	}
}

func TestServerAddress(t *testing.T) {
	cases := []struct {
		name     string
		cfg      Config
		wantAddr string
		wantPort int
	}{
		{"host only", Config{Host: "db.example", Port: 6380}, "db.example", 6380},
		{"addrs only", Config{Addrs: []string{"cache.example:6381"}}, "cache.example", 6381},
		{"both set addrs wins", Config{Host: "db.example", Port: 6380, Addrs: []string{"cache.example:6381"}}, "cache.example", 6381},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			addr, port := serverAddress(tc.cfg)
			if addr != tc.wantAddr || port != tc.wantPort {
				t.Fatalf("serverAddress() = %q, %d, want %q, %d", addr, port, tc.wantAddr, tc.wantPort)
			}
		})
	}
}

func TestNew_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := New(ctx, Config{Host: "127.0.0.1", Port: 1, ConnectTimeout: time.Second})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("New err = %v, want context.Canceled", err)
	}
}

func TestNew_InvalidConfig(t *testing.T) {
	if _, err := New(context.Background(), Config{Host: "", Port: 6379}); err == nil {
		t.Fatal("New(missing host) err = nil, want error")
	}
	if _, err := New(context.Background(), Config{Host: "h", Port: -1}); err == nil {
		t.Fatal("New(invalid port) err = nil, want error")
	}
}

func TestNew_ConnectionFailure(t *testing.T) {
	_, err := New(context.Background(), Config{Host: "127.0.0.1", Port: 1, ConnectTimeout: 100 * time.Millisecond})
	if err == nil {
		t.Fatal("New(unreachable) err = nil, want connection error")
	}
	if !strings.Contains(err.Error(), "ping") {
		t.Fatalf("New err = %q, want ping error", err.Error())
	}
}

func TestClient_Close_WithPoolMetrics(t *testing.T) {
	raw := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = raw.Close() })
	stopCh := make(chan struct{})
	c := &client{
		UniversalClient: raw,
		stopPoolMetrics: stopCh,
	}
	_ = c.Close()
	// Calling close a second time should not panic
	_ = c.Close()

	select {
	case <-stopCh:
		// Successfully closed
	default:
		t.Fatal("stopPoolMetrics channel was not closed")
	}
}

// poolTestName is the db.client.connection.pool.name the pool tests expect.
const poolTestName = "cache.example.com:6380"

// poolSums collects r and returns the summed value of the named int64 sum per
// db.client.connection.state, keyed by state ("" for a point without one). It
// fails when a point's attribute keys are not exactly wantKeys or when its pool
// name is wrong. The returned metric has an empty Name when no point exists yet.
func poolSums(t *testing.T, r *sdkmetric.ManualReader, name string, wantKeys ...string) (map[string]int64, metricdata.Metrics) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	wantKeys = slices.Clone(wantKeys)
	slices.Sort(wantKeys)

	sums := make(map[string]int64)
	var found metricdata.Metrics
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			found = m
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s data type = %T, want metricdata.Sum[int64]", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				attrs := make(map[string]string)
				for _, kv := range dp.Attributes.ToSlice() {
					attrs[string(kv.Key)] = kv.Value.AsString()
				}
				if got := slices.Sorted(maps.Keys(attrs)); !slices.Equal(got, wantKeys) {
					t.Errorf("%s attribute keys = %v, want %v", name, got, wantKeys)
				}
				if got := attrs["db.client.connection.pool.name"]; got != poolTestName {
					t.Errorf("%s pool.name = %q, want %q", name, got, poolTestName)
				}
				sums[attrs["db.client.connection.state"]] += dp.Value
			}
		}
	}
	return sums, found
}

func TestPoolMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tests := []struct {
		name         string
		stats        *redis.PoolStats
		wantIdle     int64 // running count, state=idle
		wantUsed     int64 // running count, state=used
		wantTimeouts int64 // running total of timeouts
	}{
		{"first poll", &redis.PoolStats{TotalConns: 3, IdleConns: 2, Timeouts: 0}, 2, 1, 0},
		{"second poll", &redis.PoolStats{TotalConns: 5, IdleConns: 1, Timeouts: 2}, 1, 4, 2},
		{"third poll", &redis.PoolStats{TotalConns: 2, IdleConns: 2, Timeouts: 2}, 2, 0, 2},
	}

	var cur *redis.PoolStats
	p := &poolPoller{
		stats:   func() *redis.PoolStats { return cur },
		metrics: func() metrics.Client { return metrics.Wrap(mp) },
		pool:    poolTestName,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cur = tt.stats
			p.poll(context.Background())

			counts, cm := poolSums(t, reader, "db.client.connection.count",
				"db.client.connection.pool.name", "db.client.connection.state")
			if cm.Name != "" {
				if cm.Unit != "{connection}" {
					t.Errorf("count unit = %q, want %q", cm.Unit, "{connection}")
				}
				if cm.Data.(metricdata.Sum[int64]).IsMonotonic {
					t.Error("count IsMonotonic = true, want false")
				}
			}
			if got := counts["idle"]; got != tt.wantIdle {
				t.Errorf("count{state=idle} = %d, want %d", got, tt.wantIdle)
			}
			if got := counts["used"]; got != tt.wantUsed {
				t.Errorf("count{state=used} = %d, want %d", got, tt.wantUsed)
			}

			timeouts, tm := poolSums(t, reader, "db.client.connection.timeouts", "db.client.connection.pool.name")
			if tm.Name != "" {
				if tm.Unit != "{timeout}" {
					t.Errorf("timeouts unit = %q, want %q", tm.Unit, "{timeout}")
				}
				if !tm.Data.(metricdata.Sum[int64]).IsMonotonic {
					t.Error("timeouts IsMonotonic = false, want true")
				}
			}
			if got := timeouts[""]; got != tt.wantTimeouts {
				t.Errorf("timeouts = %d, want %d", got, tt.wantTimeouts)
			}
		})
	}
}

func TestPoolMetrics_StopWithdrawsCounts(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	polled := make(chan struct{}, 1)
	p := &poolPoller{
		stats: func() *redis.PoolStats {
			select {
			case polled <- struct{}{}:
			default:
			}
			return &redis.PoolStats{TotalConns: 3, IdleConns: 2, Timeouts: 1}
		},
		metrics: func() metrics.Client { return metrics.Wrap(mp) },
		pool:    poolTestName,
		// An hour-long interval proves the first poll runs at once, not after
		// one tick.
		interval: time.Hour,
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.run(context.Background(), stop)
	}()
	select {
	case <-polled:
	case <-time.After(5 * time.Second):
		t.Fatal("poller never polled the stats source")
	}
	close(stop)
	<-done

	counts, cm := poolSums(t, reader, "db.client.connection.count",
		"db.client.connection.pool.name", "db.client.connection.state")
	if cm.Name == "" {
		t.Fatal("count metric absent after polling, want data points")
	}
	for _, state := range []string{"idle", "used"} {
		if got := counts[state]; got != 0 {
			t.Errorf("count{state=%s} after stop = %d, want 0", state, got)
		}
	}
}

func TestClient_Close_WaitsForPoller(t *testing.T) {
	raw := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = raw.Close() })

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	stop := make(chan struct{})
	c := &client{UniversalClient: raw, stopPoolMetrics: stop}
	p := &poolPoller{
		stats: func() *redis.PoolStats {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release // the poller is stuck here until the test releases it
			return &redis.PoolStats{}
		},
		metrics:  func() metrics.Client { return metrics.Noop() },
		pool:     poolTestName,
		interval: time.Millisecond,
	}
	c.pollers.Go(func() { p.run(context.Background(), stop) })

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("poller never polled the stats source")
	}

	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned while the poller was still running")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the poller was released")
	}
	// A second Close must not panic or block.
	_ = c.Close()
}

func TestCreateUnderlyingClient_Topologies(t *testing.T) {
	// 1. Standalone
	standaloneCfg := Config{
		Host: "127.0.0.1",
		Port: 6379,
	}.SetDefaults()
	standaloneCli := createUnderlyingClient(standaloneCfg, nil)
	if _, ok := standaloneCli.(*redis.Client); !ok {
		t.Fatalf("createUnderlyingClient(standalone) type = %T, want *redis.Client", standaloneCli)
	}
	_ = standaloneCli.Close()

	// 2. Sentinel Standard
	sentinelCfg := Config{
		Mode:       ModeSentinel,
		MasterName: "mymaster",
		Addrs:      []string{"127.0.0.1:26379"},
	}.SetDefaults()
	sentinelCli := createUnderlyingClient(sentinelCfg, nil)
	if _, ok := sentinelCli.(*redis.Client); !ok {
		t.Fatalf("createUnderlyingClient(sentinel) type = %T, want *redis.Client", sentinelCli)
	}
	_ = sentinelCli.Close()

	// 3. Sentinel Read/Write Split (FailoverCluster)
	sentinelClusterCfg := Config{
		Mode:          ModeSentinel,
		MasterName:    "mymaster",
		Addrs:         []string{"127.0.0.1:26379"},
		RouteRandomly: true,
	}.SetDefaults()
	sentinelClusterCli := createUnderlyingClient(sentinelClusterCfg, nil)
	if _, ok := sentinelClusterCli.(*redis.ClusterClient); !ok {
		t.Fatalf("createUnderlyingClient(sentinel, read split) type = %T, want *redis.ClusterClient", sentinelClusterCli)
	}
	_ = sentinelClusterCli.Close()

	// 4. Redis Cluster
	clusterCfg := Config{
		Mode:  ModeCluster,
		Addrs: []string{"127.0.0.1:7000", "127.0.0.1:7001"},
	}.SetDefaults()
	clusterCli := createUnderlyingClient(clusterCfg, nil)
	if _, ok := clusterCli.(*redis.ClusterClient); !ok {
		t.Fatalf("createUnderlyingClient(cluster) type = %T, want *redis.ClusterClient", clusterCli)
	}
	_ = clusterCli.Close()
}

func TestNew_ReturnsUniversalClient(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"standalone", Config{Host: "127.0.0.1", Port: 1}},
		{"cluster", Config{Mode: ModeCluster, Addrs: []string{"127.0.0.1:1"}}},
		{"sentinel", Config{Mode: ModeSentinel, MasterName: "mymaster", Addrs: []string{"127.0.0.1:1"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fm, tc2, ex := setupTelemetry()
			cfg := tc.cfg
			cfg.ConnectTimeout = 200 * time.Millisecond

			// New pings the server, which refuses the connection; the ping still
			// passes through the instrumentation hook that New attaches.
			cli, err := New(context.Background(), cfg, WithMetrics(fm), WithTracer(tc2))
			if err == nil {
				_ = cli.Close()
				t.Fatal("New: got nil error, want connection error")
			}

			hist, ok := fm.lastHist()
			if !ok {
				t.Fatal("no duration recorded for the startup ping")
			}
			if got := hist.attrs["db.operation.name"]; got != "PING" {
				t.Fatalf("db.operation.name = %v, want PING", got)
			}
			if _, ok := hist.attrs["error.type"]; !ok {
				t.Fatalf("error.type missing from %v, want set on failed ping", hist.attrs)
			}
			sp, ok := ex.last()
			if !ok {
				t.Fatal("no span recorded for the startup ping")
			}
			if sp.Name() != "PING" {
				t.Fatalf("span name = %q, want PING", sp.Name())
			}
		})
	}
}

func assertAttrs(t *testing.T, got, want map[string]any) {
	t.Helper()
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("attr %s = %#v, want %#v (all: %v)", k, got[k], v, got)
		}
	}
}

func TestInstrumentHook_MetricAttributes(t *testing.T) {
	base := map[string]any{
		"server.address": "cache.example.com",
		"server.port":    6380,
		"db.system.name": "redis",
		"db.namespace":   "2",
	}
	withBase := func(extra map[string]any) map[string]any {
		out := maps.Clone(base)
		maps.Copy(out, extra)
		return out
	}

	cases := []struct {
		name      string
		pipeline  bool
		err       error
		wantOp    string
		wantAttrs map[string]any
		wantSpan  map[string]any // span-only attributes, as their string form
	}{
		{
			name:      "command success",
			wantOp:    "GET",
			wantAttrs: withBase(map[string]any{"db.operation.name": "GET"}),
			wantSpan:  map[string]any{"db.query.text": "GET ?"},
		},
		{
			name:   "command error with redis prefix",
			err:    srvError("WRONGTYPE Operation against a key holding the wrong kind of value"),
			wantOp: "GET",
			wantAttrs: withBase(map[string]any{
				"db.operation.name":       "GET",
				"db.response.status_code": "WRONGTYPE",
				"error.type":              "WRONGTYPE",
			}),
			wantSpan: map[string]any{"db.query.text": "GET ?"},
		},
		{
			name:   "command error unknown",
			err:    errors.New("dial tcp 127.0.0.1:6379: connection refused"),
			wantOp: "GET",
			wantAttrs: withBase(map[string]any{
				"db.operation.name": "GET",
				"error.type":        "_OTHER",
			}),
			wantSpan: map[string]any{"db.query.text": "GET ?"},
		},
		{
			name:   "command error timeout",
			err:    context.DeadlineExceeded,
			wantOp: "GET",
			wantAttrs: withBase(map[string]any{
				"db.operation.name": "GET",
				"error.type":        "timeout",
			}),
			wantSpan: map[string]any{"db.query.text": "GET ?"},
		},
		{
			name:   "command error canceled",
			err:    context.Canceled,
			wantOp: "GET",
			wantAttrs: withBase(map[string]any{
				"db.operation.name": "GET",
				"error.type":        "canceled",
			}),
			wantSpan: map[string]any{"db.query.text": "GET ?"},
		},
		{
			name:      "pipeline success",
			pipeline:  true,
			wantOp:    "PIPELINE",
			wantAttrs: withBase(map[string]any{"db.operation.name": "PIPELINE"}),
			wantSpan: map[string]any{
				"db.query.text":           "GET ?; SET ? ?",
				"db.operation.batch.size": "2",
			},
		},
		{
			name:     "pipeline error",
			pipeline: true,
			err:      srvError("ERR database is read-only"),
			wantOp:   "PIPELINE",
			wantAttrs: withBase(map[string]any{
				"db.operation.name":       "PIPELINE",
				"db.response.status_code": "ERR",
				"error.type":              "ERR",
			}),
			wantSpan: map[string]any{
				"db.query.text":           "GET ?; SET ? ?",
				"db.operation.batch.size": "2",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, fm, ex := setupInstrument()
			cmds := []redis.Cmder{
				redis.NewStringCmd(context.Background(), "get", "a"),
				redis.NewStatusCmd(context.Background(), "set", "b", "1"),
			}
			if !tc.pipeline {
				cmds = cmds[:1]
			}
			var hook func(context.Context) error
			if tc.pipeline {
				next := h.ProcessPipelineHook(func(_ context.Context, _ []redis.Cmder) error { return tc.err })
				hook = func(ctx context.Context) error { return next(ctx, cmds) }
			} else {
				next := h.ProcessHook(func(_ context.Context, _ redis.Cmder) error { return tc.err })
				hook = func(ctx context.Context) error { return next(ctx, cmds[0]) }
			}

			_ = hook(context.Background())

			if n := len(fm.counts); n != 0 {
				t.Errorf("count records = %d, want 0", n)
			}
			if len(fm.hists) != 1 {
				t.Fatalf("histogram records = %d, want 1", len(fm.hists))
			}
			hist := fm.hists[0]
			if hist.name != metricDuration {
				t.Errorf("metric name = %q, want %q", hist.name, metricDuration)
			}
			if !maps.Equal(hist.attrs, tc.wantAttrs) {
				t.Errorf("metric attrs = %v, want %v", hist.attrs, tc.wantAttrs)
			}

			sp, ok := ex.last()
			if !ok {
				t.Fatal("no span recorded")
			}
			if sp.Name() != tc.wantOp {
				t.Errorf("span name = %q, want %q", sp.Name(), tc.wantOp)
			}
			got := spanAttrs(sp)
			for k, v := range tc.wantSpan {
				if got[k] != v {
					t.Errorf("span attr %s = %v, want %v", k, got[k], v)
				}
			}
		})
	}
}
