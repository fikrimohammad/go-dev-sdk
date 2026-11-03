package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

// fakeMetrics captures the metrics client's records. It is a metrics fake, not a
// transport stub: the commands it observes run against a real miniredis server.
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

func (f *fakeMetrics) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts, f.hists = nil, nil
}

// stalledAddr returns the address of a TCP listener that accepts connections and
// never replies, so a client read on it times out.
func stalledAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return ln.Addr().String()
}

// closedAddr returns an address with nothing listening on it.
func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// pingNetErr returns the net error go-redis reports for a PING to addr. Retries are
// off, so the error is the first attempt's.
func pingNetErr(t *testing.T, addr string, readTimeout time.Duration) error {
	t.Helper()
	cli := redis.NewClient(&redis.Options{
		Addr:            addr,
		ReadTimeout:     readTimeout,
		DialTimeout:     time.Second,
		MaxRetries:      -1,
		Protocol:        2,
		DisableIdentity: true,
	})
	t.Cleanup(func() { _ = cli.Close() })
	err := cli.Ping(t.Context()).Err()
	if _, ok := errors.AsType[net.Error](err); !ok {
		t.Fatalf("PING err = %v, want a net.Error", err)
	}
	return err
}

// --- fake: span capture ------------------------------------------------------------

// recordingExporter captures the spans the tracer client exports.
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

// spanNames returns the names of the exported spans, in export order.
func (e *recordingExporter) spanNames() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	names := make([]string, 0, len(e.spans))
	for _, sp := range e.spans {
		names = append(names, sp.Name())
	}
	return names
}

func (e *recordingExporter) reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = nil
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

// telemetry bundles the capturing clients a test injects into New.
type telemetry struct {
	fm *fakeMetrics
	tc tracer.Client
	ex *recordingExporter
}

func newTelemetry() telemetry {
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	return telemetry{fm: &fakeMetrics{}, tc: tracer.Wrap(tp), ex: ex}
}

// options returns the options that inject tel's clients into New.
func (tel telemetry) options() []Option {
	return []Option{WithMetrics(tel.fm), WithTracer(tel.tc)}
}

// reset drops everything tel has captured so far.
func (tel telemetry) reset() {
	tel.fm.reset()
	tel.ex.reset()
}

// newClient starts a miniredis server and returns the client New opens against
// it. The client is closed when the test ends.
func newClient(t *testing.T, opts ...Option) (*miniredis.Miniredis, redis.UniversalClient) { //nolint:ireturn // returns go-redis UniversalClient, as New does
	t.Helper()
	m := miniredis.RunT(t)
	cli, err := New(t.Context(), Config{Addrs: []string{m.Addr()}}, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return m, cli
}

// newCapturedClient is newClient with tel injected. The startup PING is cleared
// from tel, so a test sees only the commands it runs.
func newCapturedClient(t *testing.T, tel telemetry, opts ...Option) (*miniredis.Miniredis, redis.UniversalClient) { //nolint:ireturn // returns go-redis UniversalClient, as New does
	t.Helper()
	m, cli := newClient(t, append(tel.options(), opts...)...)
	tel.reset()
	return m, cli
}

// replier returns a function that makes the server answer with msg and returns the
// error go-redis reports for the reply. The reply is a real RESP error, so its
// prefix is parsed by go-redis.
func replier(t *testing.T) func(msg string) error {
	t.Helper()
	m := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: m.Addr(), Protocol: 2, DisableIdentity: true})
	t.Cleanup(func() { _ = cli.Close() })
	return func(msg string) error {
		m.SetError(msg)
		return cli.Ping(t.Context()).Err()
	}
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

// collect reads r and returns the collected metrics.
func collect(t *testing.T, r *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	return rm
}

// collectedMetrics returns the names of the metrics r has collected so far.
func collectedMetrics(t *testing.T, r *sdkmetric.ManualReader) []string {
	t.Helper()
	var names []string
	for _, sm := range collect(t, r).ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
		}
	}
	return names
}

// durationOps returns the db.operation.name of every data point of the duration
// histogram that r has collected, sorted.
func durationOps(t *testing.T, r *sdkmetric.ManualReader) []string {
	t.Helper()
	var ops []string
	for _, sm := range collect(t, r).ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricDuration {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("%s data type = %T, want metricdata.Histogram[float64]", m.Name, m.Data)
			}
			for _, dp := range hist.DataPoints {
				ops = append(ops, attrStrings(dp.Attributes)["db.operation.name"])
			}
		}
	}
	slices.Sort(ops)
	return ops
}

// countOf returns how many elements of ss equal v.
func countOf(ss []string, v string) int {
	n := 0
	for _, s := range ss {
		if s == v {
			n++
		}
	}
	return n
}

// attrStrings returns the attributes of set in their string form.
func attrStrings(set attribute.Set) map[string]string {
	out := make(map[string]string)
	for _, kv := range set.ToSlice() {
		out[string(kv.Key)] = kv.Value.AsString()
	}
	return out
}

// metricBase returns the metric attributes every command against m carries, with op
// as db.operation.name.
func metricBase(m *miniredis.Miniredis, op string) map[string]any {
	port, _ := strconv.Atoi(m.Port())
	return map[string]any{
		"server.address":    m.Host(),
		"server.port":       port,
		"db.system.name":    "redis",
		"db.namespace":      "0",
		"db.operation.name": op,
	}
}

// spanBase returns the span attributes every command against m carries, in their
// string form, with op as db.operation.name.
func spanBase(m *miniredis.Miniredis, op string) map[string]any {
	return map[string]any{
		"server.address":    m.Host(),
		"server.port":       m.Port(),
		"db.system.name":    "redis",
		"db.namespace":      "0",
		"db.operation.name": op,
	}
}

// --- tests -------------------------------------------------------------------------

func TestProcessHook_Get_Success(t *testing.T) {
	tel := newTelemetry()
	m, cli := newCapturedClient(t, tel)
	m.Set("report:1", "42")

	if got, err := cli.Get(t.Context(), "report:1").Result(); err != nil || got != "42" {
		t.Fatalf("Get = %q, %v, want 42, nil", got, err)
	}

	if n := len(tel.fm.counts); n != 0 {
		t.Fatalf("count records = %d, want 0", n)
	}
	hist, ok := tel.fm.lastHist()
	if !ok || hist.name != metricDuration {
		t.Fatalf("hist = %+v", hist)
	}
	assertAttrs(t, hist.attrs, metricBase(m, "GET"))
	if _, ok := hist.attrs["db.query.text"]; ok {
		t.Fatal("metric carries db.query.text, want span only")
	}

	sp, ok := tel.ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Name() != "GET" || sp.SpanKind() != trace.SpanKindClient {
		t.Fatalf("span name=%q kind=%v", sp.Name(), sp.SpanKind())
	}
	spanWant := spanBase(m, "GET")
	spanWant["db.query.text"] = "GET ?"
	assertAttrs(t, spanAttrs(sp), spanWant)
	if sp.Status().Code != codes.Unset {
		t.Fatalf("status = %v, want unset", sp.Status().Code)
	}
}

func TestProcessHook_Set_QueryText(t *testing.T) {
	tel := newTelemetry()
	_, cli := newCapturedClient(t, tel)

	if err := cli.Set(t.Context(), "lock:1", "1", 1500*time.Millisecond).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	hist, ok := tel.fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	if hist.attrs["db.operation.name"] != "SET" {
		t.Fatalf("db.operation.name = %v, want SET", hist.attrs["db.operation.name"])
	}
	sp, ok := tel.ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if got := spanAttrs(sp)["db.query.text"]; got != "SET ? ? ? ?" {
		t.Fatalf("db.query.text = %v, want redacted args", got)
	}
}

func TestProcessHook_Error(t *testing.T) {
	tel := newTelemetry()
	m, cli := newCapturedClient(t, tel)
	m.Set("report:1", "v")

	err := cli.LPush(t.Context(), "report:1", "x").Err()
	if !strings.HasPrefix(err.Error(), "WRONGTYPE ") {
		t.Fatalf("LPush err = %v, want WRONGTYPE reply", err)
	}

	hist, ok := tel.fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	if hist.attrs["db.response.status_code"] != "WRONGTYPE" {
		t.Fatalf("db.response.status_code = %v, want WRONGTYPE", hist.attrs["db.response.status_code"])
	}
	if hist.attrs["error.type"] != "WRONGTYPE" {
		t.Fatalf("error.type = %v, want WRONGTYPE", hist.attrs["error.type"])
	}

	sp, ok := tel.ex.last()
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
	tel := newTelemetry()
	_, cli := newCapturedClient(t, tel)

	if err := cli.Get(t.Context(), "missing").Err(); !errors.Is(err, redis.Nil) {
		t.Fatalf("Get err = %v, want redis.Nil", err)
	}
	hist, ok := tel.fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	for _, k := range []string{"db.response.status_code", "error.type"} {
		if _, ok := hist.attrs[k]; ok {
			t.Fatalf("redis.Nil must not set %s, got %v", k, hist.attrs)
		}
	}
	sp, ok := tel.ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Unset {
		t.Fatalf("status = %v, want unset", sp.Status().Code)
	}
}

func TestProcessPipelineHook(t *testing.T) {
	tel := newTelemetry()
	m, cli := newCapturedClient(t, tel)
	m.Set("a", "1")

	if _, err := cli.Pipelined(t.Context(), func(p redis.Pipeliner) error {
		p.Get(t.Context(), "a")
		p.Set(t.Context(), "b", "1", 0)
		return nil
	}); err != nil {
		t.Fatalf("Pipelined: %v", err)
	}

	hist, ok := tel.fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	if hist.attrs["db.operation.name"] != "PIPELINE" {
		t.Fatalf("db.operation.name = %v, want PIPELINE", hist.attrs["db.operation.name"])
	}
	if _, ok := hist.attrs["db.operation.batch.size"]; ok {
		t.Fatal("metric carries db.operation.batch.size, want span only")
	}

	sp, ok := tel.ex.last()
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
	cases := []struct {
		name      string
		tx        bool // run through TxPipelined, which wraps the commands in MULTI and EXEC
		queue     func(ctx context.Context, p redis.Pipeliner)
		wantBatch string // empty when db.operation.batch.size must be absent
		wantText  string
	}{
		{
			name:     "single command is not a batch",
			queue:    func(ctx context.Context, p redis.Pipeliner) { p.Get(ctx, "a") },
			wantText: "GET ?",
		},
		{
			name: "two commands",
			queue: func(ctx context.Context, p redis.Pipeliner) {
				p.Get(ctx, "a")
				p.Set(ctx, "b", "1", 0)
			},
			wantBatch: "2",
			wantText:  "GET ?; SET ? ?",
		},
		{
			name: "transaction: MULTI and EXEC are not counted",
			tx:   true,
			queue: func(ctx context.Context, p redis.Pipeliner) {
				p.Get(ctx, "a")
				p.Set(ctx, "b", "1", 0)
			},
			wantBatch: "2",
			wantText:  "GET ?; SET ? ?",
		},
		{
			name:     "transaction with one command is not a batch",
			tx:       true,
			queue:    func(ctx context.Context, p redis.Pipeliner) { p.Get(ctx, "a") },
			wantText: "GET ?",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tel := newTelemetry()
			m, cli := newCapturedClient(t, tel)
			m.Set("a", "1")

			ctx := t.Context()
			exec := cli.Pipelined
			if tc.tx {
				exec = cli.TxPipelined
			}
			if _, err := exec(ctx, func(p redis.Pipeliner) error {
				tc.queue(ctx, p)
				return nil
			}); err != nil {
				t.Fatalf("pipeline: %v", err)
			}

			sp, ok := tel.ex.last()
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

	// No injected clients: New and the commands must fall back to the OTel globals.
	m, cli := newClient(t)
	m.Set("k", "v")
	if err := cli.Get(t.Context(), "k").Err(); err != nil {
		t.Fatalf("Get: %v", err)
	}

	// The GET reaches the global tracer, and so does the startup PING, with the
	// connection handshake that go-redis sends through the same hook.
	if n := countOf(ex.spanNames(), "GET"); n != 1 {
		t.Fatalf("OTel global tracer GET spans = %d, want 1 (names %v)", n, ex.spanNames())
	}
	if names := collectedMetrics(t, reader); !slices.Equal(names, []string{metricDuration}) {
		t.Errorf("OTel global meter provider collected %v, want [%s]", names, metricDuration)
	}
	if n := countOf(durationOps(t, reader), "GET"); n != 1 {
		t.Errorf("duration samples for GET = %d, want 1", n)
	}
}

func TestInstrumentHook_MetricInstrument(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, cli := newClient(t,
		WithMetrics(metrics.Wrap(mp)),
		WithTracer(tracer.Wrap(sdktrace.NewTracerProvider())),
	)
	m.Set("k", "v")
	if err := cli.Get(t.Context(), "k").Err(); err != nil {
		t.Fatalf("Get: %v", err)
	}

	var found []metricdata.Metrics
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name == metricDuration {
				found = append(found, mt)
			}
		}
	}
	if len(found) != 1 {
		t.Fatalf("metrics named %q = %d, want 1", metricDuration, len(found))
	}
	mt := found[0]
	if mt.Unit != "s" {
		t.Errorf("unit = %q, want %q", mt.Unit, "s")
	}
	hist, ok := mt.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("data type = %T, want metricdata.Histogram[float64]", mt.Data)
	}
	// Points are keyed by operation. Only the GET point is checked: the startup
	// handshake adds points of its own.
	var gets int
	for _, dp := range hist.DataPoints {
		if attrStrings(dp.Attributes)["db.operation.name"] != "GET" {
			continue
		}
		gets++
		if dp.Count != 1 {
			t.Errorf("GET data point count = %d, want 1", dp.Count)
		}
	}
	if gets != 1 {
		t.Fatalf("GET data points = %d, want 1", gets)
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
	tel := newTelemetry()
	m, cli := newCapturedClient(t, tel)
	m.Set("s", "v") // a string, so LPUSH below fails with WRONGTYPE

	// The pipeline's first error is the Get miss (redis.Nil); the later LPUSH is a
	// real failure that the hook must still report.
	_, err := cli.Pipelined(t.Context(), func(p redis.Pipeliner) error {
		p.Get(t.Context(), "missing")
		p.LPush(t.Context(), "s", "x")
		return nil
	})
	if !errors.Is(err, redis.Nil) {
		t.Fatalf("Pipelined err = %v, want redis.Nil", err)
	}

	hist, ok := tel.fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	if hist.attrs["db.response.status_code"] != "WRONGTYPE" {
		t.Fatalf("db.response.status_code = %v, want WRONGTYPE", hist.attrs["db.response.status_code"])
	}
	if hist.attrs["error.type"] != "WRONGTYPE" {
		t.Fatalf("error.type = %v, want WRONGTYPE", hist.attrs["error.type"])
	}

	sp, ok := tel.ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
}

func TestRedisErrorCode(t *testing.T) {
	reply := replier(t)
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"wrongtype", reply("WRONGTYPE Operation against key"), "WRONGTYPE"},
		{"err", reply("ERR unknown command"), "ERR"},
		{"oom", reply("OOM command not allowed"), "OOM"},
		{"busykey", reply("BUSYKEY Target key name already exists"), "BUSYKEY"},
		{"noscript", reply("NOSCRIPT No matching script"), "NOSCRIPT"},
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
	reply := replier(t)
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"deadline exceeded", context.DeadlineExceeded, "timeout"},
		{"wrapped deadline exceeded", fmt.Errorf("op: %w", context.DeadlineExceeded), "timeout"},
		{"canceled", context.Canceled, "canceled"},
		{"net timeout", pingNetErr(t, stalledAddr(t), 100*time.Millisecond), "timeout"},
		{"net error, connection refused", pingNetErr(t, closedAddr(t), time.Second), "_OTHER"},
		{"pool timeout", redis.ErrPoolTimeout, "timeout"},
		{"server error", reply("WRONGTYPE Operation against key"), "WRONGTYPE"},
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
	m := miniredis.RunT(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := New(ctx, Config{Addrs: []string{m.Addr()}, ConnectTimeout: time.Second})
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
	m := miniredis.RunT(t)
	m.SetError("ERR server refused connections") // the server answers the handshake with an error

	_, err := New(t.Context(), Config{Addrs: []string{m.Addr()}, ConnectTimeout: 200 * time.Millisecond})
	if err == nil {
		t.Fatal("New(refusing server) err = nil, want connection error")
	}
	if !strings.Contains(err.Error(), "ping") {
		t.Fatalf("New err = %q, want ping error", err.Error())
	}
}

func TestClient_Close_WithPoolMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, cli := newClient(t, WithMetrics(metrics.Wrap(mp)), WithPoolMetrics(time.Hour))

	c := cli.(*client)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A second call must not panic or block; go-redis reports the closed pool.
	if err := c.Close(); err == nil || !strings.Contains(err.Error(), "client is closed") {
		t.Fatalf("second Close err = %v, want client is closed", err)
	}
	select {
	case <-c.stopPoolMetrics:
	default:
		t.Fatal("stopPoolMetrics channel was not closed")
	}

	// The first poll recorded the live pool, and stopping the poller withdrew it.
	pool := net.JoinHostPort(m.Host(), m.Port()) + "/0"
	counts, cm := poolSums(t, reader, "db.client.connection.count", pool,
		"db.client.connection.pool.name", "db.client.connection.state")
	if cm.Name == "" {
		t.Fatal("count metric absent, want the first poll recorded")
	}
	for _, state := range []string{"idle", "used"} {
		if got := counts[state]; got != 0 {
			t.Errorf("count{state=%s} after Close = %d, want 0", state, got)
		}
	}
}

// poolTestName is the db.client.connection.pool.name the pool tests expect.
const poolTestName = "cache.example.com:6380"

// poolSums collects r and returns the summed value of the named int64 sum per
// db.client.connection.state, keyed by state ("" for a point without one). It
// fails when a point's attribute keys are not exactly wantKeys or when its pool
// name is not pool. The returned metric has an empty Name when no point exists yet.
func poolSums(t *testing.T, r *sdkmetric.ManualReader, name, pool string, wantKeys ...string) (map[string]int64, metricdata.Metrics) {
	t.Helper()
	wantKeys = slices.Clone(wantKeys)
	slices.Sort(wantKeys)

	sums := make(map[string]int64)
	var found metricdata.Metrics
	for _, sm := range collect(t, r).ScopeMetrics {
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
				attrs := attrStrings(dp.Attributes)
				if got := slices.Sorted(maps.Keys(attrs)); !slices.Equal(got, wantKeys) {
					t.Errorf("%s attribute keys = %v, want %v", name, got, wantKeys)
				}
				if got := attrs["db.client.connection.pool.name"]; got != pool {
					t.Errorf("%s pool.name = %q, want %q", name, got, pool)
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

			counts, cm := poolSums(t, reader, "db.client.connection.count", poolTestName,
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

			timeouts, tm := poolSums(t, reader, "db.client.connection.timeouts", poolTestName, "db.client.connection.pool.name")
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

	counts, cm := poolSums(t, reader, "db.client.connection.count", poolTestName,
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
	m := miniredis.RunT(t)
	raw := redis.NewClient(&redis.Options{Addr: m.Addr()})
	t.Cleanup(func() { _ = raw.Close() })

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	stop := make(chan struct{})
	c := &client{UniversalClient: raw, stopPoolMetrics: stop}
	p := &poolPoller{
		// kept: the test blocks the poller inside the stats source, which only an
		// injected stats func allows; a live pool cannot be held mid-poll.
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
	case <-time.After(150 * time.Millisecond):
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
	// kept: the client type per topology is decided without dialing, and miniredis
	// is standalone-only, so these cases only inspect the value returned.

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

// assertFailedStartupPing checks that the PING New sent while failing was recorded
// through the instrumentation hook, with an error.type.
func assertFailedStartupPing(t *testing.T, tel telemetry) {
	t.Helper()
	hist, ok := tel.fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded for the startup ping")
	}
	if got := hist.attrs["db.operation.name"]; got != "PING" {
		t.Fatalf("db.operation.name = %v, want PING", got)
	}
	if _, ok := hist.attrs["error.type"]; !ok {
		t.Fatalf("error.type missing from %v, want set on failed ping", hist.attrs)
	}
	sp, ok := tel.ex.last()
	if !ok {
		t.Fatal("no span recorded for the startup ping")
	}
	if sp.Name() != "PING" {
		t.Fatalf("span name = %q, want PING", sp.Name())
	}
}

func TestNew_ReturnsUniversalClient(t *testing.T) {
	t.Run("standalone", func(t *testing.T) {
		m := miniredis.RunT(t)
		// Every reply is an error, so the startup PING fails inside the hook.
		m.SetError("ERR server refused connections")
		tel := newTelemetry()

		cli, err := New(t.Context(), Config{Addrs: []string{m.Addr()}, ConnectTimeout: 200 * time.Millisecond}, tel.options()...)
		if err == nil {
			_ = cli.Close()
			t.Fatal("New: got nil error, want connection error")
		}
		assertFailedStartupPing(t, tel)
	})

	// kept: miniredis is standalone-only. Cluster and sentinel topologies can only
	// be reached through an unreachable seed address, so the failing PING is the
	// only way to see the hook run for them.
	cases := []struct {
		name string
		cfg  Config
	}{
		{"cluster", Config{Mode: ModeCluster, Addrs: []string{"127.0.0.1:1"}}},
		{"sentinel", Config{Mode: ModeSentinel, MasterName: "mymaster", Addrs: []string{"127.0.0.1:1"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tel := newTelemetry()
			cfg := tc.cfg
			cfg.ConnectTimeout = 200 * time.Millisecond

			cli, err := New(t.Context(), cfg, tel.options()...)
			if err == nil {
				_ = cli.Close()
				t.Fatal("New: got nil error, want connection error")
			}
			assertFailedStartupPing(t, tel)
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
	cases := []struct {
		name      string
		run       func(ctx context.Context, cli redis.UniversalClient) error
		wantOp    string
		wantAttrs map[string]any // metric attributes beyond the base and db.operation.name
		wantSpan  map[string]any // span attributes, as their string form, beyond the base
	}{
		{
			name: "command success",
			run: func(ctx context.Context, cli redis.UniversalClient) error {
				return cli.Get(ctx, "k").Err()
			},
			wantOp:   "GET",
			wantSpan: map[string]any{"db.query.text": "GET ?"},
		},
		{
			name: "command error with redis prefix",
			run: func(ctx context.Context, cli redis.UniversalClient) error {
				return cli.LPush(ctx, "k", "x").Err() // k is a string
			},
			wantOp: "LPUSH",
			wantAttrs: map[string]any{
				"db.response.status_code": "WRONGTYPE",
				"error.type":              "WRONGTYPE",
			},
			wantSpan: map[string]any{"db.query.text": "LPUSH ? ?"},
		},
		{
			name: "command error unknown",
			run: func(ctx context.Context, cli redis.UniversalClient) error {
				_ = cli.Close() // a closed client fails the command without a server reply
				return cli.Get(ctx, "k").Err()
			},
			wantOp:    "GET",
			wantAttrs: map[string]any{"error.type": "_OTHER"},
			wantSpan:  map[string]any{"db.query.text": "GET ?"},
		},
		{
			name: "command error timeout",
			run: func(ctx context.Context, cli redis.UniversalClient) error {
				ctx, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				return cli.Get(ctx, "k").Err()
			},
			wantOp:    "GET",
			wantAttrs: map[string]any{"error.type": "timeout"},
			wantSpan:  map[string]any{"db.query.text": "GET ?"},
		},
		{
			name: "command error canceled",
			run: func(ctx context.Context, cli redis.UniversalClient) error {
				ctx, cancel := context.WithCancel(ctx)
				cancel()
				return cli.Get(ctx, "k").Err()
			},
			wantOp:    "GET",
			wantAttrs: map[string]any{"error.type": "canceled"},
			wantSpan:  map[string]any{"db.query.text": "GET ?"},
		},
		{
			name: "pipeline success",
			run: func(ctx context.Context, cli redis.UniversalClient) error {
				_, err := cli.Pipelined(ctx, func(p redis.Pipeliner) error {
					p.Get(ctx, "k")
					p.Set(ctx, "b", "1", 0)
					return nil
				})
				return err
			},
			wantOp: "PIPELINE",
			wantSpan: map[string]any{
				"db.query.text":           "GET ?; SET ? ?",
				"db.operation.batch.size": "2",
			},
		},
		{
			name: "pipeline error",
			run: func(ctx context.Context, cli redis.UniversalClient) error {
				_, err := cli.Pipelined(ctx, func(p redis.Pipeliner) error {
					p.Set(ctx, "b", "1", 0)
					p.LPush(ctx, "k", "x") // k is a string
					return nil
				})
				return err
			},
			wantOp: "PIPELINE",
			wantAttrs: map[string]any{
				"db.response.status_code": "WRONGTYPE",
				"error.type":              "WRONGTYPE",
			},
			wantSpan: map[string]any{
				"db.query.text":           "SET ? ?; LPUSH ? ?",
				"db.operation.batch.size": "2",
				"db.response.status_code": "WRONGTYPE",
				"error.type":              "WRONGTYPE",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tel := newTelemetry()
			m, cli := newCapturedClient(t, tel)
			m.Set("k", "v")

			_ = tc.run(t.Context(), cli)

			if n := len(tel.fm.counts); n != 0 {
				t.Errorf("count records = %d, want 0", n)
			}
			if len(tel.fm.hists) != 1 {
				t.Fatalf("histogram records = %d, want 1", len(tel.fm.hists))
			}
			hist := tel.fm.hists[0]
			if hist.name != metricDuration {
				t.Errorf("metric name = %q, want %q", hist.name, metricDuration)
			}
			wantAttrs := metricBase(m, tc.wantOp)
			maps.Copy(wantAttrs, tc.wantAttrs)
			if !maps.Equal(hist.attrs, wantAttrs) {
				t.Errorf("metric attrs = %v, want %v", hist.attrs, wantAttrs)
			}

			sp, ok := tel.ex.last()
			if !ok {
				t.Fatal("no span recorded")
			}
			if sp.Name() != tc.wantOp {
				t.Errorf("span name = %q, want %q", sp.Name(), tc.wantOp)
			}
			got := spanAttrs(sp)
			wantSpan := spanBase(m, tc.wantOp)
			maps.Copy(wantSpan, tc.wantSpan)
			for k, v := range wantSpan {
				if got[k] != v {
					t.Errorf("span attr %s = %v, want %v", k, got[k], v)
				}
			}
		})
	}
}
