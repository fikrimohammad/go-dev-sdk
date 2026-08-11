package redis

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
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

func (f *fakeMetrics) Count(_ context.Context, name string, value int64, attrs map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts = append(f.counts, metricRec{name, float64(value), normalizeAttrs(attrs)})
	return nil
}

func (f *fakeMetrics) Histogram(_ context.Context, name string, value float64, attrs map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hists = append(f.hists, metricRec{name, value, normalizeAttrs(attrs)})
	return nil
}

func (f *fakeMetrics) Stop(context.Context) error { return nil }

func (f *fakeMetrics) lastCount() (metricRec, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.counts) == 0 {
		return metricRec{}, false
	}
	return f.counts[len(f.counts)-1], true
}

func (f *fakeMetrics) lastHist() (metricRec, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.hists) == 0 {
		return metricRec{}, false
	}
	return f.hists[len(f.hists)-1], true
}

func (f *fakeMetrics) nums() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.counts), len(f.hists)
}

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

// setupInstrument builds an instrumentHook with injected capturing clients.
func setupInstrument() (*instrumentHook, *fakeMetrics, *recordingExporter) {
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	fm := &fakeMetrics{}
	return &instrumentHook{
		meta: meta{
			serverAddr: "cache.example.com",
			serverPort: 6380,
			namespace:  "2",
			metrics:    fm,
			tracer:     tracer.Wrap(tp),
		},
	}, fm, ex
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

	count, ok := fm.lastCount()
	if !ok {
		t.Fatal("no count recorded")
	}
	if count.name != metricCount || count.value != 1 {
		t.Fatalf("count = %+v", count)
	}
	if hist, ok := fm.lastHist(); !ok || hist.name != metricDuration {
		t.Fatalf("hist = %+v", hist)
	}

	wantAttrs := map[string]any{
		"server.address":          "cache.example.com",
		"server.port":             6380,
		"db.system.name":          "redis",
		"db.operation.name":       "GET",
		"db.namespace":            "2",
		"db.query.text":           "GET ?",
		"db.response.status.code": "",
		"error.type":              "",
	}
	assertAttrs(t, count.attrs, wantAttrs)

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Name() != "GET" || sp.SpanKind() != trace.SpanKindClient {
		t.Fatalf("span name=%q kind=%v", sp.Name(), sp.SpanKind())
	}
	spanWant := map[string]any{
		"server.address":          "cache.example.com",
		"server.port":             "6380",
		"db.system.name":          "redis",
		"db.operation.name":       "GET",
		"db.namespace":            "2",
		"db.query.text":           "GET ?",
		"db.response.status.code": "",
		"error.type":              "",
	}
	assertAttrs(t, spanAttrs(sp), spanWant)
	if sp.Status().Code != codes.Unset {
		t.Fatalf("status = %v, want unset", sp.Status().Code)
	}
}

func TestProcessHook_Set_QueryText(t *testing.T) {
	h, fm, _ := setupInstrument()
	cmd := redis.NewStatusCmd(context.Background(), "set", "lock:1", true, "px", "1000")
	hook := h.ProcessHook(func(_ context.Context, _ redis.Cmder) error { return nil })

	if err := hook(context.Background(), cmd); err != nil {
		t.Fatalf("ProcessHook: %v", err)
	}
	count, ok := fm.lastCount()
	if !ok {
		t.Fatal("no count recorded")
	}
	if count.attrs["db.operation.name"] != "SET" {
		t.Fatalf("db.operation.name = %v, want SET", count.attrs["db.operation.name"])
	}
	if count.attrs["db.query.text"] != "SET ? ? ? ?" {
		t.Fatalf("db.query.text = %v, want redacted args", count.attrs["db.query.text"])
	}
}

func TestProcessHook_Error(t *testing.T) {
	h, fm, ex := setupInstrument()
	cmd := redis.NewStringCmd(context.Background(), "get", "report:1")
	boom := errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	hook := h.ProcessHook(func(_ context.Context, _ redis.Cmder) error { return boom })

	if err := hook(context.Background(), cmd); !errors.Is(err, boom) {
		t.Fatalf("ProcessHook err = %v, want %v", err, boom)
	}

	count, ok := fm.lastCount()
	if !ok {
		t.Fatal("no count recorded")
	}
	if count.attrs["db.response.status.code"] != "WRONGTYPE" {
		t.Fatalf("db.response.status.code = %v, want WRONGTYPE", count.attrs["db.response.status.code"])
	}
	if count.attrs["error.type"] != boom.Error() {
		t.Fatalf("error.type = %v, want %q", count.attrs["error.type"], boom.Error())
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
	attrs := spanAttrs(sp)
	if attrs["db.response.status.code"] != "WRONGTYPE" || attrs["error.type"] != boom.Error() {
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
	count, ok := fm.lastCount()
	if !ok {
		t.Fatal("no count recorded")
	}
	if count.attrs["db.response.status.code"] != "" || count.attrs["error.type"] != "" {
		t.Fatalf("redis.Nil must not set error telemetry, got %v", count.attrs)
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

	count, ok := fm.lastCount()
	if !ok {
		t.Fatal("no count recorded")
	}
	if count.attrs["db.operation.name"] != "PIPELINE" {
		t.Fatalf("db.operation.name = %v, want PIPELINE", count.attrs["db.operation.name"])
	}
	if count.attrs["db.operation.batch.size"] != 2 {
		t.Fatalf("db.operation.batch.size = %v, want 2", count.attrs["db.operation.batch.size"])
	}
	if count.attrs["db.query.text"] != "GET ?; SET ? ?" {
		t.Fatalf("db.query.text = %v", count.attrs["db.query.text"])
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Name() != "PIPELINE" {
		t.Fatalf("span name = %q, want PIPELINE", sp.Name())
	}
}

func TestPackageDefaults_Fallback(t *testing.T) {
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tracer.SetDefault(tracer.Wrap(tp))
	fm := &fakeMetrics{}
	metrics.SetDefault(fm)

	h := &instrumentHook{meta: meta{serverAddr: "h", serverPort: 6379, namespace: "0"}}
	cmd := redis.NewStringCmd(context.Background(), "get", "k")
	hook := h.ProcessHook(func(_ context.Context, _ redis.Cmder) error { return nil })

	if err := hook(context.Background(), cmd); err != nil {
		t.Fatalf("ProcessHook: %v", err)
	}
	if n := ex.count(); n != 1 {
		t.Fatalf("spans = %d, want 1", n)
	}
	nc, nh := fm.nums()
	if nc != 1 || nh != 1 {
		t.Fatalf("metrics counts=%d hists=%d, want 1/1", nc, nh)
	}
}

func TestPipeline_QueuesCommands(t *testing.T) {
	raw := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = raw.Close() })
	p := &pipeline{pipe: raw.Pipeline()}

	p.Get(context.Background(), "a")
	p.Set(context.Background(), "b", "1", 0)
	p.Del(context.Background(), "c")
	p.Ping(context.Background())

	if n := p.pipe.Len(); n != 4 {
		t.Fatalf("queued commands = %d, want 4", n)
	}
	if err := p.Exec(context.Background()); err == nil {
		t.Fatal("Exec: expected error, no server running")
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := queryText(tc.cmd); got != tc.want {
				t.Fatalf("queryText = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRedisErrorCode(t *testing.T) {
	if got := redisErrorCode(errors.New("WRONGTYPE bad thing")); got != "WRONGTYPE" {
		t.Fatalf("redisErrorCode = %q, want WRONGTYPE", got)
	}
	if got := redisErrorCode(errors.New("ERR no connection")); got != "ERR" {
		t.Fatalf("redisErrorCode = %q, want ERR", got)
	}
	if got := redisErrorCode(errors.New("boom")); got != "boom" {
		t.Fatalf("redisErrorCode(plain) = %q, want boom", got)
	}
}

func TestNew_InvalidConfig(t *testing.T) {
	if _, err := New(Config{Host: "", Port: 6379}); err == nil {
		t.Fatal("New: expected error for missing host")
	}
	if _, err := New(Config{Host: "h", Port: -1}); err == nil {
		t.Fatal("New: expected error for invalid port")
	}
}

func TestNew_ConnectionFailure(t *testing.T) {
	_, err := New(Config{Host: "127.0.0.1", Port: 1, ConnectTimeout: 100})
	if err == nil {
		t.Fatal("New: expected connection error")
	}
	if !strings.Contains(err.Error(), "ping") {
		t.Fatalf("New err = %q, want ping error", err.Error())
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
