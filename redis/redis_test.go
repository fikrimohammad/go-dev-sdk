package redis

import (
	"context"
	"errors"
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
		"db.response.status_code": "",
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
		"db.response.status_code": "",
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
	if count.attrs["db.response.status_code"] != "WRONGTYPE" {
		t.Fatalf("db.response.status_code = %v, want WRONGTYPE", count.attrs["db.response.status_code"])
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
	if attrs["db.response.status_code"] != "WRONGTYPE" || attrs["error.type"] != boom.Error() {
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
	if count.attrs["db.response.status_code"] != "" || count.attrs["error.type"] != "" {
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
	for _, want := range []string{metricCount, metricDuration} {
		if !slices.Contains(names, want) {
			t.Errorf("OTel global meter provider collected %v, want %q among them", names, want)
		}
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
	boom := errors.New("ERR database is read-only")
	cmd2.SetErr(boom)

	cmds := []redis.Cmder{cmd1, cmd2}
	hook := h.ProcessPipelineHook(func(_ context.Context, _ []redis.Cmder) error {
		// In go-redis, pipe.Exec returns the first error encountered, which is redis.Nil
		return redis.Nil
	})

	if err := hook(context.Background(), cmds); !errors.Is(err, redis.Nil) {
		t.Fatalf("ProcessPipelineHook err = %v, want redis.Nil", err)
	}

	count, ok := fm.lastCount()
	if !ok {
		t.Fatal("no count recorded")
	}
	if count.attrs["db.response.status_code"] != "ERR" {
		t.Fatalf("db.response.status_code = %v, want ERR", count.attrs["db.response.status_code"])
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
}

func TestRedisErrorCode(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{errors.New("WRONGTYPE Operation against key"), "WRONGTYPE"},
		{errors.New("ERR unknown command"), "ERR"},
		{errors.New("OOM command not allowed"), "OOM"},
		{errors.New("BUSYKEY Target key name already exists"), "BUSYKEY"},
		{errors.New("NOSCRIPT No matching script"), "NOSCRIPT"},
		{errors.New("dial tcp 127.0.0.1:6379: connect: connection refused"), ""},
		{errors.New("context deadline exceeded"), ""},
		{errors.New("read tcp 127.0.0.1:6379: i/o timeout"), ""},
		{errors.New("redis: client is closed"), ""},
		{errors.New("plain error"), ""},
	}
	for _, tc := range cases {
		if got := redisErrorCode(tc.err); got != tc.want {
			t.Errorf("redisErrorCode(%q) = %q, want %q", tc.err.Error(), got, tc.want)
		}
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
		t.Fatalf("expected nil tlsCfg, got %v, err=%v", tlsCfg, err)
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
		t.Fatal("expected error for non-existent CA file")
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

func TestNew_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := New(ctx, Config{Host: "127.0.0.1", Port: 6379, ConnectTimeout: time.Second})
	if err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

func TestNew_InvalidConfig(t *testing.T) {
	if _, err := New(context.Background(), Config{Host: "", Port: 6379}); err == nil {
		t.Fatal("New: expected error for missing host")
	}
	if _, err := New(context.Background(), Config{Host: "h", Port: -1}); err == nil {
		t.Fatal("New: expected error for invalid port")
	}
}

func TestNew_ConnectionFailure(t *testing.T) {
	_, err := New(context.Background(), Config{Host: "127.0.0.1", Port: 1, ConnectTimeout: 100})
	if err == nil {
		t.Fatal("New: expected connection error")
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

func TestCreateUnderlyingClient_Topologies(t *testing.T) {
	// 1. Standalone
	standaloneCfg := Config{
		Host: "127.0.0.1",
		Port: 6379,
	}.SetDefaults()
	standaloneCli := createUnderlyingClient(standaloneCfg, nil)
	if _, ok := standaloneCli.(*redis.Client); !ok {
		t.Fatalf("expected *redis.Client for standalone, got %T", standaloneCli)
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
		t.Fatalf("expected *redis.Client for failover, got %T", sentinelCli)
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
		t.Fatalf("expected *redis.ClusterClient for failover cluster, got %T", sentinelClusterCli)
	}
	_ = sentinelClusterCli.Close()

	// 4. Redis Cluster
	clusterCfg := Config{
		Mode:  ModeCluster,
		Addrs: []string{"127.0.0.1:7000", "127.0.0.1:7001"},
	}.SetDefaults()
	clusterCli := createUnderlyingClient(clusterCfg, nil)
	if _, ok := clusterCli.(*redis.ClusterClient); !ok {
		t.Fatalf("expected *redis.ClusterClient for cluster, got %T", clusterCli)
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
			h, fm, ex := setupInstrument()
			cli := createUnderlyingClient(tc.cfg.SetDefaults(), nil)
			t.Cleanup(func() { _ = cli.Close() })
			cli.AddHook(h)

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			if err := cli.Ping(ctx).Err(); err == nil {
				t.Fatal("Ping: got nil error, want connection error")
			}

			count, ok := fm.lastCount()
			if !ok {
				t.Fatal("no count recorded for command sent through UniversalClient")
			}
			if got := count.attrs["db.operation.name"]; got != "PING" {
				t.Fatalf("db.operation.name = %v, want PING", got)
			}
			sp, ok := ex.last()
			if !ok {
				t.Fatal("no span recorded for command sent through UniversalClient")
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
