package redis

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

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
	p := &pipeline{Pipeliner: raw.Pipeline()}

	ctx := context.Background()

	// Read commands
	p.Ping(ctx)
	p.Exists(ctx, "k")
	p.TTL(ctx, "k")
	p.Type(ctx, "k")
	p.Scan(ctx, 0, "*", 10)
	p.Get(ctx, "a")
	p.MGet(ctx, "a", "b")
	p.HGet(ctx, "h", "f")
	p.HGetAll(ctx, "h")
	p.HMGet(ctx, "h", "f1", "f2")
	p.HKeys(ctx, "h")
	p.HVals(ctx, "h")
	p.HLen(ctx, "h")
	p.HExists(ctx, "h", "f")
	p.LLen(ctx, "l")
	p.LRange(ctx, "l", 0, -1)
	p.LIndex(ctx, "l", 0)
	p.SMembers(ctx, "s")
	p.SInter(ctx, "s1", "s2")
	p.SUnion(ctx, "s1", "s2")
	p.SDiff(ctx, "s1", "s2")
	p.SIsMember(ctx, "s", "m")
	p.SMIsMember(ctx, "s", "m1", "m2")
	p.SCard(ctx, "s")
	p.SRandMember(ctx, "s")
	p.SRandMemberN(ctx, "s", 2)
	p.ZScore(ctx, "z", "m")
	p.ZCard(ctx, "z")
	p.ZCount(ctx, "z", "-inf", "+inf")
	p.ZRange(ctx, "z", 0, -1)
	p.ZRangeWithScores(ctx, "z", 0, -1)
	p.ZRangeByScore(ctx, "z", &ZRangeBy{Min: "0", Max: "10"})
	p.ZRangeByScoreWithScores(ctx, "z", &ZRangeBy{Min: "0", Max: "10"})
	p.ZRevRange(ctx, "z", 0, -1)
	p.ZRevRangeWithScores(ctx, "z", 0, -1)
	p.ZRevRangeByScore(ctx, "z", &ZRangeBy{Min: "0", Max: "10"})
	p.ZRevRangeByScoreWithScores(ctx, "z", &ZRangeBy{Min: "0", Max: "10"})
	p.ZRank(ctx, "z", "m")
	p.ZRevRank(ctx, "z", "m")

	// Write commands
	p.Del(ctx, "c")
	p.Unlink(ctx, "c")
	p.Expire(ctx, "k", time.Minute)
	p.ExpireAt(ctx, "k", time.Now().Add(time.Minute))
	p.Persist(ctx, "k")
	p.Set(ctx, "b", "1", 0)
	p.SetNX(ctx, "b", "1", 0)
	p.SetXX(ctx, "b", "1", 0)
	p.GetDel(ctx, "b")
	p.GetEx(ctx, "b", time.Minute)
	p.MSet(ctx, "k1", "v1", "k2", "v2")
	p.Incr(ctx, "counter")
	p.IncrBy(ctx, "counter", 5)
	p.IncrByFloat(ctx, "counter", 2.5)
	p.Decr(ctx, "counter")
	p.DecrBy(ctx, "counter", 3)
	p.HSet(ctx, "h", "f", "v")
	p.HSetNX(ctx, "h", "f", "v")
	p.HDel(ctx, "h", "f")
	p.HIncrBy(ctx, "h", "f", 1)
	p.HIncrByFloat(ctx, "h", "f", 1.5)
	p.LPush(ctx, "l", "v1", "v2")
	p.RPush(ctx, "l", "v1", "v2")
	p.LPop(ctx, "l")
	p.RPop(ctx, "l")
	p.LPopCount(ctx, "l", 2)
	p.RPopCount(ctx, "l", 2)
	p.LTrim(ctx, "l", 0, 10)
	p.LRem(ctx, "l", 1, "v1")
	p.LSet(ctx, "l", 0, "v1")
	p.SAdd(ctx, "s", "m1", "m2")
	p.SRem(ctx, "s", "m1")
	p.SPop(ctx, "s")
	p.SPopN(ctx, "s", 2)
	p.ZAdd(ctx, "z", Z{Score: 1.0, Member: "m"})
	p.ZRem(ctx, "z", "m")
	p.ZIncrBy(ctx, "z", 1.5, "m")
	p.ZRemRangeByRank(ctx, "z", 0, 1)
	p.ZRemRangeByScore(ctx, "z", "0", "10")
	p.Publish(ctx, "channel", "msg")

	// Script commands
	p.Eval(ctx, "return 1", nil)
	p.EvalSha(ctx, "abc", nil)
	p.ScriptExists(ctx, "abc")
	p.ScriptLoad(ctx, "return 1")

	const expected = 83
	if n := p.Len(); n != expected {
		t.Fatalf("queued commands = %d, want %d", n, expected)
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

func TestTxFailedErr_Alias(t *testing.T) {
	if TxFailedErr != redis.TxFailedErr {
		t.Fatalf("TxFailedErr does not match redis.TxFailedErr")
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
	if count.attrs["db.response.status.code"] != "ERR" {
		t.Fatalf("db.response.status.code = %v, want ERR", count.attrs["db.response.status.code"])
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

func TestClient_TxPipeline_And_Pipelined(t *testing.T) {
	raw := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = raw.Close() })
	cli := &client{Client: raw}

	// TxPipeline
	txPipe := cli.TxPipeline()
	txPipe.Get(context.Background(), "k")
	if n := txPipe.(*pipeline).Len(); n != 1 {
		t.Fatalf("txPipe len = %d, want 1", n)
	}

	// Pipelined with error closure
	errBoom := errors.New("boom")
	err := cli.Pipelined(context.Background(), func(p Pipeline) error {
		p.Get(context.Background(), "k")
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("Pipelined err = %v, want %v", err, errBoom)
	}

	// TxPipelined with error closure
	err = cli.TxPipelined(context.Background(), func(p Pipeline) error {
		p.Get(context.Background(), "k")
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("TxPipelined err = %v, want %v", err, errBoom)
	}

	// PoolStats
	stats := cli.PoolStats()
	if stats == nil {
		t.Fatal("PoolStats is nil")
	}
}

func TestNewWithContext_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewWithContext(ctx, Config{Host: "127.0.0.1", Port: 6379, ConnectTimeout: time.Second})
	if err == nil {
		t.Fatal("expected error with cancelled context")
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

func TestClient_Close_WithPoolMetrics(t *testing.T) {
	raw := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = raw.Close() })
	stopCh := make(chan struct{})
	c := &client{
		Client:          raw,
		stopPoolMetrics: stopCh,
	}
	if err := c.Close(); err != nil {
		// Connection close on dummy client is fine
	}
	// Calling close a second time should not panic
	_ = c.Close()

	select {
	case <-stopCh:
		// Successfully closed
	default:
		t.Fatal("stopPoolMetrics channel was not closed")
	}
}

func TestClient_Watch_And_Subscribe(t *testing.T) {
	raw := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = raw.Close() })
	cli := &client{Client: raw}

	// Subscribe returns *PubSub
	ps := cli.Subscribe(context.Background(), "test-channel")
	if ps == nil {
		t.Fatal("Subscribe returned nil")
	}
	_ = ps.Close()

	// Watch attempts to acquire a connection to watch keys
	err := cli.Watch(context.Background(), func(tx *Tx) error {
		return nil
	}, "watch-key")
	if err == nil {
		t.Fatal("expected connection error from Watch")
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
