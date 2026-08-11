package producer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

var errSend = errors.New("send failed")

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

func spanAttrs(s sdktrace.ReadOnlySpan) map[string]any {
	out := make(map[string]any)
	for _, kv := range s.Attributes() {
		out[string(kv.Key)] = kv.Value.String()
	}
	return out
}

// assertAttrs fails the test if any wanted attribute is missing or different.
func assertAttrs(t *testing.T, got, want map[string]any) {
	t.Helper()
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("attr %s = %#v, want %#v (all: %v)", k, got[k], v, got)
		}
	}
}

// --- fake: producer setup ----------------------------------------------------------

// setupProducer builds a Producer registered with a mock producer and capturing
// metrics and tracer clients.
func setupProducer(t *testing.T, mock *mockProducer) (*Producer, *fakeMetrics, *recordingExporter) {
	t.Helper()

	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	fm := &fakeMetrics{}

	p, err := New(testAppInfo, WithMetrics(fm), WithTracer(tracer.Wrap(tp)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.setProducer("reporting", mock)
	p.producerMeta.Store("reporting", producerMeta{serverAddr: "localhost", serverPort: 9876})
	return p, fm, ex
}

// --- tests -------------------------------------------------------------------------

func TestPublishSync_Success(t *testing.T) {
	mock := &mockProducer{sendSyncFunc: func(ctx context.Context, mq ...*primitive.Message) (*primitive.SendResult, error) {
		return &primitive.SendResult{MsgID: "MSG-123"}, nil
	}}
	p, fm, ex := setupProducer(t, mock)

	if err := p.PublishSync(context.Background(), "reporting", "export_report_process", "key-1", []byte("hello")); err != nil {
		t.Fatalf("PublishSync: %v", err)
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
	wantCountAttrs := map[string]any{
		"messaging.system":                "rocketmq",
		"messaging.operation":             "publish",
		"messaging.destination.name":      "reporting",
		"messaging.message.body.size":     5,
		"server.address":                  "localhost",
		"server.port":                     9876,
		"messaging.rocketmq.message.tag":  "export_report_process",
		"messaging.rocketmq.message.keys": "key-1",
		"messaging.message.id":            "MSG-123",
		"error.type":                      "",
	}
	assertAttrs(t, count.attrs, wantCountAttrs)

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Name() != "reporting publish" || sp.SpanKind() != trace.SpanKindProducer {
		t.Fatalf("span name=%q kind=%v", sp.Name(), sp.SpanKind())
	}
	wantSpanAttrs := map[string]any{
		"messaging.system":                "rocketmq",
		"messaging.operation":             "publish",
		"messaging.destination.name":      "reporting",
		"messaging.message.body.size":     "5",
		"server.address":                  "localhost",
		"server.port":                     "9876",
		"messaging.rocketmq.message.tag":  "export_report_process",
		"messaging.rocketmq.message.keys": "key-1",
		"messaging.message.id":            "MSG-123",
		"error.type":                      "",
	}
	assertAttrs(t, spanAttrs(sp), wantSpanAttrs)
	if sp.Status().Code == codes.Error {
		t.Fatalf("status = %v, want not error", sp.Status().Code)
	}
}

func TestPublishSync_Error(t *testing.T) {
	mock := &mockProducer{sendSyncErr: errSend}
	p, _, ex := setupProducer(t, mock)

	err := p.PublishSync(context.Background(), "reporting", "tag", "", []byte("hello"))
	if err == nil {
		t.Fatal("expected error")
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
	assertAttrs(t, spanAttrs(sp), map[string]any{"error.type": "producer: sync send to topic reporting: send failed"})
}

func TestPublishSyncWithDelay_Success(t *testing.T) {
	mock := &mockProducer{}
	p, fm, ex := setupProducer(t, mock)

	if err := p.PublishSyncWithDelay(context.Background(), "reporting", "tag", "", []byte("hello"), time.Minute); err != nil {
		t.Fatalf("PublishSyncWithDelay: %v", err)
	}

	if _, ok := fm.lastCount(); !ok {
		t.Fatal("no count recorded")
	}
	if _, ok := ex.last(); !ok {
		t.Fatal("no span recorded")
	}
}

func TestPublishAsync_FullLifecycle(t *testing.T) {
	called := false
	mock := &mockProducer{sendAsyncFunc: func(ctx context.Context, cb func(context.Context, *primitive.SendResult, error), msg ...*primitive.Message) error {
		cb(ctx, &primitive.SendResult{MsgID: "ASYNC-1"}, nil)
		return nil
	}}
	p, fm, ex := setupProducer(t, mock)

	err := p.PublishAsync(context.Background(), "reporting", "tag", "k", []byte("hello"),
		func(ctx context.Context, result *primitive.SendResult, err error) {
			called = true
			if result == nil || result.MsgID != "ASYNC-1" {
				t.Errorf("callback result = %+v", result)
			}
		})
	if err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	if !called {
		t.Fatal("callback was not invoked")
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	assertAttrs(t, spanAttrs(sp), map[string]any{"messaging.message.id": "ASYNC-1"})
	if _, ok := fm.lastCount(); !ok {
		t.Fatal("no count recorded")
	}
}

func TestPublishAsync_SendError(t *testing.T) {
	mock := &mockProducer{sendAsyncErr: errSend}
	p, _, ex := setupProducer(t, mock)

	err := p.PublishAsync(context.Background(), "reporting", "tag", "", []byte("hello"), nil)
	if err == nil {
		t.Fatal("expected error")
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
}

func TestBuildMessage_InjectsTraceContext(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	defer span.End()

	msg := p.buildMessage(ctx, "reporting", "tag", "key", []byte("body"))
	if got := msg.GetProperty("traceparent"); got == "" {
		t.Fatal("expected traceparent property to be injected")
	}
}
