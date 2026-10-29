package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

var errHandler = errors.New("process failed")

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

// --- fake: consumer setup ----------------------------------------------------------

var testPropagator = propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{},
	propagation.Baggage{},
)

// setupAdapter builds a handlerAdapter wired to a capturing metrics and tracer
// client, plus the given handler.
func setupAdapter(t *testing.T, handler HandlerFunc) (*handlerAdapter, *fakeMetrics, *recordingExporter) {
	t.Helper()

	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	fm := &fakeMetrics{}

	a, err := newHandlerAdapter(meta{
		topic:      "reporting",
		group:      "export_consumer",
		serverAddr: "localhost",
		serverPort: 9876,
		propagator: testPropagator,
		metrics:    fm,
		tracer:     tracer.Wrap(tp),
	}, handler)
	if err != nil {
		t.Fatalf("newHandlerAdapter: %v", err)
	}
	return a, fm, ex
}

// testMsg builds a message on the "reporting" topic with a tag and an ID.
func testMsg(body string) *primitive.MessageExt {
	msg := &primitive.MessageExt{
		Message: primitive.Message{Topic: "reporting", Body: []byte(body)},
		MsgId:   "MSG-1",
	}
	msg.WithTag("export_report_process")
	return msg
}

// --- tests -------------------------------------------------------------------------

func TestHandle_Success(t *testing.T) {
	var gotBody []byte
	a, fm, ex := setupAdapter(t, func(ctx context.Context, msgBody []byte) error {
		gotBody = msgBody
		return nil
	})

	result, err := a.Handle(context.Background(), testMsg("hello"))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if result != consumer.ConsumeSuccess {
		t.Fatalf("result = %v, want success", result)
	}
	if string(gotBody) != "hello" {
		t.Fatalf("handler body = %q, want hello", gotBody)
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
		"messaging.operation":             "process",
		"messaging.destination.name":      "reporting",
		"messaging.rocketmq.client.group": "export_consumer",
		"messaging.rocketmq.message.tag":  "export_report_process",
		"messaging.message.id":            "MSG-1",
		"messaging.message.body.size":     5,
		"server.address":                  "localhost",
		"server.port":                     9876,
		"error.type":                      "",
	}
	assertAttrs(t, count.attrs, wantCountAttrs)

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Name() != "reporting process" || sp.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf("span name=%q kind=%v", sp.Name(), sp.SpanKind())
	}
	wantSpanAttrs := map[string]any{
		"messaging.system":                "rocketmq",
		"messaging.operation":             "process",
		"messaging.destination.name":      "reporting",
		"messaging.rocketmq.client.group": "export_consumer",
		"messaging.rocketmq.message.tag":  "export_report_process",
		"messaging.message.id":            "MSG-1",
		"messaging.message.body.size":     "5",
		"server.address":                  "localhost",
		"server.port":                     "9876",
		"error.type":                      "",
	}
	assertAttrs(t, spanAttrs(sp), wantSpanAttrs)
	if sp.Status().Code == codes.Error {
		t.Fatalf("status = %v, want not error", sp.Status().Code)
	}
}

func TestHandle_Error(t *testing.T) {
	a, _, ex := setupAdapter(t, func(ctx context.Context, msgBody []byte) error {
		return errHandler
	})

	result, err := a.Handle(context.Background(), testMsg("hello"))
	if err == nil {
		t.Fatal("expected error")
	}
	if result != consumer.ConsumeRetryLater {
		t.Fatalf("result = %v, want retry later", result)
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
	assertAttrs(t, spanAttrs(sp), map[string]any{"error.type": errHandler.Error()})
}

func TestHandle_PanicRecovery(t *testing.T) {
	a, _, _ := setupAdapter(t, func(ctx context.Context, msgBody []byte) error {
		panic("boom")
	})

	result, err := a.Handle(context.Background(), testMsg("hello"))
	if err == nil {
		t.Fatal("expected error from recovered panic")
	}
	if result != consumer.ConsumeRetryLater {
		t.Fatalf("result = %v, want retry later", result)
	}
}

func TestHandle_ExtractsParentTrace(t *testing.T) {
	parentEx := &recordingExporter{}
	parentTP := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(parentEx)))
	t.Cleanup(func() { _ = parentTP.Shutdown(context.Background()) })

	parentCtx, parentSpan := parentTP.Tracer("test").Start(context.Background(), "parent")
	msg := testMsg("hello")
	testPropagator.Inject(parentCtx, messagePropertyCarrier{msg: msg})
	parentSpan.End()

	a, _, ex := setupAdapter(t, func(ctx context.Context, msgBody []byte) error { return nil })
	if _, err := a.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Parent().SpanID() != parentSpan.SpanContext().SpanID() {
		t.Fatalf("consumer span parent = %v, want %v", sp.Parent().SpanID(), parentSpan.SpanContext().SpanID())
	}
}

func TestHandle_BatchAggregatesErrors(t *testing.T) {
	calls := 0
	a, _, _ := setupAdapter(t, func(ctx context.Context, msgBody []byte) error {
		calls++
		return errors.New("fail")
	})

	_, err := a.Handle(context.Background(), testMsg("a"), testMsg("b"))
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
}
