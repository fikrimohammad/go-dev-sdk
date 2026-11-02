package producer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
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
	startStubbed(t, p, map[string]jsProducer{"reporting": mock})
	return p, fm, ex
}

// --- tests -------------------------------------------------------------------------

func TestPublishSync_Success(t *testing.T) {
	mock := &mockProducer{sendSyncFunc: func(ctx context.Context, msg *nats.Msg) (*jetstream.PubAck, error) {
		return &jetstream.PubAck{Stream: "reporting", Sequence: 100}, nil
	}}
	p, fm, ex := setupProducer(t, mock)

	if err := p.PublishSync(context.Background(), Message{Topic: "reporting", Tag: "export_report_process", Key: "key-1", Body: []byte("hello")}); err != nil {
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
		"messaging.system":            "nats",
		"messaging.operation":         "publish",
		"messaging.destination.name":  "reporting",
		"messaging.nats.subject":      "reporting.export_report_process",
		"messaging.message.body.size": 5,
		"server.address":              "localhost",
		"server.port":                 4222,
		"messaging.nats.message.tag":  "export_report_process",
		"messaging.nats.message.keys": "key-1",
		"messaging.message.id":        "key-1",
		"error.type":                  "",
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
		"messaging.system":            "nats",
		"messaging.operation":         "publish",
		"messaging.destination.name":  "reporting",
		"messaging.nats.subject":      "reporting.export_report_process",
		"messaging.message.body.size": "5",
		"server.address":              "localhost",
		"server.port":                 "4222",
		"messaging.nats.message.tag":  "export_report_process",
		"messaging.nats.message.keys": "key-1",
		"messaging.message.id":        "key-1",
		"error.type":                  "",
	}
	assertAttrs(t, spanAttrs(sp), wantSpanAttrs)
	if sp.Status().Code == codes.Error {
		t.Fatalf("status = %v, want not error", sp.Status().Code)
	}
}

func TestPublishSync_Error(t *testing.T) {
	mock := &mockProducer{sendSyncErr: errSend}
	p, _, ex := setupProducer(t, mock)

	err := p.PublishSync(context.Background(), Message{Topic: "reporting", Tag: "tag", Key: "", Body: []byte("hello")})
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

	if err := p.PublishSyncWithDelay(context.Background(), Message{Topic: "reporting", Tag: "tag", Key: "", Body: []byte("hello")}, time.Minute); err != nil {
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
	mock := &mockProducer{sendAsyncFunc: func(ctx context.Context, msg *nats.Msg, cb func(context.Context, *jetstream.PubAck, error)) error {
		cb(ctx, &jetstream.PubAck{Stream: "reporting", Sequence: 1}, nil)
		return nil
	}}
	p, fm, ex := setupProducer(t, mock)

	err := p.PublishAsync(context.Background(), Message{Topic: "reporting", Tag: "tag", Key: "k", Body: []byte("hello")}, func(ctx context.Context, result *jetstream.PubAck, err error) {
		called = true
		if result == nil || result.Sequence != 1 {
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
	assertAttrs(t, spanAttrs(sp), map[string]any{"messaging.message.id": "k"})
	if _, ok := fm.lastCount(); !ok {
		t.Fatal("no count recorded")
	}
}

func TestPublishAsync_SendError(t *testing.T) {
	mock := &mockProducer{sendAsyncErr: errSend}
	p, _, ex := setupProducer(t, mock)

	err := p.PublishAsync(context.Background(), Message{Topic: "reporting", Tag: "tag", Key: "", Body: []byte("hello")}, nil)
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

// TestPublish_TraceParenting checks that the traceparent on the sent message names
// the producer span, so the consumer's span is its child.
func TestPublish_TraceParenting(t *testing.T) {
	useTraceContextPropagator(t)

	var captured *nats.Msg
	mock := &mockProducer{sendSyncFunc: func(_ context.Context, msg *nats.Msg) (*jetstream.PubAck, error) {
		captured = msg
		return &jetstream.PubAck{}, nil
	}}
	p, _, ex := setupProducer(t, mock)

	if err := p.PublishSync(context.Background(), Message{Topic: "reporting", Tag: "tag", Key: "key", Body: []byte("hello")}); err != nil {
		t.Fatalf("PublishSync: %v", err)
	}
	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}

	parts := strings.Split(captured.Header.Get("traceparent"), "-")
	if len(parts) != 4 {
		t.Fatalf("traceparent = %q, want 4 fields", strings.Join(parts, "-"))
	}
	if got, want := parts[1], sp.SpanContext().TraceID().String(); got != want {
		t.Errorf("trace id = %q, want %q", got, want)
	}
	if got, want := parts[2], sp.SpanContext().SpanID().String(); got != want {
		t.Errorf("span id = %q, want %q", got, want)
	}
}
