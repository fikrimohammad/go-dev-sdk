package consumer

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/nats/internal/wire"
	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

var errHandler = errors.New("process failed")

// metric names as the messaging semantic conventions define them.
const (
	metricConsumed = "messaging.client.consumed.messages"
	metricDuration = "messaging.process.duration"
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
func setupAdapter(t *testing.T, retryBackoff time.Duration, handler HandlerFunc) (*handlerAdapter, *fakeMetrics, *recordingExporter) {
	t.Helper()

	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	fm := &fakeMetrics{}

	a, err := newHandlerAdapter(meta{
		topic:      "reporting",
		group:      "export_consumer",
		serverAddr: "localhost",
		serverPort: 4222,
		propagator: testPropagator,
		metrics:    fm,
		tracer:     tracer.Wrap(tp),
	}, Config{RetryBackoff: retryBackoff}.SetDefaults(), handler)
	if err != nil {
		t.Fatalf("newHandlerAdapter: %v", err)
	}
	return a, fm, ex
}

// testMsg builds a message on the "reporting" topic with a tag and an ID.
func testMsg(body string) *mockMsg {
	h := make(nats.Header)
	h.Set(wire.HeaderTag, "export_report_process")
	return &mockMsg{
		subject: "reporting.export_report_process",
		data:    []byte(body),
		headers: h,
		meta: &jetstream.MsgMetadata{
			Sequence:     jetstream.SequencePair{Stream: 1},
			NumDelivered: 1,
			Timestamp:    time.Unix(1_700_000_000, 0),
		},
	}
}

// --- tests -------------------------------------------------------------------------

func TestHandle_Success(t *testing.T) {
	var gotBody []byte
	a, fm, ex := setupAdapter(t, 0, func(ctx context.Context, msg Message) error {
		gotBody = msg.Body()
		return nil
	})

	msg := testMsg("hello")
	err := a.HandleMsg(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMsg: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected message to be acked")
	}
	if msg.nacked {
		t.Fatal("message was unexpectedly nacked")
	}
	if string(gotBody) != "hello" {
		t.Fatalf("handler body = %q, want hello", gotBody)
	}

	count, ok := fm.lastCount()
	if !ok || count.name != metricConsumed || count.value != 1 {
		t.Fatalf("count = %+v, want %s with value 1", count, metricConsumed)
	}
	hist, ok := fm.lastHist()
	if !ok || hist.name != metricDuration {
		t.Fatalf("hist = %+v, want %s", hist, metricDuration)
	}
	wantMetricAttrs := map[string]any{
		"messaging.system":              "nats",
		"messaging.operation.name":      "process",
		"messaging.operation.type":      "process",
		"messaging.destination.name":    "reporting",
		"messaging.consumer.group.name": "export_consumer",
		"server.address":                "localhost",
		"server.port":                   4222,
	}
	for _, rec := range []metricRec{count, hist} {
		assertAttrs(t, rec.attrs, wantMetricAttrs)
		if len(rec.attrs) != len(wantMetricAttrs) {
			t.Fatalf("%s attrs = %v, want only %v", rec.name, rec.attrs, wantMetricAttrs)
		}
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Name() != "process reporting" || sp.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf("span name=%q kind=%v", sp.Name(), sp.SpanKind())
	}
	wantSpanAttrs := map[string]any{
		"messaging.system":                      "nats",
		"messaging.operation.name":              "process",
		"messaging.operation.type":              "process",
		"messaging.destination.name":            "reporting",
		"messaging.consumer.group.name":         "export_consumer",
		"messaging.nats.subject":                "reporting.export_report_process",
		"messaging.nats.message.tag":            "export_report_process",
		"messaging.message.id":                  "1",
		"messaging.message.body.size":           "5",
		"messaging.nats.message.delivery_count": "1",
		"server.address":                        "localhost",
		"server.port":                           "4222",
	}
	assertAttrs(t, spanAttrs(sp), wantSpanAttrs)
	if _, ok := spanAttrs(sp)["error.type"]; ok {
		t.Error("success span has error.type, want none")
	}
	if sp.Status().Code == codes.Error {
		t.Fatalf("status = %v, want not error", sp.Status().Code)
	}
}

func TestHandle_Error(t *testing.T) {
	a, _, ex := setupAdapter(t, 0, func(ctx context.Context, msg Message) error {
		return errHandler
	})

	msg := testMsg("hello")
	err := a.HandleMsg(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error")
	}
	if !msg.nacked {
		t.Fatal("expected message to be nacked")
	}
	if msg.acked {
		t.Fatal("message was unexpectedly acked")
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
	assertAttrs(t, spanAttrs(sp), map[string]any{"error.type": "_OTHER"})
}

func TestHandle_RetryBackoff(t *testing.T) {
	delay := 3 * time.Second
	a, _, _ := setupAdapter(t, delay, func(ctx context.Context, msg Message) error {
		return errHandler
	})

	msg := testMsg("hello")
	err := a.HandleMsg(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error")
	}
	if !msg.nacked {
		t.Fatal("expected message to be nacked")
	}
	if msg.nakDelay != delay {
		t.Fatalf("nakDelay = %v, want %v", msg.nakDelay, delay)
	}
}

func TestHandle_PanicRecovery(t *testing.T) {
	a, _, _ := setupAdapter(t, 0, func(ctx context.Context, msg Message) error {
		panic("boom")
	})

	msg := testMsg("hello")
	err := a.HandleMsg(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error from recovered panic")
	}
	if !msg.nacked {
		t.Fatal("expected message to be nacked after panic")
	}
}

// regression: a panicking handler is recovered inside the metrics middleware, so
// metrics record an error, the span has error status, and the message is nak'd.
func TestHandler_PanicIsMeasured(t *testing.T) {
	a, fm, ex := setupAdapter(t, 0, func(ctx context.Context, msg Message) error {
		panic("boom")
	})

	msg := testMsg("hello")
	if err := a.HandleMsg(context.Background(), msg); err == nil {
		t.Fatal("HandleMsg() error = nil, want error from recovered panic")
	}
	if !msg.nacked {
		t.Fatal("message was not nacked")
	}

	hist, ok := fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	if v, ok := hist.attrs["error.type"]; !ok || v != "panic" {
		t.Fatalf("error.type = %#v, want %q", v, "panic")
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
}

func TestHandle_ExtractsParentTrace(t *testing.T) {
	parentEx := &recordingExporter{}
	parentTP := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(parentEx)))
	t.Cleanup(func() { _ = parentTP.Shutdown(context.Background()) })

	parentCtx, parentSpan := parentTP.Tracer("test").Start(context.Background(), "parent")
	msg := testMsg("hello")
	testPropagator.Inject(parentCtx, natsHeaderCarrier{header: msg.headers})
	parentSpan.End()

	a, _, ex := setupAdapter(t, 0, func(ctx context.Context, msg Message) error { return nil })
	if err := a.HandleMsg(context.Background(), msg); err != nil {
		t.Fatalf("HandleMsg: %v", err)
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Parent().SpanID() != parentSpan.SpanContext().SpanID() {
		t.Fatalf("consumer span parent = %v, want %v", sp.Parent().SpanID(), parentSpan.SpanContext().SpanID())
	}
}

// codedError is a handler error with an errs-style code.
type codedError struct{ code int }

func (e codedError) Error() string { return "coded failure" }
func (e codedError) Code() int     { return e.code }

// TestProcess_MetricAttributes is a regression test. The process histogram
// carries the metric attribute set (with the group, and error.type on failure),
// the consumed counter counts every delivery, and no message detail reaches a
// metric.
func TestProcess_MetricAttributes(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(ctx) })

	a, err := newHandlerAdapter(meta{
		topic:      "reporting",
		group:      "export_consumer",
		serverAddr: "localhost",
		serverPort: 4222,
		propagator: testPropagator,
		metrics:    metrics.Wrap(mp),
		tracer:     tracer.Wrap(tp),
	}, Config{}.SetDefaults(), func(ctx context.Context, msg Message) error {
		if msg.Key() == "k2" {
			return codedError{code: 4001}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("newHandlerAdapter: %v", err)
	}

	okMsg := testMsg("hello")
	okMsg.headers.Set(wire.HeaderKey, "k1")
	if err := a.HandleMsg(ctx, okMsg); err != nil {
		t.Fatalf("HandleMsg(k1) error = %v, want nil", err)
	}
	badMsg := testMsg("hello")
	badMsg.headers.Set(wire.HeaderKey, "k2")
	badMsg.meta.Sequence.Stream = 2
	badMsg.meta.NumDelivered = 3
	if err := a.HandleMsg(ctx, badMsg); err == nil {
		t.Fatal("HandleMsg(k2) error = nil, want error")
	}

	got := collectMetrics(t, reader)
	wantNames := []string{"messaging.client.consumed.messages", "messaging.process.duration"}
	if names := slices.Sorted(maps.Keys(got)); !slices.Equal(names, wantNames) {
		t.Fatalf("metric names = %v, want %v", names, wantNames)
	}

	dur := got["messaging.process.duration"]
	if dur.Unit != "s" {
		t.Errorf("duration unit = %q, want %q", dur.Unit, "s")
	}
	hist, ok := dur.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("duration data = %T, want metricdata.Histogram[float64]", dur.Data)
	}
	base := map[string]string{
		"messaging.system":              "nats",
		"messaging.operation.name":      "process",
		"messaging.operation.type":      "process",
		"messaging.destination.name":    "reporting",
		"messaging.consumer.group.name": "export_consumer",
		"server.address":                "localhost",
		"server.port":                   "4222",
	}
	failed := maps.Clone(base)
	failed["error.type"] = "4001"
	durSets := make([]attribute.Set, 0, len(hist.DataPoints))
	for _, dp := range hist.DataPoints {
		durSets = append(durSets, dp.Attributes)
	}
	if gotSets, want := pointStrings(durSets...), mapStrings(base, failed); !slices.Equal(gotSets, want) {
		t.Errorf("duration attributes = %v, want %v", gotSets, want)
	}

	consumed := got["messaging.client.consumed.messages"]
	if consumed.Unit != "{message}" {
		t.Errorf("consumed unit = %q, want %q", consumed.Unit, "{message}")
	}
	sum, ok := consumed.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("consumed data = %T, want metricdata.Sum[int64]", consumed.Data)
	}
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 2 {
		t.Fatalf("consumed points = %+v, want one point with value 2", sum.DataPoints)
	}
	if gotSets, want := pointStrings(sum.DataPoints[0].Attributes), mapStrings(base); !slices.Equal(gotSets, want) {
		t.Errorf("consumed attributes = %v, want %v", gotSets, want)
	}

	if len(ex.spans) != 2 {
		t.Fatalf("spans = %d, want 2", len(ex.spans))
	}
	okSpan, badSpan := ex.spans[0], ex.spans[1]
	if okSpan.Name() != "process reporting" || okSpan.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf("span name=%q kind=%v, want %q kind=%v", okSpan.Name(), okSpan.SpanKind(), "process reporting", trace.SpanKindConsumer)
	}
	assertAttrs(t, spanAttrs(okSpan), map[string]any{
		"messaging.message.id":                  "1",
		"messaging.nats.message.key":            "k1",
		"messaging.nats.message.tag":            "export_report_process",
		"messaging.nats.subject":                "reporting.export_report_process",
		"messaging.message.body.size":           "5",
		"messaging.nats.message.delivery_count": "1",
	})
	assertAttrs(t, spanAttrs(badSpan), map[string]any{
		"messaging.message.id":                  "2",
		"messaging.nats.message.delivery_count": "3",
		"error.type":                            "4001",
	})
	if badSpan.Status().Code != codes.Error {
		t.Fatalf("failed span status = %v, want error", badSpan.Status().Code)
	}
}
