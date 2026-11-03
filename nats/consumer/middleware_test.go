package consumer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
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

	"github.com/fikrimohammad/go-servicekit/nats/internal/wire"
	"github.com/fikrimohammad/go-servicekit/observability/attributes"
	"github.com/fikrimohammad/go-servicekit/observability/metrics"
	"github.com/fikrimohammad/go-servicekit/observability/tracer"
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

// --- tests -------------------------------------------------------------------------

func TestHandle_Success(t *testing.T) {
	t.Parallel()

	var gotBody []byte
	a, fm, ex := setupAdapter(t, 0, func(ctx context.Context, msg Message) error {
		gotBody = msg.Body()
		return nil
	})

	msg, f := deliveredReport(t, "hello")
	f.awaitPending(t, 1)
	err := a.HandleMsg(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMsg: %v", err)
	}
	f.awaitPending(t, 0)
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
	t.Parallel()

	a, _, ex := setupAdapter(t, 0, func(ctx context.Context, msg Message) error {
		return errHandler
	})

	msg, f := deliveredReport(t, "hello")
	err := a.HandleMsg(context.Background(), msg)
	if err == nil {
		t.Fatal("error = nil, want non-nil")
	}

	// The nak brings the message back at once, with a second delivery.
	again, ok := f.next(t, 5*time.Second)
	if !ok {
		t.Fatal("message was not redelivered after the nak")
	}
	if got := deliveries(t, again); got != 2 {
		t.Fatalf("deliveries = %d, want 2", got)
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
	t.Parallel()

	delay := 1500 * time.Millisecond
	a, _, _ := setupAdapter(t, delay, func(ctx context.Context, msg Message) error {
		return errHandler
	})

	msg, f := deliveredReport(t, "hello")
	start := time.Now()
	if err := a.HandleMsg(context.Background(), msg); err == nil {
		t.Fatal("error = nil, want non-nil")
	}

	// The nak carries the backoff, so the message is not back before it passes.
	if _, ok := f.next(t, 300*time.Millisecond); ok {
		t.Fatal("message redelivered before RetryBackoff passed")
	}
	again, ok := f.next(t, 5*time.Second)
	if !ok {
		t.Fatal("message was not redelivered after RetryBackoff")
	}
	if elapsed := time.Since(start); elapsed < delay {
		t.Fatalf("redelivered after %v, want at least %v", elapsed, delay)
	}
	if got := deliveries(t, again); got != 2 {
		t.Fatalf("deliveries = %d, want 2", got)
	}
}

func TestHandle_PanicRecovery(t *testing.T) {
	t.Parallel()

	a, _, _ := setupAdapter(t, 0, func(ctx context.Context, msg Message) error {
		panic("boom")
	})

	msg, f := deliveredReport(t, "hello")
	if err := a.HandleMsg(context.Background(), msg); err == nil {
		t.Fatal("error = nil after panic, want non-nil")
	}

	// The recovered panic is an error, so the message is nak'd and comes back.
	again, ok := f.next(t, 5*time.Second)
	if !ok {
		t.Fatal("message was not redelivered after the panic")
	}
	if got := deliveries(t, again); got != 2 {
		t.Fatalf("deliveries = %d, want 2", got)
	}
}

// regression: a panicking handler is recovered inside the metrics middleware, so
// metrics record an error, the span has error status, and the message is nak'd.
func TestHandler_PanicIsMeasured(t *testing.T) {
	t.Parallel()

	a, fm, ex := setupAdapter(t, 0, func(ctx context.Context, msg Message) error {
		panic("boom")
	})

	msg, f := deliveredReport(t, "hello")
	if err := a.HandleMsg(context.Background(), msg); err == nil {
		t.Fatal("HandleMsg() error = nil, want error from recovered panic")
	}
	if again, ok := f.next(t, 5*time.Second); !ok || deliveries(t, again) != 2 {
		t.Fatal("message was not nak'd and redelivered after the panic")
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
	t.Parallel()

	parentEx := &recordingExporter{}
	parentTP := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(parentEx)))
	t.Cleanup(func() { _ = parentTP.Shutdown(context.Background()) })

	parentCtx, parentSpan := parentTP.Tracer("test").Start(context.Background(), "parent")
	header := nats.Header{wire.HeaderTag: []string{"export_report_process"}}
	testPropagator.Inject(parentCtx, natsHeaderCarrier{header: header})
	parentSpan.End()

	f := newPullFixture(t, startTestNATSServer(t), "reporting")
	f.publish(t, "reporting.export_report_process", "hello", header)
	msg := f.fetch(t)

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
// metric. It runs through the public Consumer: the failing message is termed with
// ErrNoRetry, so its single delivery is the one measured.
func TestProcess_MetricAttributes(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(ctx) })

	ns := startTestNATSServer(t)
	c, err := New(testAppInfo, WithMetrics(metrics.Wrap(mp)), WithTracer(tracer.Wrap(tp)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// MaxConcurrent 1 runs one message at a time, so the spans end in publish order.
	handled := make(chan struct{}, 2)
	cfg := Config{Endpoints: []string{ns.ClientURL()}, Topic: "reporting", Group: "export_consumer", MaxConcurrent: 1}
	if err := c.Register(cfg, func(ctx context.Context, msg Message) error {
		handled <- struct{}{}
		if msg.Key() == "k2" {
			return fmt.Errorf("%w: %w", ErrNoRetry, codedError{code: 4001})
		}
		return nil
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	js, err := jetstream.New(connectTestClient(t, ns))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	for _, key := range []string{"k1", "k2"} {
		msg := nats.NewMsg("reporting.created")
		msg.Data = []byte("hello")
		msg.Header.Set(wire.HeaderKey, key)
		msg.Header.Set(wire.HeaderTag, "created")
		if _, err := js.PublishMsg(ctx, msg); err != nil {
			t.Fatalf("PublishMsg(%s) error = %v", key, err)
		}
		select {
		case <-handled:
		case <-time.After(5 * time.Second):
			t.Fatalf("handler of %s was not called", key)
		}
	}
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
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
	host, port := serverAttrs(t, ns)
	base := map[string]string{
		"messaging.system":              "nats",
		"messaging.operation.name":      "process",
		"messaging.operation.type":      "process",
		"messaging.destination.name":    "reporting",
		"messaging.consumer.group.name": "export_consumer",
		"server.address":                host,
		"server.port":                   strconv.Itoa(port),
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
		"messaging.nats.message.tag":            "created",
		"messaging.nats.subject":                "reporting.created",
		"messaging.message.body.size":           "5",
		"messaging.nats.message.delivery_count": "1",
	})
	assertAttrs(t, spanAttrs(badSpan), map[string]any{
		"messaging.message.id":                  "2",
		"messaging.nats.message.delivery_count": "1",
		"error.type":                            "4001",
	})
	if badSpan.Status().Code != codes.Error {
		t.Fatalf("failed span status = %v, want error", badSpan.Status().Code)
	}
}
