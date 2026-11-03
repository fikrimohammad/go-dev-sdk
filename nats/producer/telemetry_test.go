package producer

import (
	"context"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-servicekit/observability/attributes"
	"github.com/fikrimohammad/go-servicekit/observability/metrics"
	"github.com/fikrimohammad/go-servicekit/observability/tracer"
)

// metric names as the messaging semantic conventions define them.
const (
	metricSent     = "messaging.client.sent.messages"
	metricDuration = "messaging.client.operation.duration"
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

// --- fake: producer setup ----------------------------------------------------------

// setupProducer starts a Producer for topic on ns with capturing metrics and
// tracer clients.
func setupProducer(t *testing.T, ns *natsserver.Server, topic string) (*Producer, *fakeMetrics, *recordingExporter) {
	t.Helper()

	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	fm := &fakeMetrics{}

	p := startJetStreamProducer(t, ns, topic, WithMetrics(fm), WithTracer(tracer.Wrap(tp)))
	return p, fm, ex
}

// serverAttrs returns the server.address and server.port a producer on ns reports,
// read from the URL the producer dials.
func serverAttrs(t *testing.T, ns *natsserver.Server) (string, int) {
	t.Helper()

	u, err := url.Parse(ns.ClientURL())
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("port of %s error = %v", u, err)
	}
	return u.Hostname(), port
}

// --- tests -------------------------------------------------------------------------

func TestPublishSync_Success(t *testing.T) {
	ns := startJetStreamServer(t)
	p, fm, ex := setupProducer(t, ns, "reporting")
	host, port := serverAttrs(t, ns)

	if err := p.PublishSync(context.Background(), Message{Topic: "reporting", Tag: "export_report_process", Key: "key-1", Body: []byte("hello")}); err != nil {
		t.Fatalf("PublishSync: %v", err)
	}

	count, ok := fm.lastCount()
	if !ok {
		t.Fatal("no count recorded")
	}
	if count.name != metricSent || count.value != 1 {
		t.Fatalf("count = %+v", count)
	}
	hist, ok := fm.lastHist()
	if !ok || hist.name != metricDuration {
		t.Fatalf("hist = %+v", hist)
	}
	wantMetricAttrs := map[string]any{
		"messaging.system":           "nats",
		"messaging.operation.name":   "publish",
		"messaging.operation.type":   "send",
		"messaging.destination.name": "reporting",
		"server.address":             host,
		"server.port":                port,
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
	if sp.Name() != "publish reporting" || sp.SpanKind() != trace.SpanKindProducer {
		t.Fatalf("span name=%q kind=%v", sp.Name(), sp.SpanKind())
	}
	wantSpanAttrs := map[string]any{
		"messaging.system":            "nats",
		"messaging.operation.name":    "publish",
		"messaging.operation.type":    "send",
		"messaging.destination.name":  "reporting",
		"messaging.nats.subject":      "reporting.export_report_process",
		"messaging.message.body.size": "5",
		"server.address":              host,
		"server.port":                 strconv.Itoa(port),
		"messaging.nats.message.tag":  "export_report_process",
		"messaging.nats.message.key":  "key-1",
		"messaging.message.id":        "1",
	}
	assertAttrs(t, spanAttrs(sp), wantSpanAttrs)
	if _, ok := spanAttrs(sp)["error.type"]; ok {
		t.Error("success span has error.type, want none")
	}
	if sp.Status().Code == codes.Error {
		t.Fatalf("status = %v, want not error", sp.Status().Code)
	}
}

func TestPublishSync_Error(t *testing.T) {
	ns := startJetStreamServer(t)
	p, _, ex := setupProducer(t, ns, "reporting")
	js := jsContext(t, ns)
	if err := js.DeleteStream(context.Background(), "reporting"); err != nil {
		t.Fatalf("DeleteStream() error = %v", err)
	}

	err := p.PublishSync(context.Background(), Message{Topic: "reporting", Tag: "tag", Key: "", Body: []byte("hello")})
	if err == nil {
		t.Fatal("error = nil, want non-nil")
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

func TestPublishSyncWithDelay_Success(t *testing.T) {
	ns := startJetStreamServer(t)
	p, fm, ex := setupProducer(t, ns, "reporting")

	if err := p.PublishSyncWithDelay(context.Background(), Message{Topic: "reporting", Tag: "tag", Key: "", Body: []byte("hello")}, time.Minute); err != nil {
		t.Fatalf("PublishSyncWithDelay: %v", err)
	}

	if _, ok := fm.lastCount(); !ok {
		t.Fatal("no count recorded")
	}
	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	assertAttrs(t, spanAttrs(sp), map[string]any{"messaging.nats.subject": "reporting.tag"})
}

func TestPublishAsync_FullLifecycle(t *testing.T) {
	ns := startJetStreamServer(t)
	p, fm, ex := setupProducer(t, ns, "reporting")

	done := make(chan *jetstream.PubAck, 1)
	err := p.PublishAsync(context.Background(), Message{Topic: "reporting", Tag: "tag", Key: "k", Body: []byte("hello")}, func(ctx context.Context, result *jetstream.PubAck, err error) {
		if err != nil {
			t.Errorf("callback error = %v, want nil", err)
		}
		done <- result
	})
	if err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}

	select {
	case result := <-done:
		if result == nil || result.Sequence != 1 {
			t.Fatalf("callback result = %+v, want sequence 1", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("callback was not invoked")
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	assertAttrs(t, spanAttrs(sp), map[string]any{"messaging.message.id": "1", "messaging.nats.message.key": "k"})
	if _, ok := fm.lastCount(); !ok {
		t.Fatal("no count recorded")
	}
}

func TestPublishAsync_SendError(t *testing.T) {
	ns := startJetStreamServer(t)
	p, _, ex := setupProducer(t, ns, "reporting")

	err := p.PublishAsync(context.Background(), Message{Topic: "reporting", Tag: "tag", Key: "", Body: make([]byte, 2<<20)}, nil)
	if err == nil {
		t.Fatal("error = nil, want non-nil")
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
}

// TestPublish_TraceParenting checks that the traceparent stored with the message
// names the producer span, so the consumer's span is its child.
func TestPublish_TraceParenting(t *testing.T) {
	useTraceContextPropagator(t)

	ns := startJetStreamServer(t)
	p, _, ex := setupProducer(t, ns, "reporting")

	if err := p.PublishSync(context.Background(), Message{Topic: "reporting", Tag: "tag", Key: "key", Body: []byte("hello")}); err != nil {
		t.Fatalf("PublishSync: %v", err)
	}
	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	stored := readMsgs(t, jsContext(t, ns), "reporting", "reporting.tag", 1)[0]

	parts := strings.Split(stored.Header.Get("traceparent"), "-")
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

// TestPublish_MetricAttributes is a regression test. The metrics must be the
// messagingconv instruments with the metric attribute set only: no message id,
// key, tag, subject, or body size. The sent counter counts successes only. The
// second publish fails inside the server, because its message exceeds the stream's
// MaxMsgSize, so the failure carries a real JetStream error code.
func TestPublish_MetricAttributes(t *testing.T) {
	ctx := context.Background()
	ns := startJetStreamServer(t)
	createStream(t, ns, jetstream.StreamConfig{Name: "orders", Subjects: []string{"orders"}, MaxMsgSize: 128})

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(ctx) })

	p := startJetStreamProducer(t, ns, "orders", WithMetrics(metrics.Wrap(mp)), WithTracer(tracer.Wrap(tp)))
	host, port := serverAttrs(t, ns)

	if err := p.PublishSync(ctx, Message{Topic: "orders", Tag: "created", Key: "k1", Body: []byte("hello")}); err != nil {
		t.Fatalf("PublishSync(k1) error = %v, want nil", err)
	}
	if err := p.PublishSync(ctx, Message{Topic: "orders", Tag: "created", Key: "k2", Body: make([]byte, 256)}); err == nil {
		t.Fatal("PublishSync(k2) error = nil, want error")
	}

	got := collectMetrics(t, reader)
	wantNames := []string{"messaging.client.operation.duration", "messaging.client.sent.messages"}
	if names := slices.Sorted(maps.Keys(got)); !slices.Equal(names, wantNames) {
		t.Fatalf("metric names = %v, want %v", names, wantNames)
	}

	dur := got["messaging.client.operation.duration"]
	if dur.Unit != "s" {
		t.Errorf("duration unit = %q, want %q", dur.Unit, "s")
	}
	hist, ok := dur.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("duration data = %T, want metricdata.Histogram[float64]", dur.Data)
	}
	base := map[string]string{
		"messaging.system":           "nats",
		"messaging.operation.name":   "publish",
		"messaging.operation.type":   "send",
		"messaging.destination.name": "orders",
		"server.address":             host,
		"server.port":                strconv.Itoa(port),
	}
	failed := maps.Clone(base)
	failed["error.type"] = "10054"
	durSets := make([]attribute.Set, 0, len(hist.DataPoints))
	for _, dp := range hist.DataPoints {
		durSets = append(durSets, dp.Attributes)
	}
	if gotSets, want := pointStrings(durSets...), mapStrings(base, failed); !slices.Equal(gotSets, want) {
		t.Errorf("duration attributes = %v, want %v", gotSets, want)
	}

	sent := got["messaging.client.sent.messages"]
	if sent.Unit != "{message}" {
		t.Errorf("sent unit = %q, want %q", sent.Unit, "{message}")
	}
	sum, ok := sent.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("sent data = %T, want metricdata.Sum[int64]", sent.Data)
	}
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
		t.Fatalf("sent points = %+v, want one point with value 1", sum.DataPoints)
	}
	if gotSets, want := pointStrings(sum.DataPoints[0].Attributes), mapStrings(base); !slices.Equal(gotSets, want) {
		t.Errorf("sent attributes = %v, want %v", gotSets, want)
	}

	if len(ex.spans) != 2 {
		t.Fatalf("spans = %d, want 2", len(ex.spans))
	}
	okSpan, badSpan := ex.spans[0], ex.spans[1]
	if okSpan.Name() != "publish orders" || okSpan.SpanKind() != trace.SpanKindProducer {
		t.Fatalf("span name=%q kind=%v, want %q kind=%v", okSpan.Name(), okSpan.SpanKind(), "publish orders", trace.SpanKindProducer)
	}
	assertAttrs(t, spanAttrs(okSpan), map[string]any{
		"messaging.message.id":        "1",
		"messaging.nats.message.key":  "k1",
		"messaging.nats.message.tag":  "created",
		"messaging.nats.subject":      "orders.created",
		"messaging.message.body.size": "5",
	})
	if badSpan.Name() != "publish orders" {
		t.Fatalf("failed span name = %q, want %q", badSpan.Name(), "publish orders")
	}
	assertAttrs(t, spanAttrs(badSpan), map[string]any{"error.type": "10054"})
	if _, ok := spanAttrs(badSpan)["messaging.message.id"]; ok {
		t.Error("failed span has messaging.message.id, want none without an ack")
	}
}

func TestNatsHeaderCarrier(t *testing.T) {
	t.Parallel()

	c := natsHeaderCarrier{header: nats.Header{}}
	c.Set("traceparent", "00-abc")

	if got := c.Get("traceparent"); got != "00-abc" {
		t.Errorf("Get(traceparent) = %q, want %q", got, "00-abc")
	}
	if got, want := c.Keys(), []string{"traceparent"}; !slices.Equal(got, want) {
		t.Errorf("Keys() = %v, want %v", got, want)
	}
}
