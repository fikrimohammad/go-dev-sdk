package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

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
// transport stub: the calls it observes run against a real gofakes3 server.
type fakeMetrics struct {
	mu    sync.Mutex
	hists []metricRec
}

func (f *fakeMetrics) Add(context.Context, metrics.Instrument, int64, map[string]any) error {
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

// --- fake: gofakes3 server ---------------------------------------------------------

// fakeS3 is an in-process S3 server: gofakes3 over an in-memory backend, served
// by httptest. It records the query parameters of every request it serves.
type fakeS3 struct {
	URL     string
	backend *s3mem.Backend

	mu      sync.Mutex
	queries []url.Values
}

// newFakeS3 starts a fake S3 server that the test closes on cleanup.
func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	f := &fakeS3{backend: s3mem.New()}
	handler := gofakes3.New(f.backend).Server()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.queries = append(f.queries, r.URL.Query())
		f.mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	f.URL = ts.URL
	return f
}

// bucket creates a bucket directly in the in-memory backend, so setup never
// reaches the instrumented client.
func (f *fakeS3) bucket(t *testing.T, name string) {
	t.Helper()
	if err := f.backend.CreateBucket(name); err != nil {
		t.Fatalf("backend.CreateBucket(%q): %v", name, err)
	}
}

// count returns how many requests carried the query parameter key.
func (f *fakeS3) count(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, q := range f.queries {
		if _, ok := q[key]; ok {
			n++
		}
	}
	return n
}

// --- client helpers ----------------------------------------------------------------

// testConfig returns a Config that targets the fake endpoint with path-style
// addressing and placeholder credentials.
func testConfig(endpoint string) Config {
	return Config{
		Region:          "us-east-1",
		Endpoint:        endpoint,
		UsePathStyle:    aws.Bool(true),
		AccessKeyID:     "x",
		SecretAccessKey: "x",
	}
}

// newTestClient builds a Client from cfg and the options, failing the test on error.
func newTestClient(t *testing.T, cfg Config, opts ...Option) Client {
	t.Helper()
	cli, err := New(t.Context(), cfg, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return cli
}

// newInstrumentedClient builds a Client from cfg that records spans and metrics
// into the returned fakes.
func newInstrumentedClient(t *testing.T, cfg Config) (Client, *fakeMetrics, *recordingExporter) {
	t.Helper()
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	fm := &fakeMetrics{}
	cli := newTestClient(t, cfg, WithMetrics(fm), WithTracer(tracer.Wrap(tp)))
	return cli, fm, ex
}

// putObject stores body under bucket/key through the client.
func putObject(t *testing.T, cli Client, bucket, key, body string) {
	t.Helper()
	if _, err := cli.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	}); err != nil {
		t.Fatalf("PutObject(%s/%s): %v", bucket, key, err)
	}
}

// readObject returns the body of bucket/key, releasing the response.
func readObject(t *testing.T, cli Client, bucket, key string) string {
	t.Helper()
	res, err := cli.GetObject(t.Context(), &transfermanager.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject(%s/%s): %v", bucket, key, err)
	}
	return readAndClose(t, res.Body)
}

// readAndClose reads body to EOF and closes it when it is an io.Closer, which
// releases the underlying response.
func readAndClose(t *testing.T, body io.Reader) string {
	t.Helper()
	if c, ok := body.(io.Closer); ok {
		defer c.Close()
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(data)
}

// --- telemetry helpers -------------------------------------------------------------

// collectMetrics returns every metric the reader has collected, keyed by name.
func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// attrStrings returns the emitted value of each attribute, keyed by attribute name.
func attrStrings(kvs []attribute.KeyValue) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		out[string(kv.Key)] = kv.Value.String()
	}
	return out
}
