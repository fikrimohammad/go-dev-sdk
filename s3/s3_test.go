package s3

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
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

// --- fake: AWS SDK stubs -----------------------------------------------------------

type stubUploader struct {
	uploadCalls int
	returnErr   error
}

func (s *stubUploader) UploadObject(_ context.Context, _ *transfermanager.UploadObjectInput, _ ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error) {
	s.uploadCalls++
	if s.returnErr != nil {
		return nil, s.returnErr
	}
	return &transfermanager.UploadObjectOutput{}, nil
}

type stubPresigner struct {
	presignCalls int
	returnErr    error
	expires      time.Duration
}

func (s *stubPresigner) PresignGetObject(_ context.Context, _ *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	s.presignCalls++
	o := s3.PresignOptions{}
	for _, fn := range optFns {
		fn(&o)
	}
	s.expires = o.Expires
	if s.returnErr != nil {
		return nil, s.returnErr
	}
	return &v4.PresignedHTTPRequest{URL: "https://s3.example.com/reports/test.csv"}, nil
}

// setup installs a capturing tracer and metrics client as package defaults and
// returns an instrumented client wired to the stubs alongside assertion handles.
func setup(uploader uploader, presigner presigner) (*client, *fakeMetrics, *recordingExporter) {
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	tracer.SetDefault(tracer.Wrap(tp))

	fm := &fakeMetrics{}
	metrics.SetDefault(fm)

	return &client{
		uploader:  uploader,
		presigner: presigner,
		meta: meta{
			region:               "us-east-1",
			serverAddr:           "localhost",
			serverPort:           9000,
			presignDefaultExpiry: 15 * time.Minute,
		},
	}, fm, ex
}

// --- tests -------------------------------------------------------------------------

func TestNew_RejectsMissingRegion(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("New: expected error for missing region")
	}
}

func TestUploadObject_Success(t *testing.T) {
	up := &stubUploader{}
	pr := &stubPresigner{}
	c, fm, ex := setup(up, pr)

	err := c.UploadObject(context.Background(), UploadObjectParams{
		Bucket:      "reports",
		Key:         "test.csv",
		Body:        io.NopCloser(strings.NewReader("x")),
		ContentType: "text/csv",
	})
	if err != nil {
		t.Fatalf("UploadObject: %v", err)
	}
	if up.uploadCalls != 1 {
		t.Fatalf("upload calls = %d, want 1", up.uploadCalls)
	}

	span, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if span.Name() != "UploadObject" {
		t.Fatalf("span name = %q, want UploadObject", span.Name())
	}
	if span.SpanKind() != trace.SpanKindClient {
		t.Fatalf("span kind = %v, want client", span.SpanKind())
	}
	attrs := spanAttrs(span)
	if attrs["aws.s3.bucket"] != "reports" {
		t.Fatalf("aws.s3.bucket = %v, want reports", attrs["aws.s3.bucket"])
	}
	if attrs["rpc.system"] != "aws-api" {
		t.Fatalf("rpc.system = %v", attrs["rpc.system"])
	}
	if attrs["rpc.service"] != "s3" {
		t.Fatalf("rpc.service = %v", attrs["rpc.service"])
	}
	if attrs["rpc.method"] != "UploadObject" {
		t.Fatalf("rpc.method = %v", attrs["rpc.method"])
	}
	if _, ok := attrs["db.operation.name"]; ok {
		t.Fatalf("db.operation.name must not be set")
	}
	if attrs["cloud.region"] != "us-east-1" {
		t.Fatalf("cloud.region = %v", attrs["cloud.region"])
	}
	if attrs["server.address"] != "localhost" {
		t.Fatalf("server.address = %v, want localhost", attrs["server.address"])
	}
	if attrs["server.port"] != "9000" {
		t.Fatalf("server.port = %v, want 9000", attrs["server.port"])
	}
	if attrs["error.type"] != "" {
		t.Fatalf("error.type = %v, want empty", attrs["error.type"])
	}
	if span.Status().Code == codes.Error {
		t.Fatalf("span status = %v, want not error", span.Status().Code)
	}

	if count, ok := fm.lastCount(); !ok || count.name != metricCount || count.value != 1 {
		t.Fatalf("last count = %+v, want %s=1", count, metricCount)
	} else if count.attrs["aws.s3.bucket"] != "reports" {
		t.Fatalf("count attrs = %+v", count.attrs)
	} else if count.attrs["server.address"] != "localhost" || count.attrs["server.port"] != 9000 {
		t.Fatalf("count attrs = %+v, want server.address/port", count.attrs)
	}
	if hist, ok := fm.lastHist(); !ok || hist.name != metricDuration {
		t.Fatalf("last hist = %+v, want %s", hist, metricDuration)
	} else if hist.attrs["rpc.method"] != "UploadObject" {
		t.Fatalf("hist attrs = %+v", hist.attrs)
	}
}

func TestUploadObject_Error(t *testing.T) {
	up := &stubUploader{returnErr: errors.New("upload failed")}
	c, _, ex := setup(up, &stubPresigner{})

	err := c.UploadObject(context.Background(), UploadObjectParams{Bucket: "reports", Key: "k"})
	if err == nil {
		t.Fatal("expected error")
	}

	span, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("span status = %v, want error", span.Status().Code)
	}
	if attrs := spanAttrs(span); attrs["error.type"] != "upload failed" {
		t.Fatalf("error.type = %v, want upload failed", attrs["error.type"])
	}
}

func TestPresignGetObject_Success(t *testing.T) {
	up := &stubUploader{}
	pr := &stubPresigner{}
	c, _, ex := setup(up, pr)

	url, err := c.PresignGetObject(context.Background(), PresignGetObjectParams{
		Bucket:    "reports",
		Key:       "test.csv",
		ExpiresIn: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("PresignGetObject: %v", err)
	}
	if url != "https://s3.example.com/reports/test.csv" {
		t.Fatalf("url = %q", url)
	}
	if pr.presignCalls != 1 {
		t.Fatalf("presign calls = %d, want 1", pr.presignCalls)
	}
	if pr.expires != 5*time.Minute {
		t.Fatalf("expires = %v, want 5m", pr.expires)
	}

	span, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if span.Name() != "PresignGetObject" {
		t.Fatalf("span name = %q, want PresignGetObject", span.Name())
	}
}

func TestPresignGetObject_AppliesDefaultExpiry(t *testing.T) {
	up := &stubUploader{}
	pr := &stubPresigner{}
	c, _, _ := setup(up, pr)

	if _, err := c.PresignGetObject(context.Background(), PresignGetObjectParams{
		Bucket: "reports",
		Key:    "test.csv",
	}); err != nil {
		t.Fatalf("PresignGetObject: %v", err)
	}
	if pr.expires != 15*time.Minute {
		t.Fatalf("expires = %v, want default 15m", pr.expires)
	}
}

func TestPresignGetObject_Error(t *testing.T) {
	up := &stubUploader{}
	pr := &stubPresigner{returnErr: errors.New("signing failed")}
	c, fm, ex := setup(up, pr)

	_, err := c.PresignGetObject(context.Background(), PresignGetObjectParams{
		Bucket: "reports",
		Key:    "test.csv",
	})
	if err == nil {
		t.Fatal("expected error")
	}

	span, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("span status = %v, want error", span.Status().Code)
	}
	if count, ok := fm.lastCount(); !ok || count.value != 1 {
		t.Fatalf("expected a count metric on failure, got %+v", count)
	}
}

func TestClient_DefaultTracerMetricsInjected(t *testing.T) {
	up := &stubUploader{}
	pr := &stubPresigner{}

	injEx := &recordingExporter{}
	injTp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(injEx)))
	t.Cleanup(func() { _ = injTp.Shutdown(context.Background()) })
	injFm := &fakeMetrics{}

	// Package defaults: fresh, must receive nothing.
	defEx := &recordingExporter{}
	defTp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(defEx)))
	t.Cleanup(func() { _ = defTp.Shutdown(context.Background()) })
	tracer.SetDefault(tracer.Wrap(defTp))
	defFm := &fakeMetrics{}
	metrics.SetDefault(defFm)

	c := &client{
		uploader:  up,
		presigner: pr,
		meta: meta{
			region:               "us-east-1",
			presignDefaultExpiry: 15 * time.Minute,
			metrics:              injFm,
			tracer:               tracer.Wrap(injTp),
		},
	}

	if err := c.UploadObject(context.Background(), UploadObjectParams{Bucket: "reports", Key: "k"}); err != nil {
		t.Fatalf("UploadObject: %v", err)
	}

	if n := injEx.count(); n != 1 {
		t.Fatalf("injected tracer spans = %d, want 1", n)
	}
	if _, ok := injFm.lastCount(); !ok {
		t.Fatal("injected metrics client received no count")
	}

	if n := defEx.count(); n != 0 {
		t.Fatalf("package default tracer spans = %d, want 0", n)
	}
	nc, nh := defFm.nums()
	if nc != 0 || nh != 0 {
		t.Fatalf("package default metrics received counts=%d hists=%d, want 0", nc, nh)
	}
}

func TestEndpointAddress(t *testing.T) {
	cases := []struct {
		endpoint string
		wantAddr string
		wantPort int
	}{
		{"", "", 0},
		{"http://localhost:9000", "localhost", 9000},
		{"http://minio.local", "minio.local", 80},
		{"https://minio.local", "minio.local", 443},
		{"https://s3.example.com:9443", "s3.example.com", 9443},
		{"not a url", "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.endpoint, func(t *testing.T) {
			addr, port := endpointAddress(tc.endpoint)
			if addr != tc.wantAddr || port != tc.wantPort {
				t.Fatalf("endpointAddress(%q) = %q/%d, want %q/%d", tc.endpoint, addr, port, tc.wantAddr, tc.wantPort)
			}
		})
	}
}

func TestServerAttrsOmittedForAWS(t *testing.T) {
	up := &stubUploader{}
	pr := &stubPresigner{}
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tracer.SetDefault(tracer.Wrap(tp))
	c := &client{
		uploader:  up,
		presigner: pr,
		meta: meta{
			region:               "us-east-1",
			presignDefaultExpiry: 15 * time.Minute,
		},
	}

	if err := c.UploadObject(context.Background(), UploadObjectParams{Bucket: "reports", Key: "k"}); err != nil {
		t.Fatalf("UploadObject: %v", err)
	}

	span, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	attrs := spanAttrs(span)
	if _, ok := attrs["server.address"]; ok {
		t.Fatalf("server.address must be omitted for AWS, got %v", attrs["server.address"])
	}
	if _, ok := attrs["server.port"]; ok {
		t.Fatalf("server.port must be omitted for AWS, got %v", attrs["server.port"])
	}
}

func TestErrorType(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "server api error",
			err: &smithy.OperationError{ServiceID: "S3", OperationName: "GetObject",
				Err: &smithy.GenericAPIError{Code: "NoSuchKey", Message: "The specified key does not exist."}},
			want: "NoSuchKey",
		},
		{
			name: "operation error wrapping api error",
			err: &smithy.OperationError{ServiceID: "S3", OperationName: "PutObject",
				Err: &smithy.OperationError{ServiceID: "S3", OperationName: "PutObject",
					Err: &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}}},
			want: "AccessDenied",
		},
		{
			name: "context deadline exceeded",
			err:  &smithy.OperationError{Err: context.DeadlineExceeded},
			want: "timeout",
		},
		{
			name: "network timeout",
			err:  &smithy.OperationError{Err: &net.OpError{Op: "read", Err: syscall.ETIMEDOUT}},
			want: "timeout",
		},
		{
			name: "connection refused",
			err:  &smithy.OperationError{Err: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}},
			want: "connection_refused",
		},
		{
			name: "connection reset",
			err:  &smithy.OperationError{Err: &net.OpError{Op: "read", Err: syscall.ECONNRESET}},
			want: "connection_reset",
		},
		{
			name: "dns error",
			err:  &smithy.OperationError{Err: &net.DNSError{Err: "no such host", Name: "minio.local"}},
			want: "dns_error",
		},
		{
			name: "unknown transport error",
			err:  &smithy.OperationError{Err: errors.New("boom")},
			want: "network_error",
		},
		{
			name: "plain error",
			err:  errors.New("boom"),
			want: "boom",
		},
		{
			name: "nil",
			err:  nil,
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := errorType(tc.err); got != tc.want {
				t.Fatalf("errorType(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}
