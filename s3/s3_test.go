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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

type stubS3API struct {
	getObjectCalls     int
	getObjectOut       *s3.GetObjectOutput
	getObjectErr       error
	deleteObjectCalls  int
	deleteObjectErr    error
	deleteObjectsCalls int
	deleteObjectsOut   *s3.DeleteObjectsOutput
	deleteObjectsErr   error
	headObjectCalls    int
	headObjectOut      *s3.HeadObjectOutput
	headObjectErr      error
	listObjectsCalls   int
	listObjectsOut     *s3.ListObjectsV2Output
	listObjectsErr     error
}

func (s *stubS3API) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	s.getObjectCalls++
	if s.getObjectErr != nil {
		return nil, s.getObjectErr
	}
	if s.getObjectOut != nil {
		return s.getObjectOut, nil
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("content"))}, nil
}

func (s *stubS3API) DeleteObject(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	s.deleteObjectCalls++
	if s.deleteObjectErr != nil {
		return nil, s.deleteObjectErr
	}
	return &s3.DeleteObjectOutput{}, nil
}

func (s *stubS3API) DeleteObjects(_ context.Context, _ *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	s.deleteObjectsCalls++
	if s.deleteObjectsErr != nil {
		return nil, s.deleteObjectsErr
	}
	if s.deleteObjectsOut != nil {
		return s.deleteObjectsOut, nil
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func (s *stubS3API) HeadObject(_ context.Context, _ *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	s.headObjectCalls++
	if s.headObjectErr != nil {
		return nil, s.headObjectErr
	}
	if s.headObjectOut != nil {
		return s.headObjectOut, nil
	}
	return &s3.HeadObjectOutput{}, nil
}

func (s *stubS3API) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	s.listObjectsCalls++
	if s.listObjectsErr != nil {
		return nil, s.listObjectsErr
	}
	if s.listObjectsOut != nil {
		return s.listObjectsOut, nil
	}
	return &s3.ListObjectsV2Output{}, nil
}

type stubTransferManager struct {
	uploadCalls       int
	uploadErr         error
	lastInput         *transfermanager.UploadObjectInput
	getObjectCalls    int
	getObjectOut      *transfermanager.GetObjectOutput
	getObjectErr      error
	lastGetInput      *transfermanager.GetObjectInput
	downloadCalls     int
	downloadOut       *transfermanager.DownloadObjectOutput
	downloadErr       error
	lastDownloadInput *transfermanager.DownloadObjectInput
}

func (s *stubTransferManager) UploadObject(_ context.Context, input *transfermanager.UploadObjectInput, _ ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error) {
	s.uploadCalls++
	s.lastInput = input
	if s.uploadErr != nil {
		return nil, s.uploadErr
	}
	return &transfermanager.UploadObjectOutput{}, nil
}

func (s *stubTransferManager) GetObject(_ context.Context, input *transfermanager.GetObjectInput, _ ...func(*transfermanager.Options)) (*transfermanager.GetObjectOutput, error) {
	s.getObjectCalls++
	s.lastGetInput = input
	if s.getObjectErr != nil {
		return nil, s.getObjectErr
	}
	if s.getObjectOut != nil {
		return s.getObjectOut, nil
	}
	return &transfermanager.GetObjectOutput{Body: io.NopCloser(strings.NewReader("content"))}, nil
}

func (s *stubTransferManager) DownloadObject(_ context.Context, input *transfermanager.DownloadObjectInput, _ ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error) {
	s.downloadCalls++
	s.lastDownloadInput = input
	if s.downloadErr != nil {
		return nil, s.downloadErr
	}
	if s.downloadOut != nil {
		return s.downloadOut, nil
	}
	return &transfermanager.DownloadObjectOutput{}, nil
}

type stubPresigner struct {
	presignGetCalls int
	presignPutCalls int
	returnErr       error
	expires         time.Duration
}

func (s *stubPresigner) PresignGetObject(_ context.Context, _ *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	s.presignGetCalls++
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

func (s *stubPresigner) PresignPutObject(_ context.Context, _ *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	s.presignPutCalls++
	o := s3.PresignOptions{}
	for _, fn := range optFns {
		fn(&o)
	}
	s.expires = o.Expires
	if s.returnErr != nil {
		return nil, s.returnErr
	}
	return &v4.PresignedHTTPRequest{URL: "https://s3.example.com/upload/test.csv"}, nil
}

// setup installs a capturing tracer and metrics client as package defaults and
// returns an instrumented client wired to the stubs alongside assertion handles.
func setup(api s3API, tm transferManager, presigner presigner) (*client, *fakeMetrics, *recordingExporter) {
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	tracer.SetDefault(tracer.Wrap(tp))

	fm := &fakeMetrics{}
	metrics.SetDefault(fm)

	return &client{
		s3API:           api,
		transferManager: tm,
		presigner:       presigner,
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
	tm := &stubTransferManager{}
	pr := &stubPresigner{}
	api := &stubS3API{}
	c, fm, ex := setup(api, tm, pr)

	err := c.UploadObject(context.Background(), UploadObjectParams{
		Bucket:             "reports",
		Key:                "test.csv",
		Body:               strings.NewReader("x"),
		ContentType:        "text/csv",
		ContentDisposition: "attachment; filename=test.csv",
		ContentEncoding:    "gzip",
		CacheControl:       "max-age=3600",
		Metadata:           map[string]string{"env": "test"},
		StorageClass:       "STANDARD",
	})
	if err != nil {
		t.Fatalf("UploadObject: %v", err)
	}
	if tm.uploadCalls != 1 {
		t.Fatalf("upload calls = %d, want 1", tm.uploadCalls)
	}
	if tm.lastInput == nil || *tm.lastInput.ContentType != "text/csv" || *tm.lastInput.ContentDisposition != "attachment; filename=test.csv" {
		t.Fatalf("unexpected upload input: %+v", tm.lastInput)
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
	tm := &stubTransferManager{uploadErr: errors.New("upload failed")}
	c, _, ex := setup(&stubS3API{}, tm, &stubPresigner{})

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

func TestGetObject_Success(t *testing.T) {
	ct := "application/json"
	cl := int64(42)
	etag := `"abcd"`
	now := time.Now()
	tm := &stubTransferManager{
		getObjectOut: &transfermanager.GetObjectOutput{
			Body:          io.NopCloser(strings.NewReader(`{"key":"value"}`)),
			ContentType:   &ct,
			ContentLength: &cl,
			ETag:          &etag,
			LastModified:  &now,
			Metadata:      map[string]string{"env": "test"},
		},
	}
	c, _, ex := setup(&stubS3API{}, tm, &stubPresigner{})

	res, err := c.GetObject(context.Background(), GetObjectParams{
		Bucket:      "reports",
		Key:         "data.json",
		IfMatch:     `"abcd"`,
		IfNoneMatch: `"efgh"`,
		Range:       "bytes=0-100",
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if res.ContentType != ct || res.ContentLength != cl || res.ETag != etag || res.Metadata["env"] != "test" {
		t.Fatalf("unexpected GetObjectResult: %+v", res)
	}
	data, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if string(data) != `{"key":"value"}` {
		t.Fatalf("body = %q", string(data))
	}

	span, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if span.Name() != "GetObject" {
		t.Fatalf("span name = %q, want GetObject", span.Name())
	}
}

func TestGetObject_Error(t *testing.T) {
	tm := &stubTransferManager{getObjectErr: errors.New("not found")}
	c, _, _ := setup(&stubS3API{}, tm, &stubPresigner{})

	_, err := c.GetObject(context.Background(), GetObjectParams{Bucket: "b", Key: "k"})
	if err == nil {
		t.Fatal("expected error")
	}
}

type bufferWriterAt struct {
	buf []byte
}

func (b *bufferWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	if int(off)+len(p) > len(b.buf) {
		newBuf := make([]byte, int(off)+len(p))
		copy(newBuf, b.buf)
		b.buf = newBuf
	}
	copy(b.buf[off:], p)
	return len(p), nil
}

func TestDownloadObject_Success(t *testing.T) {
	ct := "application/pdf"
	cl := int64(1024)
	etag := `"dl-etag"`
	now := time.Now()
	tm := &stubTransferManager{
		downloadOut: &transfermanager.DownloadObjectOutput{
			ContentType:   &ct,
			ContentLength: &cl,
			ETag:          &etag,
			LastModified:  &now,
			Metadata:      map[string]string{"type": "doc"},
		},
	}
	c, _, ex := setup(&stubS3API{}, tm, &stubPresigner{})

	buf := &bufferWriterAt{}
	res, err := c.DownloadObject(context.Background(), DownloadObjectParams{
		Bucket:      "reports",
		Key:         "doc.pdf",
		Writer:      buf,
		IfMatch:     `"dl-etag"`,
		IfNoneMatch: `"other"`,
		Range:       "bytes=0-1023",
		VersionID:   "v1",
	})
	if err != nil {
		t.Fatalf("DownloadObject: %v", err)
	}
	if res.ContentType != ct || res.ContentLength != cl || res.ETag != etag || res.Metadata["type"] != "doc" {
		t.Fatalf("unexpected DownloadObjectResult: %+v", res)
	}
	if tm.downloadCalls != 1 {
		t.Fatalf("download calls = %d, want 1", tm.downloadCalls)
	}
	if tm.lastDownloadInput == nil || tm.lastDownloadInput.WriterAt != buf || *tm.lastDownloadInput.VersionID != "v1" {
		t.Fatalf("unexpected download input: %+v", tm.lastDownloadInput)
	}

	span, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if span.Name() != "DownloadObject" {
		t.Fatalf("span name = %q, want DownloadObject", span.Name())
	}
}

func TestDownloadObject_Error(t *testing.T) {
	tm := &stubTransferManager{downloadErr: errors.New("download failed")}
	c, _, _ := setup(&stubS3API{}, tm, &stubPresigner{})

	buf := &bufferWriterAt{}
	_, err := c.DownloadObject(context.Background(), DownloadObjectParams{
		Bucket: "reports",
		Key:    "doc.pdf",
		Writer: buf,
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDeleteObject_Success(t *testing.T) {
	api := &stubS3API{}
	c, _, ex := setup(api, &stubTransferManager{}, &stubPresigner{})

	err := c.DeleteObject(context.Background(), DeleteObjectParams{
		Bucket:    "reports",
		Key:       "old.csv",
		VersionID: "v1",
	})
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if api.deleteObjectCalls != 1 {
		t.Fatalf("delete calls = %d, want 1", api.deleteObjectCalls)
	}
	span, ok := ex.last()
	if !ok || span.Name() != "DeleteObject" {
		t.Fatalf("unexpected span: %+v", span)
	}
}

func TestDeleteObject_Error(t *testing.T) {
	api := &stubS3API{deleteObjectErr: errors.New("delete failed")}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	err := c.DeleteObject(context.Background(), DeleteObjectParams{Bucket: "b", Key: "k"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDeleteObjects_Success(t *testing.T) {
	key1 := "k1"
	key2 := "k2"
	api := &stubS3API{
		deleteObjectsOut: &s3.DeleteObjectsOutput{
			Deleted: []types.DeletedObject{
				{Key: &key1},
				{Key: &key2},
			},
		},
	}
	c, _, ex := setup(api, &stubTransferManager{}, &stubPresigner{})

	res, err := c.DeleteObjects(context.Background(), DeleteObjectsParams{
		Bucket: "reports",
		Keys:   []string{"k1", "k2"},
		Quiet:  true,
	})
	if err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if len(res.Deleted) != 2 || res.Deleted[0] != "k1" {
		t.Fatalf("unexpected DeleteObjectsResult: %+v", res)
	}
	span, ok := ex.last()
	if !ok || span.Name() != "DeleteObjects" {
		t.Fatalf("unexpected span: %+v", span)
	}
}

func TestDeleteObjects_WithErrors(t *testing.T) {
	errKey := "k2"
	errCode := "AccessDenied"
	errMsg := "Access Denied"
	errVer := "v1"
	api := &stubS3API{
		deleteObjectsOut: &s3.DeleteObjectsOutput{
			Errors: []types.Error{
				{
					Key:       &errKey,
					Code:      &errCode,
					Message:   &errMsg,
					VersionId: &errVer,
				},
			},
		},
	}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	res, err := c.DeleteObjects(context.Background(), DeleteObjectsParams{
		Bucket: "reports",
		Keys:   []string{"k2"},
	})
	if err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if len(res.Errors) != 1 || res.Errors[0].Code != "AccessDenied" || res.Errors[0].VersionID != "v1" {
		t.Fatalf("unexpected errors: %+v", res.Errors)
	}
}

func TestDeleteObjects_Error(t *testing.T) {
	api := &stubS3API{deleteObjectsErr: errors.New("delete batch failed")}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.DeleteObjects(context.Background(), DeleteObjectsParams{Bucket: "b", Keys: []string{"k"}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestHeadObject_Success(t *testing.T) {
	ct := "image/png"
	cl := int64(1024)
	etag := `"img123"`
	now := time.Now()
	api := &stubS3API{
		headObjectOut: &s3.HeadObjectOutput{
			ContentType:   &ct,
			ContentLength: &cl,
			ETag:          &etag,
			LastModified:  &now,
			Metadata:      map[string]string{"author": "alice"},
			StorageClass:  types.StorageClassStandard,
		},
	}
	c, _, ex := setup(api, &stubTransferManager{}, &stubPresigner{})

	info, err := c.HeadObject(context.Background(), HeadObjectParams{
		Bucket: "images",
		Key:    "avatar.png",
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if info.ContentType != ct || info.ContentLength != cl || info.ETag != etag || info.StorageClass != "STANDARD" || info.Metadata["author"] != "alice" {
		t.Fatalf("unexpected ObjectInfo: %+v", info)
	}
	span, ok := ex.last()
	if !ok || span.Name() != "HeadObject" {
		t.Fatalf("unexpected span: %+v", span)
	}
}

func TestHeadObject_Error(t *testing.T) {
	api := &stubS3API{headObjectErr: errors.New("head failed")}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.HeadObject(context.Background(), HeadObjectParams{Bucket: "b", Key: "k"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestListObjects_Success(t *testing.T) {
	key := "docs/readme.md"
	size := int64(2048)
	etag := `"doc1"`
	prefix := "docs/sub/"
	token := "tok123"
	truncated := true
	api := &stubS3API{
		listObjectsOut: &s3.ListObjectsV2Output{
			Contents: []types.Object{
				{
					Key:          &key,
					Size:         &size,
					ETag:         &etag,
					StorageClass: types.ObjectStorageClassStandard,
				},
			},
			CommonPrefixes: []types.CommonPrefix{
				{Prefix: &prefix},
			},
			NextContinuationToken: &token,
			IsTruncated:           &truncated,
		},
	}
	c, _, ex := setup(api, &stubTransferManager{}, &stubPresigner{})

	res, err := c.ListObjects(context.Background(), ListObjectsParams{
		Bucket:            "docs",
		Prefix:            "docs/",
		Delimiter:         "/",
		ContinuationToken: "prev",
		MaxKeys:           100,
	})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(res.Objects) != 1 || res.Objects[0].Key != key || len(res.CommonPrefixes) != 1 || res.NextContinuationToken != token || !res.IsTruncated {
		t.Fatalf("unexpected ListObjectsResult: %+v", res)
	}
	span, ok := ex.last()
	if !ok || span.Name() != "ListObjects" {
		t.Fatalf("unexpected span: %+v", span)
	}
}

func TestListObjects_Error(t *testing.T) {
	api := &stubS3API{listObjectsErr: errors.New("list failed")}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.ListObjects(context.Background(), ListObjectsParams{Bucket: "b"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPresignGetObject_Success(t *testing.T) {
	up := &stubTransferManager{}
	pr := &stubPresigner{}
	c, _, ex := setup(&stubS3API{}, up, pr)

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
	if pr.presignGetCalls != 1 {
		t.Fatalf("presign calls = %d, want 1", pr.presignGetCalls)
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
	up := &stubTransferManager{}
	pr := &stubPresigner{}
	c, _, _ := setup(&stubS3API{}, up, pr)

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
	up := &stubTransferManager{}
	pr := &stubPresigner{returnErr: errors.New("signing failed")}
	c, fm, ex := setup(&stubS3API{}, up, pr)

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

func TestPresignPutObject_Success(t *testing.T) {
	up := &stubTransferManager{}
	pr := &stubPresigner{}
	c, _, ex := setup(&stubS3API{}, up, pr)

	url, err := c.PresignPutObject(context.Background(), PresignPutObjectParams{
		Bucket:      "uploads",
		Key:         "avatar.png",
		ContentType: "image/png",
		ExpiresIn:   10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("PresignPutObject: %v", err)
	}
	if url != "https://s3.example.com/upload/test.csv" {
		t.Fatalf("url = %q", url)
	}
	if pr.presignPutCalls != 1 {
		t.Fatalf("presign put calls = %d, want 1", pr.presignPutCalls)
	}
	if pr.expires != 10*time.Minute {
		t.Fatalf("expires = %v, want 10m", pr.expires)
	}

	span, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if span.Name() != "PresignPutObject" {
		t.Fatalf("span name = %q, want PresignPutObject", span.Name())
	}
}

func TestPresignPutObject_AppliesDefaultExpiry(t *testing.T) {
	up := &stubTransferManager{}
	pr := &stubPresigner{}
	c, _, _ := setup(&stubS3API{}, up, pr)

	if _, err := c.PresignPutObject(context.Background(), PresignPutObjectParams{
		Bucket: "uploads",
		Key:    "avatar.png",
	}); err != nil {
		t.Fatalf("PresignPutObject: %v", err)
	}
	if pr.expires != 15*time.Minute {
		t.Fatalf("expires = %v, want default 15m", pr.expires)
	}
}

func TestPresignPutObject_Error(t *testing.T) {
	up := &stubTransferManager{}
	pr := &stubPresigner{returnErr: errors.New("put signing failed")}
	c, fm, ex := setup(&stubS3API{}, up, pr)

	_, err := c.PresignPutObject(context.Background(), PresignPutObjectParams{
		Bucket: "uploads",
		Key:    "avatar.png",
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
	tm := &stubTransferManager{}
	pr := &stubPresigner{}
	api := &stubS3API{}

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
		s3API:           api,
		transferManager: tm,
		presigner:       pr,
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
	tm := &stubTransferManager{}
	pr := &stubPresigner{}
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tracer.SetDefault(tracer.Wrap(tp))
	c := &client{
		transferManager: tm,
		presigner:       pr,
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
