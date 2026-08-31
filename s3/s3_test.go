package s3

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
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

// --- fake: AWS SDK stubs -----------------------------------------------------------

type stubS3API struct {
	putObjectCalls        int
	putObjectOut          *s3.PutObjectOutput
	putObjectErr          error
	copyObjectCalls       int
	copyObjectOut         *s3.CopyObjectOutput
	copyObjectErr         error
	lastCopyInput         *s3.CopyObjectInput
	createBucketCalls     int
	createBucketErr       error
	lastCreateBucketInput *s3.CreateBucketInput
	deleteBucketCalls     int
	deleteBucketErr       error
	headBucketCalls       int
	headBucketErr         error
	listBucketsCalls      int
	listBucketsOut        *s3.ListBucketsOutput
	listBucketsErr        error
	deleteObjectCalls     int
	deleteObjectErr       error
	deleteObjectsCalls    int
	deleteObjectsOut      *s3.DeleteObjectsOutput
	deleteObjectsErr      error
	headObjectCalls       int
	headObjectOut         *s3.HeadObjectOutput
	headObjectErr         error
	listObjectsCalls      int
	listObjectsOut        *s3.ListObjectsV2Output
	listObjectsErr        error
}

func (s *stubS3API) PutObject(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	s.putObjectCalls++
	if s.putObjectErr != nil {
		return nil, s.putObjectErr
	}
	if s.putObjectOut != nil {
		return s.putObjectOut, nil
	}
	return &s3.PutObjectOutput{}, nil
}

func (s *stubS3API) CreateBucket(_ context.Context, input *s3.CreateBucketInput, _ ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	s.createBucketCalls++
	s.lastCreateBucketInput = input
	if s.createBucketErr != nil {
		return nil, s.createBucketErr
	}
	return &s3.CreateBucketOutput{}, nil
}

func (s *stubS3API) DeleteBucket(_ context.Context, _ *s3.DeleteBucketInput, _ ...func(*s3.Options)) (*s3.DeleteBucketOutput, error) {
	s.deleteBucketCalls++
	if s.deleteBucketErr != nil {
		return nil, s.deleteBucketErr
	}
	return &s3.DeleteBucketOutput{}, nil
}

func (s *stubS3API) HeadBucket(_ context.Context, _ *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	s.headBucketCalls++
	if s.headBucketErr != nil {
		return nil, s.headBucketErr
	}
	return &s3.HeadBucketOutput{}, nil
}

func (s *stubS3API) ListBuckets(_ context.Context, _ *s3.ListBucketsInput, _ ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	s.listBucketsCalls++
	if s.listBucketsErr != nil {
		return nil, s.listBucketsErr
	}
	if s.listBucketsOut != nil {
		return s.listBucketsOut, nil
	}
	b1 := "bucket-a"
	b2 := "bucket-b"
	return &s3.ListBucketsOutput{
		Buckets: []types.Bucket{
			{Name: &b1},
			{Name: &b2},
		},
	}, nil
}

func (s *stubS3API) CopyObject(_ context.Context, input *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	s.copyObjectCalls++
	s.lastCopyInput = input
	if s.copyObjectErr != nil {
		return nil, s.copyObjectErr
	}
	if s.copyObjectOut != nil {
		return s.copyObjectOut, nil
	}
	etag := `"copy-etag"`
	now := time.Now()
	return &s3.CopyObjectOutput{
		CopyObjectResult: &types.CopyObjectResult{
			ETag:         &etag,
			LastModified: &now,
		},
	}, nil
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
	getObjectCalls    int
	getObjectOut      *transfermanager.GetObjectOutput
	getObjectErr      error
	lastGetInput      *transfermanager.GetObjectInput
	uploadCalls       int
	uploadErr         error
	lastInput         *transfermanager.UploadObjectInput
	downloadCalls     int
	downloadOut       *transfermanager.DownloadObjectOutput
	downloadErr       error
	lastDownloadInput *transfermanager.DownloadObjectInput
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

func (s *stubTransferManager) UploadObject(_ context.Context, input *transfermanager.UploadObjectInput, _ ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error) {
	s.uploadCalls++
	s.lastInput = input
	if s.uploadErr != nil {
		return nil, s.uploadErr
	}
	etag := `"upload-etag"`
	loc := "https://s3.example.com/reports/test.csv"
	ver := "v123"
	upID := "upload-id-456"
	return &transfermanager.UploadObjectOutput{
		ETag:      &etag,
		Location:  &loc,
		VersionID: &ver,
		UploadID:  &upID,
	}, nil
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
	lastGetInput    *s3.GetObjectInput
	lastPutInput    *s3.PutObjectInput
}

func (s *stubPresigner) PresignGetObject(_ context.Context, input *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	s.presignGetCalls++
	s.lastGetInput = input
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

func (s *stubPresigner) PresignPutObject(_ context.Context, input *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	s.presignPutCalls++
	s.lastPutInput = input
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

// setup returns an instrumented client wired to the stubs alongside assertion handles.
func setup(api s3API, tm transferManager, presigner presigner) (*client, *fakeMetrics, *recordingExporter) {
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	tracer.SetDefault(tracer.Wrap(tp))

	fm := &fakeMetrics{}
	metrics.SetDefault(fm)

	return &client{
		s3API:     api,
		tmAPI:     tm,
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
	tm := &stubTransferManager{}
	pr := &stubPresigner{}
	api := &stubS3API{}
	c, _, _ := setup(api, tm, pr)

	res, err := c.UploadObject(context.Background(), &transfermanager.UploadObjectInput{
		Bucket:             aws.String("reports"),
		Key:                aws.String("test.csv"),
		Body:               strings.NewReader("x"),
		ContentType:        aws.String("text/csv"),
		ContentDisposition: aws.String("attachment; filename=test.csv"),
		ChecksumAlgorithm:  tmtypes.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		t.Fatalf("UploadObject: %v", err)
	}
	if *res.ETag != `"upload-etag"` || *res.Location != "https://s3.example.com/reports/test.csv" {
		t.Fatalf("unexpected UploadObject output: %+v", res)
	}
	if tm.uploadCalls != 1 {
		t.Fatalf("upload calls = %d, want 1", tm.uploadCalls)
	}
}

func TestUploadObject_Error(t *testing.T) {
	tm := &stubTransferManager{uploadErr: errors.New("upload failed")}
	c, _, _ := setup(&stubS3API{}, tm, &stubPresigner{})

	_, err := c.UploadObject(context.Background(), &transfermanager.UploadObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("k"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetObject_Success(t *testing.T) {
	tm := &stubTransferManager{}
	c, _, _ := setup(&stubS3API{}, tm, &stubPresigner{})

	res, err := c.GetObject(context.Background(), &transfermanager.GetObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("data.json"),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if tm.getObjectCalls != 1 {
		t.Fatalf("getObjectCalls = %d, want 1", tm.getObjectCalls)
	}
	data, _ := io.ReadAll(res.Body)
	if string(data) != "content" {
		t.Fatalf("body = %q", string(data))
	}
}

func TestGetObject_Error(t *testing.T) {
	tm := &stubTransferManager{getObjectErr: errors.New("not found")}
	c, _, _ := setup(&stubS3API{}, tm, &stubPresigner{})

	_, err := c.GetObject(context.Background(), &transfermanager.GetObjectInput{
		Bucket: aws.String("b"),
		Key:    aws.String("k"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPutObject_Success(t *testing.T) {
	api := &stubS3API{}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("data.json"),
		Body:   strings.NewReader("content"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if api.putObjectCalls != 1 {
		t.Fatalf("putObjectCalls = %d, want 1", api.putObjectCalls)
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
	tm := &stubTransferManager{}
	c, _, _ := setup(&stubS3API{}, tm, &stubPresigner{})

	buf := &bufferWriterAt{}
	_, err := c.DownloadObject(context.Background(), &transfermanager.DownloadObjectInput{
		Bucket:   aws.String("reports"),
		Key:      aws.String("doc.pdf"),
		WriterAt: buf,
	})
	if err != nil {
		t.Fatalf("DownloadObject: %v", err)
	}
	if tm.downloadCalls != 1 {
		t.Fatalf("download calls = %d, want 1", tm.downloadCalls)
	}
}

func TestDownloadObject_Error(t *testing.T) {
	tm := &stubTransferManager{downloadErr: errors.New("download failed")}
	c, _, _ := setup(&stubS3API{}, tm, &stubPresigner{})

	buf := &bufferWriterAt{}
	_, err := c.DownloadObject(context.Background(), &transfermanager.DownloadObjectInput{
		Bucket:   aws.String("reports"),
		Key:      aws.String("doc.pdf"),
		WriterAt: buf,
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCopyObject_Success(t *testing.T) {
	api := &stubS3API{}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	res, err := c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket:     aws.String("dest-bucket"),
		Key:        aws.String("copied.png"),
		CopySource: aws.String("src-bucket/source.png"),
	})
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if *res.CopyObjectResult.ETag != `"copy-etag"` {
		t.Fatalf("res.ETag = %q, want \"copy-etag\"", *res.CopyObjectResult.ETag)
	}
	if api.copyObjectCalls != 1 {
		t.Fatalf("copyObjectCalls = %d, want 1", api.copyObjectCalls)
	}
}

func TestCopyObject_Error(t *testing.T) {
	api := &stubS3API{copyObjectErr: errors.New("copy failed")}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("b2"),
		Key:    aws.String("k2"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestIsNotFound(t *testing.T) {
	if IsNotFound(nil) {
		t.Fatal("IsNotFound(nil) must be false")
	}
	if IsNotFound(errors.New("generic error")) {
		t.Fatal("IsNotFound(generic) must be false")
	}
	if !IsNotFound(&smithy.GenericAPIError{Code: "NoSuchKey"}) {
		t.Fatal("IsNotFound(NoSuchKey) must be true")
	}
	if !IsNotFound(&smithy.GenericAPIError{Code: "NotFound"}) {
		t.Fatal("IsNotFound(NotFound) must be true")
	}
	if !IsNotFound(&smithy.GenericAPIError{Code: "NoSuchBucket"}) {
		t.Fatal("IsNotFound(NoSuchBucket) must be true")
	}
	if !IsNotFound(&smithy.GenericAPIError{Code: "404"}) {
		t.Fatal("IsNotFound(404) must be true")
	}
}

func TestDeleteObject_Success(t *testing.T) {
	api := &stubS3API{}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("old.csv"),
	})
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if api.deleteObjectCalls != 1 {
		t.Fatalf("delete calls = %d, want 1", api.deleteObjectCalls)
	}
}

func TestDeleteObject_Error(t *testing.T) {
	api := &stubS3API{deleteObjectErr: errors.New("delete failed")}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String("b"),
		Key:    aws.String("k"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDeleteObjects_Success(t *testing.T) {
	api := &stubS3API{}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
		Bucket: aws.String("reports"),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String("k1")},
				{Key: aws.String("k2")},
			},
		},
	})
	if err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if api.deleteObjectsCalls != 1 {
		t.Fatalf("deleteObjectsCalls = %d, want 1", api.deleteObjectsCalls)
	}
}

func TestDeleteObjects_Error(t *testing.T) {
	api := &stubS3API{deleteObjectsErr: errors.New("delete batch failed")}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
		Bucket: aws.String("b"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestHeadObject_Success(t *testing.T) {
	ct := "image/png"
	cl := int64(1024)
	api := &stubS3API{
		headObjectOut: &s3.HeadObjectOutput{
			ContentType:   &ct,
			ContentLength: &cl,
		},
	}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	out, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String("images"),
		Key:    aws.String("avatar.png"),
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if *out.ContentType != ct || *out.ContentLength != cl {
		t.Fatalf("unexpected HeadObjectOutput: %+v", out)
	}
}

func TestHeadObject_Error(t *testing.T) {
	api := &stubS3API{headObjectErr: errors.New("head failed")}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String("b"),
		Key:    aws.String("k"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestListObjectsV2_Success(t *testing.T) {
	key := "docs/readme.md"
	api := &stubS3API{
		listObjectsOut: &s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: &key},
			},
		},
	}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	res, err := c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String("docs"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(res.Contents) != 1 || *res.Contents[0].Key != key {
		t.Fatalf("unexpected ListObjectsV2 output: %+v", res)
	}
}

func TestListObjectsV2_Error(t *testing.T) {
	api := &stubS3API{listObjectsErr: errors.New("list failed")}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String("b"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCreateBucket_Success(t *testing.T) {
	api := &stubS3API{}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String("new-bucket"),
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if api.createBucketCalls != 1 {
		t.Fatalf("createBucketCalls = %d, want 1", api.createBucketCalls)
	}
}

func TestCreateBucket_Error(t *testing.T) {
	api := &stubS3API{createBucketErr: errors.New("bucket already exists")}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String("existing-bucket"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDeleteBucket_Success(t *testing.T) {
	api := &stubS3API{}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String("old-bucket"),
	})
	if err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if api.deleteBucketCalls != 1 {
		t.Fatalf("deleteBucketCalls = %d, want 1", api.deleteBucketCalls)
	}
}

func TestDeleteBucket_Error(t *testing.T) {
	api := &stubS3API{deleteBucketErr: errors.New("bucket not empty")}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String("nonempty-bucket"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestListBuckets_Success(t *testing.T) {
	api := &stubS3API{}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	res, err := c.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(res.Buckets) != 2 || *res.Buckets[0].Name != "bucket-a" {
		t.Fatalf("unexpected buckets: %+v", res)
	}
	if api.listBucketsCalls != 1 {
		t.Fatalf("listBucketsCalls = %d, want 1", api.listBucketsCalls)
	}
}

func TestListBuckets_Error(t *testing.T) {
	api := &stubS3API{listBucketsErr: errors.New("unauthorized")}
	c, _, _ := setup(api, &stubTransferManager{}, &stubPresigner{})

	_, err := c.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPresignGetObject_Success(t *testing.T) {
	up := &stubTransferManager{}
	pr := &stubPresigner{}
	c, fm, ex := setup(&stubS3API{}, up, pr)

	req, err := c.PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("test.csv"),
	}, func(o *s3.PresignOptions) {
		o.Expires = 5 * time.Minute
	})
	if err != nil {
		t.Fatalf("PresignGetObject: %v", err)
	}
	if req.URL != "https://s3.example.com/reports/test.csv" {
		t.Fatalf("url = %q", req.URL)
	}
	if pr.presignGetCalls != 1 {
		t.Fatalf("presign calls = %d, want 1", pr.presignGetCalls)
	}
	if pr.expires != 5*time.Minute {
		t.Fatalf("expires = %v, want 5m", pr.expires)
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PresignGetObject" {
		t.Fatalf("span = %+v, want PresignGetObject", span)
	}
	if span.SpanKind() != trace.SpanKindInternal {
		t.Fatalf("span kind = %v, want SpanKindInternal", span.SpanKind())
	}
	attrs := spanAttrs(span)
	if attrs["aws.s3.bucket"] != "reports" || attrs["rpc.method"] != "PresignGetObject" {
		t.Fatalf("unexpected span attrs: %+v", attrs)
	}
	if _, hasErr := attrs["error.type"]; hasErr {
		t.Fatalf("error.type should be absent on success, got: %v", attrs["error.type"])
	}

	count, ok := fm.lastCount()
	if !ok || count.name != metricCount || count.attrs["rpc.method"] != "PresignGetObject" || count.attrs["aws.s3.bucket"] != "reports" {
		t.Fatalf("unexpected count metric: %+v", count)
	}
	if _, hasErr := count.attrs["error.type"]; hasErr {
		t.Fatalf("expected metric error.type to be absent on success, got: %v", count.attrs["error.type"])
	}
}

func TestPresignGetObject_AppliesDefaultExpiry(t *testing.T) {
	up := &stubTransferManager{}
	pr := &stubPresigner{}
	c, _, _ := setup(&stubS3API{}, up, pr)

	if _, err := c.PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("test.csv"),
	}); err != nil {
		t.Fatalf("PresignGetObject: %v", err)
	}
	if pr.expires != 15*time.Minute {
		t.Fatalf("expires = %v, want default 15m", pr.expires)
	}
}

func TestPresignGetObject_Error(t *testing.T) {
	up := &stubTransferManager{}
	pr := &stubPresigner{returnErr: errors.New("presign failed")}
	c, fm, ex := setup(&stubS3API{}, up, pr)

	_, err := c.PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("test.csv"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PresignGetObject" {
		t.Fatalf("span = %+v, want PresignGetObject", span)
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("span status = %v, want Error", span.Status().Code)
	}
	attrs := spanAttrs(span)
	if attrs["error.type"] != "unknown_error" {
		t.Fatalf("error.type = %v, want unknown_error", attrs["error.type"])
	}

	count, ok := fm.lastCount()
	if !ok || count.name != metricCount || count.attrs["error.type"] != "unknown_error" {
		t.Fatalf("unexpected count metric: %+v", count)
	}
}

func TestPresignPutObject_Success(t *testing.T) {
	up := &stubTransferManager{}
	pr := &stubPresigner{}
	c, fm, ex := setup(&stubS3API{}, up, pr)

	req, err := c.PresignPutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("uploads"),
		Key:    aws.String("avatar.png"),
	}, func(o *s3.PresignOptions) {
		o.Expires = 10 * time.Minute
	})
	if err != nil {
		t.Fatalf("PresignPutObject: %v", err)
	}
	if req.URL != "https://s3.example.com/upload/test.csv" {
		t.Fatalf("url = %q", req.URL)
	}
	if pr.presignPutCalls != 1 {
		t.Fatalf("presign put calls = %d, want 1", pr.presignPutCalls)
	}
	if pr.expires != 10*time.Minute {
		t.Fatalf("expires = %v, want 10m", pr.expires)
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PresignPutObject" {
		t.Fatalf("span = %+v, want PresignPutObject", span)
	}
	if span.SpanKind() != trace.SpanKindInternal {
		t.Fatalf("span kind = %v, want SpanKindInternal", span.SpanKind())
	}
	attrs := spanAttrs(span)
	if attrs["aws.s3.bucket"] != "uploads" || attrs["rpc.method"] != "PresignPutObject" {
		t.Fatalf("unexpected span attrs: %+v", attrs)
	}

	count, ok := fm.lastCount()
	if !ok || count.name != metricCount || count.attrs["rpc.method"] != "PresignPutObject" {
		t.Fatalf("unexpected count metric: %+v", count)
	}
}

func TestPresignPutObject_AppliesDefaultExpiry(t *testing.T) {
	up := &stubTransferManager{}
	pr := &stubPresigner{}
	c, _, _ := setup(&stubS3API{}, up, pr)

	if _, err := c.PresignPutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("uploads"),
		Key:    aws.String("avatar.png"),
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

	_, err := c.PresignPutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("uploads"),
		Key:    aws.String("avatar.png"),
	})
	if err == nil {
		t.Fatal("expected error")
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PresignPutObject" {
		t.Fatalf("span = %+v, want PresignPutObject", span)
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("span status = %v, want Error", span.Status().Code)
	}

	count, ok := fm.lastCount()
	if !ok || count.name != metricCount || count.attrs["error.type"] != "unknown_error" {
		t.Fatalf("unexpected count metric: %+v", count)
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

func TestErrorType(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "server api error",
			err:  &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "bucket not found"},
			want: "NoSuchBucket",
		},
		{
			name: "operation error wrapping api error",
			err:  &smithy.OperationError{Err: &smithy.GenericAPIError{Code: "AccessDenied"}},
			want: "AccessDenied",
		},
		{
			name: "context deadline exceeded",
			err:  &smithy.OperationError{Err: context.DeadlineExceeded},
			want: "timeout",
		},
		{
			name: "network timeout",
			err:  &smithy.OperationError{Err: &net.DNSError{IsTimeout: true}},
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
			name: "direct context canceled",
			err:  context.Canceled,
			want: "canceled",
		},
		{
			name: "direct context deadline exceeded",
			err:  context.DeadlineExceeded,
			want: "timeout",
		},
		{
			name: "plain error",
			err:  errors.New("boom"),
			want: "unknown_error",
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

func TestInstrumentationMiddleware_HandleInitialize_Success(t *testing.T) {
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	fm := &fakeMetrics{}

	mw := &instrumentationMiddleware{
		meta: meta{
			region:     "us-east-1",
			serverAddr: "minio.local",
			serverPort: 9000,
			metrics:    fm,
			tracer:     tracer.Wrap(tp),
		},
	}

	if mw.ID() != instrumentationMiddlewareID {
		t.Fatalf("ID = %q, want %q", mw.ID(), instrumentationMiddlewareID)
	}

	ctx := middleware.WithOperationName(context.Background(), "PutObject")
	bucketName := "my-bucket"
	in := middleware.InitializeInput{
		Parameters: &s3.PutObjectInput{
			Bucket: &bucketName,
		},
	}

	next := middleware.InitializeHandlerFunc(func(_ context.Context, _ middleware.InitializeInput) (middleware.InitializeOutput, middleware.Metadata, error) {
		return middleware.InitializeOutput{Result: "ok"}, middleware.Metadata{}, nil
	})

	out, _, err := mw.HandleInitialize(ctx, in, next)
	if err != nil {
		t.Fatalf("HandleInitialize: %v", err)
	}
	if out.Result != "ok" {
		t.Fatalf("result = %v, want ok", out.Result)
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PutObject" {
		t.Fatalf("span = %+v, want PutObject", span)
	}
	attrs := spanAttrs(span)
	if attrs["aws.s3.bucket"] != "my-bucket" || attrs["rpc.method"] != "PutObject" || attrs["cloud.region"] != "us-east-1" {
		t.Fatalf("unexpected attrs: %+v", attrs)
	}
	if errType, hasErr := attrs["error.type"]; hasErr && errType != "" {
		t.Fatalf("error.type should be absent or empty on success, got %v", errType)
	}
	count, ok := fm.lastCount()
	if !ok || count.name != metricCount || count.attrs["aws.s3.bucket"] != "my-bucket" {
		t.Fatalf("count metric = %+v", count)
	}
	if _, hasErr := count.attrs["error.type"]; hasErr {
		t.Fatalf("expected metric error.type to be absent on success, got: %v", count.attrs["error.type"])
	}
	if hist, ok := fm.lastHist(); !ok || hist.name != metricDuration || hist.attrs["rpc.method"] != "PutObject" {
		t.Fatalf("hist metric = %+v", hist)
	}
}

func TestInstrumentationMiddleware_HandleInitialize_Error(t *testing.T) {
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	fm := &fakeMetrics{}

	mw := &instrumentationMiddleware{
		meta: meta{
			region:  "us-east-1",
			metrics: fm,
			tracer:  tracer.Wrap(tp),
		},
	}

	ctx := middleware.WithOperationName(context.Background(), "DeleteBucket")
	bucketName := "old-bucket"
	in := middleware.InitializeInput{
		Parameters: &s3.DeleteBucketInput{
			Bucket: &bucketName,
		},
	}

	next := middleware.InitializeHandlerFunc(func(_ context.Context, _ middleware.InitializeInput) (middleware.InitializeOutput, middleware.Metadata, error) {
		return middleware.InitializeOutput{}, middleware.Metadata{}, &smithy.GenericAPIError{Code: "BucketNotEmpty", Message: "bucket not empty"}
	})

	_, _, err := mw.HandleInitialize(ctx, in, next)
	if err == nil {
		t.Fatal("expected error")
	}

	span, ok := ex.last()
	if !ok || span.Name() != "DeleteBucket" {
		t.Fatalf("span = %+v, want DeleteBucket", span)
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", span.Status().Code)
	}
	attrs := spanAttrs(span)
	if attrs["error.type"] != "BucketNotEmpty" {
		t.Fatalf("error.type = %v, want BucketNotEmpty", attrs["error.type"])
	}
}

func TestExtractBucket(t *testing.T) {
	b := "target-bucket"
	cases := []struct {
		name   string
		input  any
		expect string
	}{
		{"nil input", nil, ""},
		{"PutObjectInput", &s3.PutObjectInput{Bucket: &b}, "target-bucket"},
		{"GetObjectInput", &s3.GetObjectInput{Bucket: &b}, "target-bucket"},
		{"HeadObjectInput", &s3.HeadObjectInput{Bucket: &b}, "target-bucket"},
		{"DeleteObjectInput", &s3.DeleteObjectInput{Bucket: &b}, "target-bucket"},
		{"DeleteObjectsInput", &s3.DeleteObjectsInput{Bucket: &b}, "target-bucket"},
		{"ListObjectsV2Input", &s3.ListObjectsV2Input{Bucket: &b}, "target-bucket"},
		{"CreateBucketInput", &s3.CreateBucketInput{Bucket: &b}, "target-bucket"},
		{"DeleteBucketInput", &s3.DeleteBucketInput{Bucket: &b}, "target-bucket"},
		{"HeadBucketInput", &s3.HeadBucketInput{Bucket: &b}, "target-bucket"},
		{"CopyObjectInput", &s3.CopyObjectInput{Bucket: &b}, "target-bucket"},
		{"PutBucketPolicyInput", &s3.PutBucketPolicyInput{Bucket: &b}, "target-bucket"},
		{"GetBucketPolicyInput", &s3.GetBucketPolicyInput{Bucket: &b}, "target-bucket"},
		{"CreateMultipartUploadInput", &s3.CreateMultipartUploadInput{Bucket: &b}, "target-bucket"},
		{"UploadPartInput", &s3.UploadPartInput{Bucket: &b}, "target-bucket"},
		{"CompleteMultipartUploadInput", &s3.CompleteMultipartUploadInput{Bucket: &b}, "target-bucket"},
		{"SelectObjectContentInput", &s3.SelectObjectContentInput{Bucket: &b}, "target-bucket"},
		{"tm.GetObjectInput", &transfermanager.GetObjectInput{Bucket: &b}, "target-bucket"},
		{"tm.UploadObjectInput", &transfermanager.UploadObjectInput{Bucket: &b}, "target-bucket"},
		{"tm.DownloadObjectInput", &transfermanager.DownloadObjectInput{Bucket: &b}, "target-bucket"},
		{"Custom struct pointer with Bucket pointer", &struct{ Bucket *string }{Bucket: &b}, "target-bucket"},
		{"Custom struct value with Bucket string", struct{ Bucket string }{Bucket: "custom-str"}, "custom-str"},
		{"Struct without Bucket field", struct{ Other string }{Other: "foo"}, ""},
		{"Non-struct type", 12345, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractBucket(tc.input); got != tc.expect {
				t.Fatalf("extractBucket(%+v) = %q, want %q", tc.input, got, tc.expect)
			}
		})
	}
}

func TestOptions_WithMetrics_WithTracer(t *testing.T) {
	fm := &fakeMetrics{}
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tr := tracer.Wrap(tp)

	cfg := Config{Region: "us-east-1"}
	cli, err := New(cfg, WithMetrics(fm), WithTracer(tr))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if cli == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestMiddleware_EndToEnd_LivePutObject(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"etag-123"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	fm := &fakeMetrics{}

	cfg := Config{
		Region:          "us-east-1",
		Endpoint:        srv.URL,
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	cli, err := New(cfg, WithMetrics(fm), WithTracer(tracer.Wrap(tp)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = cli.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("production-data"),
		Key:    aws.String("report.pdf"),
		Body:   strings.NewReader("pdf-content"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	span, ok := ex.last()
	if !ok {
		t.Fatal("expected a span to be recorded by middleware")
	}
	if span.Name() != "PutObject" {
		t.Fatalf("span name = %q, want PutObject", span.Name())
	}
	attrs := spanAttrs(span)
	if attrs["aws.s3.bucket"] != "production-data" {
		t.Fatalf("span aws.s3.bucket = %v, want production-data", attrs["aws.s3.bucket"])
	}
	if attrs["rpc.method"] != "PutObject" || attrs["rpc.system"] != "aws-api" || attrs["rpc.service"] != "s3" {
		t.Fatalf("unexpected RPC attributes: %+v", attrs)
	}

	if count, ok := fm.lastCount(); !ok || count.name != metricCount || count.attrs["aws.s3.bucket"] != "production-data" {
		t.Fatalf("unexpected count metric: %+v", count)
	}
	if hist, ok := fm.lastHist(); !ok || hist.name != metricDuration || hist.attrs["rpc.method"] != "PutObject" {
		t.Fatalf("unexpected duration histogram metric: %+v", hist)
	}
}

func TestMiddleware_EndToEnd_LiveErrorHandling(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchBucket</Code>
  <Message>The specified bucket does not exist</Message>
  <BucketName>missing-bucket</BucketName>
</Error>`))
	}))
	defer srv.Close()

	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	fm := &fakeMetrics{}

	cfg := Config{
		Region:          "us-east-1",
		Endpoint:        srv.URL,
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	cli, err := New(cfg, WithMetrics(fm), WithTracer(tracer.Wrap(tp)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = cli.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String("missing-bucket"),
	})
	if err == nil {
		t.Fatal("expected DeleteBucket to fail")
	}

	span, ok := ex.last()
	if !ok {
		t.Fatal("expected span to be recorded")
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("span status = %v, want Error", span.Status().Code)
	}
	attrs := spanAttrs(span)
	if attrs["error.type"] != "NoSuchBucket" {
		t.Fatalf("error.type = %v, want NoSuchBucket", attrs["error.type"])
	}
}

func TestMiddleware_EndToEnd_PresignGetObject(t *testing.T) {
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	fm := &fakeMetrics{}

	cfg := Config{
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	cli, err := New(cfg, WithMetrics(fm), WithTracer(tracer.Wrap(tp)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	req, err := cli.PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("presigned-bucket"),
		Key:    aws.String("document.pdf"),
	})
	if err != nil {
		t.Fatalf("PresignGetObject: %v", err)
	}
	if req.URL == "" {
		t.Fatal("expected non-empty presigned URL")
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PresignGetObject" {
		t.Fatalf("span = %+v, want PresignGetObject", span)
	}
	if span.SpanKind() != trace.SpanKindInternal {
		t.Fatalf("span kind = %v, want SpanKindInternal", span.SpanKind())
	}
	attrs := spanAttrs(span)
	if attrs["aws.s3.bucket"] != "presigned-bucket" || attrs["rpc.method"] != "PresignGetObject" {
		t.Fatalf("unexpected attrs: %+v", attrs)
	}
	if _, hasErr := attrs["error.type"]; hasErr {
		t.Fatalf("expected error.type to be absent on success, got %v", attrs["error.type"])
	}

	count, ok := fm.lastCount()
	if !ok || count.name != metricCount || count.attrs["rpc.method"] != "PresignGetObject" || count.attrs["aws.s3.bucket"] != "presigned-bucket" {
		t.Fatalf("unexpected count metric: %+v", count)
	}
	if _, hasErr := count.attrs["error.type"]; hasErr {
		t.Fatalf("expected metric error.type to be absent on success, got: %v", count.attrs["error.type"])
	}
}

func TestMiddleware_EndToEnd_PresignPutObject(t *testing.T) {
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	fm := &fakeMetrics{}

	cfg := Config{
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	cli, err := New(cfg, WithMetrics(fm), WithTracer(tracer.Wrap(tp)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	req, err := cli.PresignPutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("presigned-bucket"),
		Key:    aws.String("upload.pdf"),
	})
	if err != nil {
		t.Fatalf("PresignPutObject: %v", err)
	}
	if req.URL == "" {
		t.Fatal("expected non-empty presigned URL")
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PresignPutObject" {
		t.Fatalf("span = %+v, want PresignPutObject", span)
	}
	if span.SpanKind() != trace.SpanKindInternal {
		t.Fatalf("span kind = %v, want SpanKindInternal", span.SpanKind())
	}
	attrs := spanAttrs(span)
	if attrs["aws.s3.bucket"] != "presigned-bucket" || attrs["rpc.method"] != "PresignPutObject" {
		t.Fatalf("unexpected attrs: %+v", attrs)
	}

	count, ok := fm.lastCount()
	if !ok || count.name != metricCount || count.attrs["rpc.method"] != "PresignPutObject" {
		t.Fatalf("unexpected count metric: %+v", count)
	}
}
