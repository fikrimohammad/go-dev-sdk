package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	signerv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-servicekit/observability/metrics"
	"github.com/fikrimohammad/go-servicekit/observability/tracer"
)

// --- tests -------------------------------------------------------------------------

func TestNew_RejectsMissingRegion(t *testing.T) {
	_, err := New(t.Context(), Config{})
	if err == nil {
		t.Fatal("New(missing region) error = nil, want non-nil")
	}
}

func TestNew_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	cli, err := New(ctx, Config{
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("New(canceled ctx) error = %v, want wrapping context.Canceled", err)
	}
	if cli != nil {
		t.Errorf("New(canceled ctx) client = %v, want nil", cli)
	}
}

func TestOptions_WithMetrics_WithTracer(t *testing.T) {
	fm := &fakeMetrics{}
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	cli := newTestClient(t, Config{Region: "us-east-1"}, WithMetrics(fm), WithTracer(tracer.Wrap(tp)))
	if cli == nil {
		t.Fatal("New client = nil, want non-nil")
	}
}

func TestPutObject_Success(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "reports")
	cli := newTestClient(t, testConfig(f.URL))

	putObject(t, cli, "reports", "data.json", "content")

	head, err := cli.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("data.json"),
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if got := *head.ContentLength; got != int64(len("content")) {
		t.Fatalf("ContentLength = %d, want %d", got, len("content"))
	}
}

func TestPutObject_NoSuchBucket(t *testing.T) {
	f := newFakeS3(t)
	cli := newTestClient(t, testConfig(f.URL))

	_, err := cli.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("missing"),
		Key:    aws.String("k"),
		Body:   strings.NewReader("x"),
	})
	if !IsNotFound(err) {
		t.Fatalf("IsNotFound(PutObject to missing bucket) = false, want true; err = %v", err)
	}
}

func TestGetObject(t *testing.T) {
	cases := []struct {
		name string
		rng  *string
		want string
	}{
		{name: "whole object", want: "0123456789"},
		{name: "byte range", rng: aws.String("bytes=2-4"), want: "234"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeS3(t)
			f.bucket(t, "reports")
			cli := newTestClient(t, testConfig(f.URL))
			putObject(t, cli, "reports", "digits.txt", "0123456789")

			res, err := cli.GetObject(t.Context(), &transfermanager.GetObjectInput{
				Bucket: aws.String("reports"),
				Key:    aws.String("digits.txt"),
				Range:  tc.rng,
			}, func(o *transfermanager.Options) {
				// Range applies only in ranges mode.
				if tc.rng != nil {
					o.GetObjectType = tmtypes.GetObjectRanges
				}
			})
			if err != nil {
				t.Fatalf("GetObject: %v", err)
			}
			if got := readAndClose(t, res.Body); got != tc.want {
				t.Fatalf("body = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGetObject_NoSuchKey(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "reports")
	cli := newTestClient(t, testConfig(f.URL))

	_, err := cli.GetObject(t.Context(), &transfermanager.GetObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("absent.txt"),
	})
	if !IsNotFound(err) {
		t.Fatalf("IsNotFound(GetObject of absent key) = false, want true; err = %v", err)
	}
}

func TestDownloadObject_Success(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "reports")
	cli := newTestClient(t, testConfig(f.URL))
	putObject(t, cli, "reports", "doc.pdf", "pdf-bytes")

	buf := &tmtypes.WriteAtBuffer{}
	if _, err := cli.DownloadObject(t.Context(), &transfermanager.DownloadObjectInput{
		Bucket:   aws.String("reports"),
		Key:      aws.String("doc.pdf"),
		WriterAt: buf,
	}); err != nil {
		t.Fatalf("DownloadObject: %v", err)
	}
	if got := string(buf.Bytes()); got != "pdf-bytes" {
		t.Fatalf("downloaded = %q, want %q", got, "pdf-bytes")
	}
}

func TestUploadObject_SinglePart(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "reports")
	cli := newTestClient(t, testConfig(f.URL))

	res, err := cli.UploadObject(t.Context(), &transfermanager.UploadObjectInput{
		Bucket:            aws.String("reports"),
		Key:               aws.String("small.csv"),
		Body:              strings.NewReader("x"),
		ChecksumAlgorithm: tmtypes.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		t.Fatalf("UploadObject: %v", err)
	}
	if res.ETag == nil || *res.ETag == "" {
		t.Fatalf("UploadObject ETag = %v, want non-empty", res.ETag)
	}
	if got := f.count("uploads"); got != 0 {
		t.Fatalf("multipart initiations = %d, want 0 for a small body", got)
	}
	if got := readObject(t, cli, "reports", "small.csv"); got != "x" {
		t.Fatalf("stored body = %q, want %q", got, "x")
	}
}

// TestUploadObject_Multipart uploads 12 MiB with a 5 MiB threshold and part size,
// which the transfer manager sends as three parts.
func TestUploadObject_Multipart(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "reports")
	cfg := testConfig(f.URL)
	cfg.UploadMultipartThreshold = minimumUploadPartSize
	cfg.UploadPartSizeBytes = minimumUploadPartSize
	cli := newTestClient(t, cfg)

	body := bytes.Repeat([]byte("m"), 12<<20)
	if _, err := cli.UploadObject(t.Context(), &transfermanager.UploadObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("big.bin"),
		Body:   bytes.NewReader(body),
	}); err != nil {
		t.Fatalf("UploadObject: %v", err)
	}
	if got := f.count("uploads"); got != 1 {
		t.Fatalf("multipart initiations = %d, want 1", got)
	}
	if got := f.count("partNumber"); got != 3 {
		t.Fatalf("uploaded parts = %d, want 3", got)
	}
	if got := readObject(t, cli, "reports", "big.bin"); got != string(body) {
		t.Fatalf("stored body length = %d, want %d", len(got), len(body))
	}
}

func TestHeadObject_NotFound(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "reports")
	cli := newTestClient(t, testConfig(f.URL))

	_, err := cli.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("absent"),
	})
	if !IsNotFound(err) {
		t.Fatalf("IsNotFound(HeadObject of absent key) = false, want true; err = %v", err)
	}
}

func TestHeadBucket(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "reports")
	cli := newTestClient(t, testConfig(f.URL))

	if _, err := cli.HeadBucket(t.Context(), &s3.HeadBucketInput{Bucket: aws.String("reports")}); err != nil {
		t.Fatalf("HeadBucket(reports): %v", err)
	}
	_, err := cli.HeadBucket(t.Context(), &s3.HeadBucketInput{Bucket: aws.String("private")})
	if !IsNotFound(err) {
		t.Fatalf("IsNotFound(HeadBucket of missing bucket) = false, want true; err = %v", err)
	}
}

func TestDeleteObject(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "reports")
	cli := newTestClient(t, testConfig(f.URL))
	putObject(t, cli, "reports", "old.csv", "old")

	if _, err := cli.DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("old.csv"),
	}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	_, err := cli.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("old.csv"),
	})
	if !IsNotFound(err) {
		t.Fatalf("IsNotFound(HeadObject after delete) = false, want true; err = %v", err)
	}
}

func TestDeleteObjects(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "reports")
	cli := newTestClient(t, testConfig(f.URL))
	putObject(t, cli, "reports", "k1", "1")
	putObject(t, cli, "reports", "k2", "2")
	putObject(t, cli, "reports", "keep", "3")

	if _, err := cli.DeleteObjects(t.Context(), &s3.DeleteObjectsInput{
		Bucket: aws.String("reports"),
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{
			{Key: aws.String("k1")},
			{Key: aws.String("k2")},
		}},
	}); err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if got := listKeys(t, cli, "reports", ""); !slices.Equal(got, []string{"keep"}) {
		t.Fatalf("remaining keys = %v, want [keep]", got)
	}
}

func TestListObjectsV2(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "docs")
	cli := newTestClient(t, testConfig(f.URL))
	for _, key := range []string{"guides/a.md", "guides/b.md", "readme.md"} {
		putObject(t, cli, "docs", key, "x")
	}

	if got := listKeys(t, cli, "docs", "guides/"); !slices.Equal(got, []string{"guides/a.md", "guides/b.md"}) {
		t.Fatalf("keys under guides/ = %v, want [guides/a.md guides/b.md]", got)
	}
}

// listKeys returns the keys in bucket that start with prefix.
func listKeys(t *testing.T, cli Client, bucket, prefix string) []string {
	t.Helper()
	res, err := cli.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2(%s, %q): %v", bucket, prefix, err)
	}
	keys := make([]string, 0, len(res.Contents))
	for _, obj := range res.Contents {
		keys = append(keys, *obj.Key)
	}
	return keys
}

func TestCopyObject(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "src-bucket")
	f.bucket(t, "dest-bucket")
	cli := newTestClient(t, testConfig(f.URL))
	putObject(t, cli, "src-bucket", "source.png", "pixels")

	res, err := cli.CopyObject(t.Context(), &s3.CopyObjectInput{
		Bucket:     aws.String("dest-bucket"),
		Key:        aws.String("copied.png"),
		CopySource: aws.String("src-bucket/source.png"),
	})
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if res.CopyObjectResult == nil || res.CopyObjectResult.ETag == nil {
		t.Fatalf("CopyObject result = %+v, want an ETag", res.CopyObjectResult)
	}
	if got := readObject(t, cli, "dest-bucket", "copied.png"); got != "pixels" {
		t.Fatalf("copied body = %q, want %q", got, "pixels")
	}
}

func TestCreateBucket(t *testing.T) {
	f := newFakeS3(t)
	cli := newTestClient(t, testConfig(f.URL))

	if _, err := cli.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("new-bucket")}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := cli.HeadBucket(t.Context(), &s3.HeadBucketInput{Bucket: aws.String("new-bucket")}); err != nil {
		t.Fatalf("HeadBucket after CreateBucket: %v", err)
	}
	if _, err := cli.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("new-bucket")}); err == nil {
		t.Fatal("CreateBucket of an existing bucket error = nil, want non-nil")
	}
}

func TestDeleteBucket(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "old-bucket")
	f.bucket(t, "nonempty-bucket")
	cli := newTestClient(t, testConfig(f.URL))
	putObject(t, cli, "nonempty-bucket", "k", "v")

	if _, err := cli.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String("old-bucket")}); err != nil {
		t.Fatalf("DeleteBucket(old-bucket): %v", err)
	}
	if !IsNotFound(headBucket(t, cli, "old-bucket")) {
		t.Fatal("old-bucket still exists after DeleteBucket, want NotFound")
	}

	_, err := cli.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String("nonempty-bucket")})
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "BucketNotEmpty" {
		t.Fatalf("DeleteBucket(nonempty) error = %v, want BucketNotEmpty", err)
	}
}

// headBucket returns the HeadBucket error for bucket, or nil when it exists.
func headBucket(t *testing.T, cli Client, bucket string) error {
	t.Helper()
	_, err := cli.HeadBucket(t.Context(), &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	return err
}

func TestListBuckets(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "bucket-a")
	f.bucket(t, "bucket-b")
	cli := newTestClient(t, testConfig(f.URL))

	res, err := cli.ListBuckets(t.Context(), &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	var names []string
	for _, b := range res.Buckets {
		names = append(names, *b.Name)
	}
	// The fake returns buckets in no fixed order, so compare them sorted.
	slices.Sort(names)
	if !slices.Equal(names, []string{"bucket-a", "bucket-b"}) {
		t.Fatalf("ListBuckets names = %v, want [bucket-a bucket-b]", names)
	}
}

func TestPresignGetObject_Success(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "reports")
	cli, fm, ex := newInstrumentedClient(t, testConfig(f.URL))
	putObject(t, cli, "reports", "test.csv", "a,b")

	req, err := cli.PresignGetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("test.csv"),
	}, func(o *s3.PresignOptions) {
		o.Expires = 5 * time.Minute
	})
	if err != nil {
		t.Fatalf("PresignGetObject: %v", err)
	}
	if got := queryValue(t, req.URL, "X-Amz-Expires"); got != "300" {
		t.Fatalf("X-Amz-Expires = %q, want 300", got)
	}
	if got := fetch(t, req.URL); got != "a,b" {
		t.Fatalf("presigned GET body = %q, want %q", got, "a,b")
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PresignGetObject" {
		t.Fatalf("span = %+v, want PresignGetObject", span)
	}
	if span.SpanKind() != trace.SpanKindInternal {
		t.Fatalf("span kind = %v, want SpanKindInternal", span.SpanKind())
	}
	attrs := spanAttrs(span)
	if attrs["aws.s3.bucket"] != "reports" || attrs["rpc.method"] != "S3/PresignGetObject" {
		t.Fatalf("span attrs = %+v, want bucket reports and rpc.method S3/PresignGetObject", attrs)
	}
	if _, hasErr := attrs["error.type"]; hasErr {
		t.Fatalf("error.type on success = %v, want absent", attrs["error.type"])
	}

	hist, ok := fm.lastHist()
	if !ok || hist.name != "rpc.client.call.duration" || hist.attrs["rpc.method"] != "S3/PresignGetObject" || hist.attrs["aws.s3.bucket"] != "reports" {
		t.Fatalf("duration metric = %+v, want the PresignGetObject attributes", hist)
	}
	if _, hasErr := hist.attrs["error.type"]; hasErr {
		t.Fatalf("metric error.type = %v, want absent on success", hist.attrs["error.type"])
	}
}

func TestPresignGetObject_AppliesDefaultExpiry(t *testing.T) {
	f := newFakeS3(t)
	cli := newTestClient(t, testConfig(f.URL))

	req, err := cli.PresignGetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("test.csv"),
	})
	if err != nil {
		t.Fatalf("PresignGetObject: %v", err)
	}
	if got := queryValue(t, req.URL, "X-Amz-Expires"); got != "900" {
		t.Fatalf("X-Amz-Expires = %q, want default 900", got)
	}
}

func TestPresignGetObject_Error(t *testing.T) {
	f := newFakeS3(t)
	cli, fm, ex := newInstrumentedClient(t, testConfig(f.URL))

	// A nil bucket fails parameter validation before any request is signed.
	_, err := cli.PresignGetObject(t.Context(), &s3.GetObjectInput{Key: aws.String("test.csv")})
	if err == nil {
		t.Fatal("PresignGetObject(nil bucket) error = nil, want non-nil")
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PresignGetObject" {
		t.Fatalf("span = %+v, want PresignGetObject", span)
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("span status = %v, want Error", span.Status().Code)
	}
	// The SDK wraps the validation error in an OperationError; its class is the
	// wrapped error's, which is not a network failure.
	if got := spanAttrs(span)["error.type"]; got != "_OTHER" {
		t.Fatalf("error.type = %v, want _OTHER", got)
	}

	hist, ok := fm.lastHist()
	if !ok || hist.name != "rpc.client.call.duration" || hist.attrs["error.type"] != "_OTHER" {
		t.Fatalf("duration metric = %+v, want error.type _OTHER", hist)
	}
}

func TestPresignPutObject_Success(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "uploads")
	cli, fm, ex := newInstrumentedClient(t, testConfig(f.URL))

	req, err := cli.PresignPutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("uploads"),
		Key:    aws.String("avatar.png"),
	}, func(o *s3.PresignOptions) {
		o.Expires = 10 * time.Minute
	})
	if err != nil {
		t.Fatalf("PresignPutObject: %v", err)
	}
	if got := queryValue(t, req.URL, "X-Amz-Expires"); got != "600" {
		t.Fatalf("X-Amz-Expires = %q, want 600", got)
	}
	if got := presignedPut(t, req, "uploaded-by-url"); got != http.StatusOK {
		t.Fatalf("presigned PUT status = %d, want 200", got)
	}
	if got := readObject(t, newTestClient(t, testConfig(f.URL)), "uploads", "avatar.png"); got != "uploaded-by-url" {
		t.Fatalf("stored body = %q, want %q", got, "uploaded-by-url")
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PresignPutObject" {
		t.Fatalf("span = %+v, want PresignPutObject", span)
	}
	if span.SpanKind() != trace.SpanKindInternal {
		t.Fatalf("span kind = %v, want SpanKindInternal", span.SpanKind())
	}
	attrs := spanAttrs(span)
	if attrs["aws.s3.bucket"] != "uploads" || attrs["rpc.method"] != "S3/PresignPutObject" {
		t.Fatalf("span attrs = %+v, want bucket uploads and rpc.method S3/PresignPutObject", attrs)
	}

	hist, ok := fm.lastHist()
	if !ok || hist.name != "rpc.client.call.duration" || hist.attrs["rpc.method"] != "S3/PresignPutObject" {
		t.Fatalf("duration metric = %+v, want the PresignPutObject attributes", hist)
	}
}

func TestPresignPutObject_AppliesDefaultExpiry(t *testing.T) {
	f := newFakeS3(t)
	cli := newTestClient(t, testConfig(f.URL))

	req, err := cli.PresignPutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("uploads"),
		Key:    aws.String("avatar.png"),
	})
	if err != nil {
		t.Fatalf("PresignPutObject: %v", err)
	}
	if got := queryValue(t, req.URL, "X-Amz-Expires"); got != "900" {
		t.Fatalf("X-Amz-Expires = %q, want default 900", got)
	}
}

func TestPresignPutObject_Error(t *testing.T) {
	f := newFakeS3(t)
	cli, fm, ex := newInstrumentedClient(t, testConfig(f.URL))

	_, err := cli.PresignPutObject(t.Context(), &s3.PutObjectInput{Key: aws.String("avatar.png")})
	if err == nil {
		t.Fatal("PresignPutObject(nil bucket) error = nil, want non-nil")
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PresignPutObject" {
		t.Fatalf("span = %+v, want PresignPutObject", span)
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("span status = %v, want Error", span.Status().Code)
	}

	hist, ok := fm.lastHist()
	if !ok || hist.name != "rpc.client.call.duration" || hist.attrs["error.type"] != "_OTHER" {
		t.Fatalf("duration metric = %+v, want error.type _OTHER", hist)
	}
}

// queryValue returns the query parameter key of the raw URL.
func queryValue(t *testing.T, raw, key string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", raw, err)
	}
	return u.Query().Get(key)
}

// fetch performs a GET on the presigned URL and returns the body, closing the response.
func fetch(t *testing.T, raw string) string {
	t.Helper()
	res, err := http.Get(raw)
	if err != nil {
		t.Fatalf("GET %s: %v", raw, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", res.StatusCode)
	}
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read presigned body: %v", err)
	}
	return string(data)
}

// presignedPut sends body with a presigned PUT request and returns the response
// status, closing the response.
func presignedPut(t *testing.T, req *signerv4.PresignedHTTPRequest, body string) int {
	t.Helper()
	httpReq, err := http.NewRequestWithContext(t.Context(), req.Method, req.URL, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest(%s): %v", req.URL, err)
	}
	for k, v := range req.SignedHeader {
		if k != "Host" {
			httpReq.Header[k] = v
		}
	}
	res, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("PUT %s: %v", req.URL, err)
	}
	defer res.Body.Close()
	return res.StatusCode
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

func TestIsNotFound(t *testing.T) {
	if IsNotFound(nil) {
		t.Fatal("IsNotFound(nil) = true, want false")
	}
	if IsNotFound(errors.New("generic error")) {
		t.Fatal("IsNotFound(generic) = true, want false")
	}
	if !IsNotFound(&smithy.GenericAPIError{Code: "NoSuchKey"}) {
		t.Fatal("IsNotFound(NoSuchKey) = false, want true")
	}
	if !IsNotFound(&smithy.GenericAPIError{Code: "NotFound"}) {
		t.Fatal("IsNotFound(NotFound) = false, want true")
	}
	if !IsNotFound(&smithy.GenericAPIError{Code: "NoSuchBucket"}) {
		t.Fatal("IsNotFound(NoSuchBucket) = false, want true")
	}
	if !IsNotFound(&smithy.GenericAPIError{Code: "404"}) {
		t.Fatal("IsNotFound(404) = false, want true")
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
			name: "connection timed out",
			err:  &smithy.OperationError{Err: &net.OpError{Op: "dial", Err: syscall.ETIMEDOUT}},
			want: "timeout",
		},
		{
			name: "dns error",
			err:  &smithy.OperationError{Err: &net.DNSError{Err: "no such host", Name: "minio.local"}},
			want: "dns_error",
		},
		{
			name: "operation error with an unclassified cause",
			err:  &smithy.OperationError{Err: errors.New("boom")},
			want: "_OTHER",
		},
		{
			name: "operation error with a non-timeout net.Error",
			err:  &smithy.OperationError{Err: &net.AddrError{Err: "missing port", Addr: "minio.local"}},
			want: "network_error",
		},
		{
			name: "operation error with a nil cause",
			err:  &smithy.OperationError{},
			want: "_OTHER",
		},
		{
			name: "bare non-timeout net.Error",
			err:  &url.Error{Op: "Get", URL: "http://x", Err: &net.AddrError{Err: "missing port", Addr: "x"}},
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
			name: "op error with dns cause",
			err:  &smithy.OperationError{Err: &net.OpError{Op: "dial", Err: &net.DNSError{Err: "no such host"}}},
			want: "dns_error",
		},
		{
			name: "op error with plain cause",
			err:  &smithy.OperationError{Err: &net.OpError{Op: "dial", Err: errors.New("boom")}},
			want: "network_error",
		},
		{
			name: "bare dns error",
			err:  &net.DNSError{Err: "no such host", Name: "minio.local"},
			want: "dns_error",
		},
		{
			name: "deadline exceeded in message",
			err:  errors.New("dial tcp: context deadline exceeded"),
			want: "timeout",
		},
		{
			name: "canceled in message",
			err:  errors.New("read: context canceled"),
			want: "canceled",
		},
		{
			name: "plain error",
			err:  errors.New("boom"),
			want: "_OTHER",
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

// TestPutObject_ConnectionRefused points the client at a closed server, so the
// dial fails at the transport level and the telemetry records the transport label.
func TestPutObject_ConnectionRefused(t *testing.T) {
	ts := httptest.NewServer(http.NotFoundHandler())
	endpoint := ts.URL
	ts.Close()

	cli, fm, ex := newInstrumentedClient(t, testConfig(endpoint))
	_, err := cli.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("reports"),
		Key:    aws.String("k"),
		Body:   strings.NewReader("x"),
	})
	if err == nil {
		t.Fatal("PutObject(closed endpoint) error = nil, want non-nil")
	}
	if IsNotFound(err) {
		t.Fatalf("IsNotFound(transport failure) = true, want false; err = %v", err)
	}

	span, ok := ex.last()
	if !ok || span.Status().Code != codes.Error {
		t.Fatalf("span = %+v, want an errored PutObject span", span)
	}
	if got := spanAttrs(span)["error.type"]; got != "connection_refused" {
		t.Fatalf("error.type = %v, want connection_refused", got)
	}
	hist, ok := fm.lastHist()
	if !ok || hist.attrs["error.type"] != "connection_refused" {
		t.Fatalf("duration metric = %+v, want error.type connection_refused", hist)
	}
}

// TestMiddleware_MetricAttributes asserts the exact attribute set on the span and
// the rpc.client.call.duration point, for a success and a NoSuchBucket failure.
func TestMiddleware_MetricAttributes(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		// wantErrType is the expected error.type, empty on success.
		wantErrType string
	}{
		{name: "success", bucket: "production-data"},
		{name: "failure", bucket: "missing-bucket", wantErrType: "NoSuchBucket"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3(t)
			f.bucket(t, "production-data")
			u, err := url.Parse(f.URL)
			if err != nil {
				t.Fatalf("url.Parse(%q): %v", f.URL, err)
			}

			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
			ex := &recordingExporter{}
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
			t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

			cli := newTestClient(t, testConfig(f.URL), WithMetrics(metrics.Wrap(mp)), WithTracer(tracer.Wrap(tp)))
			_, err = cli.PutObject(t.Context(), &s3.PutObjectInput{
				Bucket: aws.String(tt.bucket),
				Key:    aws.String("report.pdf"),
				Body:   strings.NewReader("pdf-content"),
			})
			if (err != nil) != (tt.wantErrType != "") {
				t.Fatalf("PutObject error = %v, want error %t", err, tt.wantErrType != "")
			}

			want := map[string]string{
				"rpc.system.name": "aws-api",
				"rpc.method":      "S3/PutObject",
				"aws.s3.bucket":   tt.bucket,
				"cloud.region":    "us-east-1",
				"server.address":  u.Hostname(),
				"server.port":     u.Port(),
			}
			if tt.wantErrType != "" {
				want["error.type"] = tt.wantErrType
			}

			all := collectMetrics(t, reader)
			names := slices.Sorted(maps.Keys(all))
			if want := []string{"rpc.client.call.duration"}; !slices.Equal(names, want) {
				t.Fatalf("metric names = %v, want %v", names, want)
			}
			m := all["rpc.client.call.duration"]
			if m.Unit != "s" {
				t.Errorf("unit = %q, want %q", m.Unit, "s")
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("data = %T, want Histogram[float64]", m.Data)
			}
			if len(hist.DataPoints) != 1 || hist.DataPoints[0].Count != 1 {
				t.Fatalf("data points = %+v, want one point with count 1", hist.DataPoints)
			}
			if got := attrStrings(hist.DataPoints[0].Attributes.ToSlice()); !maps.Equal(got, want) {
				t.Errorf("metric attributes = %v, want %v", got, want)
			}

			span, ok := ex.last()
			if !ok || span.Name() != "PutObject" || span.SpanKind() != trace.SpanKindClient {
				t.Fatalf("span = %v, want PutObject client span", span)
			}
			if got := attrStrings(span.Attributes()); !maps.Equal(got, want) {
				t.Errorf("span attributes = %v, want %v", got, want)
			}
		})
	}
}

// TestMiddleware_FallsBackToOTelGlobal checks that a client built without
// injected providers records through the OTel global providers.
func TestMiddleware_FallsBackToOTelGlobal(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "global-bucket")

	prevTP := otel.GetTracerProvider()
	prevMP := otel.GetMeterProvider()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		_ = tp.Shutdown(ctx)
		_ = mp.Shutdown(ctx)
		if otel.GetTracerProvider() != prevTP {
			otel.SetTracerProvider(prevTP)
		}
		if otel.GetMeterProvider() != prevMP {
			otel.SetMeterProvider(prevMP)
		}
	})

	cli := newTestClient(t, testConfig(f.URL))
	putObject(t, cli, "global-bucket", "k", "v")

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	if spans[0].Name() != "PutObject" || spans[0].SpanKind() != trace.SpanKindClient {
		t.Errorf("span = %q kind %v, want PutObject client span", spans[0].Name(), spans[0].SpanKind())
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	var hist metricdata.Histogram[float64]
	var found bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "rpc.client.call.duration" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("%s data = %T, want Histogram[float64]", "rpc.client.call.duration", m.Data)
			}
			hist, found = h, true
		}
	}
	if !found {
		t.Fatalf("metric %q not collected from the OTel global meter provider", "rpc.client.call.duration")
	}
	if len(hist.DataPoints) != 1 {
		t.Fatalf("%s data points = %d, want 1", "rpc.client.call.duration", len(hist.DataPoints))
	}
	dp := hist.DataPoints[0]
	if dp.Count != 1 {
		t.Errorf("%s count = %d, want 1", "rpc.client.call.duration", dp.Count)
	}
	if v, ok := dp.Attributes.Value("rpc.method"); !ok || v.AsString() != "S3/PutObject" {
		t.Errorf("%s rpc.method = %v, want S3/PutObject", "rpc.client.call.duration", v.AsString())
	}
}

func TestMiddleware_EndToEnd_PresignGetObject(t *testing.T) {
	cfg := Config{
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}
	cli, fm, ex := newInstrumentedClient(t, cfg)

	req, err := cli.PresignGetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String("presigned-bucket"),
		Key:    aws.String("document.pdf"),
	})
	if err != nil {
		t.Fatalf("PresignGetObject: %v", err)
	}
	if req.URL == "" {
		t.Fatal("PresignGetObject URL = empty, want non-empty")
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PresignGetObject" {
		t.Fatalf("span = %+v, want PresignGetObject", span)
	}
	if span.SpanKind() != trace.SpanKindInternal {
		t.Fatalf("span kind = %v, want SpanKindInternal", span.SpanKind())
	}
	attrs := spanAttrs(span)
	if attrs["aws.s3.bucket"] != "presigned-bucket" || attrs["rpc.method"] != "S3/PresignGetObject" {
		t.Fatalf("span attrs = %+v, want bucket presigned-bucket and rpc.method S3/PresignGetObject", attrs)
	}
	if _, hasErr := attrs["error.type"]; hasErr {
		t.Fatalf("error.type on success = %v, want absent", attrs["error.type"])
	}

	hist, ok := fm.lastHist()
	if !ok || hist.name != "rpc.client.call.duration" || hist.attrs["rpc.method"] != "S3/PresignGetObject" || hist.attrs["aws.s3.bucket"] != "presigned-bucket" {
		t.Fatalf("duration metric = %+v, want the PresignGetObject attributes", hist)
	}
	if _, hasErr := hist.attrs["error.type"]; hasErr {
		t.Fatalf("metric error.type = %v, want absent on success", hist.attrs["error.type"])
	}
}

func TestMiddleware_EndToEnd_PresignPutObject(t *testing.T) {
	cfg := Config{
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}
	cli, fm, ex := newInstrumentedClient(t, cfg)

	req, err := cli.PresignPutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("presigned-bucket"),
		Key:    aws.String("upload.pdf"),
	})
	if err != nil {
		t.Fatalf("PresignPutObject: %v", err)
	}
	if req.URL == "" {
		t.Fatal("PresignPutObject URL = empty, want non-empty")
	}

	span, ok := ex.last()
	if !ok || span.Name() != "PresignPutObject" {
		t.Fatalf("span = %+v, want PresignPutObject", span)
	}
	if span.SpanKind() != trace.SpanKindInternal {
		t.Fatalf("span kind = %v, want SpanKindInternal", span.SpanKind())
	}
	attrs := spanAttrs(span)
	if attrs["aws.s3.bucket"] != "presigned-bucket" || attrs["rpc.method"] != "S3/PresignPutObject" {
		t.Fatalf("span attrs = %+v, want bucket presigned-bucket and rpc.method S3/PresignPutObject", attrs)
	}

	hist, ok := fm.lastHist()
	if !ok || hist.name != "rpc.client.call.duration" || hist.attrs["rpc.method"] != "S3/PresignPutObject" {
		t.Fatalf("duration metric = %+v, want the PresignPutObject attributes", hist)
	}
}
