package s3

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func TestInstrumentationMiddleware_HandleInitialize_Success(t *testing.T) {
	if got := (&instrumentationMiddleware{}).ID(); got != instrumentationMiddlewareID {
		t.Fatalf("ID = %q, want %q", got, instrumentationMiddlewareID)
	}

	f := newFakeS3(t)
	f.bucket(t, "my-bucket")
	cli, fm, ex := newInstrumentedClient(t, testConfig(f.URL))

	putObject(t, cli, "my-bucket", "k", "v")

	span, ok := ex.last()
	if !ok || span.Name() != "PutObject" || span.SpanKind() != trace.SpanKindClient {
		t.Fatalf("span = %+v, want PutObject client span", span)
	}
	attrs := spanAttrs(span)
	if attrs["aws.s3.bucket"] != "my-bucket" || attrs["rpc.method"] != "S3/PutObject" || attrs["cloud.region"] != "us-east-1" {
		t.Fatalf("span attrs = %+v, want bucket my-bucket, rpc.method S3/PutObject, and region us-east-1", attrs)
	}
	if _, hasErr := attrs["error.type"]; hasErr {
		t.Fatalf("error.type on success = %v, want absent", attrs["error.type"])
	}
	if _, ok := attrs["rpc.system"]; ok {
		t.Errorf("span has removed rpc.system attribute")
	}
	if _, ok := attrs["rpc.service"]; ok {
		t.Errorf("span has removed rpc.service attribute")
	}

	hist, ok := fm.lastHist()
	if !ok || hist.name != "rpc.client.call.duration" || hist.attrs["aws.s3.bucket"] != "my-bucket" || hist.attrs["rpc.method"] != "S3/PutObject" {
		t.Fatalf("duration metric = %+v", hist)
	}
	if _, hasErr := hist.attrs["error.type"]; hasErr {
		t.Fatalf("metric error.type = %v, want absent on success", hist.attrs["error.type"])
	}
}

func TestInstrumentationMiddleware_HandleInitialize_Error(t *testing.T) {
	f := newFakeS3(t)
	f.bucket(t, "old-bucket")
	putObject(t, newTestClient(t, testConfig(f.URL)), "old-bucket", "k", "v")
	cli, fm, ex := newInstrumentedClient(t, testConfig(f.URL))

	// DeleteBucket on a non-empty bucket makes the fake return BucketNotEmpty.
	if _, err := cli.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String("old-bucket")}); err == nil {
		t.Fatal("DeleteBucket error = nil, want non-nil")
	}

	span, ok := ex.last()
	if !ok || span.Name() != "DeleteBucket" {
		t.Fatalf("span = %+v, want DeleteBucket", span)
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", span.Status().Code)
	}
	if got := spanAttrs(span)["error.type"]; got != "BucketNotEmpty" {
		t.Fatalf("error.type = %v, want BucketNotEmpty", got)
	}
	hist, ok := fm.lastHist()
	if !ok || hist.attrs["error.type"] != "BucketNotEmpty" {
		t.Fatalf("duration metric = %+v, want error.type BucketNotEmpty", hist)
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
