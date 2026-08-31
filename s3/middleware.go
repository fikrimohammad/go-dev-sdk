package s3

import (
	"context"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

// instrumentationMiddlewareID is the unique middleware identifier registered in the Smithy stack.
const instrumentationMiddlewareID = "OTelS3Instrumentation"

// instrumentationMiddleware implements middleware.InitializeMiddleware to automatically record
// OpenTelemetry spans, metrics, and error classification for all AWS S3 SDK operations.
type instrumentationMiddleware struct {
	meta meta
}

var _ middleware.InitializeMiddleware = (*instrumentationMiddleware)(nil)

// ID returns the unique identifier of the middleware.
func (m *instrumentationMiddleware) ID() string {
	return instrumentationMiddlewareID
}

// HandleInitialize intercepts every S3 operation at the start of the middleware pipeline,
// records an OpenTelemetry client span and duration/count metrics, and classifies any errors.
func (m *instrumentationMiddleware) HandleInitialize(
	ctx context.Context,
	in middleware.InitializeInput,
	next middleware.InitializeHandler,
) (out middleware.InitializeOutput, md middleware.Metadata, err error) {
	opName := middleware.GetOperationName(ctx)
	if opName == "" {
		opName = "S3Operation"
	}
	bucket := extractBucket(in.Parameters)

	start := time.Now()
	tr := m.meta.tracerClient()

	ctx, span := tr.Start(ctx, opName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(tracer.Attrs(m.meta.samplingAttrs(opName, bucket))...),
	)

	out, md, err = next.HandleInitialize(ctx, in)

	etype := ""
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		etype = errorType(err)
		span.SetAttributes(attribute.String("error.type", etype))
	}
	span.End()

	m.meta.recordMetrics(ctx, opName, bucket, etype, time.Since(start))

	return out, md, err
}

// newMiddleware returns an APIOptions function that registers the OpenTelemetry instrumentation
// middleware into a Smithy stack.
func newMiddleware(m meta) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Initialize.Add(&instrumentationMiddleware{meta: m}, middleware.Before)
	}
}

// extractBucket extracts the bucket name from any AWS S3 input parameter struct.
func extractBucket(params any) string {
	if params == nil {
		return ""
	}

	switch v := params.(type) {
	case *transfermanager.GetObjectInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *transfermanager.UploadObjectInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *transfermanager.DownloadObjectInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.PutObjectInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.GetObjectInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.HeadObjectInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.DeleteObjectInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.DeleteObjectsInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.ListObjectsV2Input:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.ListObjectsInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.CreateBucketInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.DeleteBucketInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.HeadBucketInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.CopyObjectInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.CreateMultipartUploadInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.UploadPartInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.CompleteMultipartUploadInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.AbortMultipartUploadInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.ListPartsInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.ListMultipartUploadsInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.PutBucketPolicyInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.GetBucketPolicyInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.DeleteBucketPolicyInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.PutBucketCorsInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.GetBucketCorsInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.DeleteBucketCorsInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.PutBucketLifecycleConfigurationInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.GetBucketLifecycleConfigurationInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.SelectObjectContentInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	case *s3.RestoreObjectInput:
		if v.Bucket != nil {
			return *v.Bucket
		}
	default:
		return reflectBucket(params)
	}
	return ""
}

// reflectBucket uses reflection as a fallback to extract the Bucket field for any unlisted S3 inputs.
func reflectBucket(params any) string {
	val := reflect.ValueOf(params)
	if !val.IsValid() {
		return ""
	}
	if val.Kind() == reflect.Pointer {
		if val.IsNil() {
			return ""
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return ""
	}

	f := val.FieldByName("Bucket")
	if !f.IsValid() {
		return ""
	}
	if f.Kind() == reflect.Pointer && !f.IsNil() && f.Elem().Kind() == reflect.String {
		return f.Elem().String()
	}
	if f.Kind() == reflect.String {
		return f.String()
	}
	return ""
}
