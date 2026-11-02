// Package s3 wraps the AWS S3 SDK transfer manager, S3 client, and presign client
// with standard AWS SDK for Go v2 types and records OpenTelemetry traces and metrics
// per operation via Smithy middleware.
//
// New builds an AWS S3 client from a Config, derives a transfer manager
// (multipart uploads and downloads) and a presign client from the same underlying
// client, registers an OpenTelemetry Smithy middleware stack, and returns an
// instrumented Client.
//
// Every operation executed through Client is automatically
// recorded with one client span named after the operation and the
// rpc.client.call.duration histogram with OTel attributes:
// rpc.system.name ("aws-api"), rpc.method ("S3/<Operation>"), aws.s3.bucket,
// cloud.region (when known), server.address and server.port (when a
// self-hosted endpoint is configured), and error.type (the AWS API error code,
// a short transport label, "timeout", "canceled", or "_OTHER"; set only on failure).
package s3

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	signerv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

// tracerScope is the OTel instrumentation scope name.
const tracerScope = "s3.client"

// rpcSystemName is the rpc.system.name value for AWS API calls. The v1.43.0
// enum has no member for it, so the value is set as a string.
const rpcSystemName = "aws-api"

// rpcCallDuration describes the rpc.client.call.duration histogram, recorded once per S3 operation.
var rpcCallDuration = metrics.Instrument{
	Name:        "rpc.client.call.duration",
	Unit:        "s",
	Description: "Duration of S3 client calls.",
}

// IsNotFound returns true if err represents an S3 404/NotFound or NoSuchKey/NoSuchBucket error.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if aerr, ok := errors.AsType[smithy.APIError](err); ok {
		switch aerr.ErrorCode() {
		case "NoSuchKey", "NotFound", "NoSuchBucket", "404":
			return true
		}
	}
	return false
}

// Client describes the unified S3 client interface, providing native AWS S3 SDK operations,
// TransferManager multipart operations, PresignClient utilities, and telemetry.
type Client interface { //nolint:interfacebloat // caller-facing S3 surface kept whole for gomock
	// Standard AWS S3 operations
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	DeleteBucket(ctx context.Context, params *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	ListBuckets(ctx context.Context, params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error)

	// Transfer Manager operations (multipart uploads / concurrent chunk streaming / downloads)
	GetObject(ctx context.Context, input *transfermanager.GetObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.GetObjectOutput, error)
	UploadObject(ctx context.Context, input *transfermanager.UploadObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error)
	DownloadObject(ctx context.Context, input *transfermanager.DownloadObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error)

	// Presign operations
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*signerv4.PresignedHTTPRequest, error)
	PresignPutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*signerv4.PresignedHTTPRequest, error)
}

// s3API abstracts the raw s3.Client operations needed by our Client implementation for unit tests.
type s3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	DeleteBucket(ctx context.Context, params *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	ListBuckets(ctx context.Context, params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
}

// transferManager is the transfer-manager contract, letting tests stub the SDK.
type transferManager interface {
	GetObject(ctx context.Context, input *transfermanager.GetObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.GetObjectOutput, error)
	UploadObject(ctx context.Context, input *transfermanager.UploadObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error)
	DownloadObject(ctx context.Context, input *transfermanager.DownloadObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error)
}

// presigner is the presign-client contract, letting tests stub the SDK.
type presigner interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*signerv4.PresignedHTTPRequest, error)
	PresignPutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*signerv4.PresignedHTTPRequest, error)
}

// meta carries the attributes held by every instrumented operation.
type meta struct {
	region               string // cloud.region
	serverAddr           string // server.address, empty for real AWS
	serverPort           int    // server.port, 0 for real AWS
	presignDefaultExpiry time.Duration

	// metrics and tracer override the OTel global providers when non-nil.
	metrics metrics.Client
	tracer  tracer.Client
}

// Option configures a Client returned by New.
type Option func(*options)

type options struct {
	metrics metrics.Client
	tracer  tracer.Client
}

// WithMetrics injects a metrics client for the instrumented operations. A nil
// client falls back to the OTel global meter provider (otel.SetMeterProvider).
func WithMetrics(m metrics.Client) Option {
	return func(o *options) { o.metrics = m }
}

// WithTracer injects a tracer client for the instrumented operations. A nil
// client falls back to the OTel global tracer provider (otel.SetTracerProvider).
func WithTracer(t tracer.Client) Option {
	return func(o *options) { o.tracer = t }
}

// New applies cfg.SetDefaults and cfg.Validate, loads the AWS configuration
// (region, optional static credentials and endpoint), derives a transfer
// manager and a presign client from the underlying AWS config, registers the
// Smithy OpenTelemetry middleware on the operational client, and returns an
// instrumented Client.
//
// ctx bounds the AWS configuration load. New checks ctx first and returns an
// error that wraps ctx.Err() when ctx is already done, because the AWS config
// load does not observe cancellation when static credentials are configured.
func New(ctx context.Context, cfg Config, opts ...Option) (Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}

	var o options
	for _, opt := range opts {
		opt(&o)
	}

	cfg = cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	serverAddr, serverPort := endpointAddress(cfg.Endpoint)
	m := meta{
		region:               cfg.Region,
		serverAddr:           serverAddr,
		serverPort:           serverPort,
		presignDefaultExpiry: cfg.PresignDefaultExpiry,
		metrics:              o.metrics,
		tracer:               o.tracer,
	}

	s3Client, uninstrumentedClient, err := buildClients(ctx, cfg, m)
	if err != nil {
		return nil, err
	}

	tm := transfermanager.New(s3Client, func(opt *transfermanager.Options) {
		if cfg.UploadPartSizeBytes > 0 {
			opt.PartSizeBytes = cfg.UploadPartSizeBytes
		}
		if cfg.UploadMultipartThreshold > 0 {
			opt.MultipartUploadThreshold = cfg.UploadMultipartThreshold
		}
		if cfg.TransferConcurrency > 0 {
			opt.Concurrency = cfg.TransferConcurrency
		}
	})
	tmPresigner := s3.NewPresignClient(uninstrumentedClient)

	return &client{
		s3API:     s3Client,
		tmAPI:     tm,
		presigner: tmPresigner,
		meta:      m,
	}, nil
}

// buildClients loads the AWS configuration from cfg and returns an instrumented
// s3.Client (with OTel Smithy middleware) and an uninstrumented s3.Client
// (for local URL presigning without fake RPC spans).
func buildClients(ctx context.Context, cfg Config, m meta) (instrumented, uninstrumented *s3.Client, err error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("s3: loading aws config: %w", err)
	}

	var baseOpts []func(*s3.Options)
	usePathStyle := false
	if cfg.UsePathStyle != nil {
		usePathStyle = *cfg.UsePathStyle
	} else if cfg.Endpoint != "" {
		usePathStyle = true
	}

	if cfg.Endpoint != "" {
		baseOpts = append(baseOpts, func(s3Opt *s3.Options) {
			s3Opt.BaseEndpoint = aws.String(cfg.Endpoint)
			s3Opt.UsePathStyle = usePathStyle
		})
	} else if usePathStyle {
		baseOpts = append(baseOpts, func(s3Opt *s3.Options) {
			s3Opt.UsePathStyle = true
		})
	}

	uninstrumentedClient := s3.NewFromConfig(awsCfg, baseOpts...)

	instrumentedOpts := make([]func(*s3.Options), len(baseOpts), len(baseOpts)+1)
	copy(instrumentedOpts, baseOpts)
	instrumentedOpts = append(instrumentedOpts, func(s3Opt *s3.Options) {
		s3Opt.APIOptions = append(s3Opt.APIOptions, newMiddleware(m))
	})
	instrumentedClient := s3.NewFromConfig(awsCfg, instrumentedOpts...)

	return instrumentedClient, uninstrumentedClient, nil
}

// endpointAddress extracts the server host and port from a self-hosted S3 endpoint URL.
func endpointAddress(endpoint string) (string, int) {
	if endpoint == "" {
		return "", 0
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", 0
	}
	addr := u.Hostname()
	if addr == "" {
		return "", 0
	}
	if p := u.Port(); p != "" {
		port, err := strconv.Atoi(p)
		if err != nil {
			return "", 0
		}
		return addr, port
	}
	switch u.Scheme {
	case "http":
		return addr, 80
	case "https":
		return addr, 443
	default:
		return addr, 0
	}
}

// client implements Client.
type client struct {
	s3API     s3API
	tmAPI     transferManager
	presigner presigner
	meta
}

var _ Client = (*client)(nil)

// PutObject uploads an object to S3.
func (c *client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return c.s3API.PutObject(ctx, params, optFns...)
}

// GetObject retrieves an object as a stream using the S3 Transfer Manager.
// gostyle:ignore getter-prefix S3's API names the operation GetObject.
func (c *client) GetObject(ctx context.Context, input *transfermanager.GetObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.GetObjectOutput, error) {
	return c.tmAPI.GetObject(ctx, input, opts...)
}

// HeadObject retrieves metadata for an object from S3 without returning the object itself.
func (c *client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return c.s3API.HeadObject(ctx, params, optFns...)
}

// DeleteObject deletes an object from S3.
func (c *client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return c.s3API.DeleteObject(ctx, params, optFns...)
}

// DeleteObjects deletes multiple objects from S3 in a single batch request.
func (c *client) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	return c.s3API.DeleteObjects(ctx, params, optFns...)
}

// CopyObject creates a copy of an object already stored in S3.
func (c *client) CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return c.s3API.CopyObject(ctx, params, optFns...)
}

// ListObjectsV2 lists objects in an S3 bucket with prefix and pagination support.
func (c *client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return c.s3API.ListObjectsV2(ctx, params, optFns...)
}

// CreateBucket creates a new S3 bucket.
func (c *client) CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	return c.s3API.CreateBucket(ctx, params, optFns...)
}

// DeleteBucket deletes an empty S3 bucket.
func (c *client) DeleteBucket(ctx context.Context, params *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error) {
	return c.s3API.DeleteBucket(ctx, params, optFns...)
}

// HeadBucket checks if a bucket exists and you have permission to access it.
func (c *client) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return c.s3API.HeadBucket(ctx, params, optFns...)
}

// ListBuckets lists all buckets owned by the authenticated sender of the request.
func (c *client) ListBuckets(ctx context.Context, params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	return c.s3API.ListBuckets(ctx, params, optFns...)
}

// UploadObject uploads an object through the transfer manager (multipart upload for large bodies).
func (c *client) UploadObject(ctx context.Context, input *transfermanager.UploadObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error) {
	return c.tmAPI.UploadObject(ctx, input, opts...)
}

// DownloadObject downloads an object directly into an io.WriterAt concurrently through the transfer manager.
func (c *client) DownloadObject(ctx context.Context, input *transfermanager.DownloadObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error) {
	return c.tmAPI.DownloadObject(ctx, input, opts...)
}

// PresignGetObject returns a presigned URL for GET requests on an object.
func (c *client) PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*signerv4.PresignedHTTPRequest, error) {
	opName := "PresignGetObject"
	bucket := ""
	if params != nil && params.Bucket != nil {
		bucket = *params.Bucket
	}

	start := time.Now()
	attrs := c.attrs(opName, bucket)
	ctx, span := c.tracerClient().Start(ctx, opName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(tracer.Attrs(attrs)...),
	)

	if c.presignDefaultExpiry > 0 {
		optFns = append([]func(*s3.PresignOptions){
			func(o *s3.PresignOptions) {
				if o.Expires == 0 {
					o.Expires = c.presignDefaultExpiry
				}
			},
		}, optFns...)
	}

	req, err := c.presigner.PresignGetObject(ctx, params, optFns...)
	c.finish(ctx, span, attrs, start, err)
	return req, err
}

// PresignPutObject returns a presigned URL for PUT requests to upload an object directly.
func (c *client) PresignPutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*signerv4.PresignedHTTPRequest, error) {
	opName := "PresignPutObject"
	bucket := ""
	if params != nil && params.Bucket != nil {
		bucket = *params.Bucket
	}

	start := time.Now()
	attrs := c.attrs(opName, bucket)
	ctx, span := c.tracerClient().Start(ctx, opName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(tracer.Attrs(attrs)...),
	)

	if c.presignDefaultExpiry > 0 {
		optFns = append([]func(*s3.PresignOptions){
			func(o *s3.PresignOptions) {
				if o.Expires == 0 {
					o.Expires = c.presignDefaultExpiry
				}
			},
		}, optFns...)
	}

	req, err := c.presigner.PresignPutObject(ctx, params, optFns...)
	c.finish(ctx, span, attrs, start, err)
	return req, err
}

// metricsClient returns the injected metrics client, or the OTel global meter
// provider's client when none was injected. It is resolved on every call.
func (m meta) metricsClient() metrics.Client {
	if m.metrics != nil {
		return m.metrics
	}
	return metrics.Default()
}

// tracerClient returns the tracer for the S3 instrumentation scope, from the
// injected tracer client or the OTel global tracer provider when none was
// injected. It is resolved on every call.
func (m meta) tracerClient() trace.Tracer {
	if m.tracer != nil {
		return m.tracer.Tracer(tracerScope)
	}
	return tracer.Default().Tracer(tracerScope)
}

// attrs returns the attributes that one S3 operation puts on its span and on
// its duration point. Attributes with no known value are left out, so none is
// ever empty.
func (m meta) attrs(op, bucket string) map[string]any {
	a := map[string]any{
		string(semconv.RPCSystemNameKey): rpcSystemName,
		string(semconv.RPCMethodKey):     "S3/" + op,
	}
	if bucket != "" {
		a[string(semconv.AWSS3BucketKey)] = bucket
	}
	if m.region != "" {
		a[string(semconv.CloudRegionKey)] = m.region
	}
	if m.serverAddr != "" {
		a[string(semconv.ServerAddressKey)] = m.serverAddr
	}
	if m.serverPort != 0 {
		a[string(semconv.ServerPortKey)] = m.serverPort
	}
	return a
}

// finish completes one operation. When err is non-nil it sets error.type and
// the error status on the span. It then ends the span and records the
// rpc.client.call.duration point, which carries the same attributes as the
// span. Telemetry errors never fail the call.
func (m meta) finish(ctx context.Context, span trace.Span, attrs map[string]any, start time.Time, err error) {
	if err != nil {
		etype := errorType(err)
		attrs[string(semconv.ErrorTypeKey)] = etype
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(semconv.ErrorTypeKey.String(etype))
	}
	span.End()
	_ = m.metricsClient().Record(ctx, rpcCallDuration, time.Since(start).Seconds(), attrs)
}

// errorType returns the error.type value for a non-nil err: the AWS API error
// code, a transport label, "timeout", "canceled", or "_OTHER".
func errorType(err error) string {
	if aerr, ok := errors.AsType[smithy.APIError](err); ok {
		if code := aerr.ErrorCode(); code != "" {
			return code
		}
	}

	if operr, ok := errors.AsType[*smithy.OperationError](err); ok {
		if operr.Err != nil {
			if code := errorType(operr.Err); code != "_OTHER" {
				return code
			}
		}
		return "network_error"
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}

	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return "timeout"
	}

	if opErr, ok := errors.AsType[*net.OpError](err); ok {
		if opErr.Err != nil {
			if scErr, ok := errors.AsType[syscall.Errno](opErr.Err); ok {
				switch scErr {
				case syscall.ECONNREFUSED:
					return "connection_refused"
				case syscall.ECONNRESET:
					return "connection_reset"
				case syscall.ETIMEDOUT:
					return "timeout"
				}
			}
		}
		if _, ok := errors.AsType[*net.DNSError](opErr.Err); ok {
			return "dns_error"
		}
		return "network_error"
	}

	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return "dns_error"
	}

	msg := err.Error()
	if strings.Contains(msg, "context deadline exceeded") {
		return "timeout"
	}
	if strings.Contains(msg, "context canceled") {
		return "canceled"
	}

	return "_OTHER"
}
