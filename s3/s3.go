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
// s3.client.operation.{count,duration} metrics with OTel attributes:
// rpc.system, rpc.service, rpc.method, aws.s3.bucket, cloud.region (when known),
// server.address and server.port (when a self-hosted endpoint is configured), and
// error.type (the AWS API error code, a short transport label, or unknown_error).
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

	aws "github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

const (
	// tracerScope is the OTel instrumentation scope name.
	tracerScope = "s3.client"

	// metricCount and metricDuration are the OTel metric names emitted per performed operation.
	metricCount    = "s3.client.operation.count"
	metricDuration = "s3.client.operation.duration"
)

// IsNotFound returns true if err represents an S3 404/NotFound or NoSuchKey/NoSuchBucket error.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	var aerr smithy.APIError
	if errors.As(err, &aerr) {
		switch aerr.ErrorCode() {
		case "NoSuchKey", "NotFound", "NoSuchBucket", "404":
			return true
		}
	}
	return false
}

// Client describes the unified S3 client interface, providing native AWS S3 SDK operations,
// TransferManager multipart operations, PresignClient utilities, and telemetry.
type Client interface {
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
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
	PresignPutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
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
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
	PresignPutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

// meta carries the attributes held by every instrumented operation.
type meta struct {
	region               string // cloud.region
	serverAddr           string // server.address, empty for real AWS
	serverPort           int    // server.port, 0 for real AWS
	presignDefaultExpiry time.Duration

	// metrics and tracer override the package-level defaults when non-nil.
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
// client falls back to the package-level default (metrics.SetDefault).
func WithMetrics(m metrics.Client) Option {
	return func(o *options) { o.metrics = m }
}

// WithTracer injects a tracer client for the instrumented operations. A nil
// client falls back to the package-level default (tracer.SetDefault).
func WithTracer(t tracer.Client) Option {
	return func(o *options) { o.tracer = t }
}

// New applies cfg.SetDefaults and cfg.Validate, loads the AWS configuration
// New applies cfg.SetDefaults and cfg.Validate, loads the AWS configuration
// (region, optional static credentials and endpoint), derives a transfer
// manager and a presign client from the underlying AWS config, registers the
// Smithy OpenTelemetry middleware on the operational client, and returns an
// instrumented Client.
func New(cfg Config, opts ...Option) (Client, error) {
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

	s3Client, uninstrumentedClient, err := buildClients(cfg, m)
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
func buildClients(cfg Config, m meta) (*s3.Client, *s3.Client, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
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
func (c *client) PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	opName := "PresignGetObject"
	bucket := ""
	if params != nil && params.Bucket != nil {
		bucket = *params.Bucket
	}

	start := time.Now()
	tr := c.tracerClient()
	ctx, span := tr.Start(ctx, opName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(tracer.Attrs(c.samplingAttrs(opName, bucket))...),
	)
	defer span.End()

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
	etype := ""
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		etype = errorType(err)
		span.SetAttributes(attribute.String("error.type", etype))
	}

	c.recordMetrics(ctx, opName, bucket, etype, time.Since(start))
	return req, err
}

// PresignPutObject returns a presigned URL for PUT requests to upload an object directly.
func (c *client) PresignPutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	opName := "PresignPutObject"
	bucket := ""
	if params != nil && params.Bucket != nil {
		bucket = *params.Bucket
	}

	start := time.Now()
	tr := c.tracerClient()
	ctx, span := tr.Start(ctx, opName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(tracer.Attrs(c.samplingAttrs(opName, bucket))...),
	)
	defer span.End()

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
	etype := ""
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		etype = errorType(err)
		span.SetAttributes(attribute.String("error.type", etype))
	}

	c.recordMetrics(ctx, opName, bucket, etype, time.Since(start))
	return req, err
}

// tracerClient returns the configured tracer client or falls back to package default.
func (m meta) tracerClient() trace.Tracer {
	if m.tracer != nil {
		return m.tracer.Tracer(tracerScope)
	}
	return tracer.Tracer(tracerScope)
}

// recordMetrics records count and duration histograms for an operation.
func (m meta) recordMetrics(ctx context.Context, op, bucket, etype string, duration time.Duration) {
	attrs := m.attrs(op, bucket, etype)
	if m.metrics != nil {
		_ = m.metrics.Count(ctx, metricCount, 1, attrs)
		_ = m.metrics.Histogram(ctx, metricDuration, duration.Seconds(), attrs)
	} else {
		_ = metrics.Count(ctx, metricCount, 1, attrs)
		_ = metrics.Histogram(ctx, metricDuration, duration.Seconds(), attrs)
	}
}

// samplingAttrs returns the attributes that matter for sampling decisions and
// are therefore set at span creation time.
func (m meta) samplingAttrs(op, bucket string) map[string]any {
	a := map[string]any{
		"rpc.system":  "aws-api",
		"rpc.service": "s3",
		"rpc.method":  op,
	}
	if bucket != "" {
		a["aws.s3.bucket"] = bucket
	}
	if m.region != "" {
		a["cloud.region"] = m.region
	}
	if m.serverAddr != "" {
		a["server.address"] = m.serverAddr
		a["server.port"] = m.serverPort
	}
	return a
}

// attrs builds the OTel attributes for a single span / metric event.
func (m meta) attrs(op, bucket, etype string) map[string]any {
	a := map[string]any{
		"rpc.system":  "aws-api",
		"rpc.service": "s3",
		"rpc.method":  op,
	}
	if etype != "" {
		a["error.type"] = etype
	}
	if bucket != "" {
		a["aws.s3.bucket"] = bucket
	}
	if m.region != "" {
		a["cloud.region"] = m.region
	}
	if m.serverAddr != "" {
		a["server.address"] = m.serverAddr
		a["server.port"] = m.serverPort
	}
	return a
}

// errorType extracts a standardized error.type label from err.
func errorType(err error) string {
	if err == nil {
		return ""
	}

	var aerr smithy.APIError
	if errors.As(err, &aerr) {
		if code := aerr.ErrorCode(); code != "" {
			return code
		}
	}

	var operr *smithy.OperationError
	if errors.As(err, &operr) {
		if operr.Err != nil {
			if code := errorType(operr.Err); code != "" && code != "unknown_error" {
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

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Err != nil {
			var scErr syscall.Errno
			if errors.As(opErr.Err, &scErr) {
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
		var dnsErr *net.DNSError
		if errors.As(opErr.Err, &dnsErr) {
			return "dns_error"
		}
		return "network_error"
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns_error"
	}

	msg := err.Error()
	if strings.Contains(msg, "context deadline exceeded") {
		return "timeout"
	}
	if strings.Contains(msg, "context canceled") {
		return "canceled"
	}

	return "unknown_error"
}
