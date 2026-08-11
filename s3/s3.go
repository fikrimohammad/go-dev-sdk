// Package s3 wraps the AWS S3 SDK transfer manager and presign client with a
// small, standardized API and records OpenTelemetry traces and metrics per
// operation.
//
// New builds an AWS S3 client from a Config, derives a transfer manager
// (multipart uploads) and a presign client from the same underlying client,
// and returns an instrumented Client. Metrics and tracing clients are
// injectable via WithMetrics / WithTracer and fall back to the package-level
// defaults.
//
// The exported surface is minimal and consistent with the db and redis
// packages: the Client interface exposes the two operations the application
// needs — UploadObject (a transfer-manager upload that transparently performs
// multipart uploads) and PresignGetObject (a presigned download URL) — backed
// by an unexported implementation.
//
// Per performed operation one span named after the operation (client kind) and
// the s3.client.operation.{count,duration} metrics are recorded with OTel
// attributes: rpc.system, rpc.service, rpc.method, aws.s3.bucket, cloud.region
// (when known), server.address and server.port (when a self-hosted endpoint is
// configured) and error.type (the AWS API error code, a short transport label,
// or the error message; empty on success).
//
// Tracing and metrics use the package-level defaults of the observability
// packages (tracer.Tracer / metrics.Count & metrics.Histogram).
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"syscall"
	"time"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
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

// UploadObjectParams describes an object to upload.
type UploadObjectParams struct {
	Bucket      string
	Key         string
	Body        io.ReadCloser
	ContentType string
}

// PresignGetObjectParams describes a presigned download URL request.
type PresignGetObjectParams struct {
	Bucket                     string
	Key                        string
	ResponseContentType        string
	ResponseContentDisposition string
	// ExpiresIn is the URL validity. Zero uses the Config default
	// (PresignDefaultExpiry).
	ExpiresIn time.Duration
}

// Client is the minimal S3 surface: upload an object and presign a download URL.
type Client interface {
	UploadObject(ctx context.Context, params UploadObjectParams) error
	PresignGetObject(ctx context.Context, params PresignGetObjectParams) (string, error)
}

// uploader is the transfer-manager contract, letting tests stub the SDK.
type uploader interface {
	UploadObject(ctx context.Context, input *transfermanager.UploadObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error)
}

// presigner is the presign-client contract, letting tests stub the SDK.
type presigner interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
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
// (region, optional static credentials and endpoint), derives a transfer
// manager and a presign client from a single s3.Client, and returns an
// instrumented Client. Metrics and tracing use the package-level defaults
// unless overridden via WithMetrics/WithTracer.
func New(cfg Config, opts ...Option) (Client, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	cfg = cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	s3Client, serverAddr, serverPort, err := buildClient(cfg)
	if err != nil {
		return nil, err
	}

	uploader := transfermanager.New(s3Client, func(opt *transfermanager.Options) {
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
	presigner := s3.NewPresignClient(s3Client)

	return &client{
		uploader:  uploader,
		presigner: presigner,
		meta: meta{
			region:               cfg.Region,
			serverAddr:           serverAddr,
			serverPort:           serverPort,
			presignDefaultExpiry: cfg.PresignDefaultExpiry,
			metrics:              o.metrics,
			tracer:               o.tracer,
		},
	}, nil
}

// buildClient loads the AWS configuration from cfg and returns a raw s3.Client
// together with the server address and port derived from the endpoint for
// telemetry.
func buildClient(cfg Config) (*s3.Client, string, int, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, "", 0, fmt.Errorf("s3: loading aws config: %w", err)
	}

	serverAddr, serverPort := endpointAddress(cfg.Endpoint)

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		endpointURL, err := url.Parse(cfg.Endpoint)
		if err != nil {
			return nil, "", 0, fmt.Errorf("s3: invalid endpoint %q: %w", cfg.Endpoint, err)
		}
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpointURL.String())
			o.UsePathStyle = true
		})
	}

	return s3.NewFromConfig(awsCfg, s3Opts...), serverAddr, serverPort, nil
}

// endpointAddress extracts the server host and port from a self-hosted S3
// endpoint URL, defaulting the port from the scheme when absent. An empty (or
// unparsable) endpoint yields an empty address so the server attrs are omitted
// for real AWS.
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

// client implements Client over the transfer manager and presign client.
type client struct {
	uploader  uploader
	presigner presigner
	meta
}

var _ Client = (*client)(nil)

// UploadObject uploads an object through the transfer manager, which performs
// a multipart upload for large bodies.
func (c *client) UploadObject(ctx context.Context, params UploadObjectParams) error {
	return c.instrument(ctx, "UploadObject", params.Bucket, func(ctx context.Context) error {
		_, err := c.uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
			Bucket:      aws.String(params.Bucket),
			Key:         aws.String(params.Key),
			Body:        params.Body,
			ContentType: aws.String(params.ContentType),
		})
		return err
	})
}

// PresignGetObject returns a presigned download URL for the object. ExpiresIn
// defaults to the Config default (PresignDefaultExpiry) when not set.
func (c *client) PresignGetObject(ctx context.Context, params PresignGetObjectParams) (string, error) {
	if params.ExpiresIn <= 0 {
		params.ExpiresIn = c.presignDefaultExpiry
	}

	var presignURL string
	err := c.instrument(ctx, "PresignGetObject", params.Bucket, func(ctx context.Context) error {
		out, err := c.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket:                     aws.String(params.Bucket),
			Key:                        aws.String(params.Key),
			ResponseContentType:        aws.String(params.ResponseContentType),
			ResponseContentDisposition: aws.String(params.ResponseContentDisposition),
		}, func(o *s3.PresignOptions) {
			o.Expires = params.ExpiresIn
		})
		if err != nil {
			return err
		}
		presignURL = out.URL
		return nil
	})
	if err != nil {
		return "", err
	}
	return presignURL, nil
}

// instrument records one span and one count + duration histogram around fn,
// which is the actual SDK operation.
func (m meta) instrument(ctx context.Context, op, bucket string, fn func(context.Context) error) error {
	start := time.Now()

	var tr trace.Tracer
	if m.tracer != nil {
		tr = m.tracer.Tracer(tracerScope)
	} else {
		tr = tracer.Tracer(tracerScope)
	}
	ctx, span := tr.Start(ctx, op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(tracer.Attrs(m.samplingAttrs(op, bucket))...),
	)
	err := fn(ctx)

	// error.type is set on every event: to a short error label on failure (the
	// AWS API error code or a transport classification), and to the empty
	// string on success.
	etype := ""
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		etype = errorType(err)
	}

	span.SetAttributes(tracer.Attrs(m.attrs(op, bucket, etype))...)
	span.End()

	attrs := m.attrs(op, bucket, etype)
	if m.metrics != nil {
		_ = m.metrics.Count(ctx, metricCount, 1, attrs)
		_ = m.metrics.Histogram(ctx, metricDuration, time.Since(start).Seconds(), attrs)
	} else {
		_ = metrics.Count(ctx, metricCount, 1, attrs)
		_ = metrics.Histogram(ctx, metricDuration, time.Since(start).Seconds(), attrs)
	}
	return err
}

// samplingAttrs returns the attributes that matter for sampling decisions and
// are therefore set at span creation time.
func (m meta) samplingAttrs(op, bucket string) map[string]any {
	a := map[string]any{
		"rpc.system":    "aws-api",
		"rpc.service":   "s3",
		"rpc.method":    op,
		"aws.s3.bucket": bucket,
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
	a := m.samplingAttrs(op, bucket)
	a["error.type"] = etype
	return a
}

// errorType maps an operation error to the OTel error.type value: the AWS API
// error code when the server responded, a short transport label when the
// request never completed, and the raw error message otherwise.
func errorType(err error) string {
	if err == nil {
		return ""
	}
	var aerr smithy.APIError
	if errors.As(err, &aerr) {
		return aerr.ErrorCode()
	}

	var opErr *smithy.OperationError
	if errors.As(err, &opErr) && opErr.Err != nil {
		return transportType(opErr.Err)
	}
	return err.Error()
}

// transportType classifies the transport-level failure wrapped by an
// *smithy.OperationError (timeout, unreachable, reset) into a short label.
func transportType(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns_error"
	}
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused"
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.ECONNABORTED):
		return "connection_reset"
	case errors.Is(err, syscall.ETIMEDOUT):
		return "timeout"
	default:
		return "network_error"
	}
}
