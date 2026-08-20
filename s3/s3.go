// Package s3 wraps the AWS S3 SDK transfer manager, S3 client, and presign client
// with a standardized API and records OpenTelemetry traces and metrics per operation.
//
// New builds an AWS S3 client from a Config, derives a transfer manager
// (multipart uploads) and a presign client from the same underlying client,
// and returns an instrumented Client. Metrics and tracing clients are
// injectable via WithMetrics / WithTracer and fall back to the package-level
// defaults.
//
// The Client interface provides comprehensive object operations:
// - UploadObject: transfer-manager upload with transparent multipart buffering.
// - GetObject: stream-based object retrieval with headers and metadata.
// - DeleteObject / DeleteObjects: single and batch object deletion.
// - HeadObject: object existence and metadata lookup.
// - ListObjects: prefix-based object listing with pagination.
// - PresignGetObject / PresignPutObject: presigned download and upload URLs.
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
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	Bucket             string
	Key                string
	Body               io.Reader
	ContentType        string
	ContentDisposition string
	ContentEncoding    string
	CacheControl       string
	Metadata           map[string]string
	StorageClass       string
}

// GetObjectParams describes a request to retrieve an object.
type GetObjectParams struct {
	Bucket      string
	Key         string
	IfMatch     string
	IfNoneMatch string
	Range       string
}

// GetObjectResult contains the downloaded object stream and its metadata.
type GetObjectResult struct {
	Body          io.ReadCloser
	ContentType   string
	ContentLength int64
	ETag          string
	LastModified  *time.Time
	Metadata      map[string]string
}

// DownloadObjectParams describes an object download request writing directly into an io.WriterAt.
type DownloadObjectParams struct {
	Bucket      string
	Key         string
	Writer      io.WriterAt
	IfMatch     string
	IfNoneMatch string
	Range       string
	VersionID   string
}

// DownloadObjectResult contains metadata about the downloaded object.
type DownloadObjectResult struct {
	ContentType   string
	ContentLength int64
	ETag          string
	LastModified  *time.Time
	Metadata      map[string]string
}

// DeleteObjectParams describes an object to delete.
type DeleteObjectParams struct {
	Bucket    string
	Key       string
	VersionID string
}

// DeleteObjectsParams describes batch deletion of multiple objects.
type DeleteObjectsParams struct {
	Bucket string
	Keys   []string
	Quiet  bool
}

// DeleteObjectsResult contains the result of a batch delete operation.
type DeleteObjectsResult struct {
	Deleted []string
	Errors  []DeleteError
}

// DeleteError describes a failed object deletion in a batch delete operation.
type DeleteError struct {
	Key       string
	Code      string
	Message   string
	VersionID string
}

// HeadObjectParams describes an object metadata query.
type HeadObjectParams struct {
	Bucket string
	Key    string
}

// ObjectInfo holds metadata about an S3 object.
type ObjectInfo struct {
	Bucket        string
	Key           string
	ContentType   string
	ContentLength int64
	ETag          string
	LastModified  *time.Time
	Metadata      map[string]string
	StorageClass  string
}

// ListObjectsParams describes a query to list objects matching a prefix.
type ListObjectsParams struct {
	Bucket            string
	Prefix            string
	Delimiter         string
	ContinuationToken string
	MaxKeys           int32
}

// ListObjectsResult contains the objects and folders returned by a list query.
type ListObjectsResult struct {
	Objects               []ObjectSummary
	CommonPrefixes        []string
	NextContinuationToken string
	IsTruncated           bool
}

// ObjectSummary summarizes an object within a list result.
type ObjectSummary struct {
	Key          string
	Size         int64
	ETag         string
	LastModified *time.Time
	StorageClass string
}

// PresignGetObjectParams describes a presigned download URL request.
type PresignGetObjectParams struct {
	Bucket                     string
	Key                        string
	VersionID                  string
	ResponseContentType        string
	ResponseContentDisposition string
	// ExpiresIn is the URL validity. Zero uses the Config default
	// (PresignDefaultExpiry).
	ExpiresIn time.Duration
}

// PresignPutObjectParams describes a presigned upload URL request.
type PresignPutObjectParams struct {
	Bucket       string
	Key          string
	ContentType  string
	StorageClass string
	Metadata     map[string]string
	// ExpiresIn is the URL validity. Zero uses the Config default
	// (PresignDefaultExpiry).
	ExpiresIn time.Duration
}

// Client describes the S3 client interface.
type Client interface {
	UploadObject(ctx context.Context, params UploadObjectParams) error
	GetObject(ctx context.Context, params GetObjectParams) (*GetObjectResult, error)
	DownloadObject(ctx context.Context, params DownloadObjectParams) (*DownloadObjectResult, error)
	DeleteObject(ctx context.Context, params DeleteObjectParams) error
	DeleteObjects(ctx context.Context, params DeleteObjectsParams) (*DeleteObjectsResult, error)
	HeadObject(ctx context.Context, params HeadObjectParams) (*ObjectInfo, error)
	ListObjects(ctx context.Context, params ListObjectsParams) (*ListObjectsResult, error)
	PresignGetObject(ctx context.Context, params PresignGetObjectParams) (string, error)
	PresignPutObject(ctx context.Context, params PresignPutObjectParams) (string, error)
}

// s3API abstracts the raw s3.Client operations needed by our Client implementation.
type s3API interface {
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// transferManager is the transfer-manager contract, letting tests stub the SDK.
type transferManager interface {
	UploadObject(ctx context.Context, input *transfermanager.UploadObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error)
	GetObject(ctx context.Context, input *transfermanager.GetObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.GetObjectOutput, error)
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
	tmPresigner := s3.NewPresignClient(s3Client)

	return &client{
		s3API:           s3Client,
		transferManager: tm,
		presigner:       tmPresigner,
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
	usePathStyle := false
	if cfg.UsePathStyle != nil {
		usePathStyle = *cfg.UsePathStyle
	} else if cfg.Endpoint != "" {
		usePathStyle = true
	}

	if cfg.Endpoint != "" {
		endpointURL, err := url.Parse(cfg.Endpoint)
		if err != nil {
			return nil, "", 0, fmt.Errorf("s3: invalid endpoint %q: %w", cfg.Endpoint, err)
		}
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpointURL.String())
			o.UsePathStyle = usePathStyle
		})
	} else if usePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
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

// client implements Client over the s3 client, transfer manager, and presign client.
type client struct {
	s3API           s3API
	transferManager transferManager
	presigner       presigner
	meta
}

var _ Client = (*client)(nil)

// UploadObject uploads an object through the transfer manager, which performs
// a multipart upload for large bodies.
func (c *client) UploadObject(ctx context.Context, params UploadObjectParams) error {
	return c.instrument(ctx, "UploadObject", params.Bucket, func(ctx context.Context) error {
		input := &transfermanager.UploadObjectInput{
			Bucket:   aws.String(params.Bucket),
			Key:      aws.String(params.Key),
			Body:     params.Body,
			Metadata: params.Metadata,
		}
		if params.ContentType != "" {
			input.ContentType = aws.String(params.ContentType)
		}
		if params.ContentDisposition != "" {
			input.ContentDisposition = aws.String(params.ContentDisposition)
		}
		if params.ContentEncoding != "" {
			input.ContentEncoding = aws.String(params.ContentEncoding)
		}
		if params.CacheControl != "" {
			input.CacheControl = aws.String(params.CacheControl)
		}
		if params.StorageClass != "" {
			input.StorageClass = tmtypes.StorageClass(params.StorageClass)
		}
		_, err := c.transferManager.UploadObject(ctx, input)
		return err
	})
}

// GetObject retrieves an object through the transfer manager, providing
// high-throughput parallelized part downloading for large objects while
// exposing a standard sequential io.ReadCloser stream.
func (c *client) GetObject(ctx context.Context, params GetObjectParams) (*GetObjectResult, error) {
	var res *GetObjectResult
	err := c.instrument(ctx, "GetObject", params.Bucket, func(ctx context.Context) error {
		input := &transfermanager.GetObjectInput{
			Bucket: aws.String(params.Bucket),
			Key:    aws.String(params.Key),
		}
		if params.IfMatch != "" {
			input.IfMatch = aws.String(params.IfMatch)
		}
		if params.IfNoneMatch != "" {
			input.IfNoneMatch = aws.String(params.IfNoneMatch)
		}
		if params.Range != "" {
			input.Range = aws.String(params.Range)
		}

		out, err := c.transferManager.GetObject(ctx, input)
		if err != nil {
			return err
		}

		var body io.ReadCloser
		if out.Body != nil {
			if rc, ok := out.Body.(io.ReadCloser); ok {
				body = rc
			} else {
				body = io.NopCloser(out.Body)
			}
		}

		res = &GetObjectResult{
			Body:         body,
			Metadata:     out.Metadata,
			LastModified: out.LastModified,
		}
		if out.ContentType != nil {
			res.ContentType = *out.ContentType
		}
		if out.ContentLength != nil {
			res.ContentLength = *out.ContentLength
		}
		if out.ETag != nil {
			res.ETag = *out.ETag
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// DownloadObject downloads an object from S3 directly into a destination io.WriterAt
// (such as an *os.File), writing concurrent multipart chunks in parallel.
func (c *client) DownloadObject(ctx context.Context, params DownloadObjectParams) (*DownloadObjectResult, error) {
	var res *DownloadObjectResult
	err := c.instrument(ctx, "DownloadObject", params.Bucket, func(ctx context.Context) error {
		input := &transfermanager.DownloadObjectInput{
			Bucket:   aws.String(params.Bucket),
			Key:      aws.String(params.Key),
			WriterAt: params.Writer,
		}
		if params.IfMatch != "" {
			input.IfMatch = aws.String(params.IfMatch)
		}
		if params.IfNoneMatch != "" {
			input.IfNoneMatch = aws.String(params.IfNoneMatch)
		}
		if params.Range != "" {
			input.Range = aws.String(params.Range)
		}
		if params.VersionID != "" {
			input.VersionID = aws.String(params.VersionID)
		}

		out, err := c.transferManager.DownloadObject(ctx, input)
		if err != nil {
			return err
		}

		res = &DownloadObjectResult{
			Metadata:     out.Metadata,
			LastModified: out.LastModified,
		}
		if out.ContentType != nil {
			res.ContentType = *out.ContentType
		}
		if out.ContentLength != nil {
			res.ContentLength = *out.ContentLength
		}
		if out.ETag != nil {
			res.ETag = *out.ETag
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// DeleteObject removes a single object from S3.
func (c *client) DeleteObject(ctx context.Context, params DeleteObjectParams) error {
	return c.instrument(ctx, "DeleteObject", params.Bucket, func(ctx context.Context) error {
		input := &s3.DeleteObjectInput{
			Bucket: aws.String(params.Bucket),
			Key:    aws.String(params.Key),
		}
		if params.VersionID != "" {
			input.VersionId = aws.String(params.VersionID)
		}
		_, err := c.s3API.DeleteObject(ctx, input)
		return err
	})
}

const maxDeleteObjectsBatchSize = 1000

// DeleteObjects removes multiple objects from S3 in batch requests (automatically chunking
// up to 1,000 keys per batch as mandated by the S3 API).
func (c *client) DeleteObjects(ctx context.Context, params DeleteObjectsParams) (*DeleteObjectsResult, error) {
	if len(params.Keys) == 0 {
		return &DeleteObjectsResult{}, nil
	}

	var totalDeleted []string
	var totalErrors []DeleteError

	err := c.instrument(ctx, "DeleteObjects", params.Bucket, func(ctx context.Context) error {
		for i := 0; i < len(params.Keys); i += maxDeleteObjectsBatchSize {
			end := i + maxDeleteObjectsBatchSize
			if end > len(params.Keys) {
				end = len(params.Keys)
			}
			batchKeys := params.Keys[i:end]

			objectIDs := make([]types.ObjectIdentifier, 0, len(batchKeys))
			for _, k := range batchKeys {
				objectIDs = append(objectIDs, types.ObjectIdentifier{
					Key: aws.String(k),
				})
			}

			input := &s3.DeleteObjectsInput{
				Bucket: aws.String(params.Bucket),
				Delete: &types.Delete{
					Objects: objectIDs,
					Quiet:   aws.Bool(params.Quiet),
				},
			}

			out, err := c.s3API.DeleteObjects(ctx, input)
			if err != nil {
				return err
			}

			for _, d := range out.Deleted {
				if d.Key != nil {
					totalDeleted = append(totalDeleted, *d.Key)
				}
			}

			for _, e := range out.Errors {
				de := DeleteError{}
				if e.Key != nil {
					de.Key = *e.Key
				}
				if e.Code != nil {
					de.Code = *e.Code
				}
				if e.Message != nil {
					de.Message = *e.Message
				}
				if e.VersionId != nil {
					de.VersionID = *e.VersionId
				}
				totalErrors = append(totalErrors, de)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &DeleteObjectsResult{
		Deleted: totalDeleted,
		Errors:  totalErrors,
	}, nil
}

// HeadObject retrieves metadata for an object without downloading its content.
func (c *client) HeadObject(ctx context.Context, params HeadObjectParams) (*ObjectInfo, error) {
	var info *ObjectInfo
	err := c.instrument(ctx, "HeadObject", params.Bucket, func(ctx context.Context) error {
		out, err := c.s3API.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(params.Bucket),
			Key:    aws.String(params.Key),
		})
		if err != nil {
			return err
		}

		info = &ObjectInfo{
			Bucket:       params.Bucket,
			Key:          params.Key,
			Metadata:     out.Metadata,
			LastModified: out.LastModified,
			StorageClass: string(out.StorageClass),
		}
		if out.ContentType != nil {
			info.ContentType = *out.ContentType
		}
		if out.ContentLength != nil {
			info.ContentLength = *out.ContentLength
		}
		if out.ETag != nil {
			info.ETag = *out.ETag
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return info, nil
}

// ListObjects lists objects within a bucket matching the given prefix and criteria.
func (c *client) ListObjects(ctx context.Context, params ListObjectsParams) (*ListObjectsResult, error) {
	var res *ListObjectsResult
	err := c.instrument(ctx, "ListObjects", params.Bucket, func(ctx context.Context) error {
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(params.Bucket),
		}
		if params.Prefix != "" {
			input.Prefix = aws.String(params.Prefix)
		}
		if params.Delimiter != "" {
			input.Delimiter = aws.String(params.Delimiter)
		}
		if params.ContinuationToken != "" {
			input.ContinuationToken = aws.String(params.ContinuationToken)
		}
		if params.MaxKeys > 0 {
			input.MaxKeys = aws.Int32(params.MaxKeys)
		}

		out, err := c.s3API.ListObjectsV2(ctx, input)
		if err != nil {
			return err
		}

		objs := make([]ObjectSummary, 0, len(out.Contents))
		for _, o := range out.Contents {
			summary := ObjectSummary{
				StorageClass: string(o.StorageClass),
				LastModified: o.LastModified,
			}
			if o.Key != nil {
				summary.Key = *o.Key
			}
			if o.Size != nil {
				summary.Size = *o.Size
			}
			if o.ETag != nil {
				summary.ETag = *o.ETag
			}
			objs = append(objs, summary)
		}

		prefixes := make([]string, 0, len(out.CommonPrefixes))
		for _, cp := range out.CommonPrefixes {
			if cp.Prefix != nil {
				prefixes = append(prefixes, *cp.Prefix)
			}
		}

		res = &ListObjectsResult{
			Objects:        objs,
			CommonPrefixes: prefixes,
		}
		if out.NextContinuationToken != nil {
			res.NextContinuationToken = *out.NextContinuationToken
		}
		if out.IsTruncated != nil {
			res.IsTruncated = *out.IsTruncated
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// PresignGetObject returns a presigned download URL for the object. ExpiresIn
// defaults to the Config default (PresignDefaultExpiry) when not set.
func (c *client) PresignGetObject(ctx context.Context, params PresignGetObjectParams) (string, error) {
	if params.ExpiresIn <= 0 {
		params.ExpiresIn = c.presignDefaultExpiry
	}

	var presignURL string
	err := c.instrument(ctx, "PresignGetObject", params.Bucket, func(ctx context.Context) error {
		input := &s3.GetObjectInput{
			Bucket: aws.String(params.Bucket),
			Key:    aws.String(params.Key),
		}
		if params.VersionID != "" {
			input.VersionId = aws.String(params.VersionID)
		}
		if params.ResponseContentType != "" {
			input.ResponseContentType = aws.String(params.ResponseContentType)
		}
		if params.ResponseContentDisposition != "" {
			input.ResponseContentDisposition = aws.String(params.ResponseContentDisposition)
		}

		out, err := c.presigner.PresignGetObject(ctx, input, func(o *s3.PresignOptions) {
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

// PresignPutObject returns a presigned upload URL for the object. ExpiresIn
// defaults to the Config default (PresignDefaultExpiry) when not set.
func (c *client) PresignPutObject(ctx context.Context, params PresignPutObjectParams) (string, error) {
	if params.ExpiresIn <= 0 {
		params.ExpiresIn = c.presignDefaultExpiry
	}

	var presignURL string
	err := c.instrument(ctx, "PresignPutObject", params.Bucket, func(ctx context.Context) error {
		input := &s3.PutObjectInput{
			Bucket:   aws.String(params.Bucket),
			Key:      aws.String(params.Key),
			Metadata: params.Metadata,
		}
		if params.ContentType != "" {
			input.ContentType = aws.String(params.ContentType)
		}
		if params.StorageClass != "" {
			input.StorageClass = types.StorageClass(params.StorageClass)
		}

		out, err := c.presigner.PresignPutObject(ctx, input, func(o *s3.PresignOptions) {
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
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var aerr smithy.APIError
	if errors.As(err, &aerr) {
		return aerr.ErrorCode()
	}

	var opErr *smithy.OperationError
	if errors.As(err, &opErr) && opErr.Err != nil {
		return transportType(opErr.Err)
	}

	var ne net.Error
	if errors.As(err, &ne) {
		return transportType(err)
	}
	return err.Error()
}

// transportType classifies the transport-level failure wrapped by an
// *smithy.OperationError (timeout, unreachable, reset) into a short label.
func transportType(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
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
