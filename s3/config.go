package s3

import (
	"errors"
	"fmt"
	"net/url"
	"time"
)

// minimumUploadPartSize is the smallest part size S3 accepts for multipart
// uploads.
const minimumUploadPartSize = 5 * 1024 * 1024

// DefaultPresignExpiry is the default validity of presigned URLs when a
// per-call expiry is not supplied.
const DefaultPresignExpiry = 15 * time.Minute

// Config holds the AWS S3 connection and transfer settings.
type Config struct {
	Region          string `yaml:"region"`
	Endpoint        string `yaml:"endpoint"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`

	// UploadPartSizeBytes is the buffer size (in bytes) used when buffering
	// data into parts for a multipart upload. Zero uses the transfer manager
	// default (8MB). The minimum allowed part size is 5MB.
	UploadPartSizeBytes int64 `yaml:"upload_part_size_bytes"`
	// UploadMultipartThreshold is the size (in bytes) above which an upload is
	// performed as a multipart upload. Zero uses the transfer manager default
	// (16MB).
	UploadMultipartThreshold int64 `yaml:"upload_multipart_threshold"`
	// TransferConcurrency is the number of goroutines used when uploading.
	// Zero uses the transfer manager default (5).
	TransferConcurrency int `yaml:"transfer_concurrency"`
	// PresignDefaultExpiry is the default validity of presigned URLs when a
	// per-call expiry is not supplied. Zero defaults to 15 minutes.
	PresignDefaultExpiry time.Duration `yaml:"presign_default_expiry"`
}

// SetDefaults fills the zero-valued PresignDefaultExpiry with the package
// default (DefaultPresignExpiry) and returns the updated copy.
// The multipart transfer settings are left at zero, which preserves the AWS
// transfer manager defaults. Connection settings (Region, Endpoint,
// AccessKeyID, SecretAccessKey) are never defaulted.
func (c Config) SetDefaults() Config {
	if c.PresignDefaultExpiry == 0 {
		c.PresignDefaultExpiry = DefaultPresignExpiry
	}
	return c
}

// Validate reports configuration problems, such as a missing region, partial
// credentials, an invalid endpoint, or out-of-range transfer settings. It
// returns nil when the config is usable.
func (c Config) Validate() error {
	var errs []error
	if c.Region == "" {
		errs = append(errs, errors.New("s3: region is required"))
	}
	if (c.AccessKeyID == "") != (c.SecretAccessKey == "") {
		errs = append(errs, errors.New("s3: access_key_id and secret_access_key must both be set or both be empty"))
	}
	if c.Endpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil {
			errs = append(errs, fmt.Errorf("s3: endpoint %q is not a valid URL: %w", c.Endpoint, err))
		} else if u.Scheme != "http" && u.Scheme != "https" {
			errs = append(errs, fmt.Errorf("s3: endpoint %q must use http or https scheme", c.Endpoint))
		}
	}
	if c.UploadPartSizeBytes < 0 {
		errs = append(errs, fmt.Errorf("s3: UploadPartSizeBytes must not be negative, got %d", c.UploadPartSizeBytes))
	}
	if c.UploadPartSizeBytes > 0 && c.UploadPartSizeBytes < minimumUploadPartSize {
		errs = append(errs, fmt.Errorf("s3: UploadPartSizeBytes must be at least %d bytes, got %d", minimumUploadPartSize, c.UploadPartSizeBytes))
	}
	if c.UploadMultipartThreshold < 0 {
		errs = append(errs, fmt.Errorf("s3: UploadMultipartThreshold must not be negative, got %d", c.UploadMultipartThreshold))
	}
	if c.TransferConcurrency < 0 {
		errs = append(errs, fmt.Errorf("s3: TransferConcurrency must not be negative, got %d", c.TransferConcurrency))
	}
	if c.PresignDefaultExpiry <= 0 {
		errs = append(errs, fmt.Errorf("s3: PresignDefaultExpiry must be positive, got %v", c.PresignDefaultExpiry))
	}
	return errors.Join(errs...)
}
