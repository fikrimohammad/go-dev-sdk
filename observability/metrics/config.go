package metrics

import (
	"errors"
	"fmt"
	"time"
)

const (
	defaultEndpoint            = "localhost:4317"
	defaultMetricsTimeout      = 10 * time.Second
	defaultExportInterval      = 10 * time.Second
	defaultHistogramMaxBuckets = 160
	defaultHistogramMaxScale   = 20
	// maxHistogramMaxBuckets is an app-level guard against unbounded bucket
	// memory usage. It matches the OTel data model cap of 320; the SDK itself
	// validates only that MaxSize is positive.
	maxHistogramMaxBuckets = 320
)

// Config describes the metrics exporter configuration.
type Config struct {
	// Endpoint is the OTLP gRPC collector endpoint.
	Endpoint string `yaml:"endpoint" json:"endpoint"`
	// Timeout limits exporter connection and export operations.
	Timeout time.Duration `yaml:"timeout" json:"timeout"`
	// ExportInterval controls how often pending metrics are exported.
	ExportInterval time.Duration `yaml:"export_interval" json:"export_interval"`
	// Insecure disables TLS for the exporter connection. Unset defaults to
	// insecure, preserving the historical default.
	Insecure *bool `yaml:"insecure" json:"insecure"`
	// GlobalKV contains resource attributes attached to exported metrics.
	GlobalKV map[string]any `yaml:"global_kv" json:"global_kv"`
	// HistogramMaxBuckets caps the number of buckets used per histogram.
	// Zero means 160; the maximum is 320.
	HistogramMaxBuckets int32 `yaml:"histogram_max_buckets" json:"histogram_max_buckets"`
	// HistogramMaxScale is the maximum resolution scale for exponential
	// buckets, from -10 to 20. Higher means finer buckets. Zero means 20.
	HistogramMaxScale int32 `yaml:"histogram_max_scale" json:"histogram_max_scale"`
}

func (c *Config) setDefaults() {
	if c.Endpoint == "" {
		c.Endpoint = defaultEndpoint
	}
	if c.Timeout == 0 {
		c.Timeout = defaultMetricsTimeout
	}
	if c.ExportInterval == 0 {
		c.ExportInterval = defaultExportInterval
	}
	if c.HistogramMaxBuckets == 0 {
		c.HistogramMaxBuckets = defaultHistogramMaxBuckets
	}
	if c.HistogramMaxScale == 0 {
		c.HistogramMaxScale = defaultHistogramMaxScale
	}
}

func (c *Config) validate() error {
	var errs []error
	if c.Timeout <= 0 {
		errs = append(errs, fmt.Errorf("metrics: timeout must be positive"))
	}
	if c.ExportInterval <= 0 {
		errs = append(errs, fmt.Errorf("metrics: export_interval must be positive"))
	}
	if c.HistogramMaxBuckets < 0 {
		errs = append(errs, fmt.Errorf("metrics: histogram_max_buckets must not be negative"))
	}
	if c.HistogramMaxBuckets > maxHistogramMaxBuckets {
		errs = append(errs, fmt.Errorf("metrics: histogram_max_buckets must not exceed %d", maxHistogramMaxBuckets))
	}
	if c.HistogramMaxScale < -10 || c.HistogramMaxScale > 20 {
		errs = append(errs, fmt.Errorf("metrics: histogram_max_scale must be between -10 and 20"))
	}
	return errors.Join(errs...)
}

// isInsecure reports whether the exporter connection skips TLS. Unset defaults
// to insecure, preserving the historical default.
func (c Config) isInsecure() bool {
	return c.Insecure == nil || *c.Insecure
}
