package tracer

import (
	"errors"
	"fmt"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	defaultEndpoint           = "localhost:4317"
	defaultExportTimeout      = 5 * time.Second
	defaultBatchTimeout       = 5 * time.Second
	defaultMaxQueueSize       = 2048
	defaultMaxExportBatchSize = 512
)

// Config describes the trace exporter configuration.
type Config struct {
	// Endpoint is the OTLP gRPC collector endpoint.
	Endpoint string `yaml:"endpoint" json:"endpoint"`
	// ExportTimeout limits each OTLP export.
	ExportTimeout time.Duration `yaml:"export_timeout" json:"export_timeout"`
	// Insecure disables TLS for the exporter connection. The zero value uses TLS.
	// The OTEL_EXPORTER_OTLP_INSECURE or OTEL_EXPORTER_OTLP_TRACES_INSECURE
	// environment variable can still enable plaintext, because WithInsecure is
	// only added when this field is true.
	Insecure bool `yaml:"insecure" json:"insecure"`
	// Headers are extra headers sent with OTLP exports.
	Headers map[string]string `yaml:"headers" json:"headers"`
	// GlobalKV contains resource attributes attached to exported spans.
	GlobalKV map[string]any `yaml:"global_kv" json:"global_kv"`
	// Sampler overrides the OTel sampler. Nil uses the SDK default
	// (parent-based always-on). This field is configured programmatically and
	// is not decoded from YAML or JSON.
	Sampler sdktrace.Sampler `yaml:"-" json:"-"`
	// BatchTimeout is the maximum delay before a batch is exported.
	BatchTimeout time.Duration `yaml:"batch_timeout" json:"batch_timeout"`
	// MaxQueueSize is the maximum number of spans queued for export.
	MaxQueueSize int `yaml:"max_queue_size" json:"max_queue_size"`
	// MaxExportBatchSize is the maximum number of spans in one export batch.
	MaxExportBatchSize int `yaml:"max_export_batch_size" json:"max_export_batch_size"`
}

// SetDefaults returns a copy of c with zero-valued fields set to their
// defaults: Endpoint "localhost:4317", ExportTimeout 5s, BatchTimeout 5s,
// MaxQueueSize 2048, and MaxExportBatchSize 512. Sampler stays nil when unset,
// which selects the SDK default sampler. Other fields are returned unchanged.
// New calls SetDefaults before Validate.
func (c Config) SetDefaults() Config {
	if c.Endpoint == "" {
		c.Endpoint = defaultEndpoint
	}
	if c.ExportTimeout == 0 {
		c.ExportTimeout = defaultExportTimeout
	}
	if c.BatchTimeout == 0 {
		c.BatchTimeout = defaultBatchTimeout
	}
	if c.MaxQueueSize == 0 {
		c.MaxQueueSize = defaultMaxQueueSize
	}
	if c.MaxExportBatchSize == 0 {
		c.MaxExportBatchSize = defaultMaxExportBatchSize
	}
	return c
}

// Validate reports every problem with c, joined with errors.Join, or nil when
// c is usable. Call SetDefaults first; New does.
func (c Config) Validate() error {
	var errs []error
	if c.ExportTimeout <= 0 {
		errs = append(errs, fmt.Errorf("tracer: export_timeout must be positive"))
	}
	if c.BatchTimeout <= 0 {
		errs = append(errs, fmt.Errorf("tracer: batch_timeout must be positive"))
	}
	if c.MaxQueueSize <= 0 {
		errs = append(errs, fmt.Errorf("tracer: max_queue_size must be positive"))
	}
	if c.MaxExportBatchSize <= 0 {
		errs = append(errs, fmt.Errorf("tracer: max_export_batch_size must be positive"))
	}
	if c.MaxExportBatchSize > c.MaxQueueSize {
		errs = append(errs, fmt.Errorf("tracer: max_export_batch_size must not exceed max_queue_size"))
	}
	return errors.Join(errs...)
}
