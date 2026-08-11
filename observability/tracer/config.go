package tracer

import (
	"errors"
	"fmt"
	"time"
)

const (
	defaultEndpoint      = "localhost:4317"
	defaultExportTimeout = 5 * time.Second
)

// Config describes the trace exporter configuration.
type Config struct {
	// Endpoint is the OTLP gRPC collector endpoint.
	Endpoint string `yaml:"endpoint" json:"endpoint"`
	// ExportTimeout limits each OTLP export.
	ExportTimeout time.Duration `yaml:"export_timeout" json:"export_timeout"`
	// Insecure disables TLS for the exporter connection. Unset defaults to
	// insecure, preserving the historical default.
	Insecure *bool `yaml:"insecure" json:"insecure"`
	// Headers are extra headers sent with OTLP exports.
	Headers map[string]string `yaml:"headers" json:"headers"`
}

func (c *Config) setDefaults() {
	if c.Endpoint == "" {
		c.Endpoint = defaultEndpoint
	}
	if c.ExportTimeout == 0 {
		c.ExportTimeout = defaultExportTimeout
	}
}

func (c *Config) validate() error {
	var errs []error
	if c.ExportTimeout <= 0 {
		errs = append(errs, fmt.Errorf("tracer: export_timeout must be positive"))
	}
	return errors.Join(errs...)
}

// isInsecure reports whether the exporter connection skips TLS. Unset defaults
// to insecure, preserving the historical default.
func (c Config) isInsecure() bool {
	return c.Insecure == nil || *c.Insecure
}
