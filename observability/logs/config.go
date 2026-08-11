package logs

import (
	"errors"
)

// Format values for Config.Format.
const (
	// FormatText emits key-value text records.
	FormatText = "text"
	// FormatJSON emits JSON records.
	FormatJSON = "json"

	defaultLogFormat = FormatText
	defaultLogLevel  = "debug"
)

// Config validation errors.
var (
	ErrInvalidLogFormat = errors.New("logs: format must be \"text\" or \"json\"")
	ErrInvalidLogLevel  = errors.New("logs: level must be \"debug\", \"info\", \"warn\", or \"error\"")
)

// Config describes the logger configuration.
type Config struct {
	// Format controls whether records use text or JSON encoding.
	Format string `yaml:"format" json:"format"`
	// Level is the minimum severity to emit.
	Level string `yaml:"level" json:"level"`
	// GlobalKV contains fields attached to every record.
	GlobalKV map[string]any `yaml:"global_kv" json:"global_kv"`
}

func (c *Config) setDefaults() {
	if c.Format == "" {
		c.Format = defaultLogFormat
	}
	if c.Level == "" {
		c.Level = defaultLogLevel
	}
}

func (c *Config) validate() error {
	var errs []error
	if c.Format != FormatText && c.Format != FormatJSON {
		errs = append(errs, ErrInvalidLogFormat)
	}
	if c.Level != "" && c.Level != "debug" && c.Level != "info" && c.Level != "warn" && c.Level != "error" {
		errs = append(errs, ErrInvalidLogLevel)
	}
	return errors.Join(errs...)
}
