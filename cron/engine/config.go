package engine

import (
	"errors"
	"time"
)

const (
	// DefaultGlobalTimeout is applied when neither job nor engine specifies a timeout.
	DefaultGlobalTimeout = 1 * time.Minute

	// DefaultShutdownTimeout is the maximum duration Engine.Stop waits for in-flight jobs.
	DefaultShutdownTimeout = 10 * time.Second

	// FixedLockTTLBuffer is the fixed duration added to job timeout for distributed lock TTL.
	FixedLockTTLBuffer = 15 * time.Second
)

// Config holds engine-level configurations.
type Config struct {
	GlobalTimeout   time.Duration `yaml:"global_timeout" json:"global_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`
}

// SetDefaults returns a copy of Config with unset fields populated with defaults.
func (c Config) SetDefaults() Config {
	if c.GlobalTimeout <= 0 {
		c.GlobalTimeout = DefaultGlobalTimeout
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = DefaultShutdownTimeout
	}
	return c
}

// Validate verifies that the configuration values are valid.
func (c Config) Validate() error {
	if c.GlobalTimeout < 0 {
		return errors.New("cron: global_timeout must not be negative")
	}
	if c.ShutdownTimeout < 0 {
		return errors.New("cron: shutdown_timeout must not be negative")
	}
	return nil
}
