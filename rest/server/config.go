package server

import (
	"errors"
	"fmt"
	"net"
	"time"
)

// Defaults applied by Config.SetDefaults when the corresponding field is zero.
const (
	defaultAddr            = ":3000"
	defaultReadTimeout     = 30 * time.Second
	defaultWriteTimeout    = 30 * time.Second
	defaultShutdownTimeout = 10 * time.Second
)

// Config holds server settings. The zero value is valid: every field has a
// default.
type Config struct {
	// Addr is the listen address in host:port form. Defaults to ":3000".
	Addr string `yaml:"addr" json:"addr"`
	// ReadTimeout bounds reading a request. Defaults to 30s.
	ReadTimeout time.Duration `yaml:"read_timeout" json:"read_timeout"`
	// WriteTimeout bounds writing a response. Defaults to 30s.
	WriteTimeout time.Duration `yaml:"write_timeout" json:"write_timeout"`
	// ShutdownTimeout bounds the graceful shutdown in Server.Run. Defaults
	// to 10s.
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`
}

// SetDefaults returns a copy of c with each zero field replaced by its default:
// Addr ":3000", ReadTimeout 30s, WriteTimeout 30s, and ShutdownTimeout 10s.
// Fields that are not zero are kept.
func (c Config) SetDefaults() Config {
	if c.Addr == "" {
		c.Addr = defaultAddr
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = defaultShutdownTimeout
	}
	return c
}

// Validate reports every problem with c, joined with errors.Join, or nil when c
// is usable. Call SetDefaults first; New does.
func (c Config) Validate() error {
	var errs []error
	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		errs = append(errs, fmt.Errorf("rest/server: addr must be host:port: %w", err))
	}
	if c.ReadTimeout < 0 {
		errs = append(errs, fmt.Errorf("rest/server: read_timeout %v must not be negative", c.ReadTimeout))
	}
	if c.WriteTimeout < 0 {
		errs = append(errs, fmt.Errorf("rest/server: write_timeout %v must not be negative", c.WriteTimeout))
	}
	if c.ShutdownTimeout < 0 {
		errs = append(errs, fmt.Errorf("rest/server: shutdown_timeout %v must not be negative", c.ShutdownTimeout))
	}
	return errors.Join(errs...)
}
