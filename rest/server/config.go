package server

import (
	"fmt"
	"net"
	"time"
)

// Defaults applied by Config.setDefaults when the corresponding field is zero.
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

// setDefaults replaces each zero field with its default.
func (c *Config) setDefaults() {
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
}

// validate reports whether the defaulted config is usable. Zero timeouts
// were already replaced by defaults, so only negative values are rejected.
func (c *Config) validate() error {
	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		return fmt.Errorf("rest/server: addr must be host:port: %w", err)
	}
	if c.ReadTimeout < 0 {
		return fmt.Errorf("rest/server: read_timeout %v must not be negative", c.ReadTimeout)
	}
	if c.WriteTimeout < 0 {
		return fmt.Errorf("rest/server: write_timeout %v must not be negative", c.WriteTimeout)
	}
	if c.ShutdownTimeout < 0 {
		return fmt.Errorf("rest/server: shutdown_timeout %v must not be negative", c.ShutdownTimeout)
	}
	return nil
}
