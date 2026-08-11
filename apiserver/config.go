package apiserver

import (
	"fmt"
	"net"
	"time"
)

// Defaults applied by Config.setDefaults when the corresponding field is unset.
const (
	defaultAddr            = ":3000"
	defaultReadTimeout     = 30 * time.Second
	defaultWriteTimeout    = 30 * time.Second
	defaultShutdownTimeout = 10 * time.Second
)

// Config holds server-level configuration.
type Config struct {
	Addr            string        `yaml:"addr" json:"addr"`
	ReadTimeout     time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout" json:"write_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`
}

// setDefaults fills in unset optional fields.
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

// validate returns an error when the config is unusable.
func (c *Config) validate() error {
	if c.Addr == "" {
		return fmt.Errorf("apiserver: addr must not be empty")
	}
	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		return fmt.Errorf("apiserver: addr must be host:port: %w", err)
	}
	if c.ReadTimeout < 0 {
		return fmt.Errorf("apiserver: read_timeout must not be negative")
	}
	if c.WriteTimeout < 0 {
		return fmt.Errorf("apiserver: write_timeout must not be negative")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("apiserver: shutdown_timeout must be positive")
	}
	return nil
}
