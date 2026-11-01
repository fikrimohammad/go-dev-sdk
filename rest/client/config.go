package client

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"time"
)

// Defaults applied by Config.setDefaults when the corresponding field is zero.
const (
	defaultTimeout        = 30 * time.Second
	defaultAttemptTimeout = 10 * time.Second
	defaultMaxAttempts    = 3
	defaultBackoffBase    = 100 * time.Millisecond
	defaultBackoffMax     = 2 * time.Second
	defaultMaxRetryAfter  = 10 * time.Second
	defaultDialTimeout    = 5 * time.Second
)

// Config holds client settings. The zero value is valid: every field has a
// default.
type Config struct {
	// Timeout bounds a whole call, retries included, when its context has no
	// deadline. Defaults to 30s.
	Timeout time.Duration `yaml:"timeout" json:"timeout"`
	// AttemptTimeout bounds each attempt. Defaults to 10s.
	AttemptTimeout time.Duration `yaml:"attempt_timeout" json:"attempt_timeout"`
	// MaxAttempts is the number of attempts, including the first. Defaults to
	// 3. A value of 1 disables retry.
	MaxAttempts int `yaml:"max_attempts" json:"max_attempts"`
	// BackoffBase is the base of the exponential delay between attempts.
	// Defaults to 100ms. It must not exceed BackoffMax.
	BackoffBase time.Duration `yaml:"backoff_base" json:"backoff_base"`
	// BackoffMax caps the delay between attempts. Defaults to 2s.
	BackoffMax time.Duration `yaml:"backoff_max" json:"backoff_max"`
	// MaxRetryAfter is the longest Retry-After delay the client honours. A
	// longer one stops retrying, and the last response is returned. Defaults
	// to 10s.
	MaxRetryAfter time.Duration `yaml:"max_retry_after" json:"max_retry_after"`
	// DialTimeout bounds establishing a TCP connection. Defaults to 5s.
	DialTimeout time.Duration `yaml:"dial_timeout" json:"dial_timeout"`
	// Proxy is the egress proxy URL, for example http://egress:3128. Its
	// scheme must be http, because an https proxy breaks the TLS ServerName of
	// the vendor. Credentials in the URL go only to the proxy. Empty sends
	// requests directly.
	Proxy string `yaml:"proxy" json:"proxy"`
	// TLS is the TLS configuration for HTTPS requests, for example the client
	// certificate for mTLS. Nil uses the system defaults. It is not read from
	// or written to YAML or JSON.
	TLS *tls.Config `yaml:"-" json:"-"`
}

// setDefaults replaces each zero field with its default.
func (c *Config) setDefaults() {
	if c.Timeout == 0 {
		c.Timeout = defaultTimeout
	}
	if c.AttemptTimeout == 0 {
		c.AttemptTimeout = defaultAttemptTimeout
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.BackoffBase == 0 {
		c.BackoffBase = defaultBackoffBase
	}
	if c.BackoffMax == 0 {
		c.BackoffMax = defaultBackoffMax
	}
	if c.MaxRetryAfter == 0 {
		c.MaxRetryAfter = defaultMaxRetryAfter
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = defaultDialTimeout
	}
}

// validate reports whether the defaulted config is usable. Zero values were
// already replaced by defaults, so a zero MaxAttempts never reaches this check.
func (c *Config) validate() error {
	durations := []struct {
		name  string
		value time.Duration
	}{
		{"timeout", c.Timeout},
		{"attempt_timeout", c.AttemptTimeout},
		{"backoff_base", c.BackoffBase},
		{"backoff_max", c.BackoffMax},
		{"max_retry_after", c.MaxRetryAfter},
		{"dial_timeout", c.DialTimeout},
	}
	for _, d := range durations {
		if d.value < 0 {
			return fmt.Errorf("rest/client: %s %v must not be negative", d.name, d.value)
		}
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("rest/client: max_attempts %d must be at least 1", c.MaxAttempts)
	}
	if c.BackoffBase > c.BackoffMax {
		return fmt.Errorf("rest/client: backoff_base %v must not exceed backoff_max %v", c.BackoffBase, c.BackoffMax)
	}
	if c.Proxy != "" {
		return validateProxy(c.Proxy)
	}
	return nil
}

// validateProxy reports whether proxy is an http URL with a host. The messages
// leave out the URL itself, because it may carry credentials.
func validateProxy(proxy string) error {
	u, err := url.Parse(proxy)
	if err != nil {
		return errors.New("rest/client: proxy must be a valid URL")
	}
	if u.Scheme != "http" {
		return errors.New("rest/client: proxy scheme must be http")
	}
	if u.Host == "" {
		return errors.New("rest/client: proxy must have a host")
	}
	return nil
}
