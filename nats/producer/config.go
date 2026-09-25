package producer

import (
	"errors"
	"strings"
	"time"
)

// Defaults applied by Config.SetDefaults when the corresponding field is unset.
const (
	DefaultTimeout          = 10 * time.Second
	DefaultMaxRetryAttempts = 3
)

// Config describes a single NATS JetStream producer instance.
type Config struct {
	// Endpoints is the list of NATS server addresses (e.g. "nats://127.0.0.1:4222" or "127.0.0.1:4222").
	Endpoints []string `yaml:"endpoints" json:"endpoints"`
	// Topic is the JetStream stream name the producer is registered for.
	Topic string `yaml:"topic" json:"topic"`
	// Timeout is the send-message timeout. Defaults to DefaultTimeout.
	Timeout time.Duration `yaml:"timeout" json:"timeout"`
	// MaxRetryAttempts is the number of send retries. Defaults to DefaultMaxRetryAttempts.
	MaxRetryAttempts int `yaml:"max_retry_attempts" json:"max_retry_attempts"`

	// Optional authentication settings.
	Token           string `yaml:"token" json:"token"`
	Username        string `yaml:"username" json:"username"`
	Password        string `yaml:"password" json:"password"`
	NKey            string `yaml:"nkey" json:"nkey"`
	CredentialsFile string `yaml:"credentials_file" json:"credentials_file"`
}

// SetDefaults fills the zero-valued optional fields with the package defaults
// and returns the updated copy.
func (c Config) SetDefaults() Config {
	if c.Timeout <= 0 {
		c.Timeout = DefaultTimeout
	}
	if c.MaxRetryAttempts <= 0 {
		c.MaxRetryAttempts = DefaultMaxRetryAttempts
	}
	return c
}

// Validate reports configuration problems, such as missing endpoints or topic.
// It returns nil when the config is usable.
func (c Config) Validate() error {
	var errs []error
	if len(c.Endpoints) == 0 {
		errs = append(errs, ErrEndpointsRequired)
	}
	if c.Topic == "" {
		errs = append(errs, ErrTopicRequired)
	} else if strings.Contains(c.Topic, ".") {
		errs = append(errs, ErrInvalidTopic)
	}
	return errors.Join(errs...)
}
