package producer

import (
	"errors"
	"strings"
	"time"
)

// DefaultTimeout is the Config.Timeout applied by Config.SetDefaults when it is
// unset.
const DefaultTimeout = 10 * time.Second

// Config describes a single NATS JetStream producer instance.
type Config struct {
	// Endpoints is the list of NATS server addresses (e.g. "nats://127.0.0.1:4222" or "127.0.0.1:4222").
	Endpoints []string `yaml:"endpoints" json:"endpoints"`
	// Topic is the JetStream stream name the producer is registered for.
	Topic string `yaml:"topic" json:"topic"`
	// Timeout is the send-message timeout. Defaults to DefaultTimeout.
	Timeout time.Duration `yaml:"timeout" json:"timeout"`

	// Token is the access token for token authentication.
	Token string `yaml:"token" json:"token"`
	// Username is the user for userinfo authentication, used with Password.
	Username string `yaml:"username" json:"username"`
	// Password is the password for userinfo authentication, used with Username.
	Password string `yaml:"password" json:"password"`
	// NKey is the path to an NKey user seed file.
	NKey string `yaml:"nkey" json:"nkey"`
	// CredentialsFile is the path to a NATS credentials file.
	CredentialsFile string `yaml:"credentials_file" json:"credentials_file"`
}

// SetDefaults fills the zero-valued optional fields with the package defaults
// and returns the updated copy.
func (c Config) SetDefaults() Config {
	if c.Timeout <= 0 {
		c.Timeout = DefaultTimeout
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
