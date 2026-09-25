package consumer

import (
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	DefaultTimeout          = 60 * time.Second
	DefaultMaxRetryAttempts = 5
	DefaultMaxConcurrent    = 10
	DefaultTags             = "*"
)

// ConsumeFromWhere values for Config.ConsumeFromWhere.
const (
	// ConsumeFromLast resumes from the last message in the stream (default).
	ConsumeFromLast = "last"
	// ConsumeFromFirst starts from the earliest available message.
	ConsumeFromFirst = "first"
	// ConsumeFromTimestamp starts from Config.ConsumeTimestamp.
	ConsumeFromTimestamp = "timestamp"
)

// Config describes a single NATS JetStream consumer instance.
type Config struct {
	Endpoints        []string      `yaml:"endpoints" json:"endpoints"`
	Topic            string        `yaml:"topic" json:"topic"`
	Group            string        `yaml:"group" json:"group"`
	Tags             []string      `yaml:"consumption_tags" json:"consumption_tags"`
	Timeout          time.Duration `yaml:"timeout" json:"timeout"`
	MaxRetryAttempts int           `yaml:"max_retry_attempts" json:"max_retry_attempts"`
	MaxConcurrent    int           `yaml:"max_concurrent" json:"max_concurrent"`

	// ConsumeFromWhere is "last" (default), "first", or "timestamp".
	ConsumeFromWhere string `yaml:"consume_from_where" json:"consume_from_where"`
	// ConsumeTimestamp is the RFC3339 start point, used only when
	// ConsumeFromWhere is "timestamp".
	ConsumeTimestamp string `yaml:"consume_timestamp" json:"consume_timestamp"`
	// RetryBackoff is how long a failed message waits before redelivery.
	// Zero uses the SDK default (immediate NAK).
	RetryBackoff time.Duration `yaml:"retry_backoff" json:"retry_backoff"`

	// Optional authentication parameters.
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

	if c.MaxConcurrent <= 0 {
		c.MaxConcurrent = DefaultMaxConcurrent
	}

	if len(c.Tags) == 0 {
		c.Tags = []string{DefaultTags}
	}

	if c.ConsumeFromWhere == "" {
		c.ConsumeFromWhere = ConsumeFromLast
	}

	return c
}

// Validate reports configuration problems, such as missing endpoints, topic,
// group, or invalid behavior settings. It returns nil when the config is usable.
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
	if c.Group == "" {
		errs = append(errs, ErrGroupRequired)
	} else if strings.Contains(c.Group, ".") {
		errs = append(errs, ErrInvalidGroup)
	}
	for _, tag := range c.Tags {
		if tag == "" {
			errs = append(errs, ErrEmptyTag)
			break
		}
		if strings.Contains(tag, ".") {
			errs = append(errs, ErrInvalidTag)
			break
		}
	}

	switch c.ConsumeFromWhere {
	case ConsumeFromLast, ConsumeFromFirst, ConsumeFromTimestamp:
	default:
		errs = append(errs, ErrInvalidConsumeFromWhere)
	}

	if c.ConsumeFromWhere == ConsumeFromTimestamp {
		if _, err := time.Parse(time.RFC3339, c.ConsumeTimestamp); err != nil {
			errs = append(errs, ErrInvalidConsumeTimestamp)
		}
	}

	if c.RetryBackoff < 0 {
		errs = append(errs, ErrNegativeRetryBackoff)
	}

	return errors.Join(errs...)
}

// buildFilterSubjects converts Tags into full NATS subjects for the topic.
func (c Config) buildFilterSubjects() []string {
	if len(c.Tags) == 0 || (len(c.Tags) == 1 && c.Tags[0] == DefaultTags) {
		return nil
	}

	var subjects []string
	for _, tag := range c.Tags {
		switch tag {
		case DefaultTags:
			subjects = append(subjects, c.Topic+".>")
		case "", c.Topic:
			subjects = append(subjects, c.Topic)
		default:
			subjects = append(subjects, c.Topic+"."+tag)
		}
	}
	return subjects
}

// buildConsumerConfig assembles the SDK consumer configuration from cfg.
func (c Config) buildConsumerConfig(durableName string) jetstream.ConsumerConfig {
	cfg := jetstream.ConsumerConfig{
		Durable:    durableName,
		Name:       durableName,
		AckWait:    c.Timeout,
		MaxDeliver: c.MaxRetryAttempts,
	}

	filters := c.buildFilterSubjects()
	if len(filters) == 1 {
		cfg.FilterSubject = filters[0]
	} else if len(filters) > 1 {
		cfg.FilterSubjects = filters
	}

	switch c.ConsumeFromWhere {
	case ConsumeFromFirst:
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	case ConsumeFromTimestamp:
		cfg.DeliverPolicy = jetstream.DeliverByStartTimePolicy
		if t, err := time.Parse(time.RFC3339, c.ConsumeTimestamp); err == nil {
			cfg.OptStartTime = &t
		}
	default:
		cfg.DeliverPolicy = jetstream.DeliverLastPolicy
	}

	return cfg
}
