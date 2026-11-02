package consumer

import (
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/fikrimohammad/go-servicekit/nats/internal/wire"
)

// Defaults applied by Config.SetDefaults when the corresponding field is unset.
const (
	// DefaultTimeout is the AckWait used when Config.Timeout is unset.
	DefaultTimeout = 60 * time.Second
	// DefaultMaxRetryAttempts is the MaxDeliver used when Config.MaxRetryAttempts
	// is unset.
	DefaultMaxRetryAttempts = 5
	// DefaultMaxConcurrent is the number of messages handled at once when
	// Config.MaxConcurrent is unset.
	DefaultMaxConcurrent = 10
	// DefaultTags is the tag that selects every message of the topic.
	DefaultTags = "*"
)

// MinTimeout is the smallest Config.Timeout the consumer accepts. It is the
// smallest AckWait the heartbeat can serve: the heartbeat runs every Timeout/2.
const MinTimeout = 100 * time.Millisecond

// ConsumeFromWhere values for Config.ConsumeFromWhere.
const (
	// ConsumeFromLast delivers only messages published after the durable consumer
	// is created (default).
	ConsumeFromLast = "last"
	// ConsumeFromFirst starts from the earliest available message.
	ConsumeFromFirst = "first"
	// ConsumeFromTimestamp starts from Config.ConsumeTimestamp.
	ConsumeFromTimestamp = "timestamp"
)

// Config describes a single NATS JetStream consumer instance.
type Config struct {
	// Endpoints is the list of NATS server addresses (e.g. "nats://127.0.0.1:4222" or "127.0.0.1:4222").
	Endpoints []string `yaml:"endpoints" json:"endpoints"`
	// Topic is the JetStream stream name the consumer reads from.
	Topic string `yaml:"topic" json:"topic"`
	// Group names the durable consumer, which is {appName}_{Topic}_{Group}. Each
	// group gets its own copy of every message.
	Group string `yaml:"group" json:"group"`
	// Tags selects the messages to deliver: each tag is a sub-subject of Topic,
	// and "*" selects every message of the topic. Defaults to ["*"].
	Tags []string `yaml:"consumption_tags" json:"consumption_tags"`
	// Timeout is the JetStream AckWait: how long the server waits for an ack
	// before it redelivers. While a handler runs, the consumer sends an InProgress
	// heartbeat every Timeout/2. It must be at least MinTimeout.
	Timeout time.Duration `yaml:"timeout" json:"timeout"`
	// HandlerTimeout bounds each handler call. Its context is canceled when the
	// deadline passes or when Shutdown starts. Zero uses Timeout.
	HandlerTimeout time.Duration `yaml:"handler_timeout" json:"handler_timeout"`
	// MaxRetryAttempts is the JetStream MaxDeliver: the number of deliveries
	// before the server stops redelivering a message. Defaults to
	// DefaultMaxRetryAttempts. There is no dead-letter queue.
	MaxRetryAttempts int `yaml:"max_retry_attempts" json:"max_retry_attempts"`
	// MaxConcurrent is the number of messages handled at once. Defaults to
	// DefaultMaxConcurrent.
	MaxConcurrent int `yaml:"max_concurrent" json:"max_concurrent"`

	// ConsumeFromWhere is "last" (default), "first", or "timestamp".
	ConsumeFromWhere string `yaml:"consume_from_where" json:"consume_from_where"`
	// ConsumeTimestamp is the RFC3339 start point, used only when
	// ConsumeFromWhere is "timestamp".
	ConsumeTimestamp string `yaml:"consume_timestamp" json:"consume_timestamp"`
	// RetryBackoff is how long a failed message waits before redelivery.
	// Zero uses the SDK default (immediate NAK).
	RetryBackoff time.Duration `yaml:"retry_backoff" json:"retry_backoff"`

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

	if c.HandlerTimeout == 0 {
		c.HandlerTimeout = c.Timeout
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

	if c.HandlerTimeout <= 0 {
		errs = append(errs, ErrInvalidHandlerTimeout)
	}

	if c.Timeout < MinTimeout {
		errs = append(errs, ErrInvalidTimeout)
	}

	return errors.Join(errs...)
}

// buildFilterSubjects converts Tags into full NATS subjects for the topic. Empty
// Tags or only the "*" wildcard select the data subjects of the topic, so schedule
// messages are never delivered.
func (c Config) buildFilterSubjects() []string {
	if len(c.Tags) == 0 || (len(c.Tags) == 1 && c.Tags[0] == DefaultTags) {
		return wire.DataSubjects(c.Topic)
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
		// Cap the unacked messages at the handler slots, so a message waiting for a
		// free slot is not redelivered after AckWait.
		MaxAckPending: c.MaxConcurrent,
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
		cfg.DeliverPolicy = jetstream.DeliverNewPolicy
	}

	return cfg
}
