package consumer

import (
	"errors"
	"strings"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

const (
	DefaultTimeout          = 60 * time.Second
	DefaultMaxRetryAttempts = 5
	DefaultMaxConcurrent    = 10
	DefaultTags             = "*"
)

// ConsumeModel values for Config.ConsumeModel.
const (
	// ConsumeModelClustering consumes messages once across the group (default).
	ConsumeModelClustering = "clustering"
	// ConsumeModelBroadcasting consumes every message on every instance.
	ConsumeModelBroadcasting = "broadcasting"
)

// ConsumeFromWhere values for Config.ConsumeFromWhere.
const (
	// ConsumeFromLast resumes from the last committed offset (default).
	ConsumeFromLast = "last"
	// ConsumeFromFirst starts from the earliest available message.
	ConsumeFromFirst = "first"
	// ConsumeFromTimestamp starts from Config.ConsumeTimestamp.
	ConsumeFromTimestamp = "timestamp"
)

// configTimestampLayout is the RocketMQ consume-timestamp format.
const configTimestampLayout = "20060102150405"

type Config struct {
	Endpoints        []string      `yaml:"endpoints" json:"endpoints"`
	Topic            string        `yaml:"topic" json:"topic"`
	Group            string        `yaml:"group" json:"group"`
	Tags             []string      `yaml:"consumption_tags" json:"consumption_tags"`
	Timeout          time.Duration `yaml:"timeout" json:"timeout"`
	MaxRetryAttempts int           `yaml:"max_retry_attempts" json:"max_retry_attempts"`
	MaxConcurrent    int           `yaml:"max_concurrent" json:"max_concurrent"`

	// ConsumeModel is "clustering" (default) or "broadcasting".
	ConsumeModel string `yaml:"consume_model" json:"consume_model"`
	// ConsumeFromWhere is "last" (default), "first", or "timestamp".
	ConsumeFromWhere string `yaml:"consume_from_where" json:"consume_from_where"`
	// ConsumeTimestamp is the RFC3339 start point, used only when
	// ConsumeFromWhere is "timestamp".
	ConsumeTimestamp string `yaml:"consume_timestamp" json:"consume_timestamp"`
	// ConsumeOrderly consumes messages in queue order when true.
	ConsumeOrderly bool `yaml:"consume_orderly" json:"consume_orderly"`

	// ConsumeMessageBatchMaxSize is the max messages delivered per callback.
	// Zero uses the SDK default.
	ConsumeMessageBatchMaxSize int `yaml:"consume_message_batch_max_size" json:"consume_message_batch_max_size"`
	// MaxCachedMessagesPerQueue caps the pending messages held per queue.
	// Zero uses the SDK default.
	MaxCachedMessagesPerQueue int64 `yaml:"max_cached_messages_per_queue" json:"max_cached_messages_per_queue"`
	// MaxCachedMessagesPerTopic caps the pending messages held per topic.
	// Zero uses the SDK default.
	MaxCachedMessagesPerTopic int `yaml:"max_cached_messages_per_topic" json:"max_cached_messages_per_topic"`
	// RetryBackoff is how long a failed message waits before redelivery.
	// Zero uses the SDK default.
	RetryBackoff time.Duration `yaml:"retry_backoff" json:"retry_backoff"`
}

// SetDefaults fills the zero-valued optional fields with the package defaults
// and returns the updated copy. Zero-valued flow-control settings are left at
// zero, which preserves the SDK defaults.
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

	if c.ConsumeModel == "" {
		c.ConsumeModel = ConsumeModelClustering
	}

	if c.ConsumeFromWhere == "" {
		c.ConsumeFromWhere = ConsumeFromLast
	}

	return c
}

// Validate reports configuration problems, such as missing endpoints, topic,
// group, or invalid behavior/flow-control settings. It returns nil when the
// config is usable.
func (c Config) Validate() error {
	var errs []error
	if len(c.Endpoints) == 0 {
		errs = append(errs, ErrEndpointsRequired)
	}
	if c.Topic == "" {
		errs = append(errs, ErrTopicRequired)
	}
	if c.Group == "" {
		errs = append(errs, ErrGroupRequired)
	}

	switch c.ConsumeModel {
	case ConsumeModelClustering, ConsumeModelBroadcasting:
	default:
		errs = append(errs, ErrInvalidConsumeModel)
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

	if c.ConsumeMessageBatchMaxSize < 0 {
		errs = append(errs, ErrNegativeBatchMaxSize)
	}
	if c.MaxCachedMessagesPerQueue < 0 {
		errs = append(errs, ErrNegativeCachedPerQueue)
	}
	if c.MaxCachedMessagesPerTopic < 0 {
		errs = append(errs, ErrNegativeCachedPerTopic)
	}
	if c.RetryBackoff < 0 {
		errs = append(errs, ErrNegativeRetryBackoff)
	}

	return errors.Join(errs...)
}

// messageModel maps the string ConsumeModel to the SDK enum.
func (c Config) messageModel() consumer.MessageModel {
	if c.ConsumeModel == ConsumeModelBroadcasting {
		return consumer.BroadCasting
	}
	return consumer.Clustering
}

// consumeFromWhere maps the string ConsumeFromWhere to the SDK enum.
func (c Config) consumeFromWhere() consumer.ConsumeFromWhere {
	switch c.ConsumeFromWhere {
	case ConsumeFromFirst:
		return consumer.ConsumeFromFirstOffset
	case ConsumeFromTimestamp:
		return consumer.ConsumeFromTimestamp
	default:
		return consumer.ConsumeFromLastOffset
	}
}

// consumeTimestamp returns the RocketMQ-formatted consume timestamp, or "" when
// the config does not start from a timestamp.
func (c Config) consumeTimestamp() string {
	if c.ConsumeFromWhere != ConsumeFromTimestamp {
		return ""
	}
	t, err := time.Parse(time.RFC3339, c.ConsumeTimestamp)
	if err != nil {
		return ""
	}
	return t.Format(configTimestampLayout)
}

// buildConsumerOptions assembles the SDK options from cfg, applying the
// flow-control knobs only when they are set.
func (c Config) buildConsumerOptions(groupName string) []consumer.Option {
	opts := []consumer.Option{
		consumer.WithGroupName(groupName),
		consumer.WithNsResolver(primitive.NewPassthroughResolver(c.Endpoints)),
		consumer.WithConsumeTimeout(c.Timeout),
		consumer.WithConsumeGoroutineNums(c.MaxConcurrent),
		consumer.WithMaxReconsumeTimes(int32(c.MaxRetryAttempts)),
		consumer.WithConsumerModel(c.messageModel()),
		consumer.WithConsumeFromWhere(c.consumeFromWhere()),
		consumer.WithConsumerOrder(c.ConsumeOrderly),
	}

	if ts := c.consumeTimestamp(); ts != "" {
		opts = append(opts, consumer.WithConsumeTimestamp(ts))
	}
	if c.ConsumeMessageBatchMaxSize > 0 {
		opts = append(opts, consumer.WithConsumeMessageBatchMaxSize(c.ConsumeMessageBatchMaxSize))
	}
	if c.MaxCachedMessagesPerQueue > 0 {
		opts = append(opts, consumer.WithPullThresholdForQueue(c.MaxCachedMessagesPerQueue))
	}
	if c.MaxCachedMessagesPerTopic > 0 {
		opts = append(opts, consumer.WithPullThresholdForTopic(c.MaxCachedMessagesPerTopic))
	}
	if c.RetryBackoff > 0 {
		opts = append(opts, consumer.WithSuspendCurrentQueueTimeMillis(c.RetryBackoff))
	}

	return opts
}

func (c Config) buildMessageSelectorConfig() consumer.MessageSelector {
	return consumer.MessageSelector{
		Type:       consumer.TAG,
		Expression: strings.Join(c.Tags, " || "),
	}
}
