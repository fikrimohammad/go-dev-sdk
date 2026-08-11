package consumer

import "errors"

// Consumer-related sentinel errors. Callers can match them with errors.Is.
var (
	ErrConsumerExists   = errors.New("consumer: already registered for this topic and group")
	ErrConsumerNotFound = errors.New("consumer: not registered for this topic and group")
)

// Config-related sentinel errors. Callers can match them with errors.Is.
var (
	ErrEndpointsRequired = errors.New("consumer: endpoints is required")
	ErrTopicRequired     = errors.New("consumer: topic is required")
	ErrGroupRequired     = errors.New("consumer: group is required")

	ErrInvalidConsumeModel     = errors.New("consumer: consume_model must be \"clustering\" or \"broadcasting\"")
	ErrInvalidConsumeFromWhere = errors.New("consumer: consume_from_where must be \"last\", \"first\", or \"timestamp\"")
	ErrInvalidConsumeTimestamp = errors.New("consumer: consume_timestamp must be an RFC3339 timestamp when consume_from_where is \"timestamp\"")

	ErrNegativeBatchMaxSize   = errors.New("consumer: consume_message_batch_max_size must not be negative")
	ErrNegativeCachedPerQueue = errors.New("consumer: max_cached_messages_per_queue must not be negative")
	ErrNegativeCachedPerTopic = errors.New("consumer: max_cached_messages_per_topic must not be negative")
	ErrNegativeRetryBackoff   = errors.New("consumer: retry_backoff must not be negative")
)
