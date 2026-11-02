package consumer

import "errors"

// Consumer-related sentinel errors. Callers can match them with errors.Is.
var (
	ErrConsumerExists = errors.New("consumer: already registered for this topic and group")
	// ErrAlreadyStarted is returned by Register after Start. Register first, then
	// Start.
	ErrAlreadyStarted = errors.New("consumer: cannot register after Start")
	ErrInvalidAppName = errors.New("consumer: service name must not contain dots")

	// ErrNoRetry marks a handler error that must not be retried. Wrap it, e.g.
	// fmt.Errorf("%w: bad payload", consumer.ErrNoRetry), to terminate a poison
	// message instead of redelivering it.
	ErrNoRetry = errors.New("consumer: do not retry")
)

// Config-related sentinel errors. Callers can match them with errors.Is.
var (
	ErrEndpointsRequired = errors.New("consumer: endpoints is required")
	ErrTopicRequired     = errors.New("consumer: topic is required")
	ErrGroupRequired     = errors.New("consumer: group is required")

	ErrInvalidConsumeFromWhere = errors.New("consumer: consume_from_where must be \"last\", \"first\", or \"timestamp\"")
	ErrInvalidConsumeTimestamp = errors.New("consumer: consume_timestamp must be an RFC3339 timestamp when consume_from_where is \"timestamp\"")
	ErrNegativeRetryBackoff    = errors.New("consumer: retry_backoff must not be negative")
	ErrInvalidHandlerTimeout   = errors.New("consumer: handler_timeout must be positive")

	ErrInvalidTopic = errors.New("consumer: topic must not contain dots")
	ErrInvalidGroup = errors.New("consumer: group must not contain dots")
	ErrInvalidTag   = errors.New("consumer: tag must not contain dots")
	ErrEmptyTag     = errors.New("consumer: tag must not be empty")
)
