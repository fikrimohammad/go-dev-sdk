package producer

import "errors"

// Producer-related sentinel errors. Callers can match them with errors.Is.
var (
	ErrProducerExists   = errors.New("producer: already registered for this topic")
	ErrProducerNotFound = errors.New("producer: not registered for this topic")
)

// Config-related sentinel errors. Callers can match them with errors.Is.
var (
	ErrEndpointsRequired = errors.New("producer: endpoints is required")
	ErrTopicRequired     = errors.New("producer: topic is required")
)
