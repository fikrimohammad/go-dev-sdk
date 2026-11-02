package metrics

import "errors"

var (
	// ErrStopped indicates that the client no longer accepts measurements.
	ErrStopped = errors.New("metrics: client is stopped")
	// ErrInvalidMetricName indicates that a metric name is empty or invalid.
	ErrInvalidMetricName = errors.New("metrics: invalid metric name")
	// ErrInstrumentConflict indicates that a metric name was reused with
	// different metadata or a different instrument kind.
	ErrInstrumentConflict = errors.New("metrics: instrument conflict")
)
