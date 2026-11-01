package logs

import "errors"

// Config validation errors.
var (
	ErrInvalidLogFormat = errors.New("logs: format must be \"text\" or \"json\"")
	ErrInvalidLogLevel  = errors.New(
		"logs: level must be \"debug\", \"info\", \"warn\", or \"error\"")
)
