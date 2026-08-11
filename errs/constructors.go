package errs

import (
	"errors"
	"runtime"
)

const maxStackDepth = 32

// New creates a new Error with the given code and message.
// Stack trace is eagerly captured at the call site.
func New(code Code, message string) *Error {
	return &Error{
		code:    code,
		message: message,
		stack:   captureStack(1),
	}
}

// Wrap creates a new Error wrapping a cause with the given code and message.
// Stack trace is eagerly captured at the call site.
func Wrap(code Code, message string, cause error) *Error {
	return &Error{
		code:    code,
		message: message,
		cause:   cause,
		stack:   captureStack(1),
	}
}

// AsError extracts an *Error from an error chain.
// If err is nil, returns nil. If err wraps an *Error, returns that *Error.
// If err is a plain error, wraps it with Internal code.
func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return &Error{
		code:    Internal,
		message: err.Error(),
		cause:   err,
		stack:   captureStack(1),
	}
}

func captureStack(skip int) []uintptr {
	var pcs [maxStackDepth]uintptr
	n := runtime.Callers(skip+1, pcs[:])
	return pcs[:n]
}
