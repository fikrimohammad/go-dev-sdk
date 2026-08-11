package errs

import (
	"fmt"
	"runtime"
)

// Error is the core error type carrying code, message, metadata, and stack trace.
type Error struct {
	code    Code
	message string
	cause   error
	stack   []uintptr
	meta    map[string]any
}

// Code returns the error code.
func (e *Error) Code() Code { return e.code }

// Message returns the error message.
func (e *Error) Message() string { return e.message }

// Cause returns the underlying error that caused this error.
func (e *Error) Cause() error { return e.cause }

// Debug returns the debug detail, or empty string if not set.
func (e *Error) Debug() string {
	if e.meta == nil {
		return ""
	}
	if v, ok := e.meta["debug"]; ok {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

// Meta returns a copy of the error metadata. Returns nil if no metadata is set.
func (e *Error) Meta() map[string]any {
	if e.meta == nil {
		return nil
	}
	m := make(map[string]any, len(e.meta))
	for k, v := range e.meta {
		m[k] = v
	}
	return m
}

// Error returns a concise error string: message: cause
func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %v", e.message, e.cause)
	}
	return e.message
}

// Unwrap returns the underlying cause, enabling errors.Is/As chain traversal.
func (e *Error) Unwrap() error {
	return e.cause
}

// Is reports whether any error in e's chain matches target.
// It matches when the target is an *Error and codes are equal.
func (e *Error) Is(target error) bool {
	if t, ok := target.(*Error); ok {
		return e.code == t.code
	}
	return false
}

// WithMeta returns a new Error with the given key-value metadata pair added.
// The original error is not modified.
func (e *Error) WithMeta(key string, value any) *Error {
	newErr := *e
	if newErr.meta == nil {
		newErr.meta = make(map[string]any)
	} else {
		m := make(map[string]any, len(newErr.meta)+1)
		for k, v := range newErr.meta {
			m[k] = v
		}
		newErr.meta = m
	}
	newErr.meta[key] = value
	return &newErr
}

// WithDebug returns a new Error with the debug detail set.
// Shorthand for WithMeta("debug", detail).
func (e *Error) WithDebug(detail string) *Error {
	return e.WithMeta("debug", detail)
}

// StackFrames returns formatted stack frames from the captured PC values.
func (e *Error) StackFrames() []string {
	if len(e.stack) == 0 {
		return nil
	}
	var frames []string
	runtimeFrames := runtime.CallersFrames(e.stack)
	for {
		frame, more := runtimeFrames.Next()
		frames = append(frames, fmt.Sprintf("%s\n    %s:%d", frame.Function, frame.File, frame.Line))
		if !more {
			break
		}
	}
	return frames
}
