// Package errs provides structured errors for Go services.
//
// An [Error] carries an integer code, a message, an optional cause, metadata
// added with [Error.WithMeta], a debug detail added with [Error.WithDebug], and
// a stack trace captured when the error is constructed. Build errors with
// [New], [Wrap], or [AsError]. [AsError] returns the *Error found in an error
// chain, or wraps a plain error with code 0.
//
// [errors.Is] matches an *Error by non-zero code. Errors with code 0 (unknown)
// never match by code.
//
// [Error.LogValue] and [SlogAttr] produce structured slog output with code,
// message, debug, meta, cause, and stack fields.
package errs
