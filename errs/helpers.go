package errs

import "errors"

// As extracts the first *Error from err's chain and reports whether it was found.
// If found, the *Error is stored in target. Usage:
//
//	var e *errs.Error
//	if errs.As(err, &e) {
//	    code := e.Code()
//	}
func As(err error, target **Error) bool {
	return errors.As(err, target)
}

// CodeFromError extracts the Code from an error chain.
// Returns Internal if the error is not an *Error.
func CodeFromError(err error) Code {
	if err == nil {
		return OK
	}
	var e *Error
	if errors.As(err, &e) {
		return e.code
	}
	return Internal
}
