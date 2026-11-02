package wire

import "errors"

// ErrPanic marks an error made from a panic recovered while a message is
// handled. ErrorType classifies it as "panic".
var ErrPanic = errors.New("panic recovered")
