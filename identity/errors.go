package identity

import "errors"

// ErrName reports an identity that has no service name.
var ErrName = errors.New("identity: service name is required")
