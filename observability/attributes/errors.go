package attributes

import "github.com/fikrimohammad/go-dev-sdk/identity"

// ErrMissingServiceName indicates that a identity.Info has no service name. It
// is the same value as identity.ErrName, so errors.Is matches either sentinel.
var ErrMissingServiceName = identity.ErrName
