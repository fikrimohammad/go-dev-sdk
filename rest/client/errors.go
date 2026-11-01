package client

import "errors"

// ErrInformationalResponse is returned when a vendor's final response has a
// 1xx status other than 100 Continue. Hertz returns such a response as final
// and leaves the real response on the connection, so the attempt fails and is
// not retried. Callers can test for it with errors.Is.
var ErrInformationalResponse = errors.New("rest/client: unexpected informational response")
