// Package semconv holds the HTTP telemetry contract shared by rest/server and
// rest/client: the instrument descriptors, method normalization, span naming,
// protocol-version parsing, and the error.type and errs.code rules. The
// package is internal to the rest module, so its exported names are a
// contract between those two packages and not public API. The rules follow
// the OpenTelemetry HTTP span and metric conventions at
// https://opentelemetry.io/docs/specs/semconv/http/.
package semconv

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/fikrimohammad/go-dev-sdk/errs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
)

// ErrsCodeKey is the custom attribute key that carries the code of a bound
// *errs.Error. It is named after the errs package because no semantic
// convention covers business error codes.
const ErrsCodeKey = "errs.code"

// Instrument descriptors for the HTTP metrics. Their names, units, and
// descriptions are part of the public telemetry contract.
var (
	// ServerRequestDuration describes the http.server.request.duration
	// histogram, recorded once per request by rest/server.
	ServerRequestDuration = metrics.Instrument{
		Name:        "http.server.request.duration",
		Unit:        "s",
		Description: "Duration of HTTP server requests.",
	}

	// ServerActiveRequests describes the http.server.active_requests
	// up-down counter, incremented when a request starts and decremented
	// when it ends.
	ServerActiveRequests = metrics.Instrument{
		Name:        "http.server.active_requests",
		Unit:        "{request}",
		Description: "Number of active HTTP server requests.",
	}

	// ClientRequestDuration describes the http.client.request.duration
	// histogram, recorded once per outbound attempt by rest/client.
	ClientRequestDuration = metrics.Instrument{
		Name:        "http.client.request.duration",
		Unit:        "s",
		Description: "Duration of HTTP client requests.",
	}
)

// Method normalizes a raw HTTP request method for telemetry. The known
// methods GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE, and PATCH
// are matched case-sensitively, as HTTP methods are, and returned unchanged
// with an empty original. Any other value, including the empty string, is
// returned as "_OTHER" with raw as original, so the caller can record the
// original on spans.
func Method(raw string) (method, original string) {
	switch raw {
	case "GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH":
		return raw, ""
	}
	return "_OTHER", raw
}

// SpanName returns the span name for an already-normalized method (see
// [Method]) and the matched route. It is "{method} {route}" when route is
// non-empty and "{method}" otherwise. For the "_OTHER" method it uses "HTTP"
// in place of the method, as the HTTP span convention requires.
func SpanName(method, route string) string {
	if method == "_OTHER" {
		method = "HTTP"
	}
	if route == "" {
		return method
	}
	return method + " " + route
}

// ProtocolVersion returns the network.protocol.version value for an HTTP
// protocol string, which is the part after "HTTP/". The semantic convention
// spells major-only versions without ".0", so "HTTP/1.1" gives "1.1", "HTTP/2"
// and "HTTP/2.0" give "2", and "HTTP/3" and "HTTP/3.0" give "3". It returns ""
// when proto does not start with "HTTP/" or has nothing after it.
func ProtocolVersion(proto string) string {
	version, ok := strings.CutPrefix(proto, "HTTP/")
	if !ok {
		return ""
	}
	switch version {
	case "2.0":
		return "2"
	case "3.0":
		return "3"
	}
	return version
}

// Scheme returns the url.scheme value for a raw scheme. It returns "https" for
// "https" in any letter case, and "http" for anything else. Hertz takes the
// scheme from an absolute-form request target, which a client controls, so
// every other value is folded into "http" to keep url.scheme low-cardinality.
func Scheme(raw string) string {
	if strings.EqualFold(raw, "https") {
		return "https"
	}
	return "http"
}

// ErrsCode returns the code of the first *errs.Error in err's chain and
// reports whether that code is non-zero. It returns false for a nil error,
// for an *errs.Error whose code is 0, and for a plain error that carries no
// *errs.Error. A nil *errs.Error inside an interface counts as no *errs.Error,
// because its code can't be read.
func ErrsCode(err error) (int, bool) {
	e, ok := errors.AsType[*errs.Error](err)
	if !ok || e == nil {
		return 0, false
	}
	code := e.Code()
	return code, code != 0
}

// ServerErrorType returns the error.type value for a server response with
// the given status. It returns "" for statuses below 500, since client errors
// are not error types on the server side. For 5xx it returns the status code
// as a string, as HTTP semconv requires, so "500" and "503". It never returns
// an errs code: errs codes share a number space with HTTP statuses, so a
// business code would be indistinguishable from a status. Business codes go
// in the errs.code attribute instead.
func ServerErrorType(status int) string {
	if status < 500 {
		return ""
	}
	return strconv.Itoa(status)
}

// ClientErrorType returns the error.type value for an outbound attempt. When
// err is non-nil it returns "timeout" for an error that matches
// context.DeadlineExceeded, "canceled" for one that matches
// context.Canceled, and "_OTHER" for anything else, regardless of status.
// When err is nil it returns the status code as a string for statuses of 400
// and above, and "" otherwise.
func ClientErrorType(status int, err error) string {
	switch {
	case err == nil:
		if status >= 400 {
			return strconv.Itoa(status)
		}
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "_OTHER"
	}
}
