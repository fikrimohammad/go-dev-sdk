package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/fikrimohammad/go-servicekit/errs"
	"github.com/fikrimohammad/go-servicekit/rest/internal/semconv"
)

// problemContentType is the media type of an RFC 9457 problem details body.
const problemContentType = "application/problem+json"

// problem is the RFC 9457 problem details body. It carries the title, status,
// and detail members, with code and request_id as extensions. It has no type
// member, which means about:blank.
type problem struct {
	// Title is the status text, for example "Bad Request". A status with no
	// standard text gets "Client Error" or "Server Error".
	Title string `json:"title"`
	// Status is the HTTP status code of the response.
	Status int `json:"status"`
	// Detail is the message of an errs error. It is set only for 4xx.
	Detail string `json:"detail,omitempty"`
	// Code is the errs code of the error. It is set only for 4xx and only when
	// non-zero.
	Code int `json:"code,omitempty"`
	// RequestID is the request ID of the response.
	RequestID string `json:"request_id"`
}

// Error writes an RFC 9457 problem response with the given status and aborts
// the rest of the chain. A status outside 400 to 599 becomes 500. A non-nil
// err is bound to the request with c.Error, so the access log and the
// telemetry see it. err may be nil.
//
// The body carries the message of err only when err has an *errs.Error in
// its chain and the status is 4xx. It then also carries the errs code when
// that code is non-zero. Text of other errors is never in the body, and
// neither are errs debug data, causes, metadata, or stack frames. A 5xx body
// has no detail and no code. A nil *errs.Error counts as no *errs.Error.
func Error(c *app.RequestContext, status int, err error) {
	if status < 400 || status > 599 {
		status = http.StatusInternalServerError
	}
	if err != nil {
		_ = c.Error(err)
	}
	var detail string
	var code int
	if status < 500 {
		if e, ok := errors.AsType[*errs.Error](err); ok && e != nil {
			detail = e.Message()
		}
		code, _ = semconv.ErrsCode(err)
	}
	writeProblem(c, status, detail, code)
	c.Abort()
}

// writeProblem writes the problem body for status. The request ID comes from
// the request context, where requestID stored it, and the X-Request-ID
// response header is set to the same value, so body and header always match.
func writeProblem(c *app.RequestContext, status int, detail string, code int) {
	id := requestIDOf(c)
	c.Response.Header.Set(requestIDHeader, id)
	// Marshal can't fail: every field is a string or an int.
	body, _ := json.Marshal(problem{
		Title:     problemTitle(status),
		Status:    status,
		Detail:    detail,
		Code:      code,
		RequestID: id,
	})
	c.Data(status, problemContentType, body)
}

// problemTitle returns the title for status. It is the standard status text,
// or "Client Error" or "Server Error" when the status has none, as with 499.
func problemTitle(status int) string {
	if title := http.StatusText(status); title != "" {
		return title
	}
	if status >= http.StatusInternalServerError {
		return "Server Error"
	}
	return "Client Error"
}

// noRoute answers a request that matches no route with a 404 problem.
func noRoute() app.HandlerFunc {
	return func(_ context.Context, c *app.RequestContext) {
		Error(c, http.StatusNotFound, nil)
	}
}

// noMethod answers a request whose method has no route with a 405 problem.
func noMethod() app.HandlerFunc {
	return func(_ context.Context, c *app.RequestContext) {
		Error(c, http.StatusMethodNotAllowed, nil)
	}
}
