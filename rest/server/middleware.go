package server

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/fikrimohammad/go-dev-sdk/errs"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
	"github.com/fikrimohammad/go-dev-sdk/rest/internal/semconv"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	otelsemconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// requestIDHeader is the header that carries the request ID in both
// directions.
const requestIDHeader = "X-Request-ID"

// requestIDHeaderKey is requestIDHeader in the lowercase form that
// http.request.header.<key> uses.
const requestIDHeaderKey = "x-request-id"

// requestIDCtxKey is the key under which requestID stores the request ID on
// the request context. Handlers can change the response headers, but not
// this value.
const requestIDCtxKey = "rest/server.request_id"

// tracerScope is the instrumentation scope of the server spans.
const tracerScope = "rest.server"

// validRequestID reports whether id is an incoming request ID that is safe to
// keep. It allows 1 to 64 characters from A-Z, a-z, 0-9, ".", "_", and "-",
// a set that can't break a log line or a header. nginx's $request_id (32 hex
// characters) matches.
func validRequestID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for i := range len(id) {
		c := id[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// requestID keeps a valid incoming X-Request-ID, or generates one with
// crypto/rand. It stores the ID in the context for [RequestIDFrom], in the
// app.RequestContext for requestIDOf, and in the response header.
func requestID() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id := string(c.Request.Header.Peek(requestIDHeader))
		if !validRequestID(id) {
			id = rand.Text()
		}
		c.Set(requestIDCtxKey, id)
		c.Response.Header.Set(requestIDHeader, id)
		c.Next(context.WithValue(ctx, requestIDKey{}, id))
	}
}

// requestIDOf returns the request ID that requestID stored on c. The response
// header can be changed by a handler, so the problem body and the header are
// always written from this value.
func requestIDOf(c *app.RequestContext) string {
	return c.GetString(requestIDCtxKey)
}

// tracing records the server span of each request. The incoming traceparent
// never becomes the parent: the span always starts a new trace, and a valid
// incoming span context is added as a link. Only traceparent is extracted,
// never baggage. The span context goes down the chain, so the handlers and
// recovery see the span, and the span ends when the chain returns.
func tracing(tc tracer.Client) app.HandlerFunc {
	// A nil tc means the package-level default, looked up per call so a
	// later tracer.SetDefault still applies.
	var fixed trace.Tracer
	if tc != nil {
		fixed = tc.Tracer(tracerScope)
	}
	prop := propagation.TraceContext{}
	return func(ctx context.Context, c *app.RequestContext) {
		t := fixed
		if t == nil {
			t = tracer.Tracer(tracerScope)
		}
		// The extracted context is used only for the link. The span starts
		// from ctx, so the request ID and any other values stay visible.
		incoming := trace.SpanContextFromContext(
			prop.Extract(ctx, requestHeaderCarrier{header: &c.Request.Header}),
		)
		method, original := semconv.Method(string(c.Request.Method()))

		opts := []trace.SpanStartOption{
			trace.WithNewRoot(),
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(startAttributes(ctx, c, method, original)...),
		}
		if incoming.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: incoming}))
		}
		spanCtx, span := t.Start(ctx, semconv.SpanName(method, c.FullPath()), opts...)
		defer span.End()

		c.Next(spanCtx)

		endAttributes(span, c)
	}
}

// startAttributes returns the request attributes that are known before the
// chain runs. Empty values are left out.
func startAttributes(ctx context.Context, c *app.RequestContext, method, original string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{otelsemconv.HTTPRequestMethodKey.String(method)}
	if original != "" {
		attrs = append(attrs, otelsemconv.HTTPRequestMethodOriginalKey.String(original))
	}
	if path := string(c.Request.URI().Path()); path != "" {
		attrs = append(attrs, otelsemconv.URLPathKey.String(path))
	}
	scheme := semconv.Scheme(string(c.Request.URI().Scheme()))
	attrs = append(attrs, otelsemconv.URLSchemeKey.String(scheme))
	if route := c.FullPath(); route != "" {
		attrs = append(attrs, otelsemconv.HTTPRouteKey.String(route))
	}
	attrs = append(attrs, serverAttributes(string(c.Request.Header.Host()))...)
	attrs = append(attrs, peerAttributes(c.RemoteAddr())...)
	if version := semconv.ProtocolVersion(c.Request.Header.GetProtocol()); version != "" {
		attrs = append(attrs, otelsemconv.NetworkProtocolVersionKey.String(version))
	}
	if ua := string(c.Request.Header.UserAgent()); ua != "" {
		attrs = append(attrs, otelsemconv.UserAgentOriginalKey.String(ua))
	}
	if id := RequestIDFrom(ctx); id != "" {
		attrs = append(attrs, otelsemconv.HTTPRequestHeader(requestIDHeaderKey, id))
	}
	return attrs
}

// serverAttributes returns server.address and server.port for the Host
// header. A host without a port gives only server.address, and the whole
// host is the address.
func serverAttributes(host string) []attribute.KeyValue {
	if host == "" {
		return nil
	}
	name, port, err := net.SplitHostPort(host)
	if err != nil {
		name, port = host, ""
	}
	var attrs []attribute.KeyValue
	if name != "" {
		attrs = append(attrs, otelsemconv.ServerAddressKey.String(name))
	}
	if n, err := strconv.Atoi(port); err == nil {
		attrs = append(attrs, otelsemconv.ServerPortKey.Int(n))
	}
	return attrs
}

// peerAttributes returns network.peer.address for the remote address. An
// address that isn't host:port gives nothing.
func peerAttributes(addr net.Addr) []attribute.KeyValue {
	if addr == nil {
		return nil
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil || host == "" {
		return nil
	}
	return []attribute.KeyValue{otelsemconv.NetworkPeerAddressKey.String(host)}
}

// endAttributes records the response attributes and the span status. The
// status code is always set. errs.code is set from the first bound error with
// a non-zero code, at any status. On 5xx the span status is Error with no
// description, and error.type is the server error type. Below 500 the span
// status stays Unset.
func endAttributes(span trace.Span, c *app.RequestContext) {
	status := c.Response.StatusCode()
	attrs := []attribute.KeyValue{otelsemconv.HTTPResponseStatusCodeKey.Int(status)}

	code, bound := boundErrsCode(c)
	if bound != nil {
		attrs = append(attrs, attribute.Int(semconv.ErrsCodeKey, code))
	}
	if status >= http.StatusInternalServerError {
		span.SetStatus(codes.Error, "")
		attrs = append(attrs, otelsemconv.ErrorTypeKey.String(semconv.ServerErrorType(status)))
	}
	span.SetAttributes(attrs...)
}

// boundErrsCode returns the code of the first error bound to c that has a
// non-zero errs code, and that error. Spans and metrics both use it, so they
// agree on errs.code. The error is nil when no bound error has a code, and
// then the code is 0.
func boundErrsCode(c *app.RequestContext) (int, error) {
	for _, e := range c.Errors {
		if code, ok := semconv.ErrsCode(e.Err); ok {
			return code, e.Err
		}
	}
	return 0, nil
}

// measure records the server metrics of each request. While the request runs,
// http.server.active_requests is up by one. The matching decrement carries the
// same attributes, so both changes land on one timeseries. When the request
// ends, http.server.request.duration gets one measurement with the final
// status. measure is outside logging and recovery, so a recovered panic counts
// as 500. Metric errors are ignored, because telemetry must never fail a
// request.
func measure(mc metrics.Client) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		// A nil mc means the package-level default, looked up per request so
		// a later metrics.SetDefault still applies.
		m := mc
		if m == nil {
			m = metrics.Default()
		}
		method, _ := semconv.Method(string(c.Request.Method()))
		scheme := semconv.Scheme(string(c.Request.URI().Scheme()))
		// active holds the two attributes that both changes of the counter
		// carry. The duration attributes are built separately below.
		active := make(map[string]any, 2)
		active[string(otelsemconv.HTTPRequestMethodKey)] = method
		active[string(otelsemconv.URLSchemeKey)] = scheme
		_ = m.AddUpDown(ctx, semconv.ServerActiveRequests, 1, active)
		defer func() { _ = m.AddUpDown(ctx, semconv.ServerActiveRequests, -1, active) }()

		start := time.Now()
		c.Next(ctx)

		status := c.Response.StatusCode()
		attrs := make(map[string]any, 7)
		attrs[string(otelsemconv.HTTPRequestMethodKey)] = method
		attrs[string(otelsemconv.URLSchemeKey)] = scheme
		attrs[string(otelsemconv.HTTPResponseStatusCodeKey)] = status
		if route := c.FullPath(); route != "" {
			attrs[string(otelsemconv.HTTPRouteKey)] = route
		}
		if version := semconv.ProtocolVersion(c.Request.Header.GetProtocol()); version != "" {
			attrs[string(otelsemconv.NetworkProtocolVersionKey)] = version
		}
		code, bound := boundErrsCode(c)
		if bound != nil {
			attrs[semconv.ErrsCodeKey] = code
		}
		if status >= http.StatusInternalServerError {
			attrs[string(otelsemconv.ErrorTypeKey)] = semconv.ServerErrorType(status)
		}
		_ = m.Record(ctx, semconv.ServerRequestDuration, time.Since(start).Seconds(), attrs)
	}
}

// logging writes one access-log line per request, after the chain has run, so
// it sees the final status. The level follows the status: Error for 5xx, Warn
// for 4xx, and Info otherwise. The span's trace_id is added by logs from ctx,
// so the line doesn't set it. Its request_id key becomes request.id.
func logging() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		c.Next(ctx)

		status := c.Response.StatusCode()
		method, _ := semconv.Method(string(c.Request.Method()))
		attrs := []any{
			slog.String(string(otelsemconv.HTTPRequestMethodKey), method),
			slog.String(string(otelsemconv.URLPathKey), string(c.Request.URI().Path())),
		}
		if route := c.FullPath(); route != "" {
			attrs = append(attrs, slog.String(string(otelsemconv.HTTPRouteKey), route))
		}
		attrs = append(attrs,
			slog.Int(string(otelsemconv.HTTPResponseStatusCodeKey), status),
			slog.Float64("duration", time.Since(start).Seconds()),
		)
		if id := requestIDOf(c); id != "" {
			attrs = append(attrs, slog.String("request_id", id))
		}
		if a, ok := boundErrorAttr(c); ok {
			attrs = append(attrs, a)
		}

		logFn := logs.Info
		switch {
		case status >= http.StatusInternalServerError:
			logFn = logs.Error
		case status >= http.StatusBadRequest:
			logFn = logs.Warn
		}
		logFn(ctx, "request", attrs...)
	}
}

// boundErrorAttr returns the access-log attribute for the errors bound to c:
// "error" for one error, and an "errors" group for several. Each error keeps
// the errs.SlogAttr structure. It returns false when no error is bound.
func boundErrorAttr(c *app.RequestContext) (slog.Attr, bool) {
	switch len(c.Errors) {
	case 0:
		return slog.Attr{}, false
	case 1:
		return slog.Attr{Key: "error", Value: boundErrorValue(c.Errors[0].Err)}, true
	}
	group := make([]slog.Attr, 0, len(c.Errors))
	for i, e := range c.Errors {
		group = append(group, slog.Attr{Key: strconv.Itoa(i), Value: boundErrorValue(e.Err)})
	}
	return slog.Attr{Key: "errors", Value: slog.GroupValue(group...)}, true
}

// boundErrorValue returns the log value of one bound error. errs.SlogAttr
// dereferences a nil *errs.Error, which would panic, so a nil one in the chain
// is logged as text instead.
func boundErrorValue(err error) slog.Value {
	if e, ok := errors.AsType[*errs.Error](err); ok && e == nil {
		return slog.StringValue("nil *errs.Error")
	}
	return errs.SlogAttr(err).Value
}

// requestHeaderCarrier adapts a Hertz request header to the OTel
// propagation.TextMapCarrier interface so the incoming trace context can be
// extracted from it.
type requestHeaderCarrier struct {
	header *protocol.RequestHeader
}

// Get returns the first value of the header key.
func (c requestHeaderCarrier) Get(key string) string {
	return c.header.Get(key)
}

// Set sets the header key to value.
func (c requestHeaderCarrier) Set(key, value string) {
	c.header.Set(key, value)
}

// Keys returns the names of all the header fields.
func (c requestHeaderCarrier) Keys() []string {
	var keys []string
	c.header.VisitAll(func(key, _ []byte) {
		keys = append(keys, string(key))
	})
	return keys
}

// recovery turns a panic in the rest of the chain into a 500 problem. It
// records an exception event on the active span, logs the panic with its
// stack, and aborts the chain. Recovery sits inside the outer layers, so they
// see the 500.
//
// A response without a hijack writer is replaced by the problem. Recovery
// discards whatever the handler set, keeps the Access-Control-* headers and the
// request ID, and writes the problem. A streamed or hijacked response may
// already have sent its headers or chunks, so no problem can follow them.
// Recovery sets the status to 500 for the outer layers, replaces the hijack
// writer so that Hertz closes the connection instead of sending the
// terminating chunk, and leaves the body as it is. The client then sees a
// truncated response, not a complete one. If a streaming writer was installed
// but nothing was written yet, the client gets a closed connection with no
// response.
//
// Recovery also clears any hijack handler that the handler set with
// c.Hijack. Hertz runs that handler on the connection after the chain
// returns, so leaving it in place would write to the connection after the
// 500.
func recovery() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			stack := string(debug.Stack())
			trace.SpanFromContext(ctx).AddEvent(otelsemconv.ExceptionEventName, trace.WithAttributes(
				otelsemconv.ExceptionTypeKey.String(fmt.Sprintf("%T", r)),
				otelsemconv.ExceptionMessageKey.String(fmt.Sprint(r)),
				otelsemconv.ExceptionStacktraceKey.String(stack),
			))
			c.SetHijackHandler(nil)
			if c.Response.GetHijackWriter() != nil {
				c.Response.SetStatusCode(http.StatusInternalServerError)
				c.Response.HijackWriter(abortedWriter{})
			} else {
				cors := corsHeaders(&c.Response.Header)
				// Reset clears the response headers, so the CORS headers are
				// copied first and added back after it. writeProblem sets the
				// request ID again.
				c.Response.Reset()
				for _, h := range cors {
					c.Response.Header.Add(h.key, h.value)
				}
				writeProblem(c, http.StatusInternalServerError, "", 0)
			}
			c.Abort()
			method, _ := semconv.Method(string(c.Request.Method()))
			logs.Error(ctx, "panic recovered",
				"panic", fmt.Sprint(r),
				"stack", stack,
				string(otelsemconv.HTTPRequestMethodKey), method,
				string(otelsemconv.URLPathKey), string(c.Request.URI().Path()),
			)
		}()
		c.Next(ctx)
	}
}

// headerField is one response header, as a key and a value.
type headerField struct {
	key, value string
}

// corsHeaders returns every response header whose name starts with
// Access-Control-, in any letter case, in the order the header holds them.
func corsHeaders(h *protocol.ResponseHeader) []headerField {
	var fields []headerField
	h.VisitAll(func(key, value []byte) {
		if strings.HasPrefix(strings.ToLower(string(key)), "access-control-") {
			fields = append(fields, headerField{key: string(key), value: string(value)})
		}
	})
	return fields
}

// errStreamAborted is the error that abortedWriter returns.
var errStreamAborted = errors.New("rest/server: streamed response aborted by panic")

// abortedWriter replaces the hijack writer of a streamed response that
// panicked. Its Finalize fails, so Hertz returns the error instead of writing
// the terminating chunk, and closes the connection.
type abortedWriter struct{}

// Write fails with errStreamAborted, because the response is already aborted.
func (abortedWriter) Write([]byte) (int, error) {
	return 0, errStreamAborted
}

// Flush fails with errStreamAborted, because the response is already aborted.
func (abortedWriter) Flush() error {
	return errStreamAborted
}

// Finalize fails with errStreamAborted, so Hertz doesn't send the terminating
// chunk and closes the connection.
func (abortedWriter) Finalize() error {
	return errStreamAborted
}
