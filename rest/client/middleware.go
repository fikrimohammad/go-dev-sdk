package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	hclient "github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/common/config"
	herrs "github.com/cloudwego/hertz/pkg/common/errors"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
	"github.com/fikrimohammad/go-dev-sdk/rest/internal/semconv"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otelsemconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// callCtxKey is the context key under which logging stores the state of a
// call. It is unexported, so only this package reads or changes it.
type callCtxKey struct{}

// call is the state of one client call, shared by all of its attempts.
// logging creates it, and deadline counts each attempt in it.
type call struct {
	// attempts is the number of attempts that reached the deadline step.
	attempts int
}

// withCall returns a copy of ctx that carries c.
func withCall(ctx context.Context, c *call) context.Context {
	return context.WithValue(ctx, callCtxKey{}, c)
}

// callFrom returns the call state stored in ctx, or nil when ctx has none. A
// team that calls an inner endpoint directly has no state, so callers must
// handle nil.
func callFrom(ctx context.Context) *call {
	c, _ := ctx.Value(callCtxKey{}).(*call)
	return c
}

// logging is the outermost step. It bounds the whole call, creates the call
// state, and writes one line with the final outcome of the call. The bound is
// the caller's Hertz request timeout, set by DoTimeout or WithRequestTimeout,
// when there is one. Otherwise it is cfg.Timeout, when ctx has no deadline.
// The line is at Warn when the call failed or the status is 400 or above, and
// at Debug otherwise.
//
// A URL that is not http or https, or has no host, fails here, before any
// attempt. A nil response is replaced by one that logging acquires, so retry
// can reset it between attempts.
func logging(cfg Config) hclient.Middleware {
	return func(next hclient.Endpoint) hclient.Endpoint {
		return func(ctx context.Context, req *protocol.Request, resp *protocol.Response) error {
			// Hertz ignores a ctx deadline once a request is in flight, so the
			// caller's Hertz timeout is turned into a ctx deadline here. The
			// deadline step then bounds each attempt by it.
			if d := req.Options().RequestTimeout(); d > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, d)
				defer cancel()
			} else if _, ok := ctx.Deadline(); !ok {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
				defer cancel()
			}
			if resp == nil {
				resp = protocol.AcquireResponse()
				defer protocol.ReleaseResponse(resp)
			}
			st := &call{}
			ctx = withCall(ctx, st)

			start := time.Now()
			err := checkURL(req)
			if err == nil {
				err = next(ctx, req, resp)
			}
			logCall(ctx, req, resp, err, st.attempts, time.Since(start))
			return err
		}
	}
}

// checkURL reports an error when req cannot be sent, because its scheme is not
// http or https, or it has no host.
func checkURL(req *protocol.Request) error {
	uri := req.URI()
	if s := string(uri.Scheme()); (s != "http" && s != "https") || len(uri.Host()) == 0 {
		return errors.New("rest/client: request URL must be http or https with a host")
	}
	return nil
}

// logCall writes the final line of a call. It logs the method, the vendor
// host without its port, and the path. It never logs the query, the userinfo,
// or the full URL, because those can carry secrets. The status is logged only
// when the call got a response.
func logCall(ctx context.Context, req *protocol.Request, resp *protocol.Response, err error, attempts int, elapsed time.Duration) {
	method, _ := semconv.Method(string(req.Method()))
	attrs := []any{
		slog.String(string(otelsemconv.HTTPRequestMethodKey), method),
		slog.String(string(otelsemconv.ServerAddressKey), serverAddress(string(req.URI().Host()))),
		slog.String(string(otelsemconv.URLPathKey), string(req.URI().Path())),
	}
	level := logs.Debug
	if err == nil {
		status := resp.StatusCode()
		attrs = append(attrs, slog.Int(string(otelsemconv.HTTPResponseStatusCodeKey), status))
		if status >= 400 {
			level = logs.Warn
		}
	} else {
		level = logs.Warn
		attrs = append(attrs, slog.String("error", err.Error()))
	}
	attrs = append(attrs,
		slog.Int("attempts", attempts),
		slog.Float64("duration", elapsed.Seconds()),
	)
	level(ctx, "request", attrs...)
}

// serverAddress returns the host of a request authority, without its port and
// without the brackets of a bare IPv6 host. An authority that is not a host
// and port is returned as it is, so an empty one stays empty.
func serverAddress(authority string) string {
	if host, _, err := net.SplitHostPort(authority); err == nil {
		return host
	}
	if strings.HasPrefix(authority, "[") && strings.HasSuffix(authority, "]") {
		return authority[1 : len(authority)-1]
	}
	return authority
}

// ErrInformationalResponse is returned when a vendor's final response has a
// 1xx status other than 100 Continue. Hertz returns such a response as final
// and leaves the real response on the connection, so the attempt fails and is
// not retried. Callers can test for it with errors.Is.
var ErrInformationalResponse = errors.New("rest/client: unexpected informational response")

// deadline is the innermost step, and it runs once per attempt. It stops
// without sending when ctx is done or no time is left. Otherwise it counts the
// attempt and sets the Hertz timeout of the attempt to the shorter of
// cfg.AttemptTimeout and the time left until the ctx deadline. Hertz ignores a
// ctx deadline once a request is in flight, so this timeout is what bounds the
// attempt. A Hertz timeout, or a connection deadline that expires while the
// attempt is in flight, is also wrapped as context.DeadlineExceeded.
//
// Hertz does not stop an attempt for ctx cancellation either, so a cancelled
// call still waits until the attempt ends. When ctx has ended by then, deadline
// returns the ctx error, wrapped with the Hertz error when there is one. The
// response of such an attempt is not returned.
//
// A 1xx response other than 100 Continue fails the attempt with
// ErrInformationalResponse, and c closes its idle connections. The connection
// that carried the response still holds the real response, which the next call
// would read. The close is best effort: it also closes the idle connections of
// other hosts, and a concurrent call can take the poisoned connection from the
// pool before the close, so the race stays until Hertz is fixed.
func deadline(cfg Config, c *hclient.Client) hclient.Middleware {
	return func(next hclient.Endpoint) hclient.Endpoint {
		return func(ctx context.Context, req *protocol.Request, resp *protocol.Response) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			d := cfg.AttemptTimeout
			if dl, ok := ctx.Deadline(); ok {
				d = min(d, time.Until(dl))
			}
			if d <= 0 {
				return fmt.Errorf("rest/client: %w", context.DeadlineExceeded)
			}
			if c := callFrom(ctx); c != nil {
				c.attempts++
			}
			// Hertz keeps one options value per request and each attempt
			// overwrites the timeout and restarts its clock, so a retry does
			// not add up with the previous attempt. The caller's own timeout
			// is put back afterwards. Otherwise a reused request would keep
			// this attempt's timeout and bound its next call with it.
			caller := req.Options().RequestTimeout()
			req.SetOptions(config.WithRequestTimeout(d))
			err := next(ctx, req, resp)
			req.SetOptions(config.WithRequestTimeout(caller))
			if err == nil {
				if status := resp.StatusCode(); status >= 100 && status < 200 && status != http.StatusContinue {
					c.CloseIdleConnections()
					return fmt.Errorf("%w %d", ErrInformationalResponse, status)
				}
			}
			if cerr := ctx.Err(); cerr != nil {
				if err != nil {
					return fmt.Errorf("rest/client: %w: %w", cerr, err) //nolint:revive // the wrap puts the ctx error first, then the Hertz error, so both errors.Is targets match
				}
				return cerr
			}
			if errors.Is(err, herrs.ErrTimeout) || errors.Is(err, os.ErrDeadlineExceeded) {
				return fmt.Errorf("rest/client: %w: %w", context.DeadlineExceeded, err) //nolint:revive // the spec fixes this wrap: the rest/client prefix, then the sentinel first, so both errors.Is targets match
			}
			return err
		}
	}
}

// retry is the step between logging and the later steps. It sends the request
// again when the result is safe to retry, for at most cfg.MaxAttempts attempts
// in total. A request is retried only when its method is GET, HEAD, OPTIONS,
// PUT, DELETE, or TRACE, or it has a non-empty Idempotency-Key header, and its
// body can be sent again. A body that is a stream, or a multipart file or
// field backed by a reader, cannot be sent twice.
//
// After an attempt, the call stops when ctx is done. A transport error is
// retried only when retryErr allows it, which includes a per-attempt timeout. A
// response is retried only when its status is 429, 502, 503, or 504, so a 500
// or a 4xx is returned as it is.
//
// The delay before retry n, with n = 1 for the first retry, is full jitter: a
// random duration below min(cfg.BackoffMax, cfg.BackoffBase << (n-1)). On 429
// or 503 with a Retry-After header, the header replaces the jitter. A Retry-After
// longer than cfg.MaxRetryAfter stops the call, and so does a delay that would
// pass the ctx deadline. Each stop returns the last result.
//
// Before a retry, retry resets resp. When the call ends, resp holds the final
// response.
func retry(cfg Config) hclient.Middleware {
	return func(next hclient.Endpoint) hclient.Endpoint {
		return func(ctx context.Context, req *protocol.Request, resp *protocol.Response) error {
			if !retryable(req) {
				return next(ctx, req, resp)
			}
			for attempt := 1; ; attempt++ {
				err := next(ctx, req, resp)
				if attempt >= cfg.MaxAttempts || ctx.Err() != nil {
					return err
				}
				delay, ok := retryDelay(ctx, cfg, attempt, err, resp)
				if !ok {
					return err
				}
				if err := wait(ctx, delay); err != nil {
					return err
				}
				// Reset also clears SkipBody, which the caller set, so it is
				// restored after the reset.
				skipBody := resp.SkipBody
				resp.Reset()
				resp.SkipBody = skipBody
			}
		}
	}
}

// retryable reports whether req may be sent again. A stream body and a
// reader-backed multipart part are never retried, because they can be read
// only once. Hertz reads a reader on every write, so a retry would send it
// empty.
func retryable(req *protocol.Request) bool {
	if req.IsBodyStream() {
		return false
	}
	if slices.ContainsFunc(req.MultipartFiles(), func(f *protocol.File) bool { return f.Reader != nil }) {
		return false
	}
	if slices.ContainsFunc(req.MultipartFields(), func(f *protocol.MultipartField) bool { return f.Reader != nil }) {
		return false
	}
	switch string(req.Method()) {
	case "GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE":
		return true
	}
	return req.Header.Get("Idempotency-Key") != ""
}

// retryDelay reports whether to retry after attempt, and how long to wait
// first. It returns false when the last result must be returned. A transport
// error is retried only when retryErr allows it. A response is retried only
// with a retryable status.
func retryDelay(ctx context.Context, cfg Config, attempt int, err error, resp *protocol.Response) (time.Duration, bool) {
	delay := backoff(cfg, attempt)
	if err != nil {
		if !retryErr(err) {
			return 0, false
		}
	} else {
		status := resp.StatusCode()
		if !slices.Contains(retryableStatuses, status) {
			return 0, false
		}
		if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
			if d, ok := parseRetryAfter(string(resp.Header.Peek("Retry-After"))); ok {
				if d > cfg.MaxRetryAfter {
					return 0, false
				}
				delay = d
			}
		}
	}
	if dl, ok := ctx.Deadline(); ok && time.Now().Add(delay).After(dl) {
		return 0, false
	}
	return delay, true
}

// retryErr reports whether a transport error may succeed when the request is
// sent again. It is a deny-list: the errors below are never retried, and every
// other error is. These errors cannot change on a second try: a certificate
// that does not verify, TLS to a server that does not speak TLS, a TLS alert
// from the peer such as a rejected client certificate, a DNS name that does
// not exist, a body over its limit, a protocol that is not supported, and an
// informational response, which the vendor would send again.
//
// Proxy CONNECT failures (407 or 403) are retried, because Hertz gives them
// the same error shape as a dropped connection, so they cannot be told apart
// here.
func retryErr(err error) bool {
	var certErr *tls.CertificateVerificationError
	var recordErr tls.RecordHeaderError
	var opErr *net.OpError
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, &certErr), errors.As(err, &recordErr):
		return false
	case errors.As(err, &opErr) && opErr.Op == "remote error":
		return false
	case errors.As(err, &dnsErr) && dnsErr.IsNotFound:
		return false
	case errors.Is(err, herrs.ErrBodyTooLarge), errors.Is(err, herrs.ErrNotSupportProtocol):
		return false
	case errors.Is(err, ErrInformationalResponse):
		return false
	}
	return true
}

// retryableStatuses are the response statuses that are retried. The request
// was not handled or the vendor is overloaded.
var retryableStatuses = []int{
	http.StatusTooManyRequests,
	http.StatusBadGateway,
	http.StatusServiceUnavailable,
	http.StatusGatewayTimeout,
}

// backoff returns the full-jitter delay before retry n, with n = 1 for the
// first retry. The delay is random in [0, min(BackoffMax, BackoffBase << (n-1))).
func backoff(cfg Config, n int) time.Duration {
	capped := cfg.BackoffMax
	// base << shift is at most BackoffMax exactly when base <= BackoffMax >>
	// shift, so the test never shifts a value past its width. For a shift of
	// 64 or more, the right side is zero and the cap is BackoffMax.
	if shift := n - 1; cfg.BackoffBase <= cfg.BackoffMax>>shift {
		capped = cfg.BackoffBase << shift
	}
	return rand.N(capped) //nolint:gosec // G404: jitter only spreads retries, it is not a secret, so math/rand is enough
}

// maxRetryAfterSeconds is the largest number of seconds that fits in a
// time.Duration. A larger Retry-After is clamped to it, so the conversion
// cannot overflow.
const maxRetryAfterSeconds = math.MaxInt64 / int64(time.Second)

// parseRetryAfter reads a Retry-After value, which is either a number of seconds
// or an HTTP date. A seconds value too large for an int counts as longer than
// any cap, so it is clamped to the largest duration. It reports false when the
// value is negative or is not one of those forms, so the caller falls back to
// jitter.
func parseRetryAfter(v string) (time.Duration, bool) {
	secs, err := strconv.Atoi(v)
	if err == nil || errors.Is(err, strconv.ErrRange) {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(min(int64(secs), maxRetryAfterSeconds)) * time.Second, true
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return 0, false
	}
	return max(time.Until(t), 0), true
}

// wait blocks for d, or until ctx is done. It returns the ctx error in the
// second case. It uses a timer and never sleeps, so a cancelled call returns
// at once.
func wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		t.Stop()
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// tracerScope is the instrumentation scope of the client spans.
const tracerScope = "rest.client"

// tracing records one client span for each attempt. The span starts from the
// caller's ctx, so it is a child of any span the caller has open. Nothing is
// injected into the request: no traceparent, tracestate, or baggage, and no
// request ID. Request headers are never recorded, because Hertz may have set
// Proxy-Authorization on them.
//
// The span has the attributes known before the attempt, and then the outcome.
// http.request.resend_count is the number of attempts already sent in this
// call, so it is set from the second attempt on. A failed attempt, or a
// response with a status of 400 or above, sets the span status to Error with
// no description.
func tracing(tc tracer.Client) hclient.Middleware {
	// A nil tc means the OTel global tracer provider, looked up per call so
	// a later otel.SetTracerProvider still applies.
	var fixed trace.Tracer
	if tc != nil {
		fixed = tc.Tracer(tracerScope)
	}
	return func(next hclient.Endpoint) hclient.Endpoint {
		return func(ctx context.Context, req *protocol.Request, resp *protocol.Response) error {
			t := fixed
			if t == nil {
				t = tracer.Tracer(tracerScope)
			}
			method, original := semconv.Method(string(req.Method()))
			uri := req.URI()
			attrs := []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String(method),
				otelsemconv.ServerAddressKey.String(serverAddress(string(uri.Host()))),
				otelsemconv.ServerPortKey.Int(serverPort(uri)),
				otelsemconv.URLFullKey.String(redactURL(uri)),
			}
			if original != "" {
				attrs = append(attrs, otelsemconv.HTTPRequestMethodOriginalKey.String(original))
			}
			if c := callFrom(ctx); c != nil && c.attempts > 0 {
				attrs = append(attrs, otelsemconv.HTTPRequestResendCountKey.Int(c.attempts))
			}
			spanCtx, span := t.Start(ctx, semconv.SpanName(method, ""),
				trace.WithSpanKind(trace.SpanKindClient),
				trace.WithAttributes(attrs...),
			)
			defer span.End()

			err := next(spanCtx, req, resp)
			o := attemptOutcome(resp, err)
			var end []attribute.KeyValue
			if o.status != 0 {
				end = append(end, otelsemconv.HTTPResponseStatusCodeKey.Int(o.status))
			}
			if o.version != "" {
				end = append(end, otelsemconv.NetworkProtocolVersionKey.String(o.version))
			}
			if o.errType != "" {
				end = append(end, otelsemconv.ErrorTypeKey.String(o.errType))
				span.SetStatus(codes.Error, "")
			}
			span.SetAttributes(end...)
			return err
		}
	}
}

// measure records http.client.request.duration once per attempt, so each
// retry is measured on its own. The attributes are low-cardinality: the
// method, the vendor host and port, the scheme, and the outcome. The URL and
// its query never go into a metric. Metric errors are ignored, because
// telemetry must never fail a call.
func measure(mc metrics.Client) hclient.Middleware {
	return func(next hclient.Endpoint) hclient.Endpoint {
		return func(ctx context.Context, req *protocol.Request, resp *protocol.Response) error {
			// A nil mc means the OTel global meter provider, looked up per attempt
			// so a later otel.SetMeterProvider still applies.
			m := mc
			if m == nil {
				m = metrics.Default()
			}
			start := time.Now()
			err := next(ctx, req, resp)
			elapsed := time.Since(start).Seconds()

			method, _ := semconv.Method(string(req.Method()))
			uri := req.URI()
			o := attemptOutcome(resp, err)
			attrs := map[string]any{
				string(otelsemconv.HTTPRequestMethodKey): method,
				string(otelsemconv.ServerAddressKey):     serverAddress(string(uri.Host())),
				string(otelsemconv.ServerPortKey):        serverPort(uri),
				string(otelsemconv.URLSchemeKey):         semconv.Scheme(string(uri.Scheme())),
			}
			if o.status != 0 {
				attrs[string(otelsemconv.HTTPResponseStatusCodeKey)] = o.status
			}
			if o.version != "" {
				attrs[string(otelsemconv.NetworkProtocolVersionKey)] = o.version
			}
			if o.errType != "" {
				attrs[string(otelsemconv.ErrorTypeKey)] = o.errType
			}
			_ = m.Record(ctx, semconv.ClientRequestDuration, elapsed, attrs) // telemetry errors never fail the call
			return err
		}
	}
}

// outcome is what one attempt produced, as telemetry records it. A response
// that arrived gives its status and protocol. A failed attempt has no response,
// so it gives only its error.type.
type outcome struct {
	status  int    // response status, zero when no response arrived
	version string // network.protocol.version of the response, empty when unknown
	errType string // error.type, empty when the attempt succeeded
}

// attemptOutcome returns the outcome of an attempt from the response and the
// error that next returned. resp is read only when err is nil.
func attemptOutcome(resp *protocol.Response, err error) outcome {
	if err != nil {
		return outcome{errType: semconv.ClientErrorType(0, err)}
	}
	status := resp.StatusCode()
	return outcome{
		status:  status,
		version: semconv.ProtocolVersion(string(resp.Header.GetProtocol())),
		errType: semconv.ClientErrorType(status, nil),
	}
}

// serverPort returns server.port for a request URI. It is the port written in
// the host, or the default of the scheme when none is written: 443 for https,
// and 80 for anything else. A port that is not a number counts as none.
func serverPort(uri *protocol.URI) int {
	if _, port, err := net.SplitHostPort(string(uri.Host())); err == nil {
		if n, err := strconv.Atoi(port); err == nil {
			return n
		}
	}
	if semconv.Scheme(string(uri.Scheme())) == "https" {
		return 443
	}
	return 80
}

// redactURL returns the URL of uri for url.full. The userinfo and the fragment
// are dropped. The path is the raw path of the request, which New sends
// unchanged, so url.full shows the path the vendor received. The query keeps
// each key in its original order, repeated keys included, and every value
// becomes REDACTED. Hertz lowercases the host when it parses a URL, so the host
// is lowercase here too. Host names are case-insensitive, so the URL is the
// same.
func redactURL(uri *protocol.URI) string {
	path := string(uri.PathOriginal())
	if path == "" {
		path = "/"
	}
	u := string(uri.Scheme()) + "://" + string(uri.Host()) + path
	if q := redactQuery(string(uri.QueryString())); q != "" {
		u += "?" + q
	}
	return u
}

// redactQuery returns a raw query with every value replaced by REDACTED. Each
// segment keeps its key as it was sent. A segment with no "=" is replaced whole
// by REDACTED, because that segment may be the token itself. Empty segments are
// dropped.
func redactQuery(raw string) string {
	var b strings.Builder
	for seg := range strings.SplitSeq(raw, "&") {
		if seg == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('&')
		}
		key, _, hasValue := strings.Cut(seg, "=")
		if !hasValue {
			b.WriteString("REDACTED")
			continue
		}
		b.WriteString(key)
		b.WriteString("=REDACTED")
	}
	return b.String()
}
