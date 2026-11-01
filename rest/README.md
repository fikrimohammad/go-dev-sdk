# rest

Core HTTP server and client for gateway services, built on
[Hertz](https://github.com/cloudwego/hertz). Safe and observable by default.
The server runs a fixed middleware stack: every request gets a request ID, a
server span, HTTP metrics, an access-log line, and panic recovery, and every
error response uses RFC 9457 problem+json. The client sends outbound vendor
calls with safe retries, deadlines that hold, and the same telemetry. It never
sends trace context, request IDs, or baggage to a vendor.

```bash
go get github.com/fikrimohammad/go-dev-sdk/rest
```

Packages:

- `github.com/fikrimohammad/go-dev-sdk/rest/server` - the HTTP server (documented below).
- `github.com/fikrimohammad/go-dev-sdk/rest/client` - the HTTP client (documented below).

## Server

### Quick start

Build the observability clients from the service identity, then create the
server and register routes:

```go
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/fikrimohammad/go-dev-sdk/errs"
	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
	"github.com/fikrimohammad/go-dev-sdk/rest/server"
	"go.opentelemetry.io/otel"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	info := identity.Default()

	tc, err := tracer.New(ctx, info, tracer.Config{Endpoint: "collector:4317", Insecure: true})
	if err != nil {
		panic(err)
	}
	tracer.SetGlobal(tc) // provider + W3C TraceContext/Baggage propagator

	mc, err := metrics.New(ctx, info, metrics.Config{Endpoint: "collector:4317", Insecure: true})
	if err != nil {
		panic(err)
	}
	otel.SetMeterProvider(mc.Provider())

	l, err := logs.New(info, logs.Config{Format: "json"})
	if err != nil {
		panic(err)
	}
	logs.SetDefault(l)

	srv, err := server.New(server.Config{Addr: ":8080"})
	if err != nil {
		panic(err)
	}

	srv.Hertz().POST("/orders", func(ctx context.Context, c *app.RequestContext) {
		if c.Query("amount") == "" {
			server.Error(c, http.StatusBadRequest, errs.New(1001, "invalid amount"))
			return
		}
		c.JSON(http.StatusCreated, map[string]string{"request_id": server.RequestIDFrom(ctx)})
	})

	if err := srv.Run(ctx); err != nil {
		logs.Error(ctx, "server stopped", "error", err)
	}

	// Flush telemetry after Run returns. The signal context is already done,
	// so use a fresh one.
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tc.Stop(flushCtx)
	_ = mc.Stop(flushCtx)
}
```

`server.New` returns an error for an invalid `Config`. It reports through
`metrics.Default()` and `tracer.Default()`, read on every request, so set the
defaults at startup as above. To use other clients, pass
`server.WithMetrics(mc)` and `server.WithTracer(tc)`; a nil client falls back
to the default. `Run(ctx)` blocks until `ctx` is cancelled, which the
`signal.NotifyContext` context does on SIGINT or SIGTERM.

### Config

The zero value is valid. Every field has a default.

| Field | Default | Meaning |
| --- | --- | --- |
| `Addr` | `":3000"` | Listen address in `host:port` form. |
| `ReadTimeout` | `30s` | Bounds reading a request. |
| `WriteTimeout` | `30s` | Bounds writing a response. |
| `ShutdownTimeout` | `10s` | Bounds the graceful shutdown in `Run`. |

`New` rejects an `Addr` that isn't `host:port` and any negative timeout.

### Middleware order

Every request that Hertz routes, including requests that match no route or the
wrong method, passes through this chain. The order is fixed, and the standard
layers can't be removed or reordered.

Requests that Hertz rejects before routing never reach the chain. These are a
malformed request line, which gets a 400, and oversized headers, which get a
431. Hertz writes those responses itself, with no `X-Request-ID` header, no
span, no metric, and no access-log line.

```
requestID → tracing → metrics → logging → recovery → [team middleware] → handler
```

| Layer | What it does |
| --- | --- |
| `requestID` | Keeps a valid incoming `X-Request-ID`, or generates one. Stores it for `RequestIDFrom` and sets the response header. |
| `tracing` | Starts a new root server span (scope `rest.server`) and links it to any valid incoming `traceparent`. Ends the span with the final status. |
| `metrics` | Counts active requests and records the request duration histogram, with the final status. |
| `logging` | Writes one access-log line per request. Level: 5xx Error, 4xx Warn, otherwise Info. |
| `recovery` | Turns a panic into a 500 problem response, records an `exception` event on the span, and logs the stack. |

Recovery is innermost, so the outer layers see the final 500 after a panic.

Trailing slashes are not redirected. `GET /users/42/` against the route
`/users/:id` gets a 404 problem from the stack, with a request ID, a span, a
duration sample, and an access-log line, like any other unmatched request.

### Team middleware

Add team middleware with `srv.Hertz().Use(...)`, or on a route group. It runs
after the standard stack, inside recovery, so it is observed by the metrics,
the span, and the access log, and a panic in it is recovered.

Register `Use` middleware before the routes it should cover. Hertz applies
`Use` only to routes registered after the call. A route group created after
`New` inherits the standard stack, so a middleware passed to the group runs
after it. This is the form for a webhook signature check:

```go
import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/fikrimohammad/go-dev-sdk/errs"
	"github.com/fikrimohammad/go-dev-sdk/rest/server"
)

func registerWebhooks(srv *server.Server, secret []byte) {
	hooks := srv.Hertz().Group("/webhooks", verifySignature(secret))
	hooks.POST("/payments", func(ctx context.Context, c *app.RequestContext) {
		c.Status(http.StatusNoContent)
	})
}

func verifySignature(secret []byte) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		sig, err := hex.DecodeString(string(c.Request.Header.Peek("X-Signature")))
		mac := hmac.New(sha256.New, secret)
		mac.Write(c.Request.Body())
		if err != nil || !hmac.Equal(sig, mac.Sum(nil)) {
			server.Error(c, http.StatusUnauthorized, errs.New(2001, "invalid webhook signature"))
			return
		}
		c.Next(ctx)
	}
}
```

Call `registerWebhooks(srv, secret)` before `srv.Run`. A rejected
request gets a 401 problem, which is recorded in the metric and the access log.

### Error responses

Use `server.Error(c, status, err)` for every error response. It binds `err` to
the request, writes an RFC 9457 body with `Content-Type:
application/problem+json`, and aborts the chain. A status outside 400 to 599
becomes 500. `err` may be nil.

| Field | Value |
| --- | --- |
| `type` | Omitted (means `about:blank`). |
| `title` | `http.StatusText(status)`, or `Client Error` or `Server Error` when Go has no text for the status. |
| `status` | The HTTP status. |
| `detail` | 4xx: the `errs` message when `err` has an `*errs.Error` in its chain. Otherwise omitted. 5xx: always omitted. |
| `code` | Extension: the `errs` code when `err` has an `*errs.Error` with a non-zero code. 4xx only. Otherwise omitted. |
| `request_id` | Extension: always present. The stored request ID, not read back from the response header. |

```json
{"title":"Bad Request","status":400,"detail":"invalid amount","code":1001,"request_id":"KZ4X7Q2M3NWQ5ZB6RTV2DFHJ3A"}
```

A body never contains the `err.Error()` text of a non-`errs` error, `errs`
debug data, causes, metadata, or stack frames. `NoRoute` gives a 404 problem,
`NoMethod` gives a 405 problem, and a recovered panic gives a 500 problem.

A nil `*errs.Error` inside `err` counts as an error with no `errs` data. It
gives no detail and no code, and the access log records it as the text
`nil *errs.Error`. A typed nil never panics the stack.

### Edge trust

The server treats incoming trace and request context as untrusted:

- **Tracing.** Only `traceparent` is extracted, and baggage is never read. The
  server span always starts a new trace. A valid incoming span context becomes
  a span link, not the parent.
- **Request ID.** An incoming `X-Request-ID` is kept only when it matches
  `^[A-Za-z0-9._-]{1,64}$`. nginx's `$request_id` (32 hex characters) matches.
  Anything else is replaced by a generated ID (`crypto/rand`, 26 characters).
  The rejected raw value never reaches the span, the logs, or the response.
- **`url.scheme`.** Only `https` and `http` are recorded. Hertz takes the scheme
  from absolute-form request targets, which the client controls, so any other
  value becomes `http`.

### Recovery

When a handler panics, recovery:

- discards any partial response and writes a 500 problem body;
- keeps the `X-Request-ID` header and any `Access-Control-*` headers set before
  the panic, so a browser frontend can still read the problem;
- clears a hijack handler that the handler set with `c.Hijack`, so Hertz
  doesn't run it on the connection after the 500;
- records an `exception` event on the span (`exception.type`,
  `exception.message`, `exception.stacktrace`);
- logs `panic recovered` at Error with the panic value, the normalized method,
  the path, and the stack.

If the response is already streaming (a hijacked or chunked writer is in use),
recovery can't send a problem, because headers or data have already gone out.
It records status 500 for telemetry and closes the connection. The client sees
a truncated stream, not a second response. If a streaming writer was installed
but nothing was written yet, the client gets a closed connection with no
response at all.

### Run and shutdown

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
if err := srv.Run(ctx); err != nil {
	// The listener failed to start, or shutdown timed out.
}
```

- `Run(ctx)` serves until `ctx` is cancelled, then shuts down gracefully. There
  is no separate `Shutdown` method.
- The shutdown waits at most `ShutdownTimeout` for in-flight requests.
- `Run` returns `nil` after a graceful shutdown.
- `Run` returns an error if the listener can't start, for example because the
  port is in use. It also returns an error if shutdown times out. That error
  wraps `context.DeadlineExceeded`. **Requests still in flight after the
  timeout are not waited for. Their handlers may still be running when `Run`
  returns.**
- `Run` returns only after its serving goroutine has ended.
- If `ctx` is cancelled before the listener is up, `Run` waits for the listener
  first. Hertz refuses to shut down an engine that isn't running yet.
- The server keeps Hertz's default netpoll transport. Netpoll panics instead of
  returning when it can't create the listener. `Run` recovers that panic and
  returns it as an error, so the failure surfaces the same way as a bind error.

### Known gaps

- 405 responses have no `Allow` header, although RFC 9110 asks for one. Hertz
  doesn't expose per-method route matching, so adding it would mean
  reimplementing the router. Add it when a client needs it.

## Telemetry contract

This section is the public contract for the alert and dashboard template repo.
It covers the server and the client. Changing a metric name, unit, instrument,
or attribute, or the rules below, is a **breaking change**. Attribute keys come
from `semconv/v1.43.0`. The server and client share the rules through
`rest/internal/semconv`.

### Common rules

These rules apply to the server and the client.

- **Method normalization.** Known methods (`GET`, `HEAD`, `POST`, `PUT`,
  `DELETE`, `CONNECT`, `OPTIONS`, `TRACE`, `PATCH`) pass through. Any other
  method becomes `_OTHER`, and the original goes in
  `http.request.method_original` (spans only).
- **Span name.** `{method} {http.route}` when a route matched, otherwise
  `{method}`. The method part is `HTTP` for `_OTHER`.
- **Status code** is recorded as an int attribute.
- **No empty attributes.** An attribute with no value is left out, never sent
  as `""`.
- **Metric attributes stay low-cardinality.** Never request IDs, URLs or paths,
  query strings, or user and tenant IDs.
- **`url.scheme`** is `https` or `http` only.
- **`url.query` is deliberately omitted** on server spans. Query strings can
  carry tokens, and semconv requires redaction.
- **`errs.code`** (custom, int). The code of the first bound error with a
  non-zero `errs` code, at any status. Alert templates use it to track business
  errors on 4xx and 5xx. It is named after the `errs` package, because no semconv
  namespace covers business codes. The codes are a bounded set, so cardinality
  is safe.

### Server metrics

| Name | Instrument | Unit | Attributes |
| --- | --- | --- | --- |
| `http.server.request.duration` | Histogram | `s` | `http.request.method`, `url.scheme`, `http.route` (if matched), `http.response.status_code`, `network.protocol.version`, `error.type` (5xx only), `errs.code` (when an `errs` code is bound, any status) |
| `http.server.active_requests` | UpDownCounter | `{request}` | `http.request.method`, `url.scheme` (the same values for +1 and -1) |

Descriptions: "Duration of HTTP server requests." and "Number of active HTTP
server requests."

### Server span

- Kind `Server`, scope `rest.server`, a new root, and a link to any valid
  incoming span context.
- Set at start: `http.request.method` (and `http.request.method_original`),
  `url.path`, `url.scheme`, `http.route` (if matched), `server.address`,
  `server.port`, `network.peer.address`, `network.protocol.version`,
  `user_agent.original`, and `http.request.header.x-request-id` (a string array
  holding the request ID in effect).
- Set at end: `http.response.status_code`, `error.type` on 5xx, and `errs.code`
  when bound.
- Status: `Error` on 5xx, with no description. `Unset` otherwise, including 4xx.
- `error.type` on 5xx: the status code as a string (`"500"`, `"503"`), as HTTP
  semconv requires. It never holds an `errs` code, because those codes share a
  number space with HTTP statuses. A recovered panic gives `"500"`. It is never
  set below 500. Business codes are in `errs.code`.

### Client metric

| Name | Instrument | Unit | Attributes |
| --- | --- | --- | --- |
| `http.client.request.duration` | Histogram, one record per attempt | `s` | `http.request.method`, `server.address` (the vendor host), `server.port`, `url.scheme`, `http.response.status_code` (when a response arrived), `network.protocol.version` (when a response arrived), `error.type` (when the attempt failed or the status is 400 or above) |

Description: "Duration of HTTP client requests." `server.address` is the vendor
host, so alert templates can group by vendor. A gateway calls a handful of
vendors, so its cardinality is low. Retries record one metric record per
attempt, not one per call.

### Client span

- One per attempt. Kind `Client`, scope `rest.client`, and a child of any span
  in the caller's `ctx`. Name `{method}`, or `HTTP` for `_OTHER`. Client spans
  have no route.
- Set at start: `http.request.method` (and `http.request.method_original`),
  `server.address`, `server.port`, `url.full` (redacted, see
  [Outbound hygiene](#outbound-hygiene)), and `http.request.resend_count` on
  attempts after the first (`1`, `2`, ...).
- Set at end: `http.response.status_code`, `network.protocol.version`, and
  `error.type`.
- Status: `Error` when the status is 400 or above, or the attempt failed. No
  description.
- `error.type`: the status code as a string for 400 and above. For an attempt
  that failed with no response, it is `timeout`, `canceled`, or `_OTHER`.
- Nothing is injected into the request, and request headers are never recorded.
- An attempt that is not sent, because `ctx` is done or no time is left, still
  has its span and a duration record of about zero seconds. Its `error.type` is
  `canceled` or `timeout`, and no response attributes are set.
- net/http follows redirects through `RoundTripper` as separate calls. Each
  redirect has its own span and duration record, and none has
  `http.request.resend_count`, because each is a new call.

### Client log

One line per call, at the final outcome only. It is at Warn when the call
failed or the final status is 400 or above, and at Debug otherwise. Its fields
are `http.request.method`, `server.address`, `url.path`,
`http.response.status_code` (when a response arrived), `attempts`, `duration`
(seconds), and `error` (when the call failed). It never carries the query
string or the userinfo.

## Client

`rest/client` builds the Hertz client that gateway services use to call external
vendors. Retries, deadlines, and telemetry come from one middleware chain. Vendor
SDKs that take an `*http.Client` get the same behaviour through
`client.RoundTripper`.

### Quick start

Create the client once at startup and share it. The client is safe for
concurrent use, and each call sends one request. The client decides when to
retry it.

```go
import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	hclient "github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/fikrimohammad/go-dev-sdk/rest/client"
)

func newTaxClient() (*hclient.Client, error) {
	return client.New(client.Config{AttemptTimeout: 5 * time.Second})
}

func submitInvoice(ctx context.Context, c *hclient.Client, body []byte) error {
	req := protocol.AcquireRequest()
	defer protocol.ReleaseRequest(req)
	resp := protocol.AcquireResponse()
	defer protocol.ReleaseResponse(resp)

	req.SetMethod(http.MethodPost)
	req.SetRequestURI("https://tax.example.com/v1/invoices")
	req.Header.Set("Content-Type", "application/json")
	req.SetBody(body)

	err := c.Do(ctx, req, resp)
	if errors.Is(err, context.DeadlineExceeded) {
		// A POST is not retried, and the vendor may have processed it before the
		// timeout. Reconcile first, for example by querying the invoice status,
		// instead of submitting it again.
		return fmt.Errorf("submit invoice: timed out, reconcile before resubmitting: %w", err)
	}
	if err != nil {
		return fmt.Errorf("submit invoice: %w", err)
	}
	// A final 503 comes back with a nil error, as in net/http, so check the status.
	if resp.StatusCode() != http.StatusCreated {
		return fmt.Errorf("submit invoice: vendor answered %d", resp.StatusCode())
	}
	return nil
}
```

`New` returns the plain Hertz client, so the rest of the Hertz API works as
usual. It returns an error for an invalid `Config`. Like the server, it
reports through the package-level telemetry defaults unless
`client.WithMetrics` and `client.WithTracer` say otherwise. A URL that is not `http` or `https`, or has no host, fails before
anything is sent.

Every timeout matches `errors.Is(err, context.DeadlineExceeded)`, whether it
comes from the caller's context, `Config.Timeout`, or `Config.AttemptTimeout`.
A cancelled call returns `context.Canceled`.

### Config

The zero value is valid. Every field has a default.

| Field | Default | Meaning |
| --- | --- | --- |
| `Timeout` | `30s` | Bounds the whole call, retries included, when `ctx` has no deadline. |
| `AttemptTimeout` | `10s` | Bounds each attempt. |
| `MaxAttempts` | `3` | Attempts in total, including the first. `1` disables retry. |
| `BackoffBase` | `100ms` | Base of the exponential delay between attempts. |
| `BackoffMax` | `2s` | Cap of the delay between attempts. Must not be below `BackoffBase`. |
| `MaxRetryAfter` | `10s` | Longest `Retry-After` the client honours. A longer one returns the last response. |
| `DialTimeout` | `5s` | Bounds opening a TCP connection. |
| `Proxy` | empty (direct) | Egress proxy URL with the `http` scheme and a host, for example `http://egress.internal:3128`. Empty sends requests directly. |
| `TLS` | `nil` (system roots) | TLS configuration for HTTPS, for example a client certificate for mTLS. It is not read from or written to YAML or JSON. |

`New` rejects a negative duration, a negative `MaxAttempts`, a `BackoffBase`
above `BackoffMax`, and a `Proxy` that is not an `http` URL with a host. The
error text leaves out the proxy URL, because it can hold credentials.

### Middleware chain

```
logging → retry → tracing → metrics → deadline → [team middleware] → Hertz transport
```

| Layer | Runs | What it does |
| --- | --- | --- |
| `logging` | once per call | Bounds the call: the request's Hertz timeout (`DoTimeout` or `WithRequestTimeout`) when it has one, otherwise `Config.Timeout` when `ctx` has no deadline. Writes the final-outcome log line. |
| `retry` | once per call | Sends the attempts and decides whether to retry. See [Retry](#retry). |
| `tracing` | once per attempt | Starts the client span. |
| `metrics` | once per attempt | Records `http.client.request.duration`. |
| `deadline` | once per attempt | Sets the attempt's timeout to the shorter of `AttemptTimeout` and the time left, and reports a timeout as `context.DeadlineExceeded`. |
| team middleware | once per attempt | Whatever the team adds with `Use`. |

Add team middleware with `c.Use`, during setup, before the client is shared. It
runs innermost, once per attempt, so a signature with a timestamp is made again
on every retry:

```go
import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"

	hclient "github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/protocol"
)

// signRequests adds a timestamped HMAC signature to every attempt.
func signRequests(c *hclient.Client, key []byte) {
	c.Use(func(next hclient.Endpoint) hclient.Endpoint {
		return func(ctx context.Context, req *protocol.Request, resp *protocol.Response) error {
			ts := strconv.FormatInt(time.Now().Unix(), 10)
			mac := hmac.New(sha256.New, key)
			mac.Write([]byte(ts + "\n"))
			mac.Write(req.Body())
			req.Header.Set("X-Timestamp", ts)
			req.Header.Set("X-Signature", hex.EncodeToString(mac.Sum(nil)))
			return next(ctx, req, resp)
		}
	})
}
```

### Retry

A request is retried only when both of these hold:

- Its method is `GET`, `HEAD`, `OPTIONS`, `PUT`, `DELETE`, or `TRACE`, or it has
  a non-empty `Idempotency-Key` header. Any other request, such as a `POST`
  without the header, is sent once.
- Its body can be sent again. Use `SetBody`, which buffers the body. A body set
  with `SetBodyStream`, and a multipart file or field backed by a reader, are
  sent once and never retried, whatever the method or header, because they can
  be read only once.

These outcomes are retried, up to `MaxAttempts` in total:

- A response with status 429, 502, 503, or 504.
- A transport error that can succeed on another try, such as a refused, reset,
  or dropped connection, or an unexpected EOF.
- A per-attempt timeout (`AttemptTimeout`). An overall deadline or a cancelled
  call is not retried.

These are never retried:

- A 500, or any other status that is not listed above.
- A transport error from a condition that will not change: a certificate that
  does not verify, TLS to a server that does not speak TLS, a TLS alert from the
  peer (for example, a rejected client certificate), a DNS name that does not
  exist, a body over its limit, or an unsupported protocol.
- A 1xx response other than `100 Continue`. The attempt fails with
  `ErrInformationalResponse`, which matches with `errors.Is`. See
  [Client known gaps](#client-known-gaps).

A proxy `CONNECT` failure (407 or 403) is retried, because Hertz reports it the
same way as a dropped connection.

Before retry *n*, the client waits a random duration in
`[0, min(BackoffMax, BackoffBase * 2^(n-1)))` (full jitter). On a 429 or 503 with
`Retry-After`, in seconds or as an HTTP date, the header replaces the jitter.
If that delay is longer than `MaxRetryAfter`, or it would pass the `ctx`
deadline, the call stops and returns the last response. A `Retry-After` value
too large to parse counts as longer than any cap. A negative or unparseable
value falls back to jitter. The wait is cancellable: cancelling `ctx` during the
wait returns `ctx.Err()` at once.

A final 503 is a response with a nil error, as in net/http. Retry does not turn
an exhausted call into an error, so check the status code.

Don't add retry on top. Don't call `SetRetryIfFunc` or set `WithRetryConfig` on
the client, and don't wrap `RoundTripper` in another retry loop. Retry already
runs in the chain. Another layer multiplies the attempts and sends
non-idempotent requests again. This is documented, not enforced.

### Deadlines and timeouts

Hertz ignores a `ctx` deadline once a request is in flight, so the client
enforces the deadline itself:

- Each attempt's timeout is the shorter of `AttemptTimeout` and the time left
  until the `ctx` deadline. If no time is left, the call returns
  `context.DeadlineExceeded` without sending anything.
- `DoTimeout`, or `WithRequestTimeout` on the request, bounds the whole call,
  retries included. The per-attempt timeout never overwrites it. Each attempt is
  still capped by the time left.
- Without a `ctx` deadline, `Config.Timeout` bounds the whole call.
- Cancellation is observed before each attempt and during the wait between
  attempts. An attempt already in flight can't be aborted, so see
  [Client known gaps](#client-known-gaps).

### Vendor SDKs

A vendor SDK that takes an `*http.Client` gets the same retries, deadlines, and
telemetry when it sends through `client.RoundTripper`:

```go
import (
	"context"
	"net/http"

	hclient "github.com/cloudwego/hertz/pkg/app/client"
	"github.com/fikrimohammad/go-dev-sdk/rest/client"
)

// paymentSDK stands in for a vendor SDK that takes an *http.Client.
type paymentSDK struct {
	httpClient *http.Client
	baseURL    string
}

func newPaymentSDK(c *hclient.Client) *paymentSDK {
	return &paymentSDK{
		httpClient: &http.Client{Transport: client.RoundTripper(c)},
		baseURL:    "https://payments.example.com",
	}
}

func (s *paymentSDK) GetCharge(ctx context.Context, id string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/v1/charges/"+id, nil)
	if err != nil {
		return 0, err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}
```

- The RoundTripper runs the same chain as `c.Do`, so it emits the same client
  spans and duration records, with the same names and attributes.
- The request body is read in full before the first attempt, so it can be sent
  again on a retry. The response body is copied before the call returns. Nothing
  is streamed, so a large body is held in memory.
- `req.Body` is always closed, as net/http requires.
- Errors are returned unchanged, so `errors.Is(err, context.DeadlineExceeded)`
  still matches through the `*url.Error` that net/http wraps around it.
- `http.Client.Timeout` is a second deadline on the same call. net/http applies
  it from outside and cancels the call when it fires, which also stops any retry
  in progress. Set it to the whole-call budget you mean, or leave it at zero and
  let `Config.Timeout` and the `ctx` decide.
- Don't add SDK-level retry on top of the RoundTripper, for the same reason as
  above.

### Outbound hygiene

- **No trace context or request IDs.** The client sends no `traceparent`,
  `tracestate`, or `baggage` header, and it adds no `X-Request-ID`. Its span is a
  child of the caller's span in your telemetry, but the vendor sees none of it.
- **Paths as sent.** The path goes out exactly as the caller wrote it. Escaped
  slashes, double slashes, and dot segments are not decoded or cleaned, so
  `url.full` shows the path the vendor received.
- **Redacted URLs.** `url.full` in spans drops the userinfo and the fragment, and
  every query value becomes `REDACTED`. A query part with no `=` becomes just
  `REDACTED`, because that part may be the token itself. For example,
  `https://tax.example.com/v1/invoices?api_key=REDACTED`. Metrics never carry a
  URL. The client's own log line carries `url.path` and the vendor host, never
  the query or the userinfo.
- **Proxy credentials.** `Config.Proxy` sends every request through the proxy. The
  userinfo in the proxy URL goes to the proxy only, and it is percent-decoded
  before use. Telemetry reports the vendor host as `server.address`, not the
  proxy. For `http` targets, Hertz sets `Proxy-Authorization` on your request and
  keeps it there. Don't log request headers, and don't reuse a request object
  after a call through a proxy. This note covers `http` targets. HTTPS targets go
  through `CONNECT`.
- **Hertz's global logger (known gap).** When Hertz re-dials a closed pooled
  connection, it logs the full URL, query string included, at Warn through
  `hlog`. A library must not change global state, so the application sets this
  at startup, before any client is used:

```go
import "github.com/cloudwego/hertz/pkg/common/hlog"

// quietHertzLogs stops hlog from printing full request URLs. Hertz logs them at
// Warn when it re-dials a pooled connection. This also hides Hertz's other Warn
// lines. To keep those, route hlog through a logger that redacts query strings
// with hlog.SetLogger instead.
func quietHertzLogs() {
	hlog.SetLevel(hlog.LevelError)
}
```

### Client known gaps

- **hlog logs full URLs.** Hertz's global logger writes them at Warn, as
  described above. The client's own logs never do.
- **No cancellation mid-attempt.** Hertz can't abort an attempt in flight, so a
  cancelled call returns when that attempt ends, at most `AttemptTimeout` (or the
  `ctx` deadline) later. Cancellation is observed between attempts and during
  the wait.
- **Informational responses (Hertz gap).** Hertz returns a 1xx response other
  than `100 Continue` as the final response, and leaves the real one on the
  connection. The client fails that attempt with `ErrInformationalResponse` and
  closes its idle connections, so the next call does not read the leftover
  response. The close is racy under concurrency. A concurrent call can take the
  poisoned connection from the pool before the close, and then it reads the
  leftover response. It also closes the idle connections of other hosts. The
  fix belongs in Hertz.
- **No streaming.** Request and response bodies are buffered. A body set with
  `SetBodyStream` is sent once and never retried.
- **HTTP/1.1 only.** The Hertz client speaks HTTP/1.1, and the RoundTripper
  reports `HTTP/1.1` to net/http.
