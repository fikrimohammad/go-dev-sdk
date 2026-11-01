# Spec: rest-client

Module id: `rest-client` ([capability map](capability-map.md)). Depends on
`rest-server` for `rest/internal/semconv` and the Telemetry Contract. Status: approved 2026-10-31.

## Objective

Give every gateway service one way to call external vendors, with safe
retries, deadlines that hold, and the same telemetry, whether the call is
hand-written (government tax API) or goes through a vendor SDK that takes an
HTTP client (payment gateway). One implementation serves both.

Consumers: domain teams' gateway services calling vendors on behalf of
internal RPC services. Owner: developer experience.

### User stories

- As a domain team, I call `client.New(cfg)` and use the returned
  Hertz client. Retries, timeouts, and telemetry work with no other code.
- As the payment team, I wrap our SDK with
  `&http.Client{Transport: client.RoundTripper(c)}` and get exactly the same
  behaviour and metrics as hand-written calls.
- As a domain team, when a call runs out of time I can check
  `errors.Is(err, context.DeadlineExceeded)` and run my own reconciliation
  (for example, query the invoice status) instead of resubmitting.
- As an internal RPC caller, my deadline is honoured: the gateway stops waiting
  on the vendor when my deadline passes.
- As security, nothing internal (trace context, request IDs, API keys in query
  strings) leaks to vendors or into telemetry.

## Tech Stack

Same module and versions as [SPEC-rest-server](SPEC-rest-server.md): Go 1.26.5,
Hertz v0.10.6 (`pkg/app/client`, `pkg/protocol`, `pkg/common/config`,
`pkg/common/errors`), `observability`, OTel v1.45.0 with
`semconv/v1.43.0`, and `net/http/httptest` for tests. **No new third-party
dependencies.**

## Commands

Same as SPEC-rest-server, run from `rest/`.

## Project Structure

```
rest/
  parity_test.go             client ↔ server end-to-end; RoundTripper parity
  client/
    client.go                package doc, New
    client_test.go
    config.go                Config, defaults, validation
    config_test.go
    middleware.go            logging, retry, tracing, metrics (unexported)
    middleware_test.go
    roundtripper.go          RoundTripper
    roundtripper_test.go
    helpers_test.go          httptest vendors, span recorder, recording metrics
```

## Public API

```go
package client

// Config holds client settings. The zero value is valid: every field has a default.
type Config struct {
	Timeout        time.Duration `yaml:"timeout" json:"timeout"`                 // overall, when ctx has no deadline; default 30s
	AttemptTimeout time.Duration `yaml:"attempt_timeout" json:"attempt_timeout"` // per attempt; default 10s
	MaxAttempts    int           `yaml:"max_attempts" json:"max_attempts"`       // including the first; default 3; 1 disables retry
	BackoffBase    time.Duration `yaml:"backoff_base" json:"backoff_base"`       // default 100ms
	BackoffMax     time.Duration `yaml:"backoff_max" json:"backoff_max"`         // default 2s
	MaxRetryAfter  time.Duration `yaml:"max_retry_after" json:"max_retry_after"` // longest honoured Retry-After; default 10s
	DialTimeout    time.Duration `yaml:"dial_timeout" json:"dial_timeout"`       // default 5s
	Proxy          string        `yaml:"proxy" json:"proxy"`                     // egress proxy URL, http scheme only, e.g. http://egress:3128; empty = direct
	TLS            *tls.Config   `yaml:"-" json:"-"`                             // e.g. mTLS for government APIs; nil = system defaults
}

type Option func(*options)
func WithMetrics(m metrics.Client) Option
func WithTracer(t tracer.Client) Option
func New(cfg Config, opts ...Option) (*hertzclient.Client, error)
func RoundTripper(c *hertzclient.Client) http.RoundTripper
```

- `New` validates `cfg` (durations not negative; `MaxAttempts` ≥ 1 after
  defaults; `BackoffBase` ≤ `BackoffMax`; `Proxy`, when set, parses as an
  `http` URL with a host) and installs the middleware chain. Telemetry
  clients come from `WithMetrics`/`WithTracer`; an unset or nil client falls
  back to the package-level default, read per attempt, as in SPEC-rest-server.
- It always passes `WithTLSConfig(cfg.TLS)` (nil means system roots), which
  selects Hertz's standard dialer. Without it Hertz uses netpoll, which fails
  every https request and doesn't bound slow body reads (checkpoint D review).
  The dialer is wrapped so every new connection gets a deadline right after
  dialing; otherwise a stalled proxy `CONNECT` ignores the caller's deadline
  for Hertz's internal 60s.
- When `Proxy` is set, every request goes through it via
  `c.SetProxy(protocol.ProxyURI(...))`, built from the parsed URL with the
  percent-decoded username and password. Only `http` proxies are allowed: with
  an `https` proxy Hertz uses the proxy host as the vendor's TLS ServerName.
  Hertz sets `Proxy-Authorization` on the caller's request for http targets
  and leaves it there, so request headers must never be logged or reused. HTTPS targets use `CONNECT`, which
  Hertz supports. Telemetry still reports the vendor as `server.address`, not
  the proxy. It never sets Hertz `WithRetryConfig` or
  `RetryIfFunc`.
- It returns the plain Hertz client: no wrapper type. Middleware a team adds
  with `c.Use(...)` (for example, request signing) runs innermost, once per
  attempt, which is correct for timestamped signatures.
- `RoundTripper(c)` adapts a client from `New` to net/http. Passing a client
  not built by `New` gives no policy; the doc comment says so.
- Teams must not call `SetRetryIfFunc` or add retry on top of
  `RoundTripper`. This is documented, not enforced.

## Behaviour

### Middleware chain (outer → inner)

```
logging → retry → tracing → metrics → deadline → [team middleware] → Hertz transport
```

- **logging** (once per call): if `ctx` has no deadline, it applies
  `cfg.Timeout`. After the call it logs the final outcome: Warn on an error or
  a status ≥ 400, Debug otherwise.
- **retry**: the loop below.
- **tracing** and **metrics**: once per attempt (probed: a retry middleware
  calling `next` 3 times ran the inner middleware 3 times).
- **deadline** (once per attempt, innermost of ours): sets the per-attempt
  Hertz timeout and wraps Hertz timeouts as `context.DeadlineExceeded`. It
  sits inside tracing and metrics so they see the wrapped error and record
  `error.type=timeout` (added during task 9).

### Deadlines and timeouts

Hertz ignores a ctx deadline once a request is in flight (probed: a 50ms ctx
deadline on a 500ms request returned after 500ms with `err == nil`). So:

- Each attempt sets
  `req.SetOptions(config.WithRequestTimeout(min(cfg.AttemptTimeout, time.Until(deadline))))`.
  If no time is left, the call stops with `context.DeadlineExceeded` without
  sending.
- A caller timeout set with Hertz `DoTimeout`/`DoDeadline` or
  `WithRequestTimeout` becomes a whole-call deadline; it is never silently
  overwritten by the per-attempt timeout.
- A Hertz timeout (`errors.Is(err, herrs.ErrTimeout)`) or a connection
  deadline (`errors.Is(err, os.ErrDeadlineExceeded)`) is returned as
  `fmt.Errorf("rest/client: %w: %w", context.DeadlineExceeded, err)`, so both
  the standard sentinel and the Hertz error match.
- Cancellation (`ctx.Done`) before an attempt returns `ctx.Err()` unchanged.
- A URL whose scheme isn't http/https, or that has no host, fails before
  sending: `rest/client: request URL must be http or https with a host`.
- `c.Do(ctx, req, nil)` works: a nil response is replaced internally.
- Cancellation is observed before each attempt and during retry waits. Hertz
  can't abort an attempt already in flight, so cancelling mid-attempt returns
  when that attempt ends (bounded by the per-attempt timeout or the deadline).
- The caller's own Hertz request timeout is restored after each attempt, so a
  reused request doesn't keep the last attempt's timeout.

### Retry

A request is **retryable** when both hold:

- the method is GET, HEAD, OPTIONS, PUT, DELETE, or TRACE, **or** an
  `Idempotency-Key` header is set; and
- `!req.IsBodyStream()` and no multipart file is reader-backed (the body can
  be re-sent; Hertz re-reads a file reader on every write, so a retry would
  send an empty file).

An attempt **should be retried** when it failed with a transport error that
may succeed if sent again, or a per-attempt timeout (not the overall deadline
or cancellation), or returned 429, 502, 503, or 504. A 500 is not retried.
Transport errors use a deny-list: TLS certificate verification failures, TLS
record-header errors (TLS to a non-TLS port), TLS alerts from the peer
(`net.OpError` with `Op == "remote error"`, for example mTLS rejected), DNS
not-found, body too large, and unsupported protocol are never retried.
Everything else (refused, reset, dropped, EOF, DNS timeout) is. An allow-list
would miss Hertz's opaque dropped-connection error. Proxy `CONNECT` failures
(407/403) are retried because Hertz gives them the same error shape.

Delay before attempt n (n ≥ 1): full jitter, a random value in
`[0, min(BackoffMax, BackoffBase·2^(n-1)))`. On a 429 or 503 with
`Retry-After` (seconds or an HTTP date), use that instead. A seconds value too
large to parse counts as longer than any cap; negative or garbage values fall
back to jitter. If it is longer
than `MaxRetryAfter`, or the delay would pass the deadline, stop and return
the last response.

- Wait with `select` on `ctx.Done()` and a timer. Never `time.Sleep`.
- `resp.Reset()` before each retry, keeping the caller's `resp.SkipBody`.
  (Hertz v0.10.6 already resets the response before every attempt, so this is
  defensive and not observable in tests.)
- The final result is returned as is: a final 503 is a response with
  `err == nil`, as in net/http.
- The attempt index travels in `ctx` (unexported key) for tracing and metrics.

### Outbound hygiene

- Paths are sent exactly as given: `New` disables Hertz path normalization,
  which otherwise decodes and cleans escaped paths (an escaped `..%2F` would
  traverse vendor paths, and URI-signing SDKs would break). `url.full` shows
  the path actually sent (final review).
- A 1xx response other than `100 Continue` returns
  `ErrInformationalResponse` (not retried). Hertz would otherwise return it as
  final and leave the real response on a pooled connection. Idle connections
  are closed as a best-effort guard; the remaining race is a Hertz gap.
- `RoundTripper` drops `traceparent`, `tracestate` and `baggage` headers an SDK
  may have injected through a global propagator.
- In `url.full`, a query segment without `=` is redacted whole (`REDACTED`).

- Known gap: Hertz's global `hlog` logs the full URL, query string included,
  at Warn when it re-dials a closed pooled connection. A library must not
  change global state, so applications route `hlog` through a redacting logger
  or set its level above Warn (documented in the package doc and README).

- No trace-context injection: no `traceparent`, `tracestate`, or baggage
  headers.
- No `X-Request-ID` added.
- `url.full` in spans and the logged URL are redacted: userinfo is removed and
  every query value becomes `REDACTED` (`?key=REDACTED`). Logs carry
  `url.path` plus `server.address`, never the raw query.

### RoundTripper

`RoundTrip(req)`:

1. Builds a Hertz request: URI from `req.URL.String()`, method, all headers,
   `Host` from `req.Host` when set. The body is read fully (`io.ReadAll`) and
   set with `SetBody`, so it is buffered and retryable. `req.Body` is always
   closed, as the `http.RoundTripper` contract requires.
2. Calls `c.Do(req.Context(), hreq, hresp)`.
3. Builds an `*http.Response`: `StatusCode`, `Status` (`"200 OK"`), `Proto`
   `HTTP/1.1`, all headers (multi-valued), `ContentLength`, `Request: req`, and
   `Body` as an `io.NopCloser` over a copy of the response body. It then
   releases the Hertz objects.
4. For HEAD, `ContentLength` comes from the response's Content-Length
   header and `Body` is `http.NoBody`.
5. Returns errors unchanged, so `errors.Is(err, context.DeadlineExceeded)`
   still works through net/http's `*url.Error`.

It doesn't use `adaptor.CopyToHertzRequest`. That function reads
`req.RequestURI`, which net/http leaves empty on client requests, and it sets
the body as a stream, which would make every SDK request non-retryable.
Streaming bodies are out of scope.

## Telemetry Contract (client)

These extend the contract in [SPEC-rest-server](SPEC-rest-server.md#telemetry-contract):
method normalization, the no-empty-attributes rule, int status code, and
low-cardinality metric attributes apply here too.

### Client metric

| Name | Instrument | Unit | Attributes |
| --- | --- | --- | --- |
| `http.client.request.duration` | Histogram (`metrics.Record`), one per attempt | `s` | `http.request.method`, `server.address`, `server.port`, `url.scheme`, `http.response.status_code` (when a response arrived), `network.protocol.version`, `error.type` (status >= 400 or a failed attempt) |

Description: "Duration of HTTP client requests." `server.address` is the
vendor host (low-cardinality: a handful of vendors per gateway), which lets
alert templates group by vendor.

### Client span

- One per attempt. Kind Client, scope `rest.client`. Name: `{method}`, or
  `HTTP` for `_OTHER`.
- Attributes: `http.request.method` (and `http.request.method_original`),
  `server.address`, `server.port`, `url.full` (redacted),
  `http.request.resend_count` (on attempts after the first: 1, 2, ...),
  `http.response.status_code`, `network.protocol.version`, and `error.type`.
- Status: Error when the status is ≥ 400 or the attempt failed. No
  description for status-code errors.
- `error.type` (client): the status code as a string for ≥ 400; for failures
  with no response, one of `timeout`, `canceled`, or `_OTHER`.

### Client log fields

Final outcome only: `http.request.method`, `server.address`, `url.path`,
`http.response.status_code` (if any), `attempts`, `duration` (seconds), and
`error` (if any).

## Code Style

As SPEC-rest-server, with error strings starting `rest/client:`. For example,
the timeout wrap:

```go
if errors.Is(err, herrs.ErrTimeout) || errors.Is(err, os.ErrDeadlineExceeded) {
	return fmt.Errorf("rest/client: %w: %w", context.DeadlineExceeded, err)
}
```

## Testing Strategy

- Stdlib `testing`, table-driven, `httptest.NewServer` as the vendor, a
  `tracetest.SpanRecorder`, and a recording `metrics.Client`. No mockey.
- Required tests:
  - **Retry table:** for each method and outcome (GET 503 → retried; POST 503
    → not retried; POST plus `Idempotency-Key` 503 → retried; GET 500 → not
    retried; GET network error → retried; stream body → not retried), assert
    vendor hit count, final status, span count, and `resend_count` values
    `[absent, 1, 2]`.
  - **Retry-After:** 429 with `Retry-After: 1` waits about 1s; a value above
    `MaxRetryAfter` returns the 429 without retrying; an HTTP-date form is
    parsed.
  - **Cancellable wait:** cancelling `ctx` during backoff returns
    `context.Canceled` promptly (well before the backoff ends).
  - **Deadlines:** a slow vendor with only a ctx deadline returns about when
    the deadline passes with `errors.Is(err, context.DeadlineExceeded)`; the
    same with only `AttemptTimeout`; the Hertz `ErrTimeout` stays reachable
    with `errors.Is`.
  - **Hygiene:** the vendor receives no `traceparent`, `tracestate`,
    `baggage`, or `X-Request-ID`; `url.full` and the logs contain no query
    values or userinfo.
  - **Proxy:** with `Proxy` set to an `httptest` proxy, the request reaches
    the vendor through it, and `server.address` is the vendor host; an invalid
    `Proxy` URL fails `New`.
  - **Contract:** exact metric name, unit, and attribute sets for 200, 404,
    503-after-retries, and a timeout (`error.type=timeout`).
  - **RoundTripper:** URL, method, headers, and body arrive intact; `req.Body`
    is closed; a POST body is re-sent on a retried attempt (with
    `Idempotency-Key`); errors match `context.DeadlineExceeded` through
    `*url.Error`.
- **`rest/parity_test.go`** (module root, like `nats/parity_test.go`): the same
  scenarios through `c.Do` and through `&http.Client{Transport: RoundTripper(c)}`
  produce identical metric names, units, attributes, and span sets. A
  `rest/client` call to a `rest/server` server produces two separate traces.

## Boundaries

- **Always:** as SPEC-rest-server; keep the client contract exactly as above.
- **Ask first:** any new dependency; contract changes; new `Config` fields;
  exporting the middleware.
- **Never:** Hertz `WithRetryConfig` or `RetryIfFunc`; `time.Sleep`; retrying
  a non-idempotent request without `Idempotency-Key`; injecting trace context
  or request IDs; recording raw URLs or query values; `adaptor.CopyToHertzRequest`
  in `RoundTripper`.

## Success Criteria

- [ ] Every test above passes under `-race -shuffle=on`.
- [ ] The parity test passes: the Hertz path and the `RoundTripper` path emit
      identical telemetry.
- [ ] A ctx deadline cuts off a slow in-flight vendor call (the opposite of
      plain Hertz behaviour, which the deadline test proves).
- [ ] No outbound request carries `traceparent`, `baggage`, or
      `X-Request-ID`.
- [ ] `rest/README.md` has a client section: `New`, `Config` defaults, retry
      rules, the timeout `errors.Is` pattern, the `RoundTripper` wrap for
      SDKs, and "don't add retry on top".

## Open Questions

None. Resolved 2026-10-31:

- Egress proxy is needed: `Config.Proxy` is added.
- Defaults stay at 30s overall, 10s per attempt, and 3 attempts; teams
  override them per client.
