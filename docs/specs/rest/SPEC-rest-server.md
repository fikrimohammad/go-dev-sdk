# Spec: rest-server

Module id: `rest-server` ([capability map](capability-map.md)). Status: approved 2026-10-31.

## Objective

Give every gateway service one HTTP server that domain teams can't wire up
wrong. The server enforces, in a fixed order: request ID, tracing, metrics,
access logging, and panic recovery. Teams register routes and add their own
middleware (for example, webhook signature checks); they can't remove or
reorder the standard layers.

Consumers: domain teams' gateway services behind a cloud load balancer and
nginx, serving frontends and vendor webhooks. Owner: developer experience.

This spec also owns the **telemetry contract** (see "Telemetry Contract"). The
alert/dashboard template repo depends on it; changing it is a breaking change.

### User stories

- As a domain team, I call `server.New`, register routes, and call `Run(ctx)`.
  Telemetry, request IDs, and panic handling work with no other code.
- As a domain team, I return errors with `server.Error(c, status, err)`, and
  the caller gets an RFC 9457 problem body that never leaks internals.
- As the alert-repo owner, every gateway emits the same metric names, units,
  and attributes, so one template works everywhere.
- As an on-call engineer, a handler panic shows up as a 500 in the metric, the
  span (status Error plus an exception event), and the access log.
- As a support engineer, a customer quotes the `request_id` from the error body
  and I find the trace and logs for that request.

## Tech Stack

- Go 1.26.5 (from `go.work`); module `github.com/fikrimohammad/go-dev-sdk/rest`.
- `github.com/cloudwego/hertz` v0.10.6 (server; `ut` for tests).
- `github.com/fikrimohammad/go-dev-sdk/observability` (`metrics.Client`,
  `tracer.Client`, `logs`), `errs`, `identity` (tests only), using local
  `replace` directives like other modules.
- `go.opentelemetry.io/otel` v1.45.0: `trace`, `propagation`, `codes`,
  `semconv/v1.43.0` (attribute keys only), `sdk/trace/tracetest` (tests).
- Stdlib for everything else (`crypto/rand.Text` for request IDs,
  `encoding/json`, `regexp`). **No new third-party dependencies.**

## Commands

Run from `rest/` (and from `cron/` for the deletion):

```bash
gofmt -l .
go vet ./...
go test -count=1 -shuffle=on -gcflags="all=-N -l" ./...
go test -count=1 -race -shuffle=on -gcflags="all=-N -l" ./...
golangci-lint run --new-from-merge-base=main ./...
GOWORK=off go run ../.agents/skills/go-best-practices/scripts/gostyle.go ./...
go mod tidy -diff
go doc -all ./server   # doc links resolve, no stray brackets
```

## Project Structure

```
rest/
  go.mod                       module github.com/fikrimohammad/go-dev-sdk/rest
  README.md                    usage, middleware order, contract
  parity_test.go               (added by rest-client) client ↔ server end-to-end
  internal/semconv/
    semconv.go                 instrument descriptors, method normalization,
                               error.type rules, attribute builders
    semconv_test.go
  server/
    server.go                  package doc, Server, New, Hertz, Run, RequestIDFrom
    server_test.go
    config.go                  Config, defaults, validation
    config_test.go
    middleware.go              requestID, tracing, metrics, logging, recovery (unexported)
    middleware_test.go
    problem.go                 Error, problem rendering, NoRoute/NoMethod handlers
    problem_test.go
    helpers_test.go            test server, span recorder, recording metrics
```

Changes outside `rest/`:

- Delete `apiserver/` and `cron/server/`.
- `go.work`: add `./rest`, remove `./apiserver`.
- `cron/go.mod`: drop the `apiserver` require and replace; `go mod tidy`.
- `cron/README.md`: remove the dev REST API section and note it moves to RPC.
- Root `README.md`: replace the `apiserver` row with `rest`.

## Public API

```go
package server

// Config holds server settings. The zero value is valid: every field has a default.
type Config struct {
	Addr            string        `yaml:"addr" json:"addr"`                         // default ":3000"
	ReadTimeout     time.Duration `yaml:"read_timeout" json:"read_timeout"`         // default 30s
	WriteTimeout    time.Duration `yaml:"write_timeout" json:"write_timeout"`       // default 30s
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"` // default 10s
}

type Option func(*Server)
func WithMetrics(m metrics.Client) Option
func WithTracer(t tracer.Client) Option
func New(cfg Config, opts ...Option) (*Server, error)
func (s *Server) Hertz() *server.Hertz   // route registration and team middleware
func (s *Server) Run(ctx context.Context) error
func RequestIDFrom(ctx context.Context) string
func Error(c *app.RequestContext, status int, err error)
```

- `New` validates `cfg` (`Addr` is `host:port`; timeouts not negative;
  `ShutdownTimeout` positive after defaults) and registers the standard stack plus problem+json `NoRoute` and
  `NoMethod` handlers.
- **Telemetry clients** follow the sibling modules (`redis`, `db`, `s3`):
  `WithMetrics`/`WithTracer` options, and an unset or nil client falls back
  to `metrics.Default()`/`tracer.Default()`, read per request so a later
  `SetDefault` applies. `New` never panics.
- **Team middleware slot.** Middleware added later with `s.Hertz().Use(...)`
  or on route groups runs after the standard stack, inside Recovery. Hertz
  only appends, so the standard layers can't be removed or reordered by
  accident. This is the documented slot; no extra API is needed. Hertz applies
  `Use` only to routes registered after the call, so team middleware must be
  added before registering routes, or attached to route groups (found in the
  checkpoint A review).
- `Run` serves until `ctx` is cancelled, then shuts down gracefully within
  `ShutdownTimeout`. It returns `nil` after a graceful shutdown and the error
  if the listener fails to start or shutdown times out. The goroutine it
  starts ends before it returns. There is no separate `Shutdown` method.
  Hertz v0.10.6 quirks it absorbs (found in task 2): the default netpoll
  transport panics instead of returning when it can't create the listener,
  so `Run` recovers that panic into an error and keeps netpoll; `Shutdown`
  before the engine is running is ignored, so `Run` waits for the listener
  first; and Hertz returns `nil` when the shutdown deadline expires, so `Run`
  reports the timeout from its own shutdown context.
- `RequestIDFrom` returns the request ID stored by the middleware, or `""`.
- `Error` binds `err` to the request (`c.Error`), writes a problem+json
  response with `status`, and aborts the chain. A `status` outside 400–599 is
  treated as 500.
- The standard middleware constructors are **unexported**. `apiserver`
  exported them; `rest/server` doesn't, because the stack is fixed.

## Behaviour

### Middleware order (outer → inner)

```
requestID → tracing → metrics → logging → recovery → [team middleware] → handler
```

Recovery is innermost, so every outer layer sees the final 500 after a panic
(probed on Hertz v0.10.6: outer middleware saw `route="/p/:id"`, status 500).

### Request ID

- Accept the incoming `X-Request-ID` only if it matches
  `^[A-Za-z0-9._-]{1,64}$`; otherwise generate one with `crypto/rand.Text()`
  (128 bits). nginx's `$request_id` (32 hex characters) is kept.
- Store it in `context.Context` (unexported key, read with `RequestIDFrom`)
  and set it on the `X-Request-ID` response header.

### Tracing

- Extract the incoming `traceparent` with `propagation.TraceContext{}` only.
  Never extract baggage.
- Always start a **new root** span (`trace.WithNewRoot()`). If the incoming
  span context is valid, add it as a span link.
- Span kind Server. Scope `rest.server`. Attributes and status follow the
  Telemetry Contract.

### Recovery

- Recovers a panic, discards any partially built response, writes a 500
  problem body, and records an `exception` event on the active span
  (`exception.type` = Go type of the panic value, `exception.message`,
  `exception.stacktrace`). The `X-Request-ID` header and any
  `Access-Control-*` headers set before the panic are kept, so browser
  frontends can still read the problem body.
- If the response was already streaming (a hijack/chunked writer is in use,
  so headers and data have gone out), recovery doesn't reset or write a
  problem. It records status 500 for telemetry and drops the connection, so
  the client sees a truncated stream instead of a second response inside the
  first.
- Logs `panic recovered` at Error with the panic value and stack. The access
  log line is still written by the logging layer.

### Problem responses (RFC 9457 §3)

`Content-Type: application/problem+json`.

| Field | Value |
| --- | --- |
| `type` | omitted (means `about:blank`) |
| `title` | `http.StatusText(status)`; `Client Error` or `Server Error` when Go has no text for the status (for example 499) |
| `status` | the HTTP status |
| `detail` | 4xx: `errs.Error.Message()` when `err` is an `*errs.Error`, otherwise omitted. 5xx: always omitted |
| `code` | extension: the `errs` code when `err` is an `*errs.Error` with a non-zero code, otherwise omitted |
| `request_id` | extension: always present; the stored request ID, never read back from the response header (handlers may overwrite it). The `X-Request-ID` header is re-set to the same value |

Never in the body: `err.Error()` of non-`errs` errors, `errs` debug data,
causes, metadata, or stack frames. `NoRoute` gives 404, `NoMethod` gives 405,
and a recovered panic gives 500, all with problem bodies.

Known gap: 405 responses have no `Allow` header (RFC 9110 says they should).
Hertz doesn't expose per-method route matching, so computing it would mean
reimplementing the router. Add it if a client needs it.

```json
{"title":"Bad Request","status":400,"detail":"invalid amount","code":1001,"request_id":"KZ4X7Q2M3NWQ5ZB6RTV2DFHJ3A"}
```

### Access log

One line per request through `logs`. Level: 5xx Error, 4xx Warn, otherwise
Info. Fields: `http.request.method`, `url.path`, `http.route` (when matched),
`http.response.status_code`, `duration` (seconds), `request.id` (`logs` normalizes the `request_id` key to this, as `apiserver` already emits it), and bound
errors through `errs.SlogAttr`, at any status: one under `error`, several
under `errors` as a group keyed `0`, `1`, ...
`trace_id` and `span_id` are added by `logs`; the server doesn't add them.

## Telemetry Contract

Public API for the alert/dashboard repo. Attribute keys come from
`semconv/v1.43.0`. Shared by `rest/server` and `rest/client` through
`rest/internal/semconv`.

### Common rules

- **Method normalization.** Known methods (GET, HEAD, POST, PUT, DELETE,
  CONNECT, OPTIONS, TRACE, PATCH) pass through. Anything else becomes
  `_OTHER`, and the original goes in `http.request.method_original` (spans
  only).
- **Span name.** `{method} {http.route}` when a route matched, otherwise
  `{method}`. The method part is `HTTP` when the method is `_OTHER`.
- **Status code** is recorded as an int attribute.
- **No empty attributes.** An attribute with no value is left out, never sent
  as `""`.
- **Metric attributes stay low-cardinality.** Never: request IDs, URLs or
  paths, query strings, user or tenant IDs.
- **`url.scheme`** is `https` or `http` only. Hertz takes the scheme from
  absolute-form request targets, so a raw value would be client-controlled.
- **`url.query` is deliberately omitted** on server spans (semconv: conditionally
  required when present). Query strings at this edge can carry tokens, and
  semconv requires redaction; omitting it is the safe choice.
- **`errs.code`** (custom, int). The code of the first bound error that has a
  non-zero `errs` code, at any status, so alert templates can track business
  error rates on 4xx. It's named after the `errs` package that defines the
  codes; it doesn't claim a semconv namespace (`app.*` is already registered).
  `errs` codes are a bounded set, so cardinality is safe.

### Server metrics

| Name | Instrument | Unit | Attributes |
| --- | --- | --- | --- |
| `http.server.request.duration` | Histogram (`metrics.Record`) | `s` | `http.request.method`, `url.scheme`, `http.route` (if matched), `http.response.status_code`, `network.protocol.version`, `error.type` (5xx only), `errs.code` (when an `errs` code is bound, any status) |
| `http.server.active_requests` | UpDownCounter (`metrics.AddUpDown`) | `{request}` | `http.request.method`, `url.scheme` (same values for +1 and −1) |

Descriptions: "Duration of HTTP server requests." and "Number of active HTTP
server requests."

### Server span

- Kind Server, scope `rest.server`, a new root plus a link to any valid
  incoming context.
- Set at start: `http.request.method` (and `http.request.method_original`),
  `url.path`, `url.scheme`, `http.route` (if matched), `server.address`,
  `server.port`, `network.peer.address`, `network.protocol.version`,
  `user_agent.original`, `http.request.header.x-request-id` (a string array,
  per semconv, holding the request ID in effect: the validated incoming one or
  the generated one, never a rejected raw value).
- Set at end: `http.response.status_code`, `error.type` on 5xx, and
  `errs.code` when bound.
- Status: Error on 5xx, with no description (it's inferred from the status
  code). Unset otherwise, including 4xx.
- `error.type` (server): on 5xx, the status code as a string (`"500"`,
  `"503"`), as HTTP semconv says. Business codes go only in `errs.code`, since
  `errs` codes share number space with HTTP statuses (decided 2026-10-31 after
  the checkpoint B review).

## Code Style

Follow `go-best-practices` and the existing modules. For example:

```go
// Config holds server settings. The zero value is valid: every field has a
// default.
type Config struct {
	// Addr is the listen address in host:port form. Defaults to ":3000".
	Addr string `yaml:"addr" json:"addr"`
	// ShutdownTimeout bounds graceful shutdown in [Server.Run]. Defaults to 10s.
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`
}

func (c Config) validate() error {
	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		return fmt.Errorf("rest/server: addr must be host:port: %w", err)
	}
	return nil
}
```

- Error strings start with `rest/server:`.
- Doc comments on every exported declaration and field; package comment
  `Package server ...` in `server.go`.
- No `//go:noinline`, no mockey, no exported middleware constructors.

## Testing Strategy

- Stdlib `testing`, table-driven where there is more than one case. No
  goconvey, no mockey.
- Tests mirror source files (`middleware.go` → `middleware_test.go`). Shared
  fixtures go in `helpers_test.go`: an in-process Hertz server via
  `ut.PerformRequest`, a `tracetest.SpanRecorder` wrapped by `tracer.Wrap`, a
  recording `metrics.Client`, and a log buffer installed with `logs.New` (`Config.Writer`/`ErrorWriter`) plus `logs.SetDefault`.
- Required tests:
  - **Panic full stack:** one request that panics asserts the duration metric
    with status 500, `active_requests` back to 0, a span with status Error and
    an exception event, the access log at Error, and a problem body with
    `request_id`.
  - **Contract assertions:** exact metric names, units, instrument kinds, and
    attribute sets for 200, 404 (no `http.route`), 400 with an `errs` code (no
    `error.type`, `errs.code` = code), 500 with an `errs` code (`error.type="500"`,
    `errs.code` = code), 500 without one (`error.type="500"`, no
    `errs.code`), and an unknown method (`_OTHER`).
  - **Edge trust:** an incoming valid `traceparent` gives a new trace ID plus a
    link to the incoming context; baggage is ignored; an invalid or oversized
    `X-Request-ID` is replaced; a valid one is kept and echoed.
  - **Problem bodies:** a non-`errs` error has no `detail`; a 5xx never has
    `detail` or `code`; there is no `errs` debug, cause, or stack text in any
    body.
  - **Team middleware:** middleware added with `Hertz().Use` that rejects with
    401 is recorded in the metric and the access log; a panic in team
    middleware is recovered.
  - **`Run`:** cancelling `ctx` returns `nil`, and the port is released; an
    in-use port returns an error.
- Coverage: every exported function and each table row above. No numeric
  coverage gate.

## Boundaries

- **Always:** run the Commands before each commit; update this spec first when
  a decision changes; keep tests in mirror files; keep emitted telemetry
  exactly as in the Telemetry Contract.
- **Ask first:** any new dependency; any change to a metric name, unit,
  instrument, or attribute in the contract; exporting API beyond Public API;
  changing CI or `release.yml`; adding `Config` fields.
- **Never:** extract baggage or join an incoming trace; put `errs`
  debug/cause/stack or non-`errs` error text in a response body; record
  unbounded values as metric attributes; use mockey or `//go:noinline`; reuse
  a version that proxy.golang.org already has. Releases are manual
  (`release.yml` on `workflow_dispatch`, 2026-11-01): all tags were deleted
  and every module is in development, so `rest` and `cron` are tagged by hand
  with a v0 version.

## Success Criteria

- [ ] `apiserver/` and `cron/server/` are deleted; `go.work`, `cron/go.mod`,
      `cron/README.md`, and the root `README.md` are updated; all Commands
      pass in `rest/` and `cron/`.
- [ ] The panic full-stack test passes: one panic gives a 500 in the metric,
      the span, the access log, and the problem body.
- [ ] The contract tests assert every row of "Server metrics" and "Server
      span" exactly, with no other attributes emitted.
- [ ] No response body contains `errs` internals or non-`errs` error text (a
      table test over 4xx, 5xx, and panic).
- [ ] An incoming `traceparent` never becomes the parent of the server span.
- [ ] `rest/README.md` documents the middleware order, the team middleware
      slot, and the contract. No migration guide: the SDK is in development
      and has no production consumers (2026-11-01), so backward
      compatibility is not a goal.

## Open Questions

None. Resolved 2026-10-31:

- Alert templates do use the 4xx business-error signal. `error.type` follows
  semconv (5xx only), and the signal moves to the custom `errs.code`
  attribute.
- Access-log keys switch to semconv names.
