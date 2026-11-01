# rest/server + rest/client

## Problem Statement

How might we make every external HTTP boundary — inbound and outbound — safe
and observable by default, so a service cannot ship with invisible panics,
leaked internals, or retry bugs?

## Recommended Direction

Replace `apiserver` with one `rest` module holding two packages, mirroring
`nats/producer` + `nats/consumer`. These are company core libraries, owned by
developer experience. Domain teams use them to build use-case-specific gateway
services, which handle frontend traffic and vendor webhooks and call vendors
(payment gateway, tax invoicing) on behalf of internal RPC services. In this
SDK, REST means "the external boundary" (internal traffic is RPC), so both
packages distrust the other side by default.

Emitted metric names, units, and attributes are a public contract: a separate
repo builds alert and dashboard templates on them. Changing any of them is a
breaking change for that repo. Both are built on Hertz and share HTTP semconv helpers (method
normalization to `_OTHER`, protocol parsing, attribute builders) through
`rest/internal`. All telemetry goes through the injected `metrics.Client` and
`tracer.Client` from `observability`, using registered keys from
`go.opentelemetry.io/otel/semconv/v1.43.0`. Instrumentation scopes are
`rest.server` and `rest.client`, following `redis.client`. `observability`
needs no changes.

**`rest/server`** keeps Hertz and fixes the current stack. Middleware order,
outer to inner, is RequestID → Tracer → Metrics → Logger → Recovery →
team middleware → handler, so every layer records a panic as a 500. Today
panics reach no metric, no access log, and no span status. The standard layers
are fixed; teams can't remove or reorder them. Team middleware (for example,
tax-webhook signature checks) goes in the documented slot inside Recovery, so
its rejections and panics are still observed. Telemetry follows HTTP semconv:
`http.server.request.duration` plus `http.server.active_requests`; span name
`{method} {route}`, or `{method}` alone when no route matched; span status
Error only on 5xx; `error.type` set only on failure; `http.response.status_code`
recorded as an int; unknown methods normalized to `_OTHER`; no empty-string
attributes. As an internet edge, the server extracts the incoming
`traceparent` with `propagation.TraceContext{}` only (no baggage), starts a
new root span, and links to the caller's context instead of joining it. It
accepts an incoming `X-Request-ID` only if it matches `^[A-Za-z0-9._-]{1,64}$`
and otherwise generates one with stdlib `crypto/rand.Text()` (128 bits). The ID goes into `context.Context`, the
access log, and the span as `http.request.header.x-request-id`. Errors are
rendered as RFC 9457 `application/problem+json` by
`server.Error(c, status, err)`: the handler states the status, and 5xx
responses always carry a generic `detail`. `errs` debug data, causes, and
stack traces never reach the response body. The body follows RFC 9457 §3
exactly: `type` is omitted (meaning `about:blank`), so `title` is
`http.StatusText(status)`; `status` matches the HTTP status; `detail` is the
`errs` message on 4xx and generic on 5xx. The only extension members are
`request_id` and `code` (the `errs` code). Log levels: 5xx Error, 4xx Warn, everything else
Info. `trace_id` comes from `logs` automatically. `Run(ctx)` shuts down
gracefully when ctx is cancelled and honours `ShutdownTimeout`.

**`rest/client`** is `New(cfg, opts...) (*hertzclient.Client, error)`: the
plain Hertz client with middleware installed, no wrapper type. The chain,
outer to inner:

```
Logger  → final outcome once (Warn on failure, Debug on success)
Retry   → our loop: idempotent method or Idempotency-Key header,
          !req.IsBodyStream(), on network error / 429 / 502 / 503 / 504;
          full-jitter backoff, capped Retry-After, select on ctx.Done(),
          resp.Reset() between attempts
Tracer  → one client span per attempt, http.request.resend_count,
          no traceparent injection
Metrics → http.client.request.duration per attempt
c.do
```

The overall deadline comes from ctx, or `cfg.Timeout` when ctx has none. The
Hertz client doesn't honour a ctx deadline once a request is in flight
(`HostClient.do` takes no ctx), so each attempt gets
`WithRequestTimeout(min(cfg.AttemptTimeout, time.Until(deadline)))`. Hertz
reports timeouts as its own `errs.ErrTimeout`, so the client wraps them as
`fmt.Errorf("rest/client: %w: %w", context.DeadlineExceeded, err)`, and
`errors.Is(err, context.DeadlineExceeded)` works. Handling an ambiguous
outcome (a POST that timed out after the vendor may have processed it) is the
domain team's job; the library never retries such a POST without an
`Idempotency-Key`.
Nothing internal leaks to vendors: no `traceparent`, no `X-Request-ID`, and
query strings are redacted in logs and in `url.full`.

For vendor SDKs (the payment gateway SDK takes an HTTP-client interface that
teams wrap), `client.RoundTripper(hc) http.RoundTripper` exposes the same
Hertz client to net/http through a small hand-written conversion: the URI
from `req.URL.String()` and a buffered body on the way in, and an
`*http.Response` (status, headers, buffered body) on the way out. Hertz's
`adaptor.CopyToHertzRequest` can't be used: it reads `req.RequestURI`, which
is empty on client requests, and it streams the body, which makes requests
non-retryable. Teams wrap SDKs with `&http.Client{Transport: client.RoundTripper(hc)}`.
Retry, timeouts, and telemetry are implemented once, in the Hertz middleware
chain; teams don't add a second retry layer on top.

Hertz's built-in retry (`WithRetryConfig` / `RetryIfFunc`) is deliberately
unused. Its loop runs inside `HostClient.Do`, below client middleware, so all
attempts collapse into one span. It waits with `time.Sleep`, ignoring ctx. Its
delay policy never sees the response, so it can't honour `Retry-After`. And
`MaxAttemptTimes` has no effect without a custom `RetryIfFunc`
(`pkg/protocol/http1/client.go:365` in v0.10.6).

## Key Assumptions to Validate

- [x] A real vendor integration exists: payment gateway and government tax
      invoicing.
- [x] How vendors are called: the tax API is hand-written; the payment
      gateway goes through its SDK, which teams wrap around the internal HTTP
      client. Covered by `client.RoundTripper`.
- [x] Nothing instrumented sits in front of the server: a cloud load balancer
      plus nginx. No `TrustIncomingTrace` knob. An nginx `$request_id` (32 hex
      characters) passes the request-ID check and is kept.
- [ ] `RoundTripper` parity: an `*http.Client` using `client.RoundTripper`
      emits the same metric names, units, attributes, and per-attempt spans as
      the Hertz client, asserted by one shared test.
- [x] No dashboard depends on `http.server.request.count`: every team already
      uses the duration histogram.
- [x] Hertz timeouts need wrapping. Probed: `WithRequestTimeout(50ms)` returns
      Hertz `timeout` with `errors.Is(err, context.DeadlineExceeded) == false`.
      With only a 50ms ctx deadline, a 500ms request returned after 500ms with
      `err == nil` and status 200: Hertz ignores the ctx deadline in flight, so
      converting it into `WithRequestTimeout` is required. Regression test: a
      slow `httptest` server asserts `errors.Is(err, context.DeadlineExceeded)`
      and an early return for both the per-attempt timeout and the ctx
      deadline.
- [x] Outer middleware sees the route (`FullPath`) and the final 500 after a
      recovered panic. Probed on Hertz v0.10.6: `route="/p/:id" status=500`;
      an unmatched route gives `route=""`, 404, so the `{method}` span-name
      fallback is required. Keep as a regression test: the full-stack panic
      test asserts the metric, span status, and access log.
- [x] Client Tracer middleware runs once per attempt. Probed: a retry
      middleware calling `next` 3 times ran the inner middleware 3 times, and
      the server saw 3 requests with the POST body re-sent. Keep as a
      regression test asserting N spans with `resend_count` 0..N-1.

## MVP Scope

**Slice 1, `rest/server`:** corrected middleware order; semconv fixes;
edge-trust rules for trace context and request ID; `server.Error` with
problem+json; Recovery records the exception on the span and writes a 500
problem; log levels; `Run(ctx)`. Delete `apiserver` and `cron/server`
together, and update `cron/README.md` and `cron/go.mod`. `cron/server` returns
on RPC once an RPC library exists. Tests run through a real Hertz server, without mockey or
`//go:noinline`.

**Slice 2, `rest/client`:** `New` returning `*hertzclient.Client`; overall and
per-attempt timeouts; the Retry, Tracer, Metrics, and Logger middleware
above; `RoundTripper` for vendor SDKs.

**Out:** everything below.

## Not Doing (and Why)

- **Circuit breaker:** deferred by choice. Its place is reserved: per host,
  inside Retry, with an open breaker returning a non-retryable
  `ErrCircuitOpen`. Add it when a vendor outage shows retries alone make
  things worse.
- **Hertz built-in retry:** see above; one span for all attempts, waits that
  ignore ctx, no `Retry-After`.
- **Moving to net/http:** Hertz is kept on both sides, for one stack.
- **`TrustIncomingTrace`:** nothing in front of the gateways (cloud load
  balancer plus nginx) starts traces.
- **Streaming bodies through `RoundTripper`:** request and response bodies are
  buffered, as on the Hertz client.
- **HTTP/2 on the client:** the Hertz client speaks HTTP/1.1. Add
  `hertz-contrib/http2` if a vendor requires HTTP/2.
- **Streaming request/response bodies:** retry and timeouts assume buffered
  bodies.
- **Propagating request ID, trace context, or baggage to vendors, or accepting
  baggage at the edge:** it leaks internals out and lets strangers' data in.
- **Mapping `errs` codes to HTTP status:** the handler states the status;
  `errs` stays HTTP-agnostic.
- **Changes to `observability`:** it already covers everything needed;
  HTTP-specific helpers live in `rest/internal`.
- **Retry budgets, per-host policy config, rate limiting:** YAGNI until there
  are several vendors with different needs.
- **Health/readiness endpoints:** a service-level concern of a few lines.
- **A compatibility shim for `apiserver`:** it is deleted. Hotfixes branch off
  the last tag Go can resolve, `apiserver/v1.4.1` (see Known Issues).
- **Moving `cron/server` to `rest/server`:** it's an internal API, so it
  belongs on RPC. It's deleted with `apiserver` and rebuilt on the future RPC
  library.
- **Alert rules and dashboards:** they live in a separate repo, built on the
  metric contract.
- **Webhook signature verification:** vendor-specific; teams add it as team
  middleware.
- **hertz-contrib/obs-opentelemetry:** our semconv code is small enough to own
  and test directly.

## Known Issues (separate task)

- v2+ tags for `apiserver`, `db`, `observability`, `redis`, `rocketmq`, and
  `s3` sit on module paths with no `/vN` suffix, so Go can't resolve them
  (`go get …@v2.x` fails). This blocks the "security fix = version bump" goal
  for those modules. Fix: add `/vN` to each module path, update importers,
  retag. Root cause: `release.yml` bumps the major version on any `!` or
  `BREAKING CHANGE` commit without checking for a `/vN` module path. The new
  `rest` module starts at `rest/v0.1.0` (ordinary `feat:` commits). The
  `cron/server` deletion uses an ordinary commit (tagged `cron/v1.2.0`) by
  decision, to avoid creating another unresolvable `cron/v2.0.0`.

## Open Questions

None. Resolved: problem+json follows RFC 9457 §3 (`type` omitted); `cron/server`
moves to RPC and is deleted for now; `apiserver` is deleted.
