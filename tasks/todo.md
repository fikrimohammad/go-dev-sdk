# Tasks: rest module

Plan: [tasks/plan.md](plan.md). Specs: `docs/specs/rest/`.

Every task also meets the AGENTS.md Definition of Done. "Module checks" means,
from `rest/`:

```bash
gofmt -l . && go vet ./... \
  && go test -count=1 -race -shuffle=on -gcflags="all=-N -l" ./... \
  && golangci-lint run --new-from-merge-base=main ./... \
  && GOWORK=off go run ../.agents/skills/go-best-practices/scripts/gostyle.go ./... \
  && go mod tidy -diff
```

---

## Phase 1: Foundation

## Task 1: Module scaffold and `rest/internal/semconv`

**Description:** Create the `rest` module and the shared contract package:
instrument descriptors (`http.server.request.duration`,
`http.server.active_requests`, `http.client.request.duration` with units and
descriptions), method normalization (`_OTHER` plus the original), span naming
(`{method} {route}`, `{method}`, `HTTP`), protocol-version parsing, and the
`error.type` and `errs.code` rules for server and client.

**Acceptance criteria:**
- [ ] `rest/go.mod` (Go 1.26.5, local `replace` for `observability`, `errs`, `identity`) and `./rest` in `go.work`.
- [ ] Table tests cover every method, span-name case, `error.type` case (server 5xx with and without an `errs` code, 4xx none; client ≥400 and `timeout`/`canceled`/`_OTHER`), and `errs.code` presence.
- [ ] Attribute keys come from `semconv/v1.43.0` constants, not string literals.

**Verification:**
- [ ] Module checks pass.
- [ ] `go doc -all ./internal/semconv` reads cleanly.

**Dependencies:** None

**Files likely touched:** `rest/go.mod`, `rest/go.sum`, `go.work`, `rest/internal/semconv/semconv.go`, `rest/internal/semconv/semconv_test.go`

**Estimated scope:** M

---

## Phase 2: rest/server

## Task 2: Server lifecycle: `Config`, `New`, `Hertz`, `Run(ctx)`

**Description:** `Config` with defaults and validation (no dead checks), `New`
(telemetry via `WithMetrics`/`WithTracer`, falling back to the defaults), `Hertz()`, and `Run(ctx)`: serve until `ctx` is
cancelled, shut down within `ShutdownTimeout`, and end its goroutine before
returning. Read Hertz `engine.go` `Run`/`Shutdown` first (see the risk in the
plan). Also add `helpers_test.go` fixtures: span recorder, recording metrics,
log buffer, and an in-process request helper.

**Acceptance criteria:**
- [ ] Config table test: defaults applied; invalid `Addr`, negative timeouts, and zero `ShutdownTimeout` after defaults are rejected, with `rest/server:` errors.
- [ ] Cancelling `ctx` makes `Run` return `nil`, and the port can be bound again right after.
- [ ] An in-use port makes `Run` return an error.

**Verification:**
- [ ] Module checks pass.

**Dependencies:** Task 1

**Files likely touched:** `rest/server/server.go`, `rest/server/server_test.go`, `rest/server/config.go`, `rest/server/config_test.go`, `rest/server/helpers_test.go`

**Estimated scope:** M

## Task 3: Request ID, recovery, and problem+json responses

**Description:** The `requestID` middleware (validate against
`^[A-Za-z0-9._-]{1,64}$`, else `crypto/rand.Text()`; store in ctx; echo the
header), `RequestIDFrom`, `recovery` (discard the partial response, write a
500 problem, log `panic recovered` with the stack), `Error(c, status, err)`,
and problem+json `NoRoute`/`NoMethod` handlers, registered by `New`.

**Acceptance criteria:**
- [ ] Request-ID table: valid IDs kept and echoed; empty, oversized, or bad-charset IDs replaced; nginx-style 32-hex IDs kept; `RequestIDFrom` works in handlers.
- [ ] Problem table: RFC 9457 fields exactly as in the spec for `errs` 4xx, non-`errs` 4xx (no `detail`), 5xx (no `detail`/`code`), panic, 404, 405, and an out-of-range status (→ 500); `Content-Type: application/problem+json`; no `errs` debug, cause, or stack text in any body.
- [ ] A 404 from `NoRoute` is still seen by an outer middleware (route `""`, status 404).

**Verification:**
- [ ] Module checks pass.

**Dependencies:** Task 2

**Files likely touched:** `rest/server/middleware.go`, `rest/server/middleware_test.go`, `rest/server/problem.go`, `rest/server/problem_test.go`, `rest/server/server.go`

**Estimated scope:** M

## Task 4: Server tracing (edge trust, span contract, panic exception)

**Description:** The `tracing` middleware: extract with
`propagation.TraceContext{}` only, always start a new root with a link to a
valid incoming context, span kind Server, scope `rest.server`, the
start/end attributes and status rules from the contract. `recovery` adds the
`exception` event to the active span.

**Acceptance criteria:**
- [ ] Edge trust: an incoming valid `traceparent` → a different trace ID plus one link to the incoming span context; `baggage` ignored; no incoming context → no link.
- [ ] Span contract table: name, kind, and attribute set for 200, a matched route, 404 (name `GET`), `_OTHER` (name `HTTP`, `method_original` set), 400 (status Unset), and 500 (status Error, `error.type`, `errs.code`); `http.request.header.x-request-id` is a string array.
- [ ] Panic → span status Error plus an `exception` event with type, message, and stacktrace.

**Verification:**
- [ ] Module checks pass.

**Dependencies:** Task 3

**Files likely touched:** `rest/server/middleware.go`, `rest/server/middleware_test.go`, `rest/server/server.go`, `rest/server/helpers_test.go`

**Estimated scope:** S

### Checkpoint A: server behaviour

- [ ] Module checks pass.
- [ ] Edge-trust and problem-body tests green.
- [ ] Quick human look at the middleware order in `server.go`.

## Task 5: Server metrics and access log (full contract, panic full stack, team slot)

**Description:** The `metrics` middleware (duration histogram, active-requests
up-down with the same attributes for +1/−1, `error.type` 5xx only, `errs.code`
at any status) and the `logging` middleware (5xx Error, 4xx Warn, else Info;
semconv keys; `request.id`; bound errors through `errs.SlogAttr`; no manual
`trace_id`).

**Acceptance criteria:**
- [ ] Metric contract table: exact names, units, instrument kinds, and attribute sets for every case in the spec's Testing Strategy, with no extra attributes and no empty values.
- [ ] Panic full stack: one panicking request → duration metric with 500, active requests back to 0, span Error plus exception, access log at Error, and a problem body with `request_id`.
- [ ] Team slot: `Hertz().Use` middleware returning 401 is recorded in the metric and the access log; a panic in team middleware is recovered.

**Verification:**
- [ ] Module checks pass.

**Dependencies:** Task 4

**Files likely touched:** `rest/server/middleware.go`, `rest/server/middleware_test.go`, `rest/server/helpers_test.go`

**Estimated scope:** S

## Task 6: `rest/README.md` server section and doc comments

**Description:** README with usage, middleware order, team middleware slot,
problem+json format, and the Telemetry Contract. Review every doc comment in
`rest/server`. (The apiserver migration table was dropped on 2026-11-01: no
backward compatibility while the SDK is in development.)

**Acceptance criteria:**
- [ ] README covers each item above, with a runnable-looking example.
- [ ] `go doc -all ./server` shows no unresolved `[links]`, and every exported identifier and field is documented.

**Verification:**
- [ ] Module checks pass (gostyle and lint cover the doc comments).

**Dependencies:** Task 5

**Files likely touched:** `rest/README.md`, `rest/server/*.go` (comments only)

**Estimated scope:** S

### Checkpoint B: server complete; human review

- [ ] Every rest-server success criterion except the deletion is met.
- [ ] Human reviews `rest/server` before the deletion.

## Task 7: Delete `apiserver` and `cron/server`; update `go.work`, `cron`, and READMEs

**Description:** Remove `apiserver/` and `cron/server/`. Drop `./apiserver`
from `go.work`; drop the `apiserver` require and replace from `cron/go.mod` and
tidy; remove the dev REST API section from `cron/README.md` (note that it moves
to RPC); replace the `apiserver` row in the root `README.md` with `rest`.
Ordinary commit, no `!`.

**Acceptance criteria:**
- [ ] `grep -rn "go-dev-sdk/apiserver"` finds nothing outside `docs/` and git history.
- [ ] `cron` builds, and its tests and module checks pass.
- [ ] Releases are manual, so commit markers don't create tags.

**Verification:**
- [ ] Module checks pass in `rest/` and `cron/`.
- [ ] `go build ./...` from each module in `go.work` succeeds.

**Dependencies:** Task 6

**Files likely touched:** `apiserver/` (deleted), `cron/server/` (deleted), `go.work`, `cron/go.mod`, `cron/go.sum`, `cron/README.md`, `README.md`

**Estimated scope:** S (mostly deletions)

### Checkpoint C: rest-server shipped

- [ ] Merged; `rest/v0.1.0` and a new `cron` v0 version (not v0.1.0 or v0.1.1) tagged with `gh workflow run release.yml`.

---

## Phase 3: rest/client

## Task 8: Client `Config` and `New` (dial, TLS, proxy)

**Description:** `Config` with defaults and validation (including `Proxy`
parsing as `http`/`https`), and `New` building the Hertz client with dial
timeout, TLS, and proxy (`SetProxy(protocol.ProxyURI(...))`), and no Hertz
retry. Add `helpers_test.go` with `httptest` vendors, an `httptest` proxy, a
span recorder, and recording metrics.

**Acceptance criteria:**
- [ ] Config table test: defaults, negative durations, `MaxAttempts` < 1, `BackoffBase` > `BackoffMax`, and invalid proxy URLs rejected with `rest/client:` errors.
- [ ] With `Proxy` set, a request reaches the vendor through the proxy.
- [ ] Nil `mc`/`tc` panics.

**Verification:**
- [ ] Module checks pass.

**Dependencies:** Task 1 (and Checkpoint C)

**Files likely touched:** `rest/client/client.go`, `rest/client/client_test.go`, `rest/client/config.go`, `rest/client/config_test.go`, `rest/client/helpers_test.go`

**Estimated scope:** M

## Task 9: Deadlines, timeout wrapping, and logging middleware

**Description:** The `logging` middleware (outermost: applies `cfg.Timeout`
when ctx has no deadline; logs the final outcome, Warn on error or ≥400, Debug
otherwise, with the spec's fields). Per-attempt
`WithRequestTimeout(min(AttemptTimeout, time.Until(deadline)))`; stop without
sending when no time is left; wrap Hertz `ErrTimeout` as
`rest/client: %w: %w` with `context.DeadlineExceeded`. Highest-risk client
task, so it comes first.

**Acceptance criteria:**
- [ ] A slow vendor with only a ctx deadline (50ms vs 500ms) returns within about 50ms, with `errors.Is(err, context.DeadlineExceeded)` and `errors.Is(err, herrs.ErrTimeout)` both true.
- [ ] The same with only `AttemptTimeout`, and with only `cfg.Timeout`.
- [ ] Log line fields and levels as in the spec; no query string or userinfo.

**Verification:**
- [ ] Module checks pass.

**Dependencies:** Task 8

**Files likely touched:** `rest/client/middleware.go`, `rest/client/middleware_test.go`, `rest/client/client.go`

**Estimated scope:** S

## Task 10: Retry middleware

**Description:** Retryability (idempotent method or `Idempotency-Key`, and
`!IsBodyStream()`), retry outcomes (network error, per-attempt timeout, 429,
502, 503, 504), full-jitter backoff, `Retry-After` (seconds and HTTP date,
capped by `MaxRetryAfter` and the deadline), cancellable wait, `resp.Reset()`,
and the attempt index in ctx.

**Acceptance criteria:**
- [ ] Retry table from the spec: vendor hit counts and final status for each method/outcome row.
- [ ] `Retry-After: 1` waits about 1s; above `MaxRetryAfter` → returns the 429 without retrying; HTTP-date form parsed.
- [ ] Cancelling ctx mid-backoff returns `context.Canceled` promptly; no `time.Sleep` in the package.

**Verification:**
- [ ] Module checks pass.

**Dependencies:** Task 9

**Files likely touched:** `rest/client/middleware.go`, `rest/client/middleware_test.go`

**Estimated scope:** S

### Checkpoint D: client behaviour

- [ ] Module checks pass.
- [ ] Deadline and retry tables green under `-race -shuffle=on`.

## Task 11: Client tracing and metrics (per-attempt, hygiene, contract)

**Description:** The `tracing` middleware (one Client span per attempt, scope
`rest.client`, name `{method}`/`HTTP`, `resend_count` from ctx, redacted
`url.full`, status Error on ≥400 or failure, no propagation injection) and the
`metrics` middleware (`http.client.request.duration` per attempt with contract
attributes).

**Acceptance criteria:**
- [ ] GET 503 ×3 → 3 spans with `resend_count` [absent, 1, 2] and 3 duration measurements.
- [ ] The vendor never receives `traceparent`, `tracestate`, `baggage`, or `X-Request-ID`; `url.full` has userinfo removed and query values set to `REDACTED`.
- [ ] Contract table for 200, 404, 503-after-retries, and a timeout (`error.type=timeout`); `server.address` is the vendor even with `Proxy` set.

**Verification:**
- [ ] Module checks pass.

**Dependencies:** Task 10

**Files likely touched:** `rest/client/middleware.go`, `rest/client/middleware_test.go`, `rest/client/helpers_test.go`

**Estimated scope:** S

## Task 12: `RoundTripper`

**Description:** `RoundTripper(c)`: build the Hertz request from
`req.URL.String()`, method, headers, and `Host`; read and close `req.Body`;
`SetBody`; `c.Do(req.Context(), ...)`; build the `*http.Response` (status,
`Status`, `Proto`, multi-value headers, `ContentLength`, `Request`, copied
body); release the Hertz objects; pass errors through.

**Acceptance criteria:**
- [ ] Round-trip table: URL with query, method, multi-value headers, `Host` override, and body arrive intact at an `httptest` vendor; the response maps back exactly.
- [ ] `req.Body` is closed on success and on error; a POST with `Idempotency-Key` is re-sent with its body on retry.
- [ ] Through `&http.Client{Transport: ...}`, timeouts match `context.DeadlineExceeded` despite `*url.Error`.

**Verification:**
- [ ] Module checks pass.

**Dependencies:** Task 11

**Files likely touched:** `rest/client/roundtripper.go`, `rest/client/roundtripper_test.go`

**Estimated scope:** S

## Task 13: `rest/parity_test.go` and the README client section

**Description:** A module-root parity test (`package rest_test`, like
`nats/parity_test.go`): the same scenarios (200, retried 503, timeout) through
`c.Do` and through the `RoundTripper` produce identical metric names, units,
attributes, and span sets, and a `rest/client` → `rest/server` call yields two
separate traces. Add the README client section (`New`, defaults, retry rules,
the `errors.Is` timeout pattern, the SDK wrap, "don't add retry on top").

**Acceptance criteria:**
- [ ] The parity test passes under `-race -shuffle=on`.
- [ ] Client → server: different trace IDs, and the server span has no link (the client injects nothing).
- [ ] README client section covers each item above; `go doc -all ./client` is clean.

**Verification:**
- [ ] Module checks pass.

**Dependencies:** Task 12

**Files likely touched:** `rest/parity_test.go`, `rest/README.md`, `rest/client/*.go` (comments only)

**Estimated scope:** S

### Checkpoint E: complete

- [ ] Every success criterion in SPEC-rest-server and SPEC-rest-client is checked.
- [ ] Human review, then merge (expected tag: `rest/v0.2.0`, from a `feat:` commit).
