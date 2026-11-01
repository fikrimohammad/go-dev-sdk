# Implementation Plan: rest module (rest/server + rest/client)

Specs: [SPEC-rest-server](../docs/specs/rest/SPEC-rest-server.md),
[SPEC-rest-client](../docs/specs/rest/SPEC-rest-client.md),
[capability map](../docs/specs/rest/capability-map.md). Tasks:
[tasks/todo.md](todo.md).

## Overview

Build the `rest` Go module, which replaces `apiserver`. `rest/server` is a
Hertz server with a fixed, observable middleware stack, edge-trust rules, and
RFC 9457 errors. `rest/client` is a Hertz client with safe retries, deadlines
that hold, and the same telemetry, plus a `RoundTripper` for vendor SDKs. The
Telemetry Contract in SPEC-rest-server is a public API for the alert repo.
Server ships first (tasks 1–7), then client (tasks 8–13).

## Architecture Decisions

- **Contract first.** `rest/internal/semconv` (task 1) holds instrument
  descriptors, method normalization, span naming, and the `error.type` and
  `errs.code` rules. Server and client both use it, so the contract has one
  implementation.
- **Recovery innermost.** The order is
  `requestID → tracing → metrics → logging → recovery → team → handler`.
  Probed: outer middleware sees the route and the final 500 after a panic.
- **No exported middleware.** The stack is fixed; team middleware goes through
  `Hertz().Use`, which only appends, so it lands in the slot automatically.
- **Retry is our middleware, not Hertz's.** It runs outside tracing and
  metrics, so there's one span and one measurement per attempt (probed). Hertz
  `WithRetryConfig` and `RetryIfFunc` are never set.
- **Deadline conversion.** Hertz ignores a ctx deadline in flight (probed), so
  each attempt gets `WithRequestTimeout(min(AttemptTimeout, time.Until(deadline)))`,
  and Hertz timeouts are wrapped to match `context.DeadlineExceeded`.
- **`RoundTripper` uses its own conversion**, not `adaptor.CopyToHertzRequest`
  (that reads the empty client `RequestURI` and streams the body).
- **Key normalization is a given.** `logs` and `metrics` normalize keys
  without a dot (`request_id` → `request.id`); contract keys all contain a
  dot. Span attributes that must keep an exact key are set with
  `attribute.*` directly.
- **Releases:** manual. `release.yml` runs only on `workflow_dispatch` with a
  module and version (all tags deleted 2026-11-01; every module is v0). The
  first tag is `rest/v0.1.0`; `cron` gets a v0 version that proxy.golang.org
  doesn't already have (v0.1.0 and v0.1.1 are taken).

## Task List

### Phase 1: Foundation

- [x] Task 1: Module scaffold and `rest/internal/semconv`

### Phase 2: rest/server

- [x] Task 2: Server lifecycle: `Config`, `New`, `Hertz`, `Run(ctx)`
- [x] Task 3: Request ID, recovery, and problem+json responses
- [x] Task 4: Server tracing (edge trust, span contract, panic exception)

### Checkpoint A: server behaviour (passed, review fixes applied)

- [x] Task 5: Server metrics and access log (full contract, panic full stack, team slot)
- [x] Task 6: `rest/README.md` server section and doc comments

### Checkpoint B: server complete (passed, review fixes applied)

- [x] Task 7: Delete `apiserver` and `cron/server`; update `go.work`, `cron`, and READMEs

### Checkpoint C: rest-server shipped (rest/v0.1.0 tagged by hand); alert-repo owner has the changes table

### Phase 3: rest/client

- [x] Task 8: Client `Config` and `New` (dial, TLS, proxy)
- [x] Task 9: Deadlines, timeout wrapping, and logging middleware
- [x] Task 10: Retry middleware

### Checkpoint D: client behaviour (passed, review fixes applied)

- [x] Task 11: Client tracing and metrics (per-attempt, hygiene, contract)
- [x] Task 12: `RoundTripper`
- [x] Task 13: `rest/parity_test.go` and the README client section

### Checkpoint E: complete (final review passed after fixes; ship once merged)

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Hertz `Run`/`Shutdown` semantics (what `Run` returns after `Shutdown`, port release) don't fit `Run(ctx)` | Med | Task 2 reads `pkg/route/engine.go` first and tests cancel → `nil` plus port reuse; adjust the documented return values in the spec if Hertz forces it |
| Custom `NoRoute`/`NoMethod` handlers skip global middleware, so 404s go missing from metrics | Med | Task 3 test asserts a 404 is recorded by an outer middleware; the probe already showed this for the default handler |
| A Hertz client middleware chain set with `Use` behaves differently from `c.Do` for redirects or proxies | Med | Task 8 proxy test plus task 10 retry tests run through real `httptest` servers |
| `errs.code` or log-key changes break existing alerts or log queries | High | Checkpoint C: the alert-repo owner gets the "Changes from apiserver" table before the release merges |
| `cron/server` deletion breaks an importer without a major bump | Low (accepted) | Ordinary commit by decision; note it in the `cron` release notes |
| The `RoundTripper` conversion drops headers, `Host`, or multi-value headers | Med | Task 12 round-trip table test; task 13 parity test |
| A semconv detail (span name for `_OTHER`, status rules) is misread | Low | Task 1 encodes the rules in table tests citing semconv v1.43.0 |

## Open Questions

None. All spec questions were resolved on 2026-10-31.
