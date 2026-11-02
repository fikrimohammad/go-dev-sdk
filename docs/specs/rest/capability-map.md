# Capability Map: rest module

Approved 2026-10-31. Intent: [docs/intent/rest-server-client.md](../../intent/rest-server-client.md).
Design notes: [docs/ideas/rest-server-client.md](../../ideas/rest-server-client.md).

| Module id | Responsibility | Depends on |
| --- | --- | --- |
| `rest-server` | `rest/server` + `rest/internal/semconv`. Owns the **telemetry contract** that the alert/dashboard repo depends on. Deletes `apiserver` and `cron/server`. | `observability`, `errs/v2`, Hertz v0.10.6 |
| `rest-client` | `rest/client`: Hertz client with retry, timeouts, telemetry, and `RoundTripper` for vendor SDKs. Conforms to the contract in `rest-server`. | `rest-server` (`rest/internal/semconv` + contract) |

Build order: `rest-server` → `rest-client`.

Specs:

- [SPEC-rest-server.md](SPEC-rest-server.md)
- [SPEC-rest-client.md](SPEC-rest-client.md)

Release: one Go module, `github.com/fikrimohammad/go-servicekit/rest`, tagged
`rest/v0.1.0` by hand with `release.yml` (manual since 2026-11-01). Version
1.0.0 is a deliberate later decision.

The broken v2+ tags are gone: all tags were deleted on 2026-11-01, and
`release.yml` now refuses a major that doesn't match the module path.
