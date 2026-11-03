# Capability Map: SDK review fix backlog

Status: approved 2026-11-01. It mirrors the PR plan approved in
the 2026-11-01 interview. Design notes, decisions, and the item index (X1, N18, …):
[docs/ideas/sdk-fix-backlog.md](../../ideas/sdk-fix-backlog.md).

| Module id | Responsibility | Depends on |
| --- | --- | --- |
| `foundations` | `observability`, `errs`, `errgroup`, `identity`, plus caller compile/doc fixes. Adds the strict-lint CI ratchet and the exported-interface convention. | — |
| `data-clients` | `db`, `redis`, `s3`: telemetry cardinality, semconv names, ctx constructors, `redis.UniversalClient`, testing recipes. | `foundations` |
| `messaging` | `nats`, `cron`: middleware order, lifecycle bugs, N18/N20/N21 (schedules, no dedup, `consumer.Message`), stream ownership. | `foundations` |
| `guardrails` | gostyle telemetry rule; strict lint becomes the default (confloader excluded). | `data-clients`, `messaging` |
| `release` | Module path moves to `go-servicekit`; every module starts at v0.1.0; atomic multi-module release with an installability check (added 2026-11-02, ships in PR 4). | `guardrails` |
| `test-transports` | db, redis, s3, and nats tests run against in-process fakes; the transport stubs and seams are deleted (added 2026-11-02, ships in PR 4). | `data-clients`, `messaging` |

Build order: `foundations` → `data-clients`, `messaging` (in either order; they touch
disjoint modules) → `guardrails`. Each module is one PR.

Specs:

- [SPEC-foundations.md](SPEC-foundations.md)
- [SPEC-data-clients.md](SPEC-data-clients.md) (approved 2026-11-02)
- [SPEC-messaging.md](SPEC-messaging.md) (approved 2026-11-02)
- [SPEC-guardrails.md](SPEC-guardrails.md) (approved 2026-11-02; covers `guardrails`, `release`, `test-transports`)

Out of scope for every module: confloader's logic (redesign track, compile-only changes
here), the release/versioning repair (its own track), and the testing app (migrated once
after `guardrails`).
