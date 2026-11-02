# Implementation Plan: messaging (SDK fix backlog, PR 3)

Spec: [SPEC-messaging](../docs/specs/sdk-fix-backlog/SPEC-messaging.md) (approved 2026-11-02).
Capability map: [capability-map](../docs/specs/sdk-fix-backlog/capability-map.md). Item index:
[docs/ideas/sdk-fix-backlog.md](../docs/ideas/sdk-fix-backlog.md#item-index). Tasks:
[tasks/todo.md](todo.md). PR 1 shipped as #16 and PR 2 as #17.

## Overview

One PR on branch `feat/messaging`, one commit per task. It fixes nats's delayed publish,
silent dedup, lifecycle, and handler contract, and cron's lifecycle. It moves both modules'
telemetry to semconv v1.43.0 with bounded cardinality, and makes both strict on full-tree lint.

## Execution Roles (same as PR 1 and PR 2)

- **Coder and tester:** a Haiku subagent per task. It never commits, and git is read-only for it.
- **Orchestrator:** reruns each task's checks, commits one task per commit, pushes, and ticks
  todo.md. It also:
  - applies small reviewer fixes itself;
  - owns go.mod (nuid becomes a direct dependency);
  - owns `nats/parity_test.go` and module-wide compile fixes during parallel waves.
- **Reviewer:** an Opus subagent at each checkpoint.

## Architecture Decisions

- **Two lanes.** cron is its own module and shares nothing with nats, so the cron tasks run
  alongside the nats tasks.
- **Wire first.** `nats/internal/wire` (T2) holds the stream layout, the header names, the
  connect options, and server-address parsing. The producer and consumer tasks build on it, so
  their stream configs can't drift apart again (N7).
- **Lifecycle before behaviour.** Within each nats package, I/O moves into `Start(ctx)` and
  `Shutdown(ctx)` is bounded (T3, T4) before the message contract changes (T5, T6). The
  embedded-JetStream tests then run against the final lifecycle.
- **Telemetry after the contract.** One task (T9) moves producer and consumer telemetry
  together, once the APIs it instruments are final. Both sides share the messaging attribute
  set and the `error.type` rule.
- **Package-scoped checks in parallel waves.** The producer and consumer agents share the
  `nats` module and its root `parity_test.go`. Each agent runs checks on its own package
  (`./producer/...` or `./consumer/...`). The orchestrator fixes `parity_test.go` and runs the
  module-wide checks before committing.
- **Embedded JetStream in the mirror files.** Tests that need a real server use an
  in-process `nats-server/v2/test` server through a `helpers_test.go` helper. They need no env
  var, so `go test ./...` stays hermetic.

## Coverage Baselines (main @ 216b2aa, 2026-11-02)

| Package | Coverage |
| --- | --- |
| nats/consumer | 68.6% |
| nats/producer | 70.1% |
| cron | 100.0% |
| cron/engine | 93.7% |

Full-tree lint gap: nats 37 findings plus 4 gostyle findings; cron 36 findings plus 1 gostyle
finding.

## Task List

Details, acceptance criteria, and verification are in [todo.md](todo.md).

### Phase 0: Setup
- [x] T1: Commit the spec and plan, and open a draft PR (#18)

### Wave 1 (parallel)
- [x] T2: `nats/internal/wire`: stream layout, headers, connect options, server address (N7, N15, N18)
- [x] T7: cron lifecycle: lock, contexts, middleware order, errors (C2, C3, C4, C5, X1, X7)

### Wave 2 (parallel)
- [x] T3: producer lifecycle: `Register` without I/O, `Start(ctx)` owns the stream, `Shutdown(ctx)` waits (N2, N6, N7, N14, N20 ownership)
- [x] T4: consumer lifecycle: `Register` without I/O, `Start(ctx)`, bounded `Shutdown(ctx)`, data-subject filter, `DeliverNew` (N2, N3, N4, N14, N18 filter)
- [x] T8: cron telemetry, docs, recipe, strict (C1, C6, X2–X6, N19)

### Checkpoint A: Opus review of T1–T4, T7, T8 (done; fixes in 4df0457 and the nats fix commit)
### Wave 3 (parallel)
- [ ] T5: producer `Message` API, SDK headers, delayed publish, trace parenting (N5, N9, N13, N18, N20)
- [ ] T6: consumer `Message` handler, `HandlerTimeout` heartbeat, `ErrNoRetry`, ack logging, panic innermost (N1, N8, N21, X1)

### Wave 4
- [ ] T9: nats telemetry, both sides: messagingconv metrics, bounded attributes, `error.type` (N10, N11, X2–X6)

### Wave 5
- [ ] T10: nats cleanup, docs, testing recipe, strict (N16, N17, N19)

### Checkpoint B: final Opus review; PR description; owner review

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Timing-based JetStream tests flake in CI (delay, heartbeat) | Med | Assert ordering and counts, not exact times. Use tolerances of 100 ms or more, waits of seconds, and `-count=3` locally before commit. |
| Parallel producer and consumer agents break the shared `parity_test.go` | Med | The agents check only their own package; the orchestrator fixes the root tests (see Architecture Decisions). |
| Patching the stream on `Start` misses an SDK-owned field | Med | `TestStart_PreservesOperatorStreamSettings` covers create, update, and an existing stream without `schedules.<topic>.>`. |
| The heartbeat goroutine leaks or races with ack | Med | The heartbeat stops before ack/nak/term, a test runs under `-race`, and the handler ctx bounds it. |
| The Haiku usage limit interrupts a task | Low | The orchestrator checks and finishes the saved work, as in PR 1. |

## Open Questions

- None.
