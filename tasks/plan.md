# Implementation Plan: data-clients (SDK fix backlog, PR 2)

Spec: [SPEC-data-clients](../docs/specs/sdk-fix-backlog/SPEC-data-clients.md) (approved
2026-11-02; assumptions 1–10 confirmed). Capability map:
[capability-map](../docs/specs/sdk-fix-backlog/capability-map.md). Item index:
[docs/ideas/sdk-fix-backlog.md](../docs/ideas/sdk-fix-backlog.md#item-index). Tasks:
[tasks/todo.md](todo.md). PR 1 (`foundations`) shipped as #16.

## Overview

One PR on branch `feat/data-clients`, one commit per task. It makes db, redis, and s3 telemetry
follow semconv v1.43.0 with bounded cardinality, gives their constructors a `ctx`, switches redis
to `redis.UniversalClient`, and makes all three strict on full-tree lint.

## Execution Roles (same as PR 1)

- **Coder and tester:** a Haiku subagent per task. It never commits, and git is read-only for it.
- **Orchestrator:** reruns each task's checks independently, commits one task per commit, pushes,
  and ticks todo.md. It applies small reviewer fixes itself when that is faster or safer.
- **Reviewer:** an Opus subagent at each checkpoint.

## Architecture Decisions

- **Waves by module.** db, redis, and s3 are separate modules with no shared files, so each wave
  runs one task per module in parallel. Within a module the tasks are sequential, because they
  edit the same files.
- **redis API first.** The `UniversalClient` switch (T3) changes redis's whole surface, so it lands
  before redis's telemetry and pool-metric work, which then target the final API.
- **Telemetry before cleanup.** The behaviour changes (telemetry, ctx constructors) come first.
  The lint/strict cleanup comes last in each module, so the strict list grows only once a module
  is final.
- **semconv constants.** Attribute keys come from `go.opentelemetry.io/otel/semconv/v1.43.0`, as
  rest does. Each module's telemetry test asserts the exact name, unit, and attribute set.
- **go.mod changes are the orchestrator's.** When a task needs a new direct import (for example
  semconv), the agent stops at the boundary and the orchestrator runs `go mod tidy` once, after
  the agents in that module finish. This is the lesson from PR 1's T16–T18.

## Coverage Baselines (main @ 411bb72, 2026-11-02)

| Module | Coverage |
| --- | --- |
| db | 87.4% |
| redis | 68.2% |
| s3 | 85.5% |

## Task List

Details, acceptance criteria, and verification are in [todo.md](todo.md).

### Phase 0: Setup
- [x] T1: Commit the spec and plan, and open a draft PR (#17)

### Wave 1 (parallel)
- [x] T2: db telemetry: semconv attributes, no count metric, span name, error.type (D1, D2, X2–X4, X6)
- [x] T3: redis returns `redis.UniversalClient`; interfaces and aliases deleted (R6, R5)
- [x] T4: s3 telemetry: `rpc.client.call.duration`, `rpc.system.name`/`rpc.method` (S-telemetry, X4, X6)

### Wave 2 (parallel)
- [x] T5: db `Connect(ctx)`; examples and README updated (X5)
- [x] T6: redis telemetry: semconv attributes, no count metric, error.type (R1, X2–X4, X6)
- [x] T7: s3 `New(ctx)`, lint cleanup, testing recipe, strict (X5, X8, X9, S2, N19)

### Checkpoint A: Opus review of T1–T7 (done; fixes in 326fd73 and the redis fix commit)
### Wave 3 (parallel)
- [x] T8: db cleanup: D3 (prove or drop), D4, X8, X9, testing recipe, strict (N19)
- [x] T9: redis pool metrics in semconv form; Close waits for the poller (R2, R3, R4)

### Wave 4
- [x] T10: redis `New(ctx)` check, lint cleanup, testing recipe, strict (X8, X9, N19)

### Checkpoint B: final Opus review (approved; fixes applied); PR description published; marked ready for owner review

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Renaming metrics breaks dashboards | Med | No backward compatibility is needed (only the testing app uses the SDK, and it is migrated after guardrails); the PR body lists every renamed or dropped metric |
| `UniversalClient` changes redis behaviour (cluster vs. standalone routing) | Med | `createUnderlyingClient` keeps the per-mode constructors; a test covers each mode's type and that the hook records commands |
| Pool-metric delta math drifts (an up-down counter never returning to 0) | Med | The same attribute set on every increment and decrement; a test drives PoolStats through a sequence and asserts the running count |
| Agents need a new direct import (semconv) | Low | Orchestrator-run `go mod tidy` after the module's agents finish (see Architecture Decisions) |
| The Haiku usage limit interrupts a task | Low | Saved work is checked and finished by the orchestrator, as in PR 1's T25 |

## Open Questions

- None. A draft PR is opened in T1, as in PR 1.
