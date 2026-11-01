# Implementation Plan: foundations (SDK fix backlog, PR 1)

Spec: [SPEC-foundations](../docs/specs/sdk-fix-backlog/SPEC-foundations.md) (approved
2026-11-01). Capability map: [capability-map](../docs/specs/sdk-fix-backlog/capability-map.md).
Item index: [docs/ideas/sdk-fix-backlog.md](../docs/ideas/sdk-fix-backlog.md#item-index).
Tasks: [tasks/todo.md](todo.md).

## Overview

One PR, branch `feat/foundations`, with one commit per task, so the owner can review the
PR commit by commit. It fixes `identity`, `errs`, `errgroup`, and `observability`, makes
SDK telemetry follow the OTel global providers, migrates callers off `SetDefault`, and
switches those modules plus `rest` to full-tree lint in CI.

## Execution Roles (owner decision 2026-11-01)

- **Coder and tester:** a Haiku 5.5 subagent per task (T2–T19). It gets the task, its spec
  section, and the go-best-practices skill. It doesn't commit or push.
- **Orchestrator:** the main session. It dispatches tasks, reruns the task's verification
  independently, commits one task per commit, pushes, and ticks todo.md. It does T1
  itself (setup, no code).
- **Reviewer:** an Opus 5.5 subagent at each checkpoint (A–D). It reviews the commits
  since the previous checkpoint against the spec and the go-best-practices skill.
  Findings are fixed before the next phase starts.

## Architecture Decisions

- **Ratchet first, with `rest` only.** T2 lands the `STRICT_LINT_MODULES` mechanism with
  `rest`, which is already clean, so a mistake in the CI expression shows up before any
  module depends on it. Each later task adds its module to the list in the same commit
  that makes it clean. The list only grows, and every commit stays green.
- **Small modules before observability.** identity, errs, and errgroup are independent
  and quick. They prove the TDD-plus-strict loop before the big module.
- **Convert goconvey before changing behavior.** Each observability package's tests are
  converted to package `testing` with no behavior change, guarded by coverage at least
  equal to its baseline. New regression tests then land in files that are already
  converted, so no file mixes styles. The cost is converting a few SetDefault tests
  that T19 deletes; that's accepted.
- **Transitional default, then deletion.** In T12/T13, `Default()` returns the
  `SetDefault` override if one is set. Otherwise it resolves `otel.Get*Provider()` per
  call, cached by provider identity; see the spec's design note on why an init-time
  capture is wrong. `SetDefault` still compiles for callers. T15–T18 migrate the callers. T19 deletes `SetDefault` and
  the package-level `Stop`. Every commit builds; a grep in T19 proves no transitional
  code is left.
- **Caller tests inject; one fallback test per module uses OTel.** The db, redis, and s3
  helpers build their structs directly, so they set `meta.metrics` / `meta.tracer` to
  their fakes, with no globals. Each module's "falls back to the default" test installs
  an OTel span recorder or manual reader with `otel.Set*Provider`, restored in
  `t.Cleanup`.
- **OTel delegate behavior was validated by two probes** (2026-11-01). Providers captured
  before `otel.Set*Provider` forward to the *first* install, including instruments created
  before it, but never to a later install. Hence the per-call resolution.

## Coverage Baselines (main @ a86089d, 2026-11-01)

| Package | Coverage |
| --- | --- |
| observability/attributes | 97.7% |
| observability/logs | 94.2% |
| observability/metrics | 90.0% |
| observability/tracer | 83.0% |
| errs | 98.5% |
| errgroup | 100.0% |
| identity | 100.0% |

Command: `go test -count=1 -gcflags="all=-N -l" -cover ./...` in each module.

## Task List

Details, acceptance criteria, and verification are in [todo.md](todo.md).

### Phase 0: Setup
- [x] T1: Branch, commit the docs, open a draft PR
- [x] T2: Strict-lint ratchet (`rest`), ireturn allow-list, AGENTS.md convention

### Phase 1: Small modules
- [x] T3: identity: ErrName prefix (I1), strict
- [x] T4: errs: Is, LogValue, SlogAttr fixes, test first (E1–E3)
- [x] T5: errs: package doc, AsType, test wording, strict (E4, X8, X9)
- [x] T6: errgroup: PanicError, no naked returns, no nil-ctx fallback, test first (G2, X7)
- [x] T7: errgroup: const default, docs, nolint, test wording, strict (G1, G3, G4, X9)

### Checkpoint A: small modules strict
### Phase 2: goconvey → package testing (no behavior change)
- [x] T8: attributes tests
- [x] T9: logs tests
- [x] T10: metrics tests
- [x] T11: tracer tests; remove goconvey from go.mod

### Checkpoint B: goconvey gone, coverage held
### Phase 3: observability behavior
- [x] T12: metrics: Default follows the OTel global, Insecure bool, no nil-ctx fallback (X10, O1, X7)
- [x] T13: tracer: the same, plus the doc fix (X10, O1, O3, X7)
- [x] T14: logs: delete SetStreams and the stream vars (O2)

### Checkpoint C: new behavior in place, callers still compile
### Phase 4: migrate callers, then delete
- [x] T15: db and redis tests and docs off SetDefault
- [x] T16: s3 tests and docs, nats docs off SetDefault
- [x] T17: rest/server tests and docs off SetDefault
- [x] T18: rest/client tests and docs off SetDefault
- [x] T19: delete SetDefault and the package Stop, rewrite observability/doc.go, observability strict

### Checkpoint D: complete; owner review

### Phase 5: repository conventions (owner feedback 2026-11-01; spec amendment R1–R4)
Wave 1 runs in parallel because the tasks touch disjoint files. Wave 2 waits for wave 1
because it moves declarations out of the files wave 1 edits. Wave 3 adds examples once the
API is final.
- [x] T20: Codify R1–R4 in AGENTS.md; record them as confloader redesign requirements (wave 1)
- [x] T21: R1 observability logs/metrics/tracer: export SetDefaults/Validate (wave 1)
- [ ] T22: R1 rest/client, rest/server, cron/engine: export, and Join in Validate (wave 1)
- [ ] T23: R4 cron, db, redis: move interfaces.go into <pkg>.go (wave 1)
- [ ] T24: R3 errors.go in errgroup, identity, attributes, logs, metrics, rest/client, redis, errs (wave 2)
- [ ] T25: R2 runnable examples: errs, errgroup, attributes, cron, cron/engine, plus a compile-only observability root example (wave 3)
- [ ] T26: R2 compile-only examples: db, redis, s3, nats/consumer, nats/producer (wave 3)

### Checkpoint E: conventions complete; Opus review; owner review

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| goconvey conversion silently drops assertions | High | Convert test by test, keeping names; per-package coverage must be at least baseline; the diff of `func Test` names before and after must match |
| OTel global state leaks between tests (CI runs `-shuffle=on`) | Med | Never `t.Parallel()` with globals; restore the previous provider in `t.Cleanup`; run each module's tests locally with `-shuffle=on -count=3` |
| CI ratchet expression is wrong | Med | Lands first (T2) with only `rest`, verified on the draft PR's CI run before anything depends on it |
| Transitional SetDefault survives | Low | T19's acceptance grep (spec criterion 5) |
| The PR is too large to review | Med | One commit per task with an item-ID commit subject; checkpoints A–D give natural review stops |
| `Insecure` default flip breaks the testing app | Low | Expected and accepted (the app is frozen until after `guardrails`); called out in the PR body |

## Open Questions

- None blocking. T1 opens a **draft** PR early, so CI validates the ratchet (T2) and every
  later commit. It becomes ready for review at Checkpoint D.
