# Implementation Plan: guardrails, release, test-transports (SDK fix backlog, PR 4)

Spec: [SPEC-guardrails](../docs/specs/sdk-fix-backlog/SPEC-guardrails.md) (approved
2026-11-02). Capability map: [capability-map](../docs/specs/sdk-fix-backlog/capability-map.md).
Tasks: [tasks/todo.md](todo.md). PRs 1–3 shipped as #16, #17, and #18.

## Overview

One PR on branch `feat/guardrails`, one commit per task. It covers three parts:

- **A. Guardrails:** gostyle telemetry rules, strict lint by default, and gostyle in CI.
- **B. Release:** the move to `github.com/fikrimohammad/go-servicekit`, v0.1.0 for every
  module, and an atomic release with an installability check.
- **C. Real transports:** db, redis, s3, and nats tests on in-process fakes, with the
  transport stubs and seams deleted.

## Execution Roles (owner decision 2026-11-02, changed from PRs 1–3)

- **Each phase is done by Haiku 5.5** as coder, tester, and reviewer.
  - A Haiku subagent implements and tests each task. It never commits, and git is read-only
    for it.
  - At each phase checkpoint, a separate Haiku subagent reviews that phase's tasks.
- **After all tasks are done, Opus 5.5 reviews the whole PR once**, as Checkpoint B.
- **Orchestrator:**
  - reruns every check, commits one task per commit, pushes, and ticks todo.md;
  - applies small review fixes;
  - owns go.mod changes (the three test fakes) and the module-wide checks;
  - runs the rule's recall and precision validation in worktrees, recorded in the PR.

## Architecture Decisions

- **Rename first, alone.** T2, done by Haiku like every task, touches about 106 files across every module. Landing it before
  anything else keeps every later diff free of path noise, and stops parallel agents from
  conflicting on imports.
- **Parallel by area.** gostyle (T3), release scripting (T4), and each module's transport
  rewrite (T5–T8) touch disjoint files. ci.yml is touched only by T9, which runs after them.
  The two parallel waves are capped at four agents each, to stay within the Haiku usage
  limit seen in PR 1.
- **The rule is validated before CI enforces it.** T3 builds the rules. The orchestrator then
  shows recall on `411bb72` and `216b2aa`, and precision on the branch. T9 only then turns
  gostyle on in CI. Precision is checked again after the transport rewrite, since T5–T8
  change production files.
- **Release logic in a script.** `release.yml`'s shell moves to `.github/scripts/release.sh`,
  with a `--dry-run` mode that CI runs on every PR. The workflow file stays thin, and the
  logic gets a test run before the first real tag.
- **Kept stubs are explicit.** Each kept transport stub has a one-line comment saying why no
  fake can reach its case. Checkpoint B reviews the list.

## Coverage Baselines (main @ 9373090, 2026-11-02)

| Package | Coverage |
| --- | --- |
| db | 88.8% |
| redis | 84.5% (the Checkpoint A reviewer measured main; 84.9% was a local reading) |
| s3 | 86.3% |
| nats/consumer | 93.3% |
| nats/producer | 96.4% |
| nats/internal/wire | 95.8% |

The four client modules have 10,995 lines of tests in total.

## Task List

Details, acceptance criteria, and verification are in [todo.md](todo.md).

### Phase 0: Setup
- [x] T1: Commit the spec and plan, and open a draft PR (#19)
- [x] T2: Rename the module path to go-servicekit, and set sibling requires to v0.1.0

### Wave 1 (parallel)
- [x] T3: gostyle `error-type-message` and `metric-high-cardinality` rules, with testdata and tests
- [x] T4: Release: `VERSION`, `.github/scripts/release.sh` (atomic, dry run, installability), `release.yml`
- [x] T5: db tests on sqlmock; delete the fake driver where sqlmock covers its cases
- [x] T6: redis tests on miniredis; delete the transport stubs and seams

### Checkpoint A: rule recall and precision shown; Haiku review of T1–T6 (done)
### Wave 2 (parallel)
- [ ] T7: s3 tests on gofakes3; delete the API stubs and seams
- [ ] T8: nats tests on the embedded server; delete the jsProducer/jsConsumer seams and mocks, except justified stubs

### Checkpoint A2: Haiku review of T7–T8
### Wave 3
- [ ] T9: CI: `LAX_MODULES`, a gostyle step plus a self-test job, MySQL and PostgreSQL services, the release dry run
- [ ] T10: Docs: AGENTS.md, both skills, READMEs, backlog ticks

### Checkpoint B: Haiku review of T9–T10, then the Opus review of all tasks; PR description; owner review

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| The rename misses a reference, such as a string in a test or a doc link | Med | T2 ends with `grep -rn go-dev-sdk` returning only the backlog's history lines, plus a full build, test, and tidy of every module |
| The rule's flow tracing has false positives on real code | High | Precision must be 0 on the branch before T9 enables it. If it can't be, narrow the rule (spec assumption 4) |
| A fake can't reproduce a stub's case (hung ack, mid-stream transport error) | Med | Keep that stub with a reason (spec assumption 13); the reviewer checks the list |
| The service containers make CI slow or flaky | Low | Use health-checked `services:`. The DB tests run only in the db module's job |
| The installability check can't run before a real tag | Low | The dry run covers the logic. The first real run is the owner's v0.1.0, with a documented recovery: fix forward with v0.1.1, never reuse a version |
| The Haiku usage limit interrupts a wave | Low | Waves are capped at four agents; saved work is finished by the orchestrator |

## Open Questions

- None.
