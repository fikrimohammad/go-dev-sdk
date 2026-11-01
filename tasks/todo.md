# Tasks: foundations (SDK fix backlog, PR 1)

Plan: [plan.md](plan.md). Spec: [SPEC-foundations](../docs/specs/sdk-fix-backlog/SPEC-foundations.md).

**Module checks** (in every module a task touches; AGENTS.md verification):
`gofmt -l .`, `go vet ./...`, `go test -count=1 -shuffle=on -gcflags="all=-N -l" ./...`,
lint (full `golangci-lint run ./...` for strict modules; `--new-from-merge-base=main`
otherwise), gostyle, and `go mod tidy -diff`.

**Test-wording check** (added after the Checkpoint A review: lint misses the
"X should Y" style). In every module a task touches, this must print nothing:
`grep -rnE 't\.(Errorf|Fatalf|Error|Fatal)\(.*\b(should|expected|actual|Expected|Actual)\b' --include=*_test.go .`

**TDD rule:** for every test marked *(regression)*, run it before the fix and record that
it fails, then implement.

Each task is one commit with the subject `<type>(<module>): <summary> (<item IDs>)`.

---

## Phase 0: Setup

## Task 1: Branch, commit the docs, open a draft PR

**Description:** Create `feat/foundations` from `main` (a86089d). Commit the idea doc,
the capability map, SPEC-foundations, this plan and task list, and the deletion of the
shipped rest plan files. Open a draft PR so CI checks every later commit.

**Acceptance criteria:**
- [x] The branch exists, and one `docs:` commit holds `docs/ideas/sdk-fix-backlog.md`,
      `docs/specs/sdk-fix-backlog/*`, `tasks/plan.md`, `tasks/todo.md`, and the old
      tasks' deletion.
- [x] A draft PR targets `main`, and its body links the spec.

**Verification:**
- [x] `git log --stat -1` shows only docs and tasks files.
- [x] `gh pr view --json isDraft` reports `true`.

**Dependencies:** None
**Files likely touched:** `docs/ideas/sdk-fix-backlog.md`, `docs/specs/sdk-fix-backlog/*`, `tasks/*`
**Estimated scope:** XS

## Task 2: Strict-lint ratchet, ireturn allow-list, AGENTS.md convention

**Description:** Add `STRICT_LINT_MODULES` to `ci.yml` (initially `["rest"]`) and set the
lint step's `only-new-issues` to `${{ !contains(fromJSON(env.STRICT_LINT_MODULES), matrix.module) }}`.
Add `github.com/fikrimohammad/go-dev-sdk/` to the ireturn `allow` list. Document in
AGENTS.md that client packages may export and return a caller-facing interface so users
can generate mocks, and how the strict-lint list works.

**Acceptance criteria:**
- [x] CI runs `rest` lint with `only-new-issues: false`, and every other module with `true`.
- [x] `.golangci.yml` ireturn `allow` includes the SDK path.
- [x] AGENTS.md states the exported-interface convention and the ratchet rule ("add a
      module to STRICT_LINT_MODULES in the commit that makes it clean; never remove one").

**Verification:**
- [x] `golangci-lint run ./...` in `rest` reports 0 findings locally.
- [x] On the draft PR, the `rest` CI job's lint log shows a full run; another module's
      shows only-new-issues.

**Dependencies:** T1
**Files likely touched:** `.github/workflows/ci.yml`, `.golangci.yml`, `AGENTS.md`
**Estimated scope:** S

---

## Phase 1: Small modules

## Task 3: identity: ErrName prefix (I1), strict

**Description:** Change `ErrName`'s message to `"identity: service name is required"`.
Add `identity` to `STRICT_LINT_MODULES`.

**Acceptance criteria:**
- [x] The ErrName text has the package prefix, and `errors.Is` callers are unaffected.
- [x] `identity` is in the strict list, with 0 full-tree lint and gostyle findings.

**Verification:**
- [x] Module checks pass in `identity`, plus `nats` and `observability` (they wrap ErrName).

**Dependencies:** T2
**Files likely touched:** `identity/identity.go`, `.github/workflows/ci.yml`
**Estimated scope:** XS

## Task 4: errs: Is, LogValue, SlogAttr fixes (E1–E3)

**Description:** Write the failing tests first. Then: `(*Error).Is` matches only when the
codes are equal and non-zero; `LogValue` emits `debug` once; `SlogAttr` finds an `*Error`
anywhere in the chain.

**Acceptance criteria:**
- [x] *(regression)* `TestError_Is`: two code-0 errors don't match; the same non-zero code
      matches; different codes don't.
- [x] *(regression)* `TestError_LogValue`: `debug` appears exactly once.
- [x] *(regression)* `TestSlogAttr`: `fmt.Errorf("x: %w", e)` logs structured fields.

**Verification:**
- [x] The three tests fail before the fix and pass after; errs module checks pass.
- [x] `rest` module checks pass (rest/server reads errs codes).

**Dependencies:** T2
**Files likely touched:** `errs/error.go`, `errs/slog.go`, `errs/error_test.go`, `errs/slog_test.go`
**Estimated scope:** S

## Task 5: errs: package doc, AsType, test wording, strict (E4, X8, X9)

**Description:** Add `errs/doc.go` with the package comment. Fix the godot findings, convert
`errors.As` to `errors.AsType`, and reword test messages to `Func(in) = got, want want`.
Add `errs` to the strict list.

**Acceptance criteria:**
- [x] `go doc -all .` shows a package comment and no stale references.
- [x] 0 full-tree lint and gostyle findings, and `errs` is strict.
- [x] Coverage ≥ 98.5%.

**Verification:**
- [x] Module checks (strict) pass in `errs`.

**Dependencies:** T4
**Files likely touched:** `errs/doc.go`, `errs/{error,constructors,slog}.go`, `errs/*_test.go`, `.github/workflows/ci.yml`
**Estimated scope:** S

## Task 6: errgroup: PanicError, no naked returns, no nil-ctx fallback (G2, X7)

**Description:** Write the failing panic tests first. Then recovered panics become
`*PanicError{Value, Stack}`, whose `Unwrap` returns Value when it's an error. Rewrite
`adaptTask` with no naked returns (see the spec's Code Style), and drop the nil-ctx
fallback in `New`.

**Acceptance criteria:**
- [x] *(regression)* Panic cases in `TestGroup_Go`: Wait returns an error that `errors.As`
      matches as `*PanicError`, with the original Value and a non-empty Stack; an error
      panic value is reachable with `errors.Is`.
- [x] No `fmt.Errorf` assembles stack text.

**Verification:**
- [x] The tests fail before and pass after; errgroup module checks pass; coverage stays 100%.

**Dependencies:** T2
**Files likely touched:** `errgroup/errgroup.go`, `errgroup/errgroup_test.go`
**Estimated scope:** S

## Task 7: errgroup: const default, docs, nolint, test wording, strict (G1, G3, G4, X9)

**Description:** Make `DefaultMaxConcurrency` a const. Add doc comments for `Option` and
`WithMaxConcurrency`. Add the spec's `containedctx` nolint with its reason. Reword test
messages. Add `errgroup` to the strict list.

**Acceptance criteria:**
- [x] `DefaultMaxConcurrency` is a `const`, and every exported identifier is documented.
- [x] 0 full-tree lint and gostyle findings, and `errgroup` is strict.

**Verification:**
- [x] Module checks (strict) pass in `errgroup`; coverage is 100%.

**Dependencies:** T6
**Files likely touched:** `errgroup/option.go`, `errgroup/errgroup.go`, `errgroup/*_test.go`, `.github/workflows/ci.yml`
**Estimated scope:** S

### Checkpoint A: small modules strict
- [x] identity, errs, errgroup, and rest are strict, and CI is green on the draft PR.
- [x] Module checks pass in every go.work module (confloader included).

---

## Phase 2: goconvey → package testing (no behavior change)

For each task, the set of `func Test` names stays the same (renaming only to
`Test<Identifier>_<Behavior>` where gostyle requires it), and coverage is at least the
baseline in plan.md. Only `_test.go` files change.

## Task 8: attributes tests

**Description:** Convert `observability/attributes/attributes_test.go` to table tests in
package `testing`.

**Acceptance criteria:**
- [x] No goconvey import in `attributes`; coverage ≥ 97.7%.

**Verification:**
- [x] `go test -count=3 -shuffle=on -cover ./attributes/...` passes in `observability`.

**Dependencies:** T2
**Files likely touched:** `observability/attributes/attributes_test.go`
**Estimated scope:** S

## Task 9: logs tests

**Description:** Convert `logs_test.go` and `config_test.go`. Drop the two unused
`//nolint:staticcheck` directives (O4).

**Acceptance criteria:**
- [ ] No goconvey import in `logs`; coverage ≥ 94.2%; no nolintlint findings.

**Verification:**
- [ ] `go test -count=3 -shuffle=on -cover ./logs/...` passes.

**Dependencies:** T2
**Files likely touched:** `observability/logs/logs_test.go`, `observability/logs/config_test.go`
**Estimated scope:** M

## Task 10: metrics tests

**Description:** Convert `metrics_test.go` (592 lines) and `config_test.go`.

**Acceptance criteria:**
- [ ] No goconvey import in `metrics`; coverage ≥ 90.0%.

**Verification:**
- [ ] `go test -count=3 -shuffle=on -cover ./metrics/...` passes.

**Dependencies:** T2
**Files likely touched:** `observability/metrics/metrics_test.go`, `observability/metrics/config_test.go`
**Estimated scope:** M

## Task 11: tracer tests; remove goconvey

**Description:** Convert `tracer_test.go` and `config_test.go`, remove the redundant
`noop` import alias, and run `go mod tidy` so goconvey leaves `observability/go.mod`.

**Acceptance criteria:**
- [ ] `grep -rn goconvey .` (excluding vendor) finds nothing; coverage ≥ 83.0%.
- [ ] `go mod tidy -diff` is clean.

**Verification:**
- [ ] Module checks pass in `observability`.

**Dependencies:** T8, T9, T10
**Files likely touched:** `observability/tracer/tracer_test.go`, `observability/tracer/config_test.go`, `observability/go.mod`, `observability/go.sum`
**Estimated scope:** M

### Checkpoint B: goconvey gone, coverage held
- [ ] Every observability package is at or above its baseline coverage, and no goconvey
      import is left.
- [ ] Behavior is unchanged: the other modules' tests still pass.

---

## Phase 3: observability behavior

## Task 12: metrics: Default follows OTel, Insecure bool, no nil-ctx fallback (X10, O1, X7)

**Description:** Write the failing test first. The initial and `SetDefault(nil)` default
becomes one process-wide `Wrap(otel.GetMeterProvider())` (SetDefault stays transitionally,
until T19). `Config.Insecure` becomes `bool`, with false meaning TLS. Delete
`contextOrBackground`.

**Acceptance criteria:**
- [ ] *(regression)* `TestDefault_FollowsOTelGlobal`: `metrics.Add` after
      `otel.SetMeterProvider(manual reader)` is collected, including an instrument first
      used before the install. The test restores the global in `t.Cleanup`.
- [ ] *(regression)* `TestConfig`: the zero-value Config doesn't enable insecure; `Insecure: true` does.
- [ ] No nil-ctx substitution is left in `metrics`.

**Verification:**
- [ ] The tests fail before and pass after; observability checks pass; coverage ≥ 90.0%.
- [ ] Module checks pass in the callers (db, redis, s3, nats, cron, rest).

**Dependencies:** T10
**Files likely touched:** `observability/metrics/metrics.go`, `observability/metrics/config.go`, `observability/metrics/metrics_test.go`, `observability/metrics/config_test.go`
**Estimated scope:** M

## Task 13: tracer: Default follows OTel, Insecure bool, doc fix (X10, O1, O3, X7)

**Description:** The same as T12 for `tracer`, plus removing the doc references to `Init`.

**Acceptance criteria:**
- [ ] *(regression)* `TestDefault_FollowsOTelGlobal`: a span from `tracer.Tracer("x")` is
      recorded after `otel.SetTracerProvider(recorder)`.
- [ ] *(regression)* `TestConfig`: the zero-value Config doesn't enable insecure.
- [ ] `go doc -all ./tracer` has no `Init`, and no nil-ctx substitution is left.

**Verification:**
- [ ] The tests fail before and pass after; coverage ≥ 83.0%; callers' module checks pass.

**Dependencies:** T11
**Files likely touched:** `observability/tracer/tracer.go`, `observability/tracer/config.go`, `observability/tracer/tracer_test.go`, `observability/tracer/config_test.go`
**Estimated scope:** M

## Task 14: logs: delete SetStreams and the stream vars (O2)

**Description:** Delete `SetStreams`, the package `stdout`/`stderr` vars, and
`streamWriter`. A nil `Writer` / `ErrorWriter` means `os.Stdout` / `os.Stderr`. Tests that
redirected streams pass `Config.Writer` / `ErrorWriter` instead.

**Acceptance criteria:**
- [ ] `grep -rn 'SetStreams\|streamWriter' .` finds nothing.
- [ ] Logs still route debug/info to Writer and warn/error to ErrorWriter (existing tests).

**Verification:**
- [ ] Observability checks pass; coverage ≥ 94.2%.

**Dependencies:** T9
**Files likely touched:** `observability/logs/logs.go`, `observability/logs/logs_test.go`
**Estimated scope:** S

### Checkpoint C: new behavior in place
- [ ] Every go.work module builds and passes its tests, and `SetDefault` still exists
      (transitionally).
- [ ] Review the observability diff with the owner, if wanted, before the caller migration.

---

## Phase 4: migrate callers, then delete

## Task 15: db and redis off SetDefault

**Description:** The test helpers set `meta.metrics` / `meta.tracer` to their fakes,
instead of installing package defaults. The "uses the default when nothing is injected"
tests install an OTel span recorder or manual reader with `otel.Set*Provider`. Doc comments
change from "(metrics.SetDefault)" to "the OTel global meter/tracer provider".

**Acceptance criteria:**
- [ ] No `metrics.SetDefault` / `tracer.SetDefault` in `db` or `redis`.
- [ ] Each module still has a test proving fallback to the OTel global.

**Verification:**
- [ ] Module checks pass in `db` and `redis` (lint new-from-merge-base); `-shuffle=on -count=3`.

**Dependencies:** T12, T13
**Files likely touched:** `db/db_test.go`, `db/db.go`, `redis/redis_test.go`, `redis/redis.go`
**Estimated scope:** M

## Task 16: s3, and nats docs, off SetDefault

**Description:** The same as T15 for `s3`. The nats doc comments are updated (nats has no
SetDefault in its tests).

**Acceptance criteria:**
- [ ] No `SetDefault` mention left in `s3` or `nats`.

**Verification:**
- [ ] Module checks pass in `s3` and `nats`.

**Dependencies:** T12, T13
**Files likely touched:** `s3/s3_test.go`, `s3/s3.go`, `nats/consumer/consumer.go`, `nats/producer/producer.go`
**Estimated scope:** S

## Task 17: rest/server off SetDefault

**Description:** `installDefaultTelemetry` installs OTel globals (a span recorder plus a
manual reader) and asserts through them. `example_test.go` uses `tracer.SetGlobal(tc)`
and `otel.SetMeterProvider(mc.Provider())`. The doc comments are updated.

**Acceptance criteria:**
- [ ] No `SetDefault` for metrics/tracer in `rest/server`.
- [ ] The default-fallback test still proves the server records through the defaults.
- [ ] `rest` stays at 0 full-tree lint findings (it's strict).

**Verification:**
- [ ] Module checks (strict) pass in `rest`.

**Dependencies:** T12, T13
**Files likely touched:** `rest/server/server_test.go`, `rest/server/example_test.go`, `rest/server/server.go`, `rest/server/middleware.go`
**Estimated scope:** M

## Task 18: rest/client off SetDefault

**Description:** The same as T17 for `rest/client`.

**Acceptance criteria:**
- [ ] No `SetDefault` for metrics/tracer in `rest/client`, and the fallback test is kept.

**Verification:**
- [ ] Module checks (strict) pass in `rest`.

**Dependencies:** T17
**Files likely touched:** `rest/client/client_test.go`, `rest/client/client.go`, `rest/client/middleware.go`
**Estimated scope:** S

## Task 19: delete SetDefault and the package Stop, rewrite doc.go, observability strict

**Description:** Delete `metrics.SetDefault`, `metrics.Stop`, `tracer.SetDefault`, and
`tracer.Stop`, along with their tests. `Default()` returns the init-time
`Wrap(otel.Get*Provider())`. Rewrite `observability/doc.go` for the OTel-global model.
Fix the remaining full-tree findings (contextcheck, forbidigo, recvcheck, ireturn now
allowed). Add `observability` to the strict list.

**Acceptance criteria:**
- [ ] Spec criterion 5: `grep -rn 'SetDefault\|SetStreams\|contextOrBackground' observability/metrics observability/tracer`
      finds nothing.
- [ ] 0 full-tree lint and gostyle findings in `observability`, and it's strict.
- [ ] `go doc -all` for each observability package shows no stale references.

**Verification:**
- [ ] Module checks (strict) pass in `observability`; coverage meets every baseline.

**Dependencies:** T14, T15, T16, T17, T18
**Files likely touched:** `observability/metrics/metrics.go`, `observability/tracer/tracer.go`, `observability/doc.go`, `observability/*/…_test.go`, `.github/workflows/ci.yml`
**Estimated scope:** M

### Checkpoint D: complete
- [ ] Every SPEC-foundations success criterion (1–7) is checked, with evidence in the PR body.
- [ ] Module checks pass in every go.work module, and CI is green.
- [ ] The PR body lists the breaking changes: SetDefault and the package Stop removed,
      Insecure defaults to TLS, SetStreams removed, PanicError, and the errs.Is semantics.
- [ ] The PR is marked ready for review, and the owner reviews.
