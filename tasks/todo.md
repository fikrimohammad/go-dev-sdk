# Tasks: guardrails, release, test-transports (SDK fix backlog, PR 4)

Plan: [plan.md](plan.md). Spec: [SPEC-guardrails](../docs/specs/sdk-fix-backlog/SPEC-guardrails.md).

**Module checks.** Run in every module a task touches. Every module except confloader is
strict, so its lint is full-tree:
- `gofmt -l .`
- `go vet ./...`
- `go test -count=1 -shuffle=on ./...` and `-race`
- `golangci-lint run ./...` (confloader: `--new-from-merge-base=main`)
- `GOWORK=off go run ../.agents/skills/go-best-practices/scripts/gostyle.go ./...`
- `go mod tidy -diff`

**Test-wording check.** For the lines a task changes, this must print nothing:
`git diff -U0 | grep '^+' | grep -E 't\.(Errorf|Fatalf|Error|Fatal)\(.*\b(should|expected|actual|Expected|Actual)\b'`

**TDD rule.** New behaviour (the rules, the release script) is test-first. In the transport
rewrite, each moved test must still fail when its fix is reverted. Spot-check it by mutation,
as in PR 3.

Each task is one commit, with the subject `<type>(<area>): <summary> (<task>)`.

---

## Task 1: Commit the spec and plan, open a draft PR

**Description:** On branch `feat/guardrails` (from main @ 9373090), commit SPEC-guardrails, the
capability map, this plan, and the task list. Open a draft PR on `fikrimohammad/go-servicekit`
that links the spec.

**Acceptance criteria:**
- [x] One `docs:` commit holds them.
- [x] A draft PR targets `main`.

**Verification:** `gh pr view --json isDraft` reports true.

**Dependencies:** None · **Files:** `docs/specs/**`, `tasks/*` · **Scope:** XS

## Task 2: Rename the module path; sibling requires at v0.1.0

**Description:**
- Replace `github.com/fikrimohammad/go-dev-sdk` with `github.com/fikrimohammad/go-servicekit`
  in:
  - every go.mod (`module`, `require`, `replace`);
  - every .go import and string;
  - `.golangci.yml` (the ireturn allow list);
  - the READMEs, AGENTS.md, the skills, and the specs and tasks (except history quotes in the
    backlog doc).
- Set every sibling `require` to `v0.1.0`. That replaces the zero pseudo-versions and the
  `v1.0.0` ones, including the indirect ones.
- Run `go mod tidy` per module, then `go work vendor`.

**Acceptance criteria:**
- [x] `grep -rn 'go-dev-sdk' --exclude-dir=vendor .` matches only the history lines that
      record the rename in `docs/ideas/sdk-fix-backlog.md` and the specs.
- [x] `grep -h 'go-servicekit/[a-z]* v' */go.mod | sort -u` shows only `v0.1.0`.
- [x] Every module passes the module checks.

**Dependencies:** T1 · **Files:** about 106 files across all modules · **Scope:** M (mechanical)

## Task 3: gostyle telemetry rules

**Description:** In `.agents/skills/go-best-practices/scripts/gostyle.go`:
- Add `error-type-message` and `metric-high-cardinality`, as spec assumptions 1–3 describe.
- Add a package-scoped pass that groups the non-test files of each directory.
- The rules follow the attribute map through identifiers, builder calls (to a fixpoint), map
  literals, and index writes. A clone stops the trace.
- Keys are matched as a string literal, `semconv.X`, or `string(semconv.X)`.
- Each rule gets a `ruleDocs` entry linking the semconv guidance.
- Standard library only.

**Acceptance criteria:**
- [x] `testdata/bad` triggers each new rule in each shape: a direct literal, a builder
      function, an index write, an identifier from `err.Error()`, and a builder that returns
      `err.Error()`.
- [x] `testdata/good` has the safe shapes (a cloned span-only map, a coded `error.type`, a
      bounded key) and reports nothing.
- [x] `TestTelemetryRules_Table` covers each key form.
- [x] Existing rules are unchanged; `TestCheck` passes.

**Verification:** `GOWORK=off go test -count=1 gostyle.go gostyle_test.go`. The orchestrator
then runs recall at `411bb72` and `216b2aa`, and precision on the branch (spec assumption 4).

**Dependencies:** T2 · **Files:** `gostyle.go`, `gostyle_test.go`, `testdata/**` · **Scope:** M

## Task 4: Release script and workflow

**Description:**
- Add a `VERSION` file at the repo root (`v0.1.0`).
- Add `.github/scripts/release.sh <version> [--dry-run]`, which does these steps in order:
  1. Validate the semver shape and the major against each module path.
  2. Check that every sibling require equals `<version>`.
  3. Check that no `<module>/<version>` tag exists and that the proxy doesn't have the
     version.
  4. Without `--dry-run`, tag every module on HEAD and run `git push --atomic origin <tags>`.
- Add `.github/scripts/verify-install.sh <version>`: in a temp module outside the repo, run
  `GOPROXY=https://proxy.golang.org GOFLAGS=-mod=mod` `go get` on every module at the
  version, then `go build` on a file that imports one package from each module. It retries
  while the proxy catches up, for up to about 5 minutes.
- Rework `release.yml`: one `version` input that defaults to the `VERSION` file, the release
  step, then a verify-install job.

**Acceptance criteria:**
- [x] `release.sh v0.1.0 --dry-run` passes on the branch.
- [x] It fails, with a clear message, on:
  - a mismatched sibling require;
  - a bad version string;
  - a v2 version on a non-/v2 path;
  - a version already in the proxy (use `v1.0.0` against `go-dev-sdk`, through an override
    of the module prefix, or a stubbed curl).
- [x] A bats-free shell test, `.github/scripts/release_test.sh`, runs those cases.
- [x] `shellcheck` is clean, if it is available.

**Dependencies:** T2 · **Files:** `VERSION`, `.github/scripts/*`, `.github/workflows/release.yml` · **Scope:** M

## Task 5: db tests on sqlmock

**Description:**
- Rewrite the db unit tests that use the in-repo fake driver to use go-sqlmock, through
  `Config{Driver: "sqlmock", DSN}`. That covers the query, exec, named, transaction, error,
  telemetry, and ctx paths.
- Delete the fake driver and its seams where sqlmock covers their cases. Keep any helper that
  sqlmock can't replace, with a reason.
- The env-gated MySQL and PostgreSQL integration tests stay. They must pass against real
  servers (T9 wires CI).
- The orchestrator adds go-sqlmock v1.5.2 to db/go.mod.

**Acceptance criteria:**
- [x] No test depends on the deleted fake driver.
- [x] Coverage ≥ 88.8%.
- [x] PR 2's regression tests still exist and still fail when their fix is reverted
      (orchestrator spot-check).

**Verification:** db module checks, plus the integration tests run locally against Docker
MySQL and PostgreSQL if available. Otherwise they run in T9's CI.

**Dependencies:** T2 · **Files:** `db/*_test.go`, `db/*.go` (seam removal), `db/go.mod` (orchestrator) · **Scope:** L

## Task 6: redis tests on miniredis

**Description:**
- Rewrite the redis tests to run against `miniredis.RunT(t)`. That covers commands,
  pipelines, transactions, the error prefix, telemetry, the pool poller, Close, and the New
  ctx.
- Delete the stub clients and the unexported seams that existed only for them.
- Keep a stub only for a case miniredis can't produce, such as cluster and sentinel topology
  routing (miniredis is standalone only), with a reason.
- The orchestrator adds miniredis/v2 v2.40.0.

**Acceptance criteria:**
- [x] Coverage ≥ 84.9%.
- [x] The regression tests still exist and still fail when their fix is reverted
      (spot-check).

**Verification:** redis module checks.

**Dependencies:** T2 · **Files:** `redis/*_test.go`, `redis/redis.go` (seam removal), `redis/go.mod` (orchestrator) · **Scope:** L

### Checkpoint A
- [x] Rule recall is shown at `411bb72` and `216b2aa`, and precision is 0 on the branch.
- [x] Haiku review of T1–T6, with its fixes applied; CI is green.

## Task 7: s3 tests on gofakes3

**Description:**
- Rewrite the s3 tests to run against gofakes3 with `s3mem` on httptest, through
  `Config{Endpoint: ts.URL, UsePathStyle: aws.Bool(true)}`. That covers put, get (whole and
  parts), head, delete, list, multipart upload, presign, the error codes, and telemetry.
- Delete the API stubs and seams that existed only for them. Keep stubs for transport-level
  failures that gofakes3 can't inject, with a reason. Close every response body.
- The orchestrator adds gofakes3 v1.2.0.

**Acceptance criteria:**
- [x] Coverage ≥ 86.3%.
- [x] The regression tests still exist and still fail when their fix is reverted
      (spot-check).

**Verification:** s3 module checks.

**Dependencies:** Checkpoint A · **Files:** `s3/*_test.go`, `s3/s3.go`, `s3/middleware.go` (seam removal), `s3/go.mod` (orchestrator) · **Scope:** L

## Task 8: nats tests on the embedded server

**Description:**
- Rewrite the producer and consumer unit tests that use the `jsProducer` and `jsConsumer`
  mocks and factories to run against the embedded JetStream server.
- Delete the `jsProducer`/`jsConsumer` interfaces, the factories, and the mocks.
- Keep a stub only where the server can't produce the case: a never-arriving async ack, or a
  hung drain for the Shutdown-deadline tests. Each kept stub has a reason.
- No go.mod change: nats-server is already a test dependency.

**Acceptance criteria:**
- [x] Coverage stays at or above: consumer 93.3%, producer 96.4%, wire 95.8%.
- [x] PR 3's regression tests still exist and still fail when their fix is reverted
      (spot-check).
- [x] Timing tests pass with `-count=3`.

**Verification:** nats module checks.

**Dependencies:** Checkpoint A · **Files:** `nats/producer/*`, `nats/consumer/*` (tests and seam removal) · **Scope:** L

### Checkpoint A2
- [ ] Haiku review of T7–T8, with its fixes applied; CI is green.

## Task 9: CI

**Description:** In `.github/workflows/ci.yml`:
- Replace `STRICT_LINT_MODULES` with `LAX_MODULES: '["confloader"]'`, so `only-new-issues` is
  set only for lax modules. Update the comment: the list may only shrink.
- Add a "Style" step that runs gostyle on `./...` for non-lax modules.
- Add a `gostyle` job that runs the gostyle self-test.
- Add MySQL and PostgreSQL `services:` and set `DB_MYSQL_DSN`/`DB_POSTGRES_DSN` for the db
  module only, so its integration tests run.
- Add a `release-dry-run` job: `.github/scripts/release.sh "$(cat VERSION)" --dry-run` and
  `release_test.sh`.

**Acceptance criteria:**
- [x] CI is green on the PR, with every new job present.
- [x] The db job's log shows the integration tests ran, not skipped.
- [ ] Precision is checked again: gostyle reports 0 on every non-lax module after T5–T8.

**Dependencies:** T3, T4, T5, T7, T8 · **Files:** `.github/workflows/ci.yml` · **Scope:** S

## Task 10: Docs

**Description:**
- AGENTS.md: the Verification section and the Definition of done describe strict-by-default,
  `LAX_MODULES`, gostyle in CI, the new path, and the release flow
  (`release.sh` plus `release.yml`).
- The go-best-practices skill: the two rules and their ignore syntax.
- The opentelemetry-semconv skill: a pointer to the rules.
- The module READMEs: install lines on the new path at v0.1.0. The testing recipes already
  exist.
- The backlog's key assumptions are ticked, with evidence links.

**Acceptance criteria:**
- [ ] The docs match the code and CI.
- [ ] `grep go-dev-sdk` is clean, as in T2.

**Dependencies:** T9 · **Files:** `AGENTS.md`, `.agents/skills/*/SKILL.md`, `*/README.md`, `docs/ideas/sdk-fix-backlog.md` · **Scope:** S

### Checkpoint B: complete
- [ ] SPEC-guardrails success criteria 1–8 are verified with evidence, including the rule's
      recall and precision.
- [ ] Haiku review of T9–T10, then one Opus review of the whole PR (all tasks), with its fixes
      applied.
- [ ] The PR body (prepared by the orchestrator) lists the path change, v0.1.0 and the release
      runbook, the rules and their validation, and the kept stubs. The owner marks the PR
      ready, or tells the orchestrator to.
