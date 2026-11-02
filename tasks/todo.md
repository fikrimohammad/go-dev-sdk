# Tasks: data-clients (SDK fix backlog, PR 2)

Plan: [plan.md](plan.md). Spec: [SPEC-data-clients](../docs/specs/sdk-fix-backlog/SPEC-data-clients.md).

**Module checks.** Run these in every module a task touches (AGENTS.md verification):
- `gofmt -l .`
- `go vet ./...`
- `go test -count=1 -shuffle=on ./...` and `-race`
- lint: full-tree `golangci-lint run ./...` once a module is strict, otherwise
  `--new-from-merge-base=main`
- gostyle
- `go mod tidy -diff`, including in any module that requires the changed one

**Test-wording check.** For the lines a task changes, this must print nothing:
`git diff -U0 | grep '^+' | grep -E 't\.(Errorf|Fatalf|Error|Fatal)\(.*\b(should|expected|actual|Expected|Actual)\b'`

**TDD rule.** Every test marked *(regression)* is written first and shown failing on the current
code. Only then is the fix implemented.

**Telemetry rule.** Attribute keys come from `semconv/v1.43.0` constants. Telemetry tests assert
the exact metric name, unit, and attribute set, and that no attribute is ever an empty string.

Each task is one commit, with the subject `<type>(<module>): <summary> (<task>, <item IDs>)`.

---

## Task 1: Commit the spec and plan, open a draft PR

**Description:** Branch `feat/data-clients` (from main @ 411bb72). Commit SPEC-data-clients,
the capability-map link, this plan, and the task list. Open a draft PR whose body links the spec.

**Acceptance criteria:**
- [x] One `docs:` commit holds the spec, the map, and the tasks.
- [x] A draft PR targets `main`.

**Verification:**
- [x] `gh pr view --json isDraft` reports true.

**Dependencies:** None · **Files:** `docs/specs/sdk-fix-backlog/*`, `tasks/*` · **Scope:** XS

## Task 2: db telemetry

**Description:**
- The duration metric `db.client.operation.duration` keeps only the spec's metric attributes;
  `db.query.text` moves to spans only.
- Drop `db.client.operation.count`. The span name becomes `{db.operation.name}`.
- `error.type` is the driver code, or `timeout`, `canceled`, or `_OTHER`. `db.response.status_code`
  is set only when there is a driver code, and nothing is ever set to "".
- One `metricsClient()` helper and one tracer helper replace the duplicated nil-check branches.
- Attribute keys come from semconv constants. If that needs a direct semconv import that go.mod
  lacks, stop and report it; the orchestrator tidies.

**Acceptance criteria:**
- [x] *(regression)* `TestInstrument_MetricAttributes`: exact attribute set on success and on
      failure; no `db.query.text` on the metric; the span carries it.
- [x] *(regression)* `TestErrorInfo` (rework): MySQL and PostgreSQL errors give their code; an
      unknown error gives `_OTHER`, never its message.
- [x] No `db.client.operation.count`, and the span name is the operation name.

**Verification:**
- [x] db module checks (lint new-from-merge-base); coverage ≥ 87.4%.

**Dependencies:** T1 · **Files:** `db/db.go`, `db/drivers.go`, `db/db_test.go`, `db/drivers_test.go` · **Scope:** M

## Task 3: redis returns redis.UniversalClient

**Description:**
- `New(ctx, cfg, opts...)` returns `redis.UniversalClient`; delete `NewWithContext`.
- `createUnderlyingClient` returns `redis.UniversalClient`. A small wrapper embeds it and
  overrides only `Close`, which stops the pool poller.
- Delete `ReadCommands`, `WriteCommands`, `ScriptCommands`, `Client`, `Pipeline`, the
  `pipeline` wrapper, `underlyingClient`, and every alias (`Nil`, `TxFailedErr`, `Script`,
  `NewScript`, `Z`, `ZRangeBy`, `PoolStats`, `Tx`, `PubSub`, `Message`, `Subscription`).
  Delete `redis/errors.go`.
- Update tests, `example_test.go`, and README to use go-redis types.

**Acceptance criteria:**
- [x] *(regression)* `TestNew_ReturnsUniversalClient`: each mode's `createUnderlyingClient`
      returns a `redis.UniversalClient`, and the hook records a command sent through it
      (in-process, no miniredis).
- [x] `grep -n 'type .* interface\|= redis\.' redis/*.go | grep -v _test` shows only the
      unexported seams that remain, if any.
- [x] `redis/errors.go` is gone.

**Verification:**
- [x] redis module checks; coverage ≥ 68.2%.

**Dependencies:** T1 · **Files:** `redis/redis.go`, `redis/errors.go` (delete), `redis/redis_test.go`, `redis/example_test.go`, `redis/README.md` · **Scope:** L (mostly deletions)

## Task 4: s3 telemetry

**Description:**
- The metric becomes `rpc.client.call.duration` (s); drop `s3.client.operation.{count,duration}`.
- Attributes: `rpc.system.name=aws-api`, `rpc.method=S3/<Operation>`, `aws.s3.bucket`,
  `cloud.region`, `server.address`/`server.port` when known, and `error.type` on failure.
  `rpc.system` and `rpc.service` are removed.
- Keys come from semconv constants. If `aws-api` has no constant, use
  `semconv.RPCSystemNameKey.String("aws-api")`.
- One metrics helper and one tracer helper (X6).

**Acceptance criteria:**
- [x] *(regression)* `TestMiddleware_MetricAttributes`: the exact name, unit, and attribute set;
      no `rpc.system` or `rpc.service` anywhere.
- [x] The span keeps its name (the operation) and kind (client).

**Verification:**
- [x] s3 module checks; coverage ≥ 85.5%.

**Dependencies:** T1 · **Files:** `s3/s3.go`, `s3/middleware.go`, `s3/s3_test.go` · **Scope:** M

## Task 5: db Connect(ctx)

**Description:** `Connect(ctx, cfg, opts...)`. The ctx bounds the initial ping: use
`sqlx.ConnectContext`, or open and `PingContext`. Update `example_test.go`, README, and
the integration tests' call sites.

**Acceptance criteria:**
- [x] *(regression)* `TestConnect_ContextDeadline`: an already-expired ctx makes `Connect` return
      an error matching `context.DeadlineExceeded` without waiting for a dial timeout.

**Verification:**
- [x] db module checks; `go vet` compiles the examples and integration tests.

**Dependencies:** T2 · **Files:** `db/db.go`, `db/db_test.go`, `db/example_test.go`, `db/integration_test.go`, `db/README.md` · **Scope:** S

## Task 6: redis telemetry

**Description:** The same treatment as T2, for redis's command and pipeline hooks:
- The metric keeps only the spec's attributes; `db.query.text` and `db.operation.batch.size` move
  to spans only.
- Drop `db.client.operation.count`.
- `error.type` is the simple-error prefix, or `timeout`, `canceled`, or `_OTHER`; never the
  message, and never "".
- One metrics/tracer helper each (X6); remove the trivial `errorMessage` wrapper (R4).
- Semconv constants.

**Acceptance criteria:**
- [ ] *(regression)* `TestInstrumentHook_MetricAttributes` (commands and pipelines): exact
      attribute sets; no query text, batch size, or message on metrics.

**Verification:**
- [ ] redis module checks; coverage ≥ 68.2%.

**Dependencies:** T3 · **Files:** `redis/redis.go`, `redis/redis_test.go` · **Scope:** M

## Task 7: s3 New(ctx), cleanup, testing recipe, strict

**Description:**
- `New(ctx, cfg, opts...)`: the ctx bounds `LoadDefaultConfig`.
- Fix every full-tree lint finding, including errors.As → AsType (X8) and test wording (X9).
- S2: `// gostyle:ignore getter-prefix` with the reason (S3's API names the operation
  `GetObject`), and drop the redundant import aliases.
- Add a README "Testing your code" section showing gofakes3 + httptest with
  `Config{Endpoint: ts.URL}` (N19), written as documentation, not a dependency.
- Update `example_test.go`. Add `s3` to `STRICT_LINT_MODULES`.

**Acceptance criteria:**
- [ ] *(regression)* `TestNew_ContextCanceled`: a canceled ctx makes `New` fail.
- [ ] Full-tree lint 0 and gostyle 0 in s3; `s3` is in the strict list.

**Verification:**
- [ ] s3 module checks (strict).

**Dependencies:** T4 · **Files:** `s3/s3.go`, `s3/s3_test.go`, `s3/example_test.go`, `s3/README.md`, `.github/workflows/ci.yml` · **Scope:** M

### Checkpoint A: Opus review of T1–T7
- [ ] Spec criteria 2–4 hold for every module so far; CI is green; review fixes are applied.

## Task 8: db cleanup, testing recipe, strict

**Description:**
- D3: write `TestBindQuery_TypedNilValuer` first. If it panics, mirror database/sql's nil-pointer
  Valuer handling; if it passes, drop D3 and note that in the commit.
- D4: goimports, `reflect.DeepEqual` → `==`/`slices.Equal`, and `Config` json tags.
- X8 AsType, X9 wording, and every other full-tree lint finding.
- Add a README "Testing your code" section showing sqlmock via
  `Config{Driver: "sqlmock", DSN: ...}` (N19).
- Add `db` to `STRICT_LINT_MODULES`.

**Acceptance criteria:**
- [ ] *(regression, conditional)* D3 is fixed with a failing-first test, or dropped with evidence.
- [ ] Full-tree lint 0 and gostyle 0 in db; `db` is in the strict list.

**Verification:**
- [ ] db module checks (strict); coverage ≥ 87.4%.

**Dependencies:** T5 · **Files:** `db/bind.go`, `db/bind_test.go`, `db/config.go`, `db/drivers.go`, `db/README.md`, `.github/workflows/ci.yml` · **Scope:** M

## Task 9: redis pool metrics in semconv form

**Description:**
- Replace the six custom pool instruments with `db.client.connection.count` (up-down counter,
  `{connection}`, `db.client.connection.state` = idle|used, plus `db.client.connection.pool.name`)
  and `db.client.connection.timeouts` (counter, `{timeout}`). Each poll records deltas, with the
  same attributes on increment and decrement.
- Drop hits, misses, and stale.
- The poller has a visible end: `Close` closes stop and waits on a WaitGroup (R3).
- One poll helper; no duplicated branches (R2).

**Acceptance criteria:**
- [ ] *(regression)* `TestPoolMetrics`: driving PoolStats through a sequence (via a stubbed stats
      source) gives the right running count per state and timeout total; after `Close`, the
      poller has exited (WaitGroup).

**Verification:**
- [ ] redis module checks; coverage ≥ 68.2%.

**Dependencies:** T6 · **Files:** `redis/redis.go`, `redis/redis_test.go` · **Scope:** M

## Task 10: redis cleanup, testing recipe, strict

**Description:**
- Confirm `New(ctx)` from T3 bounds the ping with ctx (add `TestNew_ContextCanceled` if T3 didn't).
- X8 AsType, X9 wording, and every other full-tree lint finding, including the removed
  `min`/`max` and `interfacebloat` nolints if those declarations are gone.
- Add a README "Testing your code" section showing miniredis with `Config{Addrs: []string{m.Addr()}}`
  (N19).
- Add `redis` to `STRICT_LINT_MODULES`.

**Acceptance criteria:**
- [ ] Full-tree lint 0 and gostyle 0 in redis; `redis` is in the strict list; no stale `//nolint`
      remains.

**Verification:**
- [ ] redis module checks (strict).

**Dependencies:** T9 · **Files:** `redis/redis.go`, `redis/redis_test.go`, `redis/README.md`, `.github/workflows/ci.yml` · **Scope:** M

### Checkpoint B: complete
- [ ] SPEC-data-clients success criteria 1–7 are verified with evidence.
- [ ] Final Opus review, with its fixes applied.
- [ ] The PR body (prepared by the orchestrator) lists every renamed or dropped metric and
      attribute and the API breaks. The owner updates it and marks the PR ready, or tells the
      orchestrator to.
