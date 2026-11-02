# Tasks: messaging (SDK fix backlog, PR 3)

Plan: [plan.md](plan.md). Spec: [SPEC-messaging](../docs/specs/sdk-fix-backlog/SPEC-messaging.md).

**Module checks.** Run these in every module a task touches (AGENTS.md verification):
- `gofmt -l .`
- `go vet ./...`
- `go test -count=1 -shuffle=on ./...` and `-race`
- lint: full-tree `golangci-lint run ./...` once a module is strict, otherwise
  `--new-from-merge-base=main`
- gostyle
- `go mod tidy -diff`

In parallel nats waves, an agent runs these on its own package path and the orchestrator runs
them on the whole module.

**Test-wording check.** For the lines a task changes, this must print nothing:
`git diff -U0 | grep '^+' | grep -E 't\.(Errorf|Fatalf|Error|Fatal)\(.*\b(should|expected|actual|Expected|Actual)\b'`

**TDD rule.** Every test marked *(regression)* is written first and shown failing on the current
code. Only then is the fix implemented.

**Telemetry rule.**
- Names, units, and descriptions come from `semconv/v1.43.0/messagingconv` where they exist.
- Attribute keys come from semconv constants.
- Tests assert the exact name, unit, instrument type, and attribute set.

**JetStream rule.** Tests that need a server use the embedded `nats-server/v2/test` helper in
`helpers_test.go`, which runs with JetStream on, a temp store dir, and cleanup. Timing tests
assert order and counts, with tolerances of at least 100 ms.

Each task is one commit, with the subject `<type>(<module>): <summary> (<task>, <item IDs>)`.

---

## Task 1: Commit the spec and plan, open a draft PR

**Description:** On branch `feat/messaging` (from main @ 216b2aa), commit SPEC-messaging, the
capability-map link, the backlog-doc sync, this plan, and the task list. Open a draft PR whose
body links the spec.

**Acceptance criteria:**
- [x] One `docs:` commit holds them.
- [x] A draft PR targets `main`.

**Verification:**
- [x] `gh pr view --json isDraft` reports true.

**Dependencies:** None · **Files:** `docs/**`, `tasks/*` · **Scope:** XS

## Task 2: nats/internal/wire

**Description:** A new internal package, the single source of the stream layout, the headers,
and the connection setup:
- `StreamConfig(topic)`: subjects `[topic, topic.>, schedules.topic.>]`, plus
  `AllowMsgSchedules`.
- `DataSubjects(topic)` returns `[topic, topic.>]`.
- `ScheduleSubject(topic)` returns `schedules.<topic>.<nuid>`.
- `PatchStream(existing, topic)` adds the missing SDK subjects and sets `AllowMsgSchedules`,
  keeping every other field.
- Header constants: `HeaderKey = "Sdk-Msg-Key"` and `HeaderTag = "Sdk-Msg-Tag"`.
- `ServerAddrPort(endpoints)`: moved from the two packages.
- `Options` plus `Connect(name, opts)`: the token, user, nkey, and creds options both factories
  copy today.
- Nothing calls it yet; T3 and T4 switch over. The nuid import is the one go.mod change, and
  the orchestrator promotes it after the agent stops.

**Acceptance criteria:**
- [x] Table tests cover `StreamConfig`, `DataSubjects`, `ScheduleSubject` (unique across
      calls), `PatchStream`, and `ServerAddrPort`. For `PatchStream`: operator `MaxAge` and
      `Replicas` are kept, subjects are de-duplicated, and an existing extra subject is kept.
- [x] The package comment and doc comments follow Go Doc Comments.

**Verification:**
- [x] nats module checks (lint new-from-merge-base); `go test ./internal/...` covers 90% or more.

**Dependencies:** T1 · **Files:** `nats/internal/wire/wire.go`, `nats/internal/wire/wire_test.go`, `nats/go.mod` (orchestrator) · **Scope:** S

## Task 3: producer lifecycle

**Description:**
- `Register(cfg)` validates and stores only, with no dial.
- `Start(ctx)` connects through `wire.Connect`. It ensures the stream: if the stream exists,
  it is `PatchStream`ed and `UpdateStream` is called only when something changed; if it is
  missing, it is created from `StreamConfig`. All of this is bounded by ctx.
- `Shutdown(ctx)` waits for pending `PublishAsync` callbacks (a WaitGroup) and then drains,
  until ctx is done; then it returns `ctx.Err()`. Without `Start`, it is a no-op.
- The `sync.Map` registry plus the mutex become a plain map under the mutex (N14).
- `serverAddrPort` is replaced by `wire.ServerAddrPort`.
- Keep the positional publish API (T5 changes it). Update the callers in `producer/*_test.go`
  and `example_test.go`.

**Acceptance criteria:**
- [x] *(regression)* `TestStart_PreservesOperatorStreamSettings` (embedded JetStream):
      precreate a stream with MaxAge 1h and only `[topic]`; after `Start`, MaxAge is kept and
      the subjects include `topic.>` and `schedules.topic.>`.
- [x] *(regression)* `TestShutdown_WaitsForAsyncPublish`: Shutdown returns only after a pending
      async callback has run (a stub transport that delays the ack).
- [x] `TestRegister_NoIO`: Register with an unreachable endpoint succeeds and dials nothing.
- [x] `TestShutdown_Deadline`: a hung drain returns `ctx.Err()` by the deadline.

**Verification:**
- [x] `./producer/...` checks; coverage ≥ 70.1%.

**Dependencies:** T2 · **Files:** `nats/producer/producer.go`, `nats/producer/producer_test.go`, `nats/producer/helpers_test.go`, `nats/producer/mock_test.go`, `nats/producer/example_test.go` · **Scope:** M

## Task 4: consumer lifecycle

**Description:**
- `Register(cfg, handler)` validates and stores only.
- `Start(ctx)`:
  - connects through `wire.Connect`;
  - looks the stream up, and creates it from `StreamConfig` only if it is missing;
  - creates or updates the durable consumer, all bounded by ctx.
- The default filter (`Tags` empty or `["*"]`) is `wire.DataSubjects(topic)`.
- `ConsumeFromLast` maps to `DeliverNewPolicy`.
- `Shutdown(ctx)`:
  - stops intake: `consumeCtx.Drain()`, then wait on `Closed()` (N4: no `wg.Add` after Wait
    can start);
  - waits for in-flight handlers until ctx is done;
  - drains the connection;
  - returns `ctx.Err()` on timeout.
  - Without `Start`, it is a no-op.
- The plain-map registry and `wire.ServerAddrPort`.
- Keep `HandlerFunc(ctx, []byte)` (T6 changes it). Handler ctx cancellation is T6.

**Acceptance criteria:**
- [x] *(regression)* `TestShutdown_WithoutStart`: Register then Shutdown dials nothing and
      leaks nothing (a factory spy counts dials).
- [x] *(regression)* `TestShutdown_RespectsDeadline`: with a handler that ignores ctx, Shutdown
      returns `context.DeadlineExceeded` by the deadline (±200 ms).
- [x] *(regression)* `TestConsumer_DefaultFilterSkipsSchedules` (embedded JetStream): a message
      published raw on `schedules.<topic>.x` is not delivered; `topic` and `topic.tag` are.
- [x] `TestBuildConsumerConfig` covers `DeliverNewPolicy` for "last".

**Verification:**
- [x] `./consumer/...` checks under `-race`; coverage ≥ 68.6%.

**Dependencies:** T2 · **Files:** `nats/consumer/consumer.go`, `nats/consumer/config.go`, `nats/consumer/consumer_test.go`, `nats/consumer/config_test.go`, `nats/consumer/helpers_test.go` · **Scope:** M

## Task 7: cron lifecycle

**Description:**
- C2: `Register`'s started/stopping check and the map write happen under the same mutex that
  `Start` takes.
- C3:
  - Scheduled runs use the engine's stop context as their parent (no `Background`).
  - `Unlock` uses `context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)`.
- X1/C4:
  - The chain becomes tracer → logger → toggler → locker → metrics → panicRecovery → handler.
  - The toggler and locker return their errors without logging.
  - The logger logs a skip (`ErrJobDisabled`, `ErrLockHeld`, `ErrJobRunning`) at Info and a
    failure at Error.
- C5:
  - `fmt.Errorf("%w: %w", ErrInvalidSchedule, err)`.
  - `inFlight` becomes a set of names under a mutex; the stored cancel funcs go, since
    `stopCancel` covers them.
- X7: drop the nil-ctx fallbacks in `runJob` and `Stop`.
- Telemetry names don't change here; that is T8.

**Acceptance criteria:**
- [x] *(regression)* `TestRegister_RacesStart`: a concurrent `Register` and `Start` under
      `-race`. Either Register returns `ErrEngineAlreadyStarted` or the job is scheduled, never
      a race report.
- [x] *(regression)* `TestUnlock_AfterCancel`: a job canceled by `Stop` still calls Unlock with
      a live ctx that keeps the span context.
- [x] *(regression)* `TestJob_PanicIsMeasured`: a panicking job produces a duration point with
      an error, and the span has error status.
- [x] `TestRegister_InvalidSchedule`: `errors.Is` matches both `ErrInvalidSchedule` and the
      parser error.

**Verification:**
- [x] cron module checks (lint new-from-merge-base); coverage: cron 100%, engine ≥ 93.7%.

**Dependencies:** T1 · **Files:** `cron/engine/engine.go`, `cron/engine/middleware.go`, `cron/engine/engine_test.go`, `cron/engine/middleware_test.go` · **Scope:** M

## Task 8: cron telemetry, docs, recipe, strict

**Description:**
- C1:
  - Drop the `cron.job.runs` counter.
  - `cron.job.duration` (s) keeps only `cron.job.name`, `cron.job.trigger`, and `error.type` on
    failure.
  - `error.type`: an `*errs.Error` code, `timeout`, `canceled`, `panic`, or `_OTHER`. The code is
    read through `errors.AsType[interface{ Code() int }]`, so cron gains no dependency on
    errs.
  - The span gets `cron.job.fencing_token`, and `status` goes.
- X6: one metrics helper and one tracer helper, resolved per call.
- C6: package comments (cron and engine) and Config field docs.
- Fix every full-tree lint finding (X8, X9).
- Add a README "Testing your code" section: call the `HandlerFunc` directly, or use
  `Engine.Trigger` with fake `Toggler`/`Locker`.
- Add `cron` to `STRICT_LINT_MODULES`.

**Acceptance criteria:**
- [x] *(regression)* `TestMetrics_Attributes`: the exact name, unit, and attribute sets on
      success and failure. No fencing token, status, or message; no `cron.job.runs`.
- [x] Full-tree lint 0 and gostyle 0 in cron; `cron` is in the strict list.

**Verification:**
- [x] cron module checks (strict).

**Dependencies:** T7 · **Files:** `cron/engine/middleware.go`, `cron/engine/middleware_test.go`, `cron/engine/config.go`, `cron/*.go` docs, `cron/README.md`, `.github/workflows/ci.yml` · **Scope:** M

### Checkpoint A: Opus review of T1–T4, T7, T8
- [ ] The spec criteria hold for cron, and the lifecycle criteria hold for nats; CI is green;
      review fixes are applied.

## Task 5: producer Message API, headers, delayed publish, trace parenting

**Description:**
- N13: `producer.Message{Topic, Tag, Key string; Body []byte}`, used by
  `PublishSync(ctx, msg)`, `PublishSyncWithDelay(ctx, msg, delay)`, and
  `PublishAsync(ctx, msg, cb)`. `Client` gets the same signatures. `Message` is a struct, not
  an interface, so it lives in `producer.go` beside `Client`.
- N20: never set `Nats-Msg-Id`; set `wire.HeaderKey` and `wire.HeaderTag`.
- N18 and N9: a delayed message publishes on `wire.ScheduleSubject(topic)`, with
  `Nats-Schedule-Target` set to the data subject and `@at` in RFC3339Nano.
- N5: the trace context is injected inside the producer span, after it starts.

**Acceptance criteria:**
- [ ] *(regression)* `TestPublishSyncWithDelay_DeliversOnceAtDelay` (embedded JetStream, read
      through a raw jetstream consumer filtered on `DataSubjects`):
  - two concurrent publishes with delays of 1 s and 1.5 s each arrive exactly once, no
    earlier than their delay minus 100 ms;
  - no schedule-subject message is seen;
  - `Sdk-Msg-Tag` and `traceparent` survive.
- [ ] *(regression)* `TestPublishSync_SameKeyNotDeduplicated`: two messages with one key give
      two stored messages.
- [ ] *(regression)* `TestPublish_TraceParenting`: the injected `traceparent` span id equals
      the producer span's.

**Verification:**
- [ ] `./producer/...` checks (`-count=3` for the timing test); coverage ≥ 70.1%.

**Dependencies:** T3, Checkpoint A · **Files:** `nats/producer/producer.go`, `nats/producer/telemetry.go` (inject only), `nats/producer/producer_test.go`, `nats/producer/example_test.go`, `nats/producer/mock_test.go` · **Scope:** M

## Task 6: consumer Message handler, heartbeat, ErrNoRetry, ack logging, panic innermost

**Description:**
- N21:
  - `type Message interface` in `consumer.go` (spec Code Style), plus an unexported wrapper
    that parses the metadata once.
  - `HandlerFunc(ctx, msg Message) error`.
  - A message without JetStream metadata is nak'd before the handler runs.
- N1 and N21 heartbeat:
  - The handler ctx derives from the consumer's run ctx, which `Shutdown` cancels.
  - Its deadline is `Config.HandlerTimeout`, a new field that defaults to `Timeout` (R1
    SetDefaults and Validate; it must be > 0).
  - While the handler runs, a heartbeat calls `msg.InProgress()` every AckWait/2.
  - The heartbeat stops before ack, nak, or term.
- N21 `ErrNoRetry` in `errors.go`: a handler error matching it with `errors.Is` terms the
  message; any other error naks with RetryBackoff.
- N8: Ack, Nak, Term, and InProgress errors are logged at Warn (topic, group, sequence) and
  added to the span as an event.
- X1: the chain is tracer → logger → metrics → panicRecovery → handler.

**Acceptance criteria:**
- [ ] *(regression)* `TestHandler_ContextCanceledOnShutdown`: a blocked handler's ctx is done
      after Shutdown, and its deadline is about `HandlerTimeout`.
- [ ] *(regression)* `TestHandler_HeartbeatPreventsRedelivery` (embedded JetStream): AckWait 1 s,
      HandlerTimeout 5 s, and a 2.5 s handler give exactly one delivery.
- [ ] *(regression)* `TestHandler_ErrNoRetryTerms`: a wrapped `ErrNoRetry` gives one delivery,
      and the message is termed.
- [ ] *(regression)* `TestHandler_MessageMetadata`: the key, tag, subject, sequence, and delivery
      count are 1, then 2 on redelivery.
- [ ] *(regression)* `TestHandler_PanicIsMeasured`: the metrics middleware sees the recovered
      panic as an error, the message is nak'd, and the span has error status.

**Verification:**
- [ ] `./consumer/...` checks under `-race` (`-count=3` for the timing tests); coverage ≥ 68.6%.

**Dependencies:** T4, Checkpoint A · **Files:** `nats/consumer/consumer.go`, `nats/consumer/handler.go`, `nats/consumer/middleware.go` (order only), `nats/consumer/config.go`, `nats/consumer/errors.go`, `*_test.go` · **Scope:** L

## Task 9: nats telemetry, both sides

**Description:**
- N11: metrics from `messagingconv`:
  - producer: `ClientOperationDuration` and `ClientSentMessages` (success only);
  - consumer: `ProcessDuration` and `ClientConsumedMessages` (every delivery).
- N10, X2, X4: the metric attributes are only `messaging.system=nats`,
  `messaging.operation.name`, `messaging.operation.type`, `messaging.destination.name`,
  `messaging.consumer.group.name` (consumer), `server.address`, `server.port`, and
  `error.type` on failure.
- The spans add these:
  - `messaging.nats.subject`
  - `messaging.message.id` (the producer's PubAck sequence, or the consumer's stream sequence)
  - `messaging.message.body.size`
  - `messaging.nats.message.key` and `.tag`
  - `messaging.nats.message.delivery_count`
- Span names become `publish <topic>` and `process <topic>`.
- X3 `error.type`: an `*errs.Error` code, a `jetstream.APIError` ErrorCode, `timeout`
  (including a `net.Error` timeout), `canceled`, `panic`, or `_OTHER`.
- X6 helpers.

**Acceptance criteria:**
- [ ] *(regression)* `TestPublish_MetricAttributes` and `TestProcess_MetricAttributes`: the exact
      names, units, and instrument types, and the exact attribute sets on success and failure.
      No id, key, tag, subject, body size, or message on metrics.
- [ ] *(regression)* `TestErrorType` (both packages): every class above.

**Verification:**
- [ ] nats module checks; coverage ≥ baselines.

**Dependencies:** T5, T6 · **Files:** `nats/producer/telemetry.go`, `nats/producer/telemetry_test.go`, `nats/consumer/middleware.go`, `nats/consumer/middleware_test.go` · **Scope:** M

## Task 10: nats cleanup, docs, testing recipe, strict

**Description:**
- N16: package comments (producer, consumer, wire) and every exported declaration.
- N17: remove the unused handlerAdapter fields and the repeated metadata lookups, and rename
  the get-prefixed helpers.
- X8 and X9 across nats, plus every other full-tree lint and gostyle finding.
- The READMEs:
  - the new APIs;
  - at-least-once delivery both ways, and deduping on `Key()`;
  - `ErrNoRetry` and `HandlerTimeout`;
  - `MaxRetryAttempts` exhaustion (JetStream stops redelivering; a DLQ is a separate
    project);
  - a "Testing your code" section: an embedded nats-server, and a fake `consumer.Message` for
    handler unit tests.
- Update `example_test.go` (compile-only) and `parity_test.go`.
- Add `nats` to `STRICT_LINT_MODULES`.

**Acceptance criteria:**
- [ ] Full-tree lint 0 and gostyle 0 in nats; `nats` is in the strict list; no stale `//nolint`.
- [ ] README snippets compile; the orchestrator checks them in a scratch module.

**Verification:**
- [ ] nats module checks (strict).

**Dependencies:** T9 · **Files:** `nats/**/*.go` (docs and renames), `nats/README.md` or the package READMEs, `.github/workflows/ci.yml` · **Scope:** M

### Checkpoint B: complete
- [ ] SPEC-messaging success criteria 1–8 are verified with evidence.
- [ ] Final Opus review, with its fixes applied.
- [ ] The PR body (prepared by the orchestrator) lists every API break and every renamed or
      dropped metric and attribute. The owner updates it and marks the PR ready, or tells the
      orchestrator to.
