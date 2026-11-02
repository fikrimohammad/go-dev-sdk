# Spec: messaging

Module id: `messaging` ([capability map](capability-map.md)). Status: approved 2026-11-02;
assumptions 1–16 confirmed by the owner, with N21's InProgress and `ErrNoRetry` brought into scope.
Items: X1, X2–X7, N1–N21, C1–C6 ([item index](../../ideas/sdk-fix-backlog.md#item-index)).
Owner decisions already made (2026-11-01) are marked **decided**. They are not open again.

## Objective

Make `nats` (producer, consumer) and `cron` (engine) correct under failure and shutdown, and
make their telemetry follow semconv v1.43.0 with bounded metric cardinality. Both modules end
clean on the full tree and strict in CI.

These are the motivating bugs:

| Item | Bug | Evidence |
| --- | --- | --- |
| N18 | `PublishSyncWithDelay` has never worked against a real server. | JetStream rejects the publish with err_code 10190. |
| N20 | Two messages with the same business key are silently deduplicated. | The key was used as `Nats-Msg-Id`. "order 42 created" and then "order 42 paid" stored only one message. |
| N1 | Handlers run on `context.Background()`. | Shutdown can't cancel them. |
| N3 | Shutdown can hang forever. | |
| N2 | A consumer that never started leaks its connection. | |
| N5 | Producer traces are parented wrong. | |
| X1 | A panicking handler or job is never measured. | Panic recovery sits outside the metrics middleware. |
| N10 | Metrics carry message ids and keys, one series per message. | |
| C1 | Metrics carry fencing tokens and error messages, one series per message or run. | |

### User stories

- As a service author, a delayed publish arrives once, at its delay, and two messages sharing
  a business key both arrive.
- As a service author, my handler gets the message key, tag, and delivery count. That lets me
  dedupe and spot redeliveries, and I can unit-test the handler without a server.
- As a service author, `Shutdown(ctx)` cancels in-flight handlers, waits for them, and returns
  by my deadline.
- As an SRE, messaging and cron metrics have bounded attribute sets. Their names match the OTel
  messaging conventions, and a panic is counted as a failure.
- As an operator, starting a producer never resets stream settings I tuned, such as replicas
  and max age.

## Assumptions (correct any of these before approving)

1. **Messaging metrics use the current semconv names** (N11). Today's
   `messaging.{publish,process}.{messages,duration}` are deprecated. Names, units, and
   descriptions come from `semconv/v1.43.0/messagingconv`, as db does with `dbconv`.

   | Side | Duration histogram (s) | Counter (`{message}`) |
   | --- | --- | --- |
   | Producer | `messaging.client.operation.duration` | `messaging.client.sent.messages` |
   | Consumer | `messaging.process.duration` | `messaging.client.consumed.messages` |

   These counters are kept, unlike db's: semconv defines them. Only a successful operation
   increments a counter, while the histogram records every outcome.
2. **Messaging attributes** (N10, X2, X4).
   - Metrics carry only these:
     - `messaging.system` (`nats`; open enum)
     - `messaging.operation.name` (`publish` or `process`)
     - `messaging.operation.type` (`send` or `process`)
     - `messaging.destination.name` (the topic)
     - `messaging.consumer.group.name` (consumer side)
     - `server.address` and `server.port`
     - `error.type`, on failure only
   - Spans also carry these:
     - `messaging.nats.subject`
     - `messaging.message.id` (the stream sequence)
     - `messaging.message.body.size`
     - `messaging.nats.message.key` and `messaging.nats.message.tag`
     - `messaging.nats.message.delivery_count` (consumer side; semconv has no generic
       key, so this mirrors `messaging.servicebus.message.delivery_count`)
   - Span names follow semconv: `publish <topic>` and `process <topic>`. Today they are
     `<topic> publish` and `<topic> process`.
3. **`error.type` is low-cardinality** (X3), as in data-clients:
   - A `*errs.Error` gives its code (the errs package, so a service's own catalog).
   - A JetStream API error gives its error code (for example `10071`).
   - `context.DeadlineExceeded` and a `net.Error` timeout give `timeout`.
   - `context.Canceled` gives `canceled`.
   - A recovered panic gives `panic`.
   - Anything else gives `_OTHER`.
   - It is never the message, and never set on success.
   - cron uses the same rule, minus the JetStream case.
4. **Lifecycle: I/O moves into `Start`, and shutdown takes a ctx** (N2, N3, X5).
   - `Register(cfg[, handler])` only validates and stores. Today it dials NATS, which is why a
     consumer that is registered but never started leaks its connection.
   - `Start(ctx)` connects, ensures the stream, and (for the consumer) creates the durable
     consumer, all bounded by ctx.
   - `Shutdown(ctx)`:
     - stops intake;
     - cancels in-flight handlers;
     - waits for them and for pending async publishes until ctx is done;
     - drains the connection;
     - returns `ctx.Err()` if the deadline wins.
   - Calling `Shutdown` without `Start` is a no-op.
5. **Handler ctx and long handlers** (N1, N21; owner decision 2026-11-02: automatic heartbeat).
   - Each handler gets a ctx derived from the consumer's run context, and `Shutdown` cancels it.
   - Its deadline is a new `Config.HandlerTimeout`, which defaults to `Config.Timeout` (the
     consumer's AckWait).
   - While the handler runs, the adapter calls `InProgress` every AckWait/2. A handler that
     outlives AckWait is therefore not redelivered mid-run, and the deadline still bounds a
     runaway handler.
   - Handlers stay unaware of acks.
6. **Producer API takes a `Message` struct** (N13):
   - `PublishSync(ctx, msg)`
   - `PublishSyncWithDelay(ctx, msg, delay)`
   - `PublishAsync(ctx, msg, callback)`

   It is `producer.Message{Topic, Tag, Key string; Body []byte}`. `producer.Client` stays, with
   the new signatures (N12, **decided**).
7. **Consumer handler takes a `Message` interface** (N21, **decided**):
   - The handler is `HandlerFunc(ctx, msg consumer.Message) error`.
   - `Message` exposes `Key`, `Tag`, `Subject`, `Body`, `Sequence`, `DeliveryCount`, and
     `Timestamp`, parsed once when the message is wrapped.
   - A message without JetStream metadata is nak'd and never reaches the handler.
   - Ack, Nak, Term, and InProgress are not exposed. The returned error decides the outcome:
     - nil acks;
     - an error matching `consumer.ErrNoRetry` (`errors.Is`) terms the message, so a poison
       message is never redelivered (owner decision 2026-11-02, in scope);
     - any other error naks it, with `RetryBackoff`.
   - `Message` lives in `consumer/consumer.go` (R4).
8. **No infra-level dedup; SDK-owned headers** (N20, **decided**):
   - The SDK never sets `Nats-Msg-Id`.
   - The key and tag travel in `Sdk-Msg-Key` and `Sdk-Msg-Tag`.
   - The READMEs document at-least-once delivery both ways and tell handlers to dedupe on
     `Key()`.
9. **One wire package owns the stream layout and headers** (N7, N15, N18, **decided**):
   - `nats/internal/wire` holds:
     - `StreamConfig(topic)`: subjects `[topic, topic.>, schedules.topic.>]`, plus
       `AllowMsgSchedules`.
     - `DataSubjects(topic)`
     - the header constants
     - `ServerAddrPort`
     - the shared connect options (today copy-pasted in both factories)
   - The **producer owns the stream**. On `Start` it reads the existing config and patches only
     the SDK-owned fields: it adds missing subjects and sets `AllowMsgSchedules`.
   - The consumer only looks the stream up, creating it from `StreamConfig` only if it is
     missing.
10. **Delayed publish** (N9, N18, **decided**):
    - A delayed message publishes on `schedules.<topic>.<nuid>`, with `Nats-Schedule-Target`
      set to the data subject.
    - `@at` is formatted with `time.RFC3339Nano`.
    - The default consumer filter (`Tags: ["*"]`) is `DataSubjects(topic)` instead of no filter,
      so the schedule message is never delivered.
11. **`ConsumeFromLast` means new messages only.** Today it maps to `DeliverLastPolicy`, so a
    new durable first gets the newest stored message, an old one. It becomes
    `DeliverNewPolicy`. `ConsumeFromFirst` and `ConsumeFromTimestamp` don't change.
12. **Ack/Nak failures are logged, not swallowed** (N8). The adapter logs them at Warn with the
    topic, group, and stream sequence, and sets them on the span. They aren't returned: the
    JetStream callback has no caller.
13. **Middleware order** (X1, C4).
    - Consumer chain: tracer → logger → metrics → panic recovery → handler.
    - cron chain: tracer → logger → toggler → locker → metrics → panic recovery → handler.
    - Panic recovery is innermost, as in rest/server, so a panic is traced, logged, and
      measured.
    - Only the logger logs an outcome. The toggler and locker return their errors
      (`ErrJobDisabled`, `ErrLockHeld`, or a wrapped lock failure) without logging them (C4).
    - The logger logs a skip at Info, not Error.
14. **cron telemetry** (C1). cron jobs have no semconv. These names are custom and
    `cron.`-namespaced:
    - The `cron.job.runs` counter is dropped, as in data-clients: the count is the histogram's.
    - `cron.job.duration` (histogram, s) keeps only `cron.job.name`, `cron.job.trigger`
      (`scheduled` or `manual`), and `error.type` on failure.
    - `status` is dropped, because the presence of `error.type` already says it failed.
    - `fencing_token` goes on the span only, as `cron.job.fencing_token`.
    - Skipped runs (disabled, or lock held) are not measured. They are logged, and the span
      records them.
15. **cron lifecycle fixes** (C2, C3, C5, X7).
    - `Register`'s started check and the job-map write happen under one lock, which `Start`
      also takes.
    - Scheduled runs derive from the engine's stop context instead of `context.Background()`.
    - `Unlock` runs on `context.WithoutCancel(ctx)` with its 5 s timeout. It keeps the trace
      context, and a canceled job still releases its lock.
    - `errors.Join(ErrInvalidSchedule, err)` becomes `fmt.Errorf("%w: %w", …)`.
    - `inFlight` keeps only names: the stop context already cancels the runs, so the stored
      cancel funcs go.
    - The `nil`-ctx fallbacks in `runJob` and `Stop` go (X7).
16. **N19 testing recipes.** nats already depends on nats-server for its parity tests, so its
    README recipe shows an embedded JetStream server (`test.RunServer` with JetStream on), and
    `example_test.go` stays compile-only. Handlers are unit-tested with a fake
    `consumer.Message`, and the README shows one. cron needs no fake: the README shows calling
    a `HandlerFunc` directly and `Engine.Trigger`.

## Tech Stack

- Go 1.26.5. Modules: `nats` (packages producer and consumer, plus the new `internal/wire`) and
  `cron` (packages cron and engine).
- `github.com/nats-io/nats.go` v1.54.0 (`jetstream`) and `github.com/nats-io/nuid` v1.0.1 (already
  in go.mod as indirect; it becomes direct for schedule subjects, so no new module) and
  `github.com/nats-io/nats-server/v2` v2.15.0 (tests only).
- `go.opentelemetry.io/otel` v1.46.0 (`semconv/v1.43.0`, `messagingconv`).
- `github.com/robfig/cron/v3` v3.0.1.

## Commands

Run inside each module (AGENTS.md verification, strict):

```bash
gofmt -l .
go vet ./...
go test -count=1 -shuffle=on ./... && go test -count=1 -race -shuffle=on ./...
golangci-lint run ./...                      # full tree; nats and cron join STRICT_LINT_MODULES
GOWORK=off go run ../.agents/skills/go-best-practices/scripts/gostyle.go ./...
go mod tidy -diff
```

Callers of the changed APIs: none outside these modules. The testing app is migrated after
`guardrails`.

## Public API

### nats/producer

| Before | After |
| --- | --- |
| `Register(cfg)` dials NATS | `Register(cfg)` validates and stores only |
| `Start() error` | `Start(ctx) error`: connects and ensures the stream (patching, never resetting) |
| `Shutdown() error` | `Shutdown(ctx) error`: waits for pending async publishes until ctx is done |
| `PublishSync(ctx, topic, tag, key, body)` (and `…WithDelay`, `PublishAsync`) | `PublishSync(ctx, Message)`, `PublishSyncWithDelay(ctx, Message, delay)`, `PublishAsync(ctx, Message, callback)` |
| `Nats-Msg-Id = key`, `Nats-Msg-Tag = tag` | `Sdk-Msg-Key`, `Sdk-Msg-Tag`; no `Nats-Msg-Id` |

### nats/consumer

| Before | After |
| --- | --- |
| `Register(cfg, HandlerFunc)` dials NATS and creates the stream | Validates and stores only |
| `Start() error`, `Shutdown() error` | `Start(ctx) error`, `Shutdown(ctx) error` (cancels handlers, waits until ctx is done) |
| `HandlerFunc(ctx, body []byte) error`; ctx is `Background` | `HandlerFunc(ctx, msg Message) error`; ctx is canceled on Shutdown, with the deadline at AckWait |
| Default filter: none | `DataSubjects(topic)` |
| Handler deadline: none; AckWait-length handlers are redelivered mid-run | `Config.HandlerTimeout` (default `Timeout`) bounds the handler, and an automatic InProgress heartbeat every AckWait/2 prevents redelivery |
| Every handler error naks | `ErrNoRetry` (in `errors.go`) terms the message; other errors nak |
| `ConsumeFromLast` = `DeliverLastPolicy` | `DeliverNewPolicy` |

### cron/engine

| Before | After |
| --- | --- |
| Metrics `cron.job.runs`, `cron.job.duration`, with attributes `job.name`, `trigger`, `status`, `fencing_token`, and the message as `error.type` | `cron.job.duration` only, with `cron.job.name`, `cron.job.trigger`, and coded `error.type` |
| `Stop(nil)` allowed | `Stop(ctx)` requires a ctx |

## Telemetry Contract

| Signal | Producer | Consumer | cron |
| --- | --- | --- | --- |
| Span | `publish <topic>`, producer | `process <topic>`, consumer, parent = extracted context | `cron.<job>`, internal |
| Duration (s) | `messaging.client.operation.duration` | `messaging.process.duration` | `cron.job.duration` |
| Counter | `messaging.client.sent.messages` (success) | `messaging.client.consumed.messages` (every delivery) | none |
| Metric attributes | system, operation.name, operation.type, destination.name, server.*, `error.type`† | Producer's attributes plus `messaging.consumer.group.name` | `cron.job.name`, `cron.job.trigger`, `error.type`† |
| Span-only | subject, message.id (PubAck sequence), body.size, key, tag | subject, message.id (stream sequence), body.size, key, tag, delivery count | `cron.job.fencing_token` |

† Present only on failure. No attribute is ever an empty string. Keys come from semconv
constants where one exists. Trace context is injected after the producer span starts (N5), so
the consumer span's parent is the producer span.

## Code Style

The consumer-facing message, as the handler sees it:

```go
// Message is one delivered message, as a handler sees it. Its values are
// parsed once when the message arrives.
type Message interface {
	// Key returns the business key set by the producer (Sdk-Msg-Key), or "".
	Key() string
	// Tag returns the producer's tag (Sdk-Msg-Tag), or "".
	Tag() string
	Subject() string
	Body() []byte
	// Sequence is the stream sequence: unique per stored message.
	Sequence() uint64
	// DeliveryCount is 1 on first delivery and grows on each redelivery.
	DeliveryCount() uint64
	Timestamp() time.Time
}
```

All of AGENTS.md applies, including R1–R4 (Config methods, examples, `errors.go`, `<pkg>.go`).
The handler adapter's middleware follows the data-clients pattern: `metricsClient()` and
`tracerClient()` helpers that resolve per call (X6), with attribute maps built once and cloned
for span-only additions.

## Testing Strategy

- Package `testing`, table tests, and the mirror rule. Telemetry is asserted through an
  `sdkmetric.ManualReader` and a span recorder.
- Behaviour that needs a real JetStream (N9, N18, N20, N7, ownership) runs against an
  in-process `nats-server/v2/test` server, started by a helper in `helpers_test.go`. It is
  hermetic, so it needs no env var. Those tests live in the mirror file of the code they cover.
- **Regression tests.** Each must fail on `main` and pass after the change:

| Test | Proves |
| --- | --- |
| producer `TestPublishSyncWithDelay_DeliversOnceAtDelay` | (N18, N9) Two concurrent delayed publishes each arrive exactly once, no sooner than their delay (within 100 ms). No schedule message reaches the handler. Tag and trace propagate. |
| producer `TestPublishSync_SameKeyNotDeduplicated` | (N20) Two messages with one key are both stored and both delivered. |
| producer `TestStart_PreservesOperatorStreamSettings` | (N7) A stream created with `MaxAge` and `Replicas` keeps them after `Start`, and gains the SDK subjects. |
| producer `TestPublish_TraceParenting` | (N5) The injected `traceparent` names the producer span, not its parent. |
| producer `TestShutdown_WaitsForAsyncPublish` | (N6) `Shutdown` returns after pending `PublishAsync` callbacks have run. |
| consumer `TestHandler_ContextCanceledOnShutdown` | (N1) A blocked handler sees ctx canceled when `Shutdown` runs, and its ctx has the `HandlerTimeout` deadline. |
| consumer `TestHandler_HeartbeatPreventsRedelivery` | (N21) With AckWait 1 s and `HandlerTimeout` 5 s, a 2.5 s handler is delivered exactly once (embedded JetStream). |
| consumer `TestHandler_ErrNoRetryTerms` | (N21) A handler returning a wrapped `ErrNoRetry` sees the message once; it is termed, not redelivered. |
| consumer `TestShutdown_RespectsDeadline` | (N3) `Shutdown(ctx)` returns `ctx.Err()` by the deadline while a handler ignores cancellation. |
| consumer `TestShutdown_WithoutStart` | (N2) It doesn't dial and doesn't leak a connection. `Register` alone dials nothing. |
| consumer `TestHandler_PanicIsMeasured` | (X1) A panicking handler produces `messaging.process.duration` with `error.type=panic`, and the message is nak'd. |
| consumer `TestHandler_MessageMetadata` | (N21) The handler sees the key, tag, sequence, and delivery count (2 on redelivery). |
| consumer `TestConsumer_DefaultFilterSkipsSchedules` | (N18) With `Tags: ["*"]`, only data subjects are delivered. |
| producer and consumer `Test…MetricAttributes` | (N10, N11) The exact metric names, units, and attribute sets. No id, key, subject, or message appears on a metric. |
| engine `TestJob_PanicIsMeasured` | (X1) As for the consumer, for cron. |
| engine `TestMetrics_Attributes` | (C1) Only `cron.job.name`, `cron.job.trigger`, and `error.type`. No fencing token or message. |
| engine `TestRegister_RacesStart` | (C2) A concurrent `Register` and `Start` never schedules a job after `Start`, under `-race`. |
| engine `TestUnlock_AfterCancel` | (C3) A canceled job still unlocks, with the trace context kept. |

- **Coverage:** not below `main`. The baselines are nats/consumer 68.6%, nats/producer 70.1%,
  cron 100%, and cron/engine 93.7%.

## Boundaries

- **Always:**
  - Write the failing test first for every changed behaviour.
  - Take attribute keys from semconv constants.
  - Update the READMEs and examples for the new signatures.
  - Run the strict checks.
- **Ask first:**
  - Adding a dependency.
  - Changing any assumption above.
  - Touching modules other than nats and cron.
  - Exposing Ack/Nak/InProgress to handlers.
- **Never:**
  - Set `Nats-Msg-Id`.
  - Put a message id, key, subject, fencing token, or error message on a metric.
  - Add deprecation shims.
  - Weaken or skip a test to get to green.

## Out of Scope

These are separate projects (owner decision 2026-11-02), not tracked by this backlog:

- A dead-letter queue for messages that exhaust `MaxRetryAttempts`. Today JetStream stops
  redelivering them. The README documents this, and `ErrNoRetry` covers known-poison messages.
- Query-by-key tooling. JetStream has no header index, so it needs a scan or an external index
  (N20 note).

## Success Criteria

1. nats and cron report 0 findings on full-tree `golangci-lint run ./...` and gostyle, and join
   `STRICT_LINT_MODULES`.
2. No metric in these modules carries a message id, key, subject, body size, fencing token, or
   error message. The telemetry tests assert the exact attribute sets.
3. The metric names, units, and instrument types match semconv v1.43.0 `messagingconv`. The
   deprecated `messaging.{publish,process}.*` and `cron.job.runs` are gone.
4. A delayed publish arrives once at its delay. Same-key messages are both delivered. `Start`
   preserves the operator's stream settings. All three are proven against an embedded
   JetStream.
5. `Start(ctx)` and `Shutdown(ctx)` are bounded by ctx. `Register` performs no I/O. Handlers get
   a cancelable ctx with a deadline. `Shutdown` without `Start` is a no-op.
6. A panic in a handler or job is traced, logged, and measured with `error.type=panic`.
7. Every regression test in the table exists and fails on `main`.
8. Every go.work module passes the module checks, and CI is green.

## Open Questions

- None. The assumptions were confirmed on 2026-11-02.
