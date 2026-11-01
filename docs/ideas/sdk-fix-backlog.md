# SDK Review Fix Backlog

## Problem Statement

How might we fix the review findings (all modules except confloader) in a few reviewable
PRs, and leave every module passing full-tree lint so the findings can't come back?

## Recommended Direction

Four PRs, each grouping whole modules and ordered by dependency. Inside each PR:

1. Delete what the decisions retire.
2. Fix what's left. Every [B] item lands with a test that fails without the fix.
3. Switch those modules to full-tree golangci-lint in ci.yml.

Claude implements and runs the AGENTS.md verification; the owner reviews and approves.

Decisions (settled 2026-11-01):

- X10/O6: tracer/metrics Default() wraps the OTel global providers; SetDefault and the
  parallel registry are deleted. logs.SetStreams is deleted.
- R6: redis.New returns redis.UniversalClient, wrapped only so Close stops pool metrics.
  interfaces.go and the alias vars are deleted.
- S1/N12: exported interfaces stay for now (owner decision, 2026-11-01), even though the
  research below shows every client can be tested against an in-process fake without them.
  The fakes are documented as example_test.go recipes (N19). Unexported test seams stay.
- G4: errgroup keeps the stored ctx, with a nolint and a reason.

Ground rules (from the 2026-11-01 interview):

- "Done" means no untracked debt at release, not zero debt. Every deferred item gets a
  GitHub issue that records why it's deferred and what would trigger fixing it.
- Release gates: the release/versioning repair and the confloader redesign.
- No backward compat. The in-dev testing app stays untouched until PR 4 merges and is
  migrated once.
- Fix everything, including the ~150 test-message rewordings.

## Testing Users' Code (S1/N12 research, 2026-11-01)

Each SDK constructor was driven against an in-process fake by a throwaway probe module
(go1.26.5).

| Package | Fake | Latest (proxy) | Works through the SDK constructor? | Caveat |
|---------|------|----------------|------------------------------------|--------|
| db | DATA-DOG/go-sqlmock | v1.5.2 (2024-01) | Yes: sqlmock.NewWithDSN + Config{Driver: "sqlmock", DSN} | Generic dialect: binds as '?', so the PostgreSQL $1 path isn't exercised. Use fergusstrange/embedded-postgres v1.34.0 (2026-03) for that |
| redis | alicebob/miniredis/v2 | v2.40.0 (2026-10) | Yes: Config{Addrs: [m.Addr()]}; Set, Pipelined, Eval, TTL via FastForward | Standalone only; cluster and sentinel topologies aren't faked |
| s3 | johannesboyne/gofakes3 + s3mem on httptest | v1.2.0 (2026-07) | Yes: Config{Endpoint: ts.URL}; CreateBucket, 12 MiB multipart UploadObject, parts-mode GetObject, presigned GET | Close every response body, or httptest.Server.Close blocks |
| nats | nats-server/v2/test RunServer, JetStream on | v2.15.0 (already in nats/go.mod) | Yes: Config{Endpoints: [srv.ClientURL()]}; Register, Start, PublishSync, consume | Precedent already in nats/parity_test.go |

Not chosen: go-redis/redismock (v9.2.0, 2023-10, expectation mocks without a server) and
pgxmock (native pgx only; the SDK uses database/sql).

The probe also found two nats bugs that the stub-based unit tests hide (N18, N9 confirmed),
which is direct evidence for testing against real transports.

## PRs

| PR | Modules | Items | Done when |
|----|---------|-------|-----------|
| 0 | (none) | Merge #15 (rocketmq removal) | Merged |
| 1 Foundations | observability, errs, errgroup, identity (+ compile fixes in callers) | X10/O6, O1–O4, E1–E4, G1–G4, I1, X7, X8; AGENTS.md mock convention; ireturn allow-list; CI ratchet list | These 4 modules are lint-clean on the full tree and strict in CI; tests prove Default() follows otel.Set*Provider, plus regressions for E1–E3 |
| 2 Data clients | db, redis, s3 | D1–D4, R1–R6, S2, X2–X6, N19 recipes for db/redis/s3 | Strict in CI; telemetry tests assert metric attributes contain no query text or messages; D3 proven or dropped; the recipes run as examples in CI |
| 3 Messaging + jobs | nats, cron | X1, N1–N19, C1–C6, X2–X7 | Strict in CI; regression tests for panic→metrics recorded, handler deadline, the Shutdown leak, trace parenting; N9/N18 tested against an embedded JetStream server |
| 4 Guardrails | gostyle, ci.yml | gostyle telemetry rule; strict becomes the default (confloader excluded) | The rule flags every pre-fix pattern on the PR 1–3 base commits, has 0 false positives on rest, and has its own gostyle_test.go cases |

## Item Index

Tags: [B] bug, [T] telemetry/semconv, [A] API/design, [C] cleanup/lint.

### Cross-cutting

- X1 [B] Panic recovery is the outermost middleware (cron/engine/middleware.go:210,
  nats/consumer/handler.go:38); move it innermost, as rest/server does.
- X2 [T] Per-event values on metric attributes (cron, nats, db, redis); keep them on spans only.
- X3 [T] error.type is the error message; use a low-cardinality class.
- X4 [T] error.type and db.response.status_code are set to "" on success; omit them.
- X5 [A] I/O constructors take no ctx: db.Connect, s3.New, redis.New/NewWithContext.
- X6 [C] The metrics-client nil-check branch is copy-pasted about 10 times; resolve the client once.
- X7 [C] Remove the nil-ctx fallbacks (errgroup, cron, metrics, tracer).
- X8 [C] errors.As → errors.AsType (12 sites).
- X9 [C] About 150 test messages don't use got/want wording.
- X10 [A] The parallel tracer/metrics default registry; decided above.

### nats

- N1 [B] consumer/handler.go:50: handlers run on context.Background() with no deadline and
  no cancellation on shutdown.
- N2 [B] consumer/consumer.go:96,330: Shutdown without a prior Start leaks the connection.
- N3 [B] consumer/consumer.go:105: Shutdown waits on wg.Wait() with no timeout.
- N4 [B] consumer/consumer.go:70-72: wg.Add can race with wg.Wait (unverified).
- N5 [B] producer/producer.go:254,472: trace context is injected before the producer span starts.
- N6 [B] producer/producer.go:73: PublishAsync's goroutine is fire-and-forget; Shutdown doesn't wait for it.
- N7 [B] CreateOrUpdateStream with a minimal config resets operator-tuned stream settings.
- N8 [B] consumer/handler.go:59-66: Ack/Nak errors are discarded.
- N9 [B] PublishSyncWithDelay rounds the schedule to whole seconds. Confirmed: a 2s delay
  fired after 1.2s. Fix: format @at with time.RFC3339Nano. Verified: a 1.5s target fired
  at 1.5s (±10ms).
- N10 [T] message.id, body.size, subject and keys are on metrics.
- N11 [T] Out-of-date messaging metric and attribute names.
- N12 [A] producer.Client: kept for now (owner decision).
- N13 [A] Positional topic/tag/key arguments; use a Message struct.
- N14 [C] sync.Map + mutex registry; use a plain map.
- N15 [C] serverAddrPort is duplicated; move it to nats/internal.
- N16 [C] Missing package and doc comments.
- N17 [C] Unused handlerAdapter fields; repeated metadata lookups; get-prefixed helpers.
- N18 [B] PublishSyncWithDelay has never worked against a real server. producer.go:270-273
  publishes the schedule on the subject it targets, and JetStream rejects that
  (err_code=10190 "message schedules target is invalid"). Moving schedules to their own
  subject isn't enough: an unfiltered consumer (the default, Tags ["*"]) gets the schedule
  message immediately, and then the target fires, so every delayed message would arrive
  twice, the first time with no delay. Fix, verified 2026-11-01 against an embedded
  nats-server v2.15.0:
  1. nats/internal/wire (new) is the single source of the stream layout:
     Config(topic) has subjects [topic, topic.>, schedules.topic.>] and
     AllowMsgSchedules; DataSubjects(topic) is [topic, topic.>]. The producer's Start and the
     consumer factory both use it, which also stops their two configs from drifting apart (N7).
  2. PublishSyncWithDelay publishes on schedules.<topic>.<nuid>, with
     Nats-Schedule-Target set to the target subject. Each schedule needs a unique subject:
     two schedules on one shared subject produced one delivery, because the second
     replaced the first.
  3. The consumer's buildFilterSubjects returns DataSubjects(topic) for the default tag
     "*" instead of nil. An existing durable consumer accepts the new filter when
     CreateOrUpdateConsumer updates it in place.
  Verified behavior: the filtered consumer gets the immediate message at 0s and each
  delayed message exactly once, at its delay. It never sees a schedule message. Nats-Msg-Tag
  and traceparent survive to the delivered message. Nats-Msg-Id is stripped from it: the key
  dedupes the scheduling publish (so retries are idempotent), but the consumer's
  message_id falls back to the stream sequence. Document that. An existing stream gains
  schedules.<topic>.> on update, and per-topic schedule prefixes don't overlap.
  Tests (PR 3): two concurrent delayed publishes; no schedule message reaches the handler;
  delivery at the delay; tag and trace propagate.
  https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md
- N20 [B] Silent message loss. The producer maps the business key straight to
  Nats-Msg-Id, which is JetStream's dedup id (2m window by default), and PublishSync
  discards the PubAck (producer.go:255). Verified 2026-11-01: "order 42 created" and then
  "order 42 paid", both keyed order-42, gave err=nil both times, duplicate=true on the
  second, and 1 message in the stream. Fix (owner decision 2026-11-01: there is no
  deduplication at the infra level, and each business domain dedupes in its own publisher
  and consumer, as with RocketMQ and NSQ; only the header name is still open):
  - The SDK never sets Nats-Msg-Id, so the server never dedupes. The business key goes
    only in its own header, which survives scheduled delivery (verified).
  - Document at-least-once delivery in both directions: a retried publish can store a
    message twice, and consumers get redeliveries after AckWait or a Nak. Handlers must be
    idempotent and can dedupe on the key header.
  - N21 follows from this. The consumer handler must expose the key, or domains can't
    dedupe.
  - Stream ownership (refines N7): the producer owns the stream. On Start it reads the
    existing config and patches only the SDK-owned fields (subjects, AllowMsgSchedules),
    so operator settings like replicas and max-age survive. The consumer only looks the
    stream up, creating it with defaults only if it's missing.
  - Rename Nats-Msg-Tag to the same SDK-owned prefix as the key header.
  - A future query-by-key tool needs a scan or an external index, because JetStream has
    no header index.
  - Header names (owner decision): Sdk-Msg-Key and Sdk-Msg-Tag, defined once as constants
    in nats/internal/wire, the package shared by producer and consumer (it also holds the
    N18 stream layout). The Nats- prefix is left to headers the NATS server interprets.
- N21 [A] HandlerFunc(ctx, msgBody []byte) hides the key and delivery metadata, so a
  domain can't dedupe or tell a redelivery from a first delivery. Fix (owner decision
  2026-11-01): HandlerFunc(ctx, msg Message), where Message is an SDK interface over
  jetstream.Msg that exposes only process-meaningful attributes: Key, Tag, Subject, Body,
  Sequence, DeliveryCount, and Timestamp.
  - Metadata is parsed once, when the message is wrapped; the getters return plain values.
    A message without JetStream metadata is nak'd by the adapter and never reaches the
    handler.
  - Ack, Nak, Term, and InProgress aren't exposed: the returned error still decides
    ack/nak.
  - Handlers can be unit-tested with a small fake Message, with no server.
  - Deferred, to be tracked as an issue: InProgress for handlers that run longer than
    AckWait, and an ErrNoRetry return that terms poison messages.
- N19 [C] Add example_test.go testing recipes (sqlmock, miniredis, gofakes3, embedded
  nats-server) for users, per the S1/N12 research.

### cron

- C1 [T] fencing_token and message-valued error.type on metrics; unnamespaced attribute keys.
- C2 [B] Register checks running outside the lock and races with Start.
- C3 [B] Unlock and scheduled runs use context.Background().
- C4 [C] Errors are both logged and returned.
- C5 [C] errors.Join → fmt.Errorf("%w: %w"); the inFlight cancel funcs are redundant.
- C6 [C] Missing package comments and Config field docs.

### db

- D1 [T] db.query.text and message-valued error.type on metrics.
- D2 [T] db.client.operation.count is not semconv; the db.query span name is legacy.
- D3 [B] bind.go:148: Value() on a typed-nil pointer Valuer may panic (unverified).
- D4 [C] goimports, reflect.DeepEqual in tests, missing json tags.

### redis

- R1 [T] db.query.text, batch.size and message-valued error.type on metrics.
- R2 [T] db.client.connections.* are invented names; use semconv connection metrics.
- R3 [B] Close doesn't wait for the pool-metrics goroutine.
- R4 [C] Duplicated pool-metrics loop body; trivial errorMessage wrapper.
- R5 [C] Mutable exported vars (TxFailedErr, NewScript); min/max shadow builtins.
- R6 [A] Curated Client interface; decided above (UniversalClient).

### errs

- E1 [B] Is matches on code alone; every AsError(plainErr) compares equal.
- E2 [B] LogValue logs debug twice.
- E3 [B] SlogAttr uses a type assertion, so wrapped errors lose structured output.
- E4 [C] Package comment, godot, AsType.

### errgroup

- G1 [A] DefaultMaxConcurrency is an exported mutable var.
- G2 [A] The panic stack is put in the error string; use a PanicError type.
- G3 [C] Naked returns, missing docs, %w placement.
- G4 [A] Stored ctx; decided above (keep).

### observability

- O1 [A] Insecure-by-default exporters.
- O2 [C] Delete the deprecated logs.SetStreams and the stream vars.
- O3 [C] tracer.go:235 doc mentions a nonexistent Init.
- O4 [C] goconvey tests, unused nolints, redundant alias, mixed receivers.
- O5 [C] Hot-path allocation in ConvertMapsToKVs (not doing; see below).
- O6 [A] tracer.Client re-wraps TracerProvider; decided with X10.

### s3, identity

- S1 [A] Unexported test-only seams: kept by decision.
- S2 [C] Duplicated New doc line, redundant import aliases, gostyle:ignore for GetObject.
- I1 [C] identity.ErrName lacks the identity: prefix.

## Key Assumptions to Validate

- [ ] An OTel global provider captured at init forwards to the provider installed later.
      Test: Wrap(otel.GetMeterProvider()), then SetMeterProvider(manual reader), record, assert.
- [x] Users can test db, redis, s3, and nats code without SDK mock interfaces.
      Validated 2026-11-01 by the probe (see Testing Users' Code).
- [ ] A gostyle AST rule can catch message-valued error.type and per-event metric attributes
      with an acceptable false-positive rate. Test: prototype it against today's code first.
- [ ] PR 3 stays reviewable. Test: if it exceeds ~1.5k lines of non-test diff, split it into
      nats consumer and nats producer + cron.

## Not Doing (and Why)

- confloader: its API and its cache warming/eviction logic are being redesigned (still in
  brainstorm), so it gets compile-only changes from PR 1 and stays on only-new-issues lint.
  The redesign is a release gate. Carry these review findings into it: the error handler
  runs under the entry lock (deadlock confirmed by a probe), errors are compared with ==
  instead of errors.Is, the poll loop uses an uncancellable context.Background(), and
  ErrProviderConnect, ErrInvalidGetterType, ErrRootFolder, ErrStale, and AllowedProviders
  are dead API.
- Shared telemetry helper: not chosen. The same fix lands in each module instead.
- O5 (attribute-conversion allocations): no profile shows it matters yet.
- Rewriting the existing unexported test seams to use real transports: about 4.3k lines of
  tests. Tracked as an issue. New tests for N9/N18 and other lifecycle fixes do use the
  embedded server, since the stubs demonstrably hide real bugs.
- Release/versioning repair: a separate track (see Open Questions).

## Open Questions

- Modules require errs/observability at a zero pseudo-version and identity v1.0.0 (tag
  deleted), and only resolve through replace directives. No module is installable outside
  this repo. Fix in a release track before the dummy consumer moves off the workspace?
