# Spec: data-clients

Module id: `data-clients` ([capability map](capability-map.md)). Status: approved 2026-11-02;
assumptions 1–10 confirmed by the owner. Items: X2–X9, D1–D4, R1–R6, S2, N19
([item index](../../ideas/sdk-fix-backlog.md#item-index)).

## Objective

Make `db`, `redis`, and `s3` emit telemetry that follows the OpenTelemetry semantic conventions
(semconv v1.43.0, the version `rest` already uses), keep metric cardinality bounded, give their
constructors a `ctx`, and replace redis's curated command interfaces with go-redis's own
`redis.UniversalClient`. All three modules end clean on the full tree and strict in CI.

The motivating problem: today db and redis put `db.query.text` and the raw error message on
metric attributes, and IN-expansion or variadic commands make that text unbounded. That is a
metrics cost and cardinality incident waiting to happen. They also emit
`db.client.operation.count`, which no convention defines, and s3 uses `rpc.system` and
`rpc.service`, which v1.43.0 removed.

### User stories

- As an SRE, the db, redis, and s3 duration histograms have bounded attribute sets, so one
  service can't blow up the metrics backend with per-query or per-message series.
- As a dashboard author, the metric names, units, and attributes match the OTel conventions, so
  stock dashboards and the alert-template repo work unchanged.
- As a service author, `db.Connect(ctx, …)`, `redis.New(ctx, …)`, and `s3.New(ctx, …)` respect my
  startup deadline.
- As a service author, `redis.New` gives me go-redis's full command set (`redis.UniversalClient`),
  instrumented, instead of a 40-method subset that needs an SDK change for every new command.

## Assumptions (correct any of these before approving)

1. **S3 metrics use the RPC convention.** Replace `s3.client.operation.{count,duration}` with
   `rpc.client.call.duration` (histogram, `s`). It carries:
   - `rpc.system.name = "aws-api"`. v1.43.0's enum lists only grpc, dubbo, connectrpc, and jsonrpc,
     but semconv enums are open, and `aws-api` is what OTel's AWS SDK instrumentation uses.
   - `rpc.method = "S3/<Operation>"`, for example `S3/PutObject`.
   - `aws.s3.bucket`, `cloud.region`, `server.address`, `server.port`, and `error.type`.

   The removed `rpc.system` and `rpc.service` attributes go away. The alternative is a custom
   `aws.s3.client.operation.duration`.
2. **No `*.operation.count` metrics.** The count is the histogram's count, so db, redis, and s3
   each drop their count counter.
3. **db span name is the operation name** (for example `SELECT`), per the DB span convention:
   `{db.query.summary}`, otherwise `{db.operation.name} {target}`, otherwise the operation.
   The SDK has no summary or collection parser, so it uses the operation name alone. The
   instrumentation scope stays `db.query`.
4. **`db.query.text` stays on spans only.** It's an opt-in attribute on the metric, and the
   semconv skill keeps opt-in attributes off metrics unless a config switch is added. No switch
   is added.
5. **`error.type` is low-cardinality, and omitted on success.**
   - db: the driver code (MySQL error number, or PostgreSQL SQLSTATE).
   - redis: the simple-error prefix (`ERR`, `WRONGTYPE`, …).
   - s3: the AWS error code, as today.
   - Otherwise `timeout` for `context.DeadlineExceeded`, `canceled` for `context.Canceled`, and
     `_OTHER` for anything else. These match rest/client.
   - `db.response.status_code` is set only when there is a driver code.
   - *Amended 2026-11-02 (owner, Checkpoint A):* a `net.Error` whose `Timeout()` is true, and
     `redis.ErrPoolTimeout`, are also `timeout`, as s3 already does. db treats `sql.ErrNoRows` as
     success in telemetry, as redis treats `redis.Nil`; the caller still gets the error.
   - Metric name, unit, and description come from semconv's `dbconv`/`rpcconv` types, so db and
     redis register an identical `db.client.operation.duration` and never conflict in one
     metrics client.
6. **redis pool metrics follow the convention:**
   - `db.client.connection.count` is an up-down counter in `{connection}`, with
     `db.client.connection.state = idle|used`.
   - `db.client.connection.timeouts` is a counter in `{timeout}`.
   - Both carry `db.client.connection.pool.name`, using server.address:port.
   - The go-redis-specific `hits`/`misses`/`stale` counters are dropped; they're go-redis
     internals that no convention defines.
   - The poll loop records deltas, using the same attributes on increment and decrement.
7. **redis drops its aliases** along with the interfaces (R6): `Nil`, `TxFailedErr`, `Script`,
   `NewScript`, `Z`, `ZRangeBy`, `PoolStats`, `Tx`, `PubSub`, `Message`, `Subscription`, and
   `Pipeline`. Callers import `github.com/redis/go-redis/v9` for those, as they must already for
   `*StringCmd`. `redis/errors.go` is deleted, because R3 says a package with no exported errors
   has no `errors.go`.
8. **Constructors take `ctx` first**: `db.Connect(ctx, cfg, opts...)`, `redis.New(ctx, cfg, opts...)`
   (`NewWithContext` is deleted), and `s3.New(ctx, cfg, opts...)`. The ctx bounds the ping or
   config load.
9. **D3 is fixed only if a test proves it.** First write a test that binds a typed-nil pointer
   `driver.Valuer`. If it panics, mirror `database/sql`'s nil-pointer Valuer handling. If it
   doesn't, drop D3.
10. **N19 testing recipes become documentation, not new dependencies.** Each package's README
    gets a short "Testing your code" section: db with sqlmock through
    `Config{Driver: "sqlmock", DSN}`, redis with miniredis, and s3 with gofakes3. These were
    all proven by the 2026-11-01 probe. Adding them as runnable examples would put those fakes
    in each go.mod, which R2's compile-only decision avoided.

## Tech Stack

- Go 1.26.5 (from `go.work`). Modules: `db`, `redis`, `s3`, with local `replace` directives as
  today.
- `go.opentelemetry.io/otel` v1.45.0, using `semconv/v1.43.0` attribute keys (as `rest` does) and
  `sdk/metric`/`sdk/trace` test helpers.
- `github.com/redis/go-redis/v9` (`UniversalClient`, `NewClient`, `NewFailoverClient`,
  `NewFailoverClusterClient`, `NewClusterClient`).
- `github.com/aws/aws-sdk-go-v2` (`config.LoadDefaultConfig(ctx, …)`).

## Commands

Run inside each module (AGENTS.md verification, strict):

```bash
gofmt -l .
go vet ./...
go test -count=1 -shuffle=on ./... && go test -count=1 -race -shuffle=on ./...
golangci-lint run ./...                      # full tree; db, redis, s3 join STRICT_LINT_MODULES
GOWORK=off go run ../.agents/skills/go-best-practices/scripts/gostyle.go ./...
go mod tidy -diff
```

Callers of changed APIs: none outside these modules (confirmed by grep on 2026-11-02; the testing
app is migrated after `guardrails`). None of the three modules depends on goleak, miniredis,
sqlmock, or gofakes3, and this PR adds none.

## Public API

### db

| Before | After |
| --- | --- |
| `Connect(cfg, opts...) (DB, error)` | `Connect(ctx, cfg, opts...) (DB, error)`. The ctx bounds the initial ping. |
| Metrics: `db.client.operation.count`, `db.client.operation.duration` | `db.client.operation.duration` only |
| Span name `db.query` | Span name `{db.operation.name}`, for example `SELECT` |
| `Config` has yaml tags only | `Config` has yaml and json tags, like every other `Config` |

### redis

| Before | After |
| --- | --- |
| `New(cfg, opts...) (Client, error)`, `NewWithContext(ctx, cfg, opts...)` | `New(ctx, cfg, opts...) (redis.UniversalClient, error)` (go-redis's interface). `Close` also stops the pool-metrics poller and waits for it to exit. |
| `Client`, `Pipeline`, `ReadCommands`, `WriteCommands`, `ScriptCommands`; aliases `Nil`, `TxFailedErr`, `Script`, `NewScript`, `Z`, … | Deleted (assumption 7) |
| Pool metrics `db.client.connections.{hits,misses,timeouts,stale,total,idle}` | `db.client.connection.count{state}`, `db.client.connection.timeouts` (assumption 6) |
| Metrics: `db.client.operation.count` + duration | `db.client.operation.duration` only |

### s3

| Before | After |
| --- | --- |
| `New(cfg, opts...) (Client, error)` | `New(ctx, cfg, opts...) (Client, error)`. The ctx bounds `LoadDefaultConfig`. `Client` stays (owner decision: exported interfaces stay). |
| Metrics: `s3.client.operation.{count,duration}` | `rpc.client.call.duration` (assumption 1) |
| Attributes `rpc.system`, `rpc.service`, `rpc.method=<Op>` | `rpc.system.name=aws-api`, `rpc.method=S3/<Op>` |

## Telemetry Contract (all three modules)

| Signal | db | redis | s3 |
| --- | --- | --- | --- |
| Span kind | client | client | client |
| Span name | `{db.operation.name}` | command name, or `PIPELINE` | `<Operation>` (unchanged) |
| Duration histogram | `db.client.operation.duration` (s) | `db.client.operation.duration` (s) | `rpc.client.call.duration` (s) |
| Metric attributes | `db.system.name`, `db.namespace`, `db.operation.name`, `server.address`, `server.port`, `db.response.status_code`†, `error.type`† | `db.system.name=redis`, `db.namespace`, `db.operation.name`, `server.address`, `server.port`, `db.response.status_code`†, `error.type`† | `rpc.system.name`, `rpc.method`, `aws.s3.bucket`, `cloud.region`, `server.address`/`server.port` (when known), `error.type`† |
| Span-only attributes | `db.query.text` | `db.query.text` (values redacted), `db.operation.batch.size` | (none added) |

† Present only on failure. No attribute is ever set to an empty string (X4).

Every attribute key comes from a `semconv/v1.43.0` constant where one exists, never a string
literal, as rest does. Telemetry tests assert the exact metric name, unit, attribute set, and the
absence of `db.query.text`, `error.type` on success, and every per-event value on metrics.

## Behaviour

- **X6:** each module resolves its metrics and tracer client in one helper
  (`m.metricsClient()`/`m.tracer()`) that returns the injected client or `metrics.Default()`/
  `tracer.Default()`. The duplicated nil-check branches go away. Resolution stays per call, so a
  later `otel.Set*Provider` still applies.
- **R3:** the pool poller has a visible end. `Close` closes the stop channel and waits on a
  `sync.WaitGroup` before closing the go-redis client.
- **UniversalClient (R6):** `createUnderlyingClient` returns `redis.UniversalClient`. The wrapper
  embeds it and overrides only `Close`. The instrumentation hook is added with `AddHook`, as today.

## Code Style

Low-cardinality error classification (assumption 5), shown for redis:

```go
// errorType returns the error.type value for a failed command: the Redis
// simple-error prefix (ERR, WRONGTYPE, ...), "timeout", "canceled", or "_OTHER".
func errorType(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	if code := redisErrorCode(err); code != "" {
		return code
	}
	return "_OTHER"
}
```

All of AGENTS.md applies, including R1–R4: Config methods, examples (the db, redis, and s3
`example_test.go` files are updated for the new signatures), `errors.go`, and `<pkg>.go`.

## Testing Strategy

- Package `testing`, table tests, and the mirror rule, as in foundations. Each module asserts its
  telemetry through an OTel manual reader and span recorder, injected or installed as a global
  and restored only if changed.
- **Regression tests.** Each must fail on `main` and pass after the change:

| Test | Proves |
| --- | --- |
| db `TestInstrument_MetricAttributes` | The duration metric has the exact attribute set: no `db.query.text`, and no `error.type`/`db.response.status_code` on success. |
| db `TestErrorInfo` (rework) | A MySQL or PostgreSQL error gives its code as `error.type`; an unknown error gives `_OTHER`, never the message. |
| db `TestBindQuery_TypedNilValuer` | (D3) It panics on main, or D3 is dropped. |
| db `TestConnect_ContextDeadline` | An expired ctx makes `Connect` fail fast. |
| redis `TestInstrumentHook_MetricAttributes` | Same as db's, for redis, including pipelines. |
| redis `TestPoolMetrics` | `db.client.connection.count{state}` and `.timeouts` follow PoolStats changes, and `Close` stops the poller and waits for it (a WaitGroup-based assertion; goleak is not a dependency). |
| redis `TestNew_ReturnsUniversalClient` | `createUnderlyingClient` returns a `redis.UniversalClient` for standalone, cluster, and sentinel, and the instrumentation hook records commands sent through it (in-process; miniredis is not a dependency). |
| s3 `TestMiddleware_MetricAttributes` | `rpc.client.call.duration` carries `rpc.system.name=aws-api` and `rpc.method=S3/PutObject`, and has no removed keys. |

- **Coverage:** not below `main` (db 87.4%, redis 68.2%, s3 85.5%).

## Boundaries

- **Always:**
  - Write the failing test first for every changed behaviour.
  - Take attribute keys from semconv constants.
  - Update each module's README and examples for the new signatures.
  - Run the strict checks.
- **Ask first:** adding a dependency (miniredis, sqlmock, gofakes3, goleak), changing any
  assumption above, or touching modules other than db, redis, and s3.
- **Never:** put unbounded values on metric attributes; add deprecation shims (there's no backward
  compatibility); weaken or skip a test to get green.

## Success Criteria

1. db, redis, and s3 report 0 findings on full-tree `golangci-lint run ./...` and gostyle, and join
   `STRICT_LINT_MODULES`.
2. No metric attribute in these modules carries `db.query.text`, an error message, a message id, or
   another unbounded value. Telemetry tests assert the exact attribute sets.
3. No metric named `*.operation.count` or `db.client.connections.*` is emitted, and
   `db.client.operation.duration`, `db.client.connection.{count,timeouts}`, and
   `rpc.client.call.duration` match semconv v1.43.0 in name, unit, and instrument type.
4. `Connect`, `redis.New`, and `s3.New` take `ctx` first, and `NewWithContext` is gone.
5. `redis.New` returns `redis.UniversalClient`; no `ReadCommands`/`WriteCommands`/`Client`
   interfaces or aliases remain; `Close` stops the poller and waits for it.
6. Every regression test in the table exists and fails on `main`.
7. Every go.work module passes the module checks, and CI is green.

## Open Questions

- None. Assumptions 1–10 were confirmed by the owner on 2026-11-02.
