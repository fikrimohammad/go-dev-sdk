# redis

A thin wrapper around [`go-redis`](https://github.com/redis/go-redis) with a
small, standardized API and automatic OpenTelemetry tracing + metrics per
executed command.

## Features

- **Instrumentation hook** — rides go-redis' hook pipeline, so every command
  (current and future) is covered without per-method wrapping. One client span
  per command plus `db.client.operation.{count,duration}` metrics.
- **Value redaction** — `db.query.text` renders the command name followed by one
  `?` per argument (`SET ? ?`), so literal values never leak into telemetry.
- **`redis.Nil` is not an error** — a missing-key result records no error
  telemetry.
- **Pipelines** — batched commands record a single `PIPELINE` span with
  `db.operation.batch.size`.
- **Minimal surface** — `ReadCommands`, `WriteCommands`, `Client`, and
  `Pipeline` interfaces expose only what the app relies on.
- **Injectable telemetry** — `WithMetrics` / `WithTracer` override the
  package-level observability defaults.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/redis
```

## Step-by-step

### 1. Configure and connect

```go
cli, err := redis.New(redis.Config{
    Host: "localhost",
    Port: 6379,
    DB:   0,
})
if err != nil { /* handle */ }
defer cli.Close()
```

`New` applies defaults (`Port=6379`, `ConnectTimeout=5s`), pings the server,
and returns an instrumented client.

### 2. Read and write

```go
// Write.
if err := cli.Set(ctx, "greeting", "hello", 5*time.Minute).Err(); err != nil { /* handle */ }

// Read. Missing keys report redis.Nil (not an error telemetry event).
val, err := cli.Get(ctx, "greeting").Result()
if errors.Is(err, redis.Nil) {
    // key does not exist
} else if err != nil {
    /* handle */
}

// Atomic set-if-absent (locks / idempotency).
ok, err := cli.SetNX(ctx, "lock:order:42", "1", 30*time.Second).Result()

// Delete one or more keys.
n, err := cli.Del(ctx, "greeting", "lock:order:42").Result()
```

### 3. Batch commands in a pipeline

```go
pipe := cli.Pipeline()
pipe.Set(ctx, "a", "1", 0)
pipe.Set(ctx, "b", "2", 0)
if err := pipe.Exec(ctx); err != nil { /* handle */ }
```

Queued commands are sent in a single round trip and recorded as one `PIPELINE`
span.

### 4. (Optional) Inject telemetry clients

```go
cli, err := redis.New(cfg, redis.WithMetrics(mc), redis.WithTracer(tc))
```

## Telemetry attributes

Per command: `server.address`, `server.port`, `db.system.name` (`redis`),
`db.namespace` (database index), `db.operation.name` (e.g. `GET`),
`db.query.text` (values redacted as `?`), `db.response.status_code` (Redis
simple-error prefix like `ERR`/`WRONGTYPE`, empty on success), and
`error.type` (error message, empty on success). Pipelines add
`db.operation.batch.size`.

## Config reference

| Field | Default | Notes |
| --- | --- | --- |
| `Host` | — | Required |
| `Port` | `6379` | |
| `Username` / `Password` | — | Optional (ACL / auth) |
| `DB` | `0` | Must not be negative |
| `DialTimeout` / `ReadTimeout` / `WriteTimeout` | go-redis defaults | |
| `ConnectTimeout` | `5s` | Ping deadline during `New` |
| `PoolSize` / `MinIdleConns` / `MaxIdleConns` | go-redis defaults | `MinIdleConns`/`MaxIdleConns` must not exceed `PoolSize` |
| `ConnMaxIdleTime` / `ConnMaxLifetime` | go-redis defaults | |

## API reference

| Symbol | Description |
| --- | --- |
| `Config` | Connection + pool settings; `SetDefaults()`, `Validate()` |
| `New(cfg, opts...)` | Build + ping an instrumented `Client` |
| `WithMetrics` / `WithTracer` | Telemetry injection options |
| `ReadCommands` | `Get`, `Ping` |
| `WriteCommands` | `Set`, `SetNX`, `Del` |
| `Client` | `ReadCommands` + `WriteCommands` + `Pipeline()` + `Close()` |
| `Pipeline` | `ReadCommands` + `WriteCommands` + `Exec(ctx)` |
