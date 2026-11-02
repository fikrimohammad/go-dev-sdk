# redis

A thin wrapper around [`go-redis`](https://github.com/redis/go-redis) with a
small, standardized API and automatic OpenTelemetry tracing + metrics per
executed command.

## Features

- **Instrumentation hook** — rides go-redis' hook pipeline, so every command
  (current and future) is covered without per-method wrapping. One client span
  per command plus one `db.client.operation.duration` histogram sample.
- **Value redaction** — `db.query.text` (span only) renders the command name
  followed by one `?` per argument (`SET ? ?`), so literal values never leak into
  telemetry.
- **`goredis.Nil` is not an error** — a missing-key result records no error
  telemetry.
- **Pipelines & Transactions** — batched commands and atomic `MULTI`...`EXEC`
  transaction blocks record a single `PIPELINE` span with `db.operation.batch.size`
  (span only).
- **TLS encryption** — built-in support for TLS with server name verification,
  insecure skip verify, and custom CA/client certificates.
- **Context-aware startup** — `New(ctx, ...)` bounds the initial ping by the
  caller's context, so startup respects graceful application lifecycles.
- **Full command set** — `New` returns go-redis's `redis.UniversalClient`, so every
  go-redis command is available and instrumented; no SDK change is needed for a new
  command.
- **Injectable telemetry** — `WithMetrics` / `WithTracer` override the
  package-level observability defaults.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/redis
```

## Step-by-step

### 1. Configure and connect

The package supports **Standalone**, **Sentinel** (High Availability), and **Cluster** (Distributed Sharding) topologies out of the box. All three return the same go-redis `UniversalClient` (see [API reference](#api-reference)).

Examples use two imports: the SDK package as `redis`, and go-redis as `goredis` for its types (`goredis.Nil`, `goredis.Z`, `goredis.Pipeliner`, `goredis.Tx`):

```go
import (
    goredis "github.com/redis/go-redis/v9"

    "github.com/fikrimohammad/go-dev-sdk/redis"
)
```

#### Option A: Standalone Mode (Default)
```go
cli, err := redis.New(ctx, redis.Config{
    Host: "localhost",
    Port: 6379,
    DB:   0,
})
if err != nil { /* handle */ }
defer cli.Close()
```

#### Option B: Redis Sentinel Mode (Consul Service Discovery & High Availability)
```go
cli, err := redis.New(ctx, redis.Config{
    Mode:       redis.ModeSentinel,
    MasterName: "mymaster",
    Addrs: []string{
        "redis-sentinel.service.consul:26379",
    },
    Password:         "redis-password",
    SentinelPassword: "sentinel-auth-password",

    // Optional: Route reads to 5 slaves and writes to Master
    ReadOnly:      true,
    RouteRandomly: true,
})
if err != nil { /* handle */ }
defer cli.Close()
```

#### Option C: Redis Cluster Mode (Sharding & High Throughput)
```go
cli, err := redis.New(ctx, redis.Config{
    Mode: redis.ModeCluster,
    Addrs: []string{
        "redis-cluster.service.consul:6379",
    },
    Password: "redis-password",
    // Redis Cluster requires DB 0
    DB: 0,
})
if err != nil { /* handle */ }
defer cli.Close()
```

`New` applies defaults (`Port=6379`, `ConnectTimeout=5s`), pings the server,
and returns an instrumented `redis.UniversalClient`. The ping is bounded by both
`ctx` and `ConnectTimeout`, so pass a context that carries your startup deadline.
`Close` releases the connection pool and stops the pool-metrics poller.

### 2. Read and write

```go
// Write.
if err := cli.Set(ctx, "greeting", "hello", 5*time.Minute).Err(); err != nil { /* handle */ }

// Read. Missing keys report goredis.Nil (not an error telemetry event).
val, err := cli.Get(ctx, "greeting").Result()
if errors.Is(err, goredis.Nil) {
    // key does not exist
} else if err != nil {
    /* handle */
}

// Atomic set-if-absent (locks / idempotency).
ok, err := cli.SetNX(ctx, "lock:order:42", "1", 30*time.Second).Result()

// Atomic update-if-present.
ok, err = cli.SetXX(ctx, "cached:item", "new_val", time.Hour).Result()

// Modern Redis 6.2+ atomics: consume-on-read & refresh-on-read.
token, err := cli.GetDel(ctx, "otp:user:123").Result()
session, err := cli.GetEx(ctx, "sess:abc", 30*time.Minute).Result()

// Safe cursor-based key scanning (non-blocking).
iter := cli.Scan(ctx, 0, "sess:*", 100).Iterator()
for iter.Next(ctx) {
    key := iter.Val()
    _ = key
}
if err := iter.Err(); err != nil { /* handle */ }

// Atomic counter / rate limiter.
count, err := cli.Incr(ctx, "rate_limit:user:123").Result()

// Hashes (objects / partial entity updates).
err = cli.HSet(ctx, "user:1", "name", "Alice", "role", "admin").Err()
user, err := cli.HGetAll(ctx, "user:1").Result()

// Sets & Sorted Sets (deduplication, leaderboards, sliding windows).
err = cli.SAdd(ctx, "tags:article:1", "tech", "golang").Err()
err = cli.ZAdd(ctx, "leaderboard", goredis.Z{Score: 100.0, Member: "player1"}).Err()

// Lua Scripting.
res, err := cli.Eval(ctx, "return redis.call('get', KEYS[1])", []string{"greeting"}).Result()

// Non-blocking asynchronous delete.
n, err = cli.Unlink(ctx, "large_hash_key", "heavy_set").Result()

// Task queues & Lists (LPUSH, RPOP, LRANGE, LTRIM).
err = cli.LPush(ctx, "tasks:queue", "task_payload_1", "task_payload_2").Err()
task, err := cli.RPop(ctx, "tasks:queue").Result()
listLen, err := cli.LLen(ctx, "tasks:queue").Result()

// Pub/Sub messaging.
err = cli.Publish(ctx, "notifications", "order_paid").Err()
pubsub := cli.Subscribe(ctx, "notifications")
defer pubsub.Close()
ch := pubsub.Channel()

// Optimistic Locking (WATCH / CAS).
err = cli.Watch(ctx, func(tx *goredis.Tx) error {
    val, err := tx.Get(ctx, "counter").Int64()
    if err != nil && !errors.Is(err, goredis.Nil) {
        return err
    }
    _, err = tx.TxPipelined(ctx, func(pipe goredis.Pipeliner) error {
        pipe.Set(ctx, "counter", val+1, 0)
        return nil
    })
    return err
}, "counter")
```

### 3. Batch commands & transactions

```go
// Closure-based pipeline (automatically calls Exec).
_, err := cli.Pipelined(ctx, func(pipe goredis.Pipeliner) error {
    pipe.Set(ctx, "a", "1", 0)
    pipe.HSet(ctx, "h", "f", "v")
    pipe.Incr(ctx, "counter")
    return nil
})

// Atomic transaction pipeline (MULTI...EXEC).
_, err = cli.TxPipelined(ctx, func(pipe goredis.Pipeliner) error {
    pipe.Incr(ctx, "order_count")
    pipe.HSet(ctx, "order:100", "status", "created")
    return nil
})
```

> [!NOTE]
> `goredis.Pipeliner` instances are not safe for concurrent use across multiple goroutines.
> Always use local, short-lived pipelines or `cli.Pipelined(...)` / `cli.TxPipelined(...)`.

### 4. (Optional) Inject telemetry & pool monitoring

```go
cli, err := redis.New(
    ctx,
    cfg,
    redis.WithMetrics(mc),
    redis.WithTracer(tc),
    redis.WithPoolMetrics(15*time.Second), // Background connection pool monitoring
)
```

## Telemetry attributes

Each command or pipeline records one `db.client.operation.duration` histogram
sample (seconds) with `db.system.name` (`redis`), `db.namespace` (database
index), `db.operation.name` (e.g. `GET`, or `PIPELINE`), `server.address`, and
`server.port`, each omitted when unknown.

On failure the metric also carries:

- `error.type`: the Redis simple-error prefix (`ERR`, `WRONGTYPE`, ...),
  `timeout` (deadline exceeded), `canceled`, or `_OTHER`. Error messages never
  appear on metrics.
- `db.response.status_code`: the simple-error prefix, when the server sends one.

`redis.Nil` is a normal lookup result and records no error telemetry.

The span (named after the command, or `PIPELINE`) carries the same attributes
plus `db.query.text`, with values redacted as `?`. Pipeline spans also carry
`db.operation.batch.size`. Neither is set on the metric, to keep its
cardinality bounded.

## Config reference

| Field | Default | Notes |
| --- | --- | --- |
| `Mode` | `standalone` | Topology: `standalone`, `sentinel`, or `cluster` |
| `Host` | — | Required for standalone (fallback if `Addrs` empty) |
| `Port` | `6379` | Standard Redis port |
| `Addrs` | — | List of endpoints (Sentinel quorum addresses or Cluster seed nodes) |
| `MasterName` | — | Master group name (required for `sentinel` mode) |
| `SentinelUsername` / `SentinelPassword` | — | Optional credentials for Sentinel authentication |
| `MaxRedirects` | `3` | Max cluster `-MOVED`/`-ASK` redirects (cluster mode only) |
| `ReadOnly` | `false` | Route read commands to replicas (`sentinel` & `cluster`) |
| `RouteByLatency` | `false` | Route read commands to closest replica |
| `RouteRandomly` | `false` | Distribute read commands randomly among replicas |
| `Username` / `Password` | — | Optional (ACL / auth for Redis nodes) |
| `DB` | `0` | Must be `0` for cluster mode; non-negative for standalone/sentinel |
| `TLSEnabled` | `false` | Enable TLS encryption |
| `TLSInsecureSkipVerify` | `false` | Skip TLS certificate verification |
| `TLSServerName` | `Host` / `Addrs[0]` | Server name for TLS SNI |
| `TLSCACert` | — | Optional PEM certificate data or file path |
| `TLSCert` / `TLSKey` | — | Optional client certificate and key for mTLS |
| `DialTimeout` / `ReadTimeout` / `WriteTimeout` | go-redis defaults | |
| `ConnectTimeout` | `5s` | Ping deadline during `New` |
| `PoolSize` / `MinIdleConns` / `MaxIdleConns` | go-redis defaults | `MinIdleConns` must not exceed `PoolSize` or `MaxIdleConns` |
| `ConnMaxIdleTime` / `ConnMaxLifetime` | go-redis defaults | |

## API reference

| Symbol | Description |
| --- | --- |
| `Mode` | Connection mode: `ModeStandalone`, `ModeSentinel`, `ModeCluster` |
| `Config` | Connection, topology, TLS, and pool settings; `SetDefaults()`, `Validate()` |
| `New(ctx, cfg, opts...)` | Build, instrument, and ping a client (standalone, sentinel, or cluster). Returns `redis.UniversalClient`, go-redis's full command interface. The ping is bounded by `ctx` and `ConnectTimeout`. |
| `WithMetrics` / `WithTracer` / `WithPoolMetrics` | Telemetry injection and pool monitoring options |
