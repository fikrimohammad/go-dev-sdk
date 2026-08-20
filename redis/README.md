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
- **Pipelines & Transactions** — batched commands and atomic `MULTI`...`EXEC`
  transaction blocks record a single `PIPELINE` span with `db.operation.batch.size`.
- **TLS encryption** — built-in support for TLS with server name verification,
  insecure skip verify, and custom CA/client certificates.
- **Context-aware startup** — `NewWithContext` integrates cleanly with
  graceful application startup and shutdown lifecycles.
- **Minimal surface** — `ReadCommands`, `WriteCommands`, `ScriptCommands`,
  `Client`, and `Pipeline` interfaces expose the essential commands for strings,
  hashes, sets, sorted sets, counters, TTL management, and Lua scripting.
- **Injectable telemetry** — `WithMetrics` / `WithTracer` override the
  package-level observability defaults.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/redis
```

## Step-by-step

### 1. Configure and connect

The package supports **Standalone**, **Sentinel** (High Availability), and **Cluster** (Distributed Sharding) topologies out of the box with the exact same [`Client`](#api-reference) interface.

#### Option A: Standalone Mode (Default)
```go
cli, err := redis.New(redis.Config{
    Host: "localhost",
    Port: 6379,
    DB:   0,
})
if err != nil { /* handle */ }
defer cli.Close()
```

#### Option B: Redis Sentinel Mode (Consul Service Discovery & High Availability)
```go
cli, err := redis.New(redis.Config{
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
cli, err := redis.New(redis.Config{
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
and returns an instrumented client. Use `NewWithContext(ctx, cfg, opts...)` when
initialization should respect a parent context deadline.

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
err = cli.ZAdd(ctx, "leaderboard", redis.Z{Score: 100.0, Member: "player1"}).Err()

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
err = cli.Watch(ctx, func(tx *redis.Tx) error {
    val, err := tx.Get(ctx, "counter").Int64()
    if err != nil && !errors.Is(err, redis.Nil) {
        return err
    }
    _, err = tx.TxPipelined(ctx, func(pipe redis.Pipeline) error {
        pipe.Set(ctx, "counter", val+1, 0)
        return nil
    })
    return err
}, "counter")
```

### 3. Batch commands & transactions

```go
// Closure-based pipeline (automatically calls Exec).
err := cli.Pipelined(ctx, func(pipe redis.Pipeline) error {
    pipe.Set(ctx, "a", "1", 0)
    pipe.HSet(ctx, "h", "f", "v")
    pipe.Incr(ctx, "counter")
    return nil
})

// Atomic transaction pipeline (MULTI...EXEC).
err = cli.TxPipelined(ctx, func(pipe redis.Pipeline) error {
    pipe.Incr(ctx, "order_count")
    pipe.HSet(ctx, "order:100", "status", "created")
    return nil
})
```

> [!NOTE]
> `Pipeline` instances are not safe for concurrent use across multiple goroutines.
> Always use local, short-lived pipelines or `cli.Pipelined(...)` / `cli.TxPipelined(...)`.

### 4. (Optional) Inject telemetry & pool monitoring

```go
cli, err := redis.New(
    cfg,
    redis.WithMetrics(mc),
    redis.WithTracer(tc),
    redis.WithPoolMetrics(15*time.Second), // Background connection pool monitoring
)
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
| `New(cfg, opts...)` | Build + ping an instrumented `Client` (standalone, sentinel, or cluster) |
| `NewWithContext(ctx, cfg, opts...)` | Build + ping an instrumented `Client` with parent context |
| `WithMetrics` / `WithTracer` / `WithPoolMetrics` | Telemetry injection and pool monitoring options |
| `ReadCommands` | `Ping`, `Exists`, `TTL`, `Type`, `Scan`, `Get`, `MGet`, Hashes (`HGet`, `HGetAll`, `HMGet`, `HKeys`, `HVals`, `HLen`, `HExists`), Lists (`LLen`, `LRange`, `LIndex`), Sets (`SMembers`, `SInter`, `SUnion`, `SDiff`, `SIsMember`, `SMIsMember`, `SCard`, `SRandMember`, `SRandMemberN`), Sorted Sets (`ZScore`, `ZCard`, `ZCount`, `ZRange*`, `ZRevRange*`, `ZRank`, `ZRevRank`) |
| `WriteCommands` | `Del`, `Unlink`, `Expire`, `ExpireAt`, `Persist`, `Set`, `SetNX`, `SetXX`, `GetDel`, `GetEx`, `MSet`, Counters (`Incr`, `IncrBy`, `IncrByFloat`, `Decr`, `DecrBy`), Hashes (`HSet`, `HSetNX`, `HDel`, `HIncrBy`, `HIncrByFloat`), Lists (`LPush`, `RPush`, `LPop`, `RPop`, `LPopCount`, `RPopCount`, `LTrim`, `LRem`, `LSet`), Sets (`SAdd`, `SRem`, `SPop`, `SPopN`), Sorted Sets (`ZAdd`, `ZRem`, `ZIncrBy`, `ZRemRange*`), Pub/Sub (`Publish`) |
| `ScriptCommands` | `Eval`, `EvalSha`, `ScriptExists`, `ScriptLoad` |
| `Client` | `ReadCommands` + `WriteCommands` + `ScriptCommands` + `Pipeline()` + `TxPipeline()` + `Pipelined()` + `TxPipelined()` + `Watch()` + `Subscribe()` + `PoolStats()` + `Close()` |
| `Pipeline` | `ReadCommands` + `WriteCommands` + `ScriptCommands` + `Exec(ctx)` |
| `Nil`, `TxFailedErr`, `Z`, `ZRangeBy`, `Script`, `NewScript`, `PoolStats`, `Tx`, `PubSub`, `Message`, `Subscription` | Common type aliases, sentinel errors, and helpers re-exported from `go-redis` |
