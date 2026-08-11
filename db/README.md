# db

A thin wrapper around [`sqlx`](https://github.com/jmoiron/sqlx) with a
standardized **named-query** API and automatic OpenTelemetry tracing + metrics
per executed statement.

## Features

- **Named queries** — write `:name` placeholders; the wrapper handles the whole
  binding pipeline (`BindNamed` → `sqlx.In` for `IN (?)` slices → `Rebind` to
  the driver's placeholder syntax). No positional-argument juggling.
- **Small consistent surface** — `Queryer`, `Execer`, `DB`, and `Tx`
  interfaces describe the contracts; implementations are unexported.
- **Transactions** — `Begin(ctx)` returns a `Tx` with the same query methods
  plus `Commit` / `Rollback`.
- **Telemetry** — every executed statement records a client span (`db.query`)
  and `db.client.operation.{count,duration}` metrics with OTel DB attributes.
  Binding failures return without any telemetry.
- **Sane pool defaults** — MySQL-oriented tuning applied by `Config.SetDefaults`.
- **Injectable telemetry** — `WithMetrics` / `WithTracer` override the
  package-level observability defaults.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/db
```

## Step-by-step

### 1. Configure the connection

```go
cfg := db.Config{
    Driver:   "mysql",
    Host:     "localhost",
    Port:     3306,
    Database: "orders",
    Username: "app",
    Password: "secret",
    // Pool settings are optional; SetDefaults fills them:
    // MaxOpenConns=25, MaxIdleConns=25, ConnMaxLifetime=3m, ConnMaxIdleTime=5m.
}
```

### 2. Connect

```go
pool, err := db.Connect(cfg) // applies SetDefaults, opens, pings
if err != nil { /* handle */ }
defer pool.Close()
```

### 3. Run queries with named parameters

```go
type Order struct {
    ID     int64     `db:"id"`
    Amount float64   `db:"amount"`
}

// Select many rows into a *[]T.
var orders []Order
err = pool.NamedSelectContext(ctx, &orders,
    "SELECT * FROM orders WHERE status = :status AND amount > :min",
    map[string]any{"status": "paid", "min": 100.0},
)

// Get a single row.
var order Order
err = pool.NamedGetContext(ctx, &order,
    "SELECT * FROM orders WHERE id = :id", map[string]any{"id": 42},
)

// Exec a write.
res, err := pool.NamedExecContext(ctx,
    "UPDATE orders SET status = :status WHERE id = :id",
    map[string]any{"status": "shipped", "id": 42},
)
```

Structs work too — keys are matched by `db` struct tags:

```go
res, err := pool.NamedExecContext(ctx,
    "INSERT INTO orders (id, amount) VALUES (:id, :amount)",
    order,
)
```

`IN (?)` slices are expanded automatically:

```go
err = pool.NamedSelectContext(ctx, &orders,
    "SELECT * FROM orders WHERE status = :status AND id IN (:ids)",
    map[string]any{"status": "paid", "ids": []int64{1, 2, 3}},
)
```

### 4. Use transactions

```go
tx, err := pool.Begin(ctx)
if err != nil { /* handle */ }
defer tx.Rollback() // no-op after a successful Commit

if _, err := tx.NamedExecContext(ctx, "UPDATE accounts SET balance = balance - :amt WHERE id = :id", args); err != nil {
    return err
}
if _, err := tx.NamedExecContext(ctx, "UPDATE accounts SET balance = balance + :amt WHERE id = :id", args); err != nil {
    return err
}
if err := tx.Commit(); err != nil { /* handle */ }
```

### 5. (Optional) Inject telemetry clients

```go
pool, err := db.Connect(cfg, db.WithMetrics(mc), db.WithTracer(tc))
```

When not injected, statements are instrumented through the package-level
`metrics` / `tracer` defaults.

## Telemetry attributes

Per executed statement: `db.system.name` (from driver), `db.namespace` (the
database name), `db.operation.name` (leading SQL verb, e.g. `SELECT`),
`db.query.text`, `db.response.status_code` (MySQL driver error number, empty on
success), and `error.type` (error message, empty on success). Transaction
begin/commit/rollback are lifecycle operations and are not instrumented.

## Config reference

| Field | Default | Notes |
| --- | --- | --- |
| `Driver` | `"mysql"` | `db.system.name` derived from it |
| `Host`, `Port`, `Database`, `Username`, `Password` | — | Required; never defaulted |
| `MaxOpenConns` | `25` | ≤ 0 → default |
| `MaxIdleConns` | `MaxOpenConns` | Must not exceed `MaxOpenConns` |
| `ConnMaxLifetime` | `3m` | |
| `ConnMaxIdleTime` | `5m` | |

## API reference

| Symbol | Description |
| --- | --- |
| `Config` | Connection + pool settings; `SetDefaults()`, `Validate()` |
| `Connect(cfg, opts...)` | Open + ping an instrumented `DB` |
| `WithMetrics` / `WithTracer` | Telemetry injection options |
| `Queryer` | `NamedSelectContext`, `NamedGetContext` |
| `Execer` | `NamedExecContext` |
| `DB` | `Begin`, `Close` + `Queryer` + `Execer` |
| `Tx` | `Commit`, `Rollback` + `Queryer` + `Execer` |
