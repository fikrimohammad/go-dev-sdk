# db

A thin wrapper around [`sqlx`](https://github.com/jmoiron/sqlx) with a
standardized **named-query** API and automatic OpenTelemetry tracing + metrics
per executed statement, for **MySQL and PostgreSQL**.

## Features

- **Named queries** — write `:name` placeholders; the wrapper handles the whole
  binding pipeline (named binding → `IN (?)` slice expansion → the driver's
  placeholder syntax). PostgreSQL receives numbered `$1`, `$2`, ... parameters.
- **MySQL and PostgreSQL** — `mysql` uses
  [`go-sql-driver/mysql`](https://github.com/go-sql-driver/mysql); `pgx`,
  `pgx/v5`, `postgres`, and `postgresql` use the bundled
  [`pgx/v5`](https://github.com/jackc/pgx) `database/sql` driver. No separate
  `lib/pq` import is required.
- **Custom drivers** — a `database/sql` driver that is not built in can be
  opened with an explicit `Config.DSN` when the driver is registered by the
  caller; placeholder conversion follows sqlx's driver registry.
- **Small consistent surface** — `Queryer`, `Execer`, `DB`, and `Tx`
  interfaces describe the contracts; implementations are unexported.
- **Transactions** — `Begin(ctx)` returns a `Tx` with the same query methods
  plus `Commit` / `Rollback`.
- **Telemetry** — every executed statement records a client span (`db.query`)
  and `db.client.operation.{count,duration}` metrics with OTel DB attributes.
  Binding failures return without any telemetry.
- **Sane pool defaults** for both engines, applied by `Config.SetDefaults`.
- **Injectable telemetry** — `WithMetrics` / `WithTracer` override the
  package-level observability defaults.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/db
```

The PostgreSQL driver is a regular module dependency, so `Driver: "pgx"` (or
`"postgres"`) works without importing anything else.

## Step-by-step

### 1. Configure the connection

MySQL:

```go
cfg := db.Config{
    Driver:   "mysql",
    Host:     "localhost",
    Port:     3306,
    Database: "orders",
    Username: "app",
    Password: "secret",
    SSLMode:  "require", // optional; disable, allow, prefer, require,
                         // verify-ca, verify-full
    // Pool settings are optional; SetDefaults fills them:
    // MaxOpenConns=25, MaxIdleConns=25, ConnMaxLifetime=3m, ConnMaxIdleTime=5m.
}
```

PostgreSQL:

```go
cfg := db.Config{
    Driver:   "postgres", // same as "pgx" / "pgx/v5" / "postgresql"
    Host:     "localhost",
    Port:     5432,       // optional; defaulted when Host is set
    Database: "orders",
    Username: "app",
    Password: "secret",
    SSLMode:  "require",  // optional: disable, allow, prefer, require,
                          // verify-ca, verify-full
}
```

For a full DSN, set `DSN`. It is used verbatim and takes precedence over
`Host`, `Port`, `Database`, `Username`, `Password`, and `SSLMode`:

```go
cfg := db.Config{
    Driver: "pgx",
    DSN:    "postgres://app:secret@localhost:5432/orders?sslmode=disable&application_name=orders-api",
}

// MySQL DSNs work the same way.
mysqlCfg := db.Config{
    Driver: "mysql",
    DSN:    "app:secret@tcp(localhost:3306)/orders?parseTime=true",
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

// Get a single row. If no row matches, sql.ErrNoRows is returned.
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

`IN (?)` slices are expanded automatically, on both engines:

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

## PostgreSQL notes

- **Driver aliases.** `pgx`, `pgx/v5`, `postgres`, and `postgresql` all select
  the bundled pgx `database/sql` driver. Setting `Driver: "postgres"` does *not* require
  `github.com/lib/pq`.
- **Placeholders.** Named placeholders are rebound to `$1`, `$2`, ... Slices in
  `IN (:ids)` become `($1, $2, $3)`.
- **Casts.** `:id::bigint` and `(:id)::bigint` are supported. The wrapper
  protects `::` from sqlx's named binder.
- **JSONB operators.** The native `?`, `?|`, and `?&` operators collide with the
  wrapper's internal `?` placeholder syntax. Use the function forms instead:
  `jsonb_exists(col, :key)`, `jsonb_exists_any`, or `jsonb_exists_all`.
- **Arrays vs. IN lists.** Slice arguments are always expanded for `IN (...)`
  semantics. Do not write `= ANY(:ids)` with a slice — write `IN (:ids)`. To
  bind a slice as a single array value (for an array column), wrap it in a
  `driver.Valuer` provided by your driver so the wrapper leaves it alone.
- **`LastInsertId`.** PostgreSQL does not support `sql.Result.LastInsertId`;
  use `INSERT ... RETURNING` with `NamedGetContext` instead.
- **String literals and comments.** Binding is literal-aware: `:` and `?`
  inside quoted strings, quoted identifiers, comments, and dollar-quoted bodies
  are left untouched, so `'12:30:00'`, `'ratio: 1'`, and
  `$tag$a:b?c$tag$` are safe.
- **Placeholder names** start with a letter or underscore; other colons are
  left for the database, so array slices (`arr[1:2]`) and assignments
  (`a := 1`) work.
- **Advanced options.** Client certificates, a custom root CA, and pgx options
  such as `default_query_exec_mode` (useful behind PgBouncer) are not exposed as
  `Config` fields — set them through `DSN`.
- **Errors.** `db.response.status_code` is the five-character SQLSTATE (for
  example `23505`); `error.type` is the server message. `errors.As` with
  `*pgconn.PgError` still works because the original error is returned.

## TLS / SSL

`Config.SSLMode` is shared by both drivers and is only used when `DSN` is
empty:

- **PostgreSQL** passes it through as the `sslmode` DSN parameter.
- **MySQL** maps it to go-sql-driver/mysql's `tls` parameter:

| `SSLMode` | MySQL `tls` | Meaning |
| --- | --- | --- |
| `disable` | `false` | No TLS |
| `allow`, `prefer` | `preferred` | TLS when the server supports it |
| `require` | `skip-verify` | TLS without certificate verification |
| `verify-ca`, `verify-full` | `true` | TLS with certificate and hostname verification |

MySQL has no CA-only verification mode, so `verify-ca` implies hostname
verification too. For custom CAs, client certificates, or a registered TLS
config name, set the `tls` parameter through `DSN`.

## Telemetry attributes

Per executed statement: `db.system.name` (`mysql` or `postgresql`),
`db.namespace` (the database name), `server.address` and `server.port` (from
`Config` or parsed from `DSN`, when known), `db.operation.name` (leading SQL
verb, e.g. `SELECT`), `db.query.text`, `db.response.status_code` (MySQL error
number or PostgreSQL SQLSTATE, empty on success), and `error.type` (error
message, empty on success). Transaction begin/commit/rollback are lifecycle
operations and are not instrumented.

## Config reference

| Field | Default | Notes |
| --- | --- | --- |
| `Driver` | `"mysql"` | `mysql`, `pgx`, `pgx/v5`, `postgres`, `postgresql`; any other name requires a registered driver and `DSN` |
| `DSN` | — | Used verbatim; overrides the discrete connection fields |
| `Host` | — | Required unless `DSN` is set |
| `Port` | `3306` / `5432` | Defaulted from the driver when `Host` is set and `DSN` is empty |
| `Database`, `Username` | — | Required unless `DSN` is set |
| `Password` | — | Optional |
| `SSLMode` | — | Both drivers, and only when `DSN` is empty |
| `MaxOpenConns` | `25` | `0` → default; negative is invalid |
| `MaxIdleConns` | `MaxOpenConns` | `0` → default; must not exceed `MaxOpenConns` |
| `ConnMaxLifetime` | `3m` | `0` → default; negative is invalid |
| `ConnMaxIdleTime` | `5m` | `0` → default; negative is invalid |

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

## Testing against real databases

Unit tests use a stub driver and need no server. Env-gated integration tests
exercise the real drivers against MySQL and PostgreSQL:

```bash
# MySQL (parseTime is enabled automatically by the test).
DB_MYSQL_DSN='app:secret@tcp(127.0.0.1:3306)/report_db' \
  go test -run TestMySQLIntegration ./...

# PostgreSQL.
DB_POSTGRES_DSN='postgres://app:secret@localhost:5432/report_db?sslmode=disable' \
  go test -run TestPostgresIntegration ./...
```

Both tests cover the DSN path, the discrete-config path, CRUD, `IN (:ids)`
expansion, casts, transaction rollback, and duplicate-key error classification
(MySQL 1062 / PostgreSQL 23505).
