// Package db provides a small, standardized named-query API for MySQL and
// PostgreSQL on top of [sqlx.DB], recording OpenTelemetry traces and metrics
// per executed statement.
//
// [Connect] applies [Config.SetDefaults] and [Config.Validate], opens a
// database from the resulting config (building a driver-specific data source
// name unless Config.DSN is set), applies the pool settings, pings it, and
// returns an instrumented [DB]. The Config.Driver field selects the dialect:
//
//   - "mysql" (default): go-sql-driver/mysql, default port 3306.
//   - "pgx", "pgx/v5", "postgres" or "postgresql": pgx/v5's database/sql
//     driver, default port 5432. The postgres aliases use the bundled pgx
//     driver, so callers do not need to import github.com/lib/pq.
//   - any other driver name with an explicit DSN: opened as-is and bound using
//     sqlx's driver registry. The driver must be registered by the caller.
//
// The generated PostgreSQL DSN is a postgres:// URL that passes Config.SSLMode
// through as sslmode; the generated MySQL DSN enables parseTime and maps
// Config.SSLMode to the driver's tls parameter. Setting Config.DSN bypasses
// DSN generation entirely and takes precedence over the discrete connection
// fields. The db.system.name attribute follows the OTel database semantic
// conventions ("mysql" or "postgresql" for the built-in drivers, the driver
// name otherwise). The db.namespace attribute is the database name, and
// server.address and server.port are recorded when known from Config or parsed
// from Config.DSN.
//
// The exported surface is minimal and consistent: the [Queryer], [Execer],
// [DB], and [Tx] interfaces describe the contracts, backed by unexported
// implementations.
//
//   - [Queryer] and [Execer] group the three query methods: NamedSelectContext,
//     NamedGetContext, and NamedExecContext. They run against either a
//     connection or a transaction.
//   - [DB] adds transaction management (Begin) and connection lifecycle
//     (Close).
//   - [Tx] adds Commit and Rollback on top of the query methods.
//
// Each query method takes a named query and a single argument (a struct or a
// map of keys to values) whose keys match the :placeholders. The wrapper
// performs the binding pipeline: named placeholders become '?', IN (?) slices
// are expanded, and the result is rewritten to the driver's placeholder
// syntax. Callers therefore only write the named query. Binding runs in
// question-mark form and is rewritten afterwards, so IN (?) slices expand
// identically on MySQL and PostgreSQL ($1, $2, ...). Only the actual database
// execution is instrumented; binding failures are returned without emitting
// telemetry.
//
// Binding is literal-aware: ':' and '?' inside string literals, quoted
// identifiers, comments, and PostgreSQL dollar-quoted bodies are left alone.
// Placeholder names start with a letter or underscore, so casts (:id::bigint),
// array slices (arr[1:2]) and assignments (a := 1) work as written.
//
// PostgreSQL-specific caveats: the native JSONB ? / ?| / ?& operators collide
// with the internal '?' placeholder syntax (use jsonb_exists, jsonb_exists_any
// or jsonb_exists_all instead), and slice arguments always expand to an IN list
// (use IN (:ids) rather than = ANY(:ids); wrap the slice in a [driver.Valuer] for
// an array column).
//
// For each executed statement, one client span named "db.query" and the
// db.client.operation.{count,duration} metrics are recorded with OTel
// attributes: db.system.name, db.namespace (when known), server.address and
// server.port (when known), db.operation.name, db.query.text,
// db.response.status_code (MySQL error number or PostgreSQL SQLSTATE, empty on
// success) and error.type (the error message, empty on success). Transaction
// begin, commit, and rollback are lifecycle operations and are not
// instrumented.
//
// Tracing and metrics use the package-level defaults of the observability
// packages ([tracer.Tracer], [metrics.Count], and [metrics.Histogram]) unless
// overridden with [WithMetrics] or [WithTracer].
package db

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

const (
	// tracerScope is both the OTel instrumentation scope name and the span name.
	tracerScope = "db.query"

	// metricCount and metricDuration are the OTel metric names emitted per executed statement.
	metricCount    = "db.client.operation.count"
	metricDuration = "db.client.operation.duration"
)

// sqlOpRE matches the leading SQL verb used for the db.operation.name
// attribute.
var sqlOpRE = regexp.MustCompile(`(?i)^\s*(SELECT|INSERT|UPDATE|DELETE|WITH|REPLACE|MERGE|CALL|CREATE|ALTER|DROP|TRUNCATE|SET|EXPLAIN|SHOW|DESCRIBE|VACUUM|ANALYZE)\b`)

// meta carries the attributes held by every instrumented execution target.
type meta struct {
	systemName string // db.system.name, from the driver
	namespace  string // db.namespace, the database name ("" when unknown)
	serverAddr string // server.address ("" when unknown)
	serverPort int    // server.port (0 when unknown)

	// metrics and tracer override the package-level defaults when non-nil.
	metrics metrics.Client
	tracer  tracer.Client
}

// Option configures a DB returned by [Connect].
type Option func(*options)

type options struct {
	metrics metrics.Client
	tracer  tracer.Client
}

// WithMetrics injects a metrics client for the instrumented executions. A nil
// client falls back to the package-level default set by [metrics.SetDefault].
func WithMetrics(m metrics.Client) Option {
	return func(o *options) { o.metrics = m }
}

// WithTracer injects a tracer client for the instrumented executions. A nil
// client falls back to the package-level default set by [tracer.SetDefault].
func WithTracer(t tracer.Client) Option {
	return func(o *options) { o.tracer = t }
}

// Connect applies [Config.SetDefaults] and [Config.Validate], opens a database
// from the resulting config (using Config.DSN when set, otherwise building a
// driver-specific data source name), applies the pool settings, pings it, and
// returns an instrumented [DB]. It returns an error if validation fails or the
// database cannot be opened or pinged. Metrics and tracing use the
// package-level defaults unless overridden with [WithMetrics] or [WithTracer].
func Connect(cfg Config, opts ...Option) (DB, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	cfg = cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	spec, ok := driverFor(cfg.Driver)
	if !ok {
		spec = genericDriver(cfg.Driver)
	}

	dsn := spec.dsn(cfg)
	if dsn == "" {
		return nil, fmt.Errorf("db: driver %q requires Config.DSN", cfg.Driver)
	}

	raw, err := sqlx.Connect(spec.name, dsn)
	if err != nil {
		return nil, fmt.Errorf("db: connect %s: %w", spec.systemName, err)
	}
	applyPool(raw, cfg)

	info := spec.connInfo(cfg)

	return &dbConn{
		DB:     raw,
		driver: spec,
		meta: meta{
			systemName: spec.systemName,
			namespace:  info.database,
			serverAddr: info.host,
			serverPort: info.port,
			metrics:    o.metrics,
			tracer:     o.tracer,
		},
	}, nil
}

// applyPool applies the non-zero pool settings from cfg to raw.
func applyPool(raw *sqlx.DB, cfg Config) {
	if cfg.MaxOpenConns > 0 {
		raw.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		raw.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		raw.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	if cfg.ConnMaxIdleTime > 0 {
		raw.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}
}

var (
	_ DB = (*dbConn)(nil)
	_ Tx = (*dbTx)(nil)
)

// dbConn implements DB against a *sqlx.DB.
type dbConn struct {
	*sqlx.DB
	driver driverSpec
	meta
}

// NamedSelectContext binds the named placeholders in query with arg, then runs
// a SELECT storing the result rows into dest (a *[]T, per sqlx semantics).
func (d *dbConn) NamedSelectContext(ctx context.Context, dest any, query string, arg any) error {
	return execute(ctx, d.driver, d.meta, "select", query, arg, func(ctx context.Context, q string, args []any) error {
		return d.SelectContext(ctx, dest, q, args...)
	})
}

// NamedGetContext binds the named placeholders in query with arg, then runs a
// query expecting a single result row and stores it into dest. It returns
// [sql.ErrNoRows] when the query returns no rows.
func (d *dbConn) NamedGetContext(ctx context.Context, dest any, query string, arg any) error {
	return execute(ctx, d.driver, d.meta, "get", query, arg, func(ctx context.Context, q string, args []any) error {
		return d.GetContext(ctx, dest, q, args...)
	})
}

// NamedExecContext binds the named placeholders in query with arg, then runs
// an INSERT/UPDATE/DELETE and returns the result.
func (d *dbConn) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	var res sql.Result
	err := execute(ctx, d.driver, d.meta, "exec", query, arg, func(ctx context.Context, q string, args []any) error {
		var e error
		res, e = d.ExecContext(ctx, q, args...)
		return e
	})
	return res, err
}

// Begin opens a transaction. Call Tx.Commit or Tx.Rollback to finish it.
func (d *dbConn) Begin(ctx context.Context) (Tx, error) {
	tx, err := d.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &dbTx{tx: tx, driver: d.driver, meta: d.meta}, nil
}

// Close releases the underlying database connection pool.
func (d *dbConn) Close() error { return d.DB.Close() }

// dbTx implements Tx against a *sqlx.Tx.
type dbTx struct {
	tx     *sqlx.Tx
	driver driverSpec
	meta
}

// NamedSelectContext runs a SELECT inside the transaction, storing the result
// rows into dest (a *[]T, per sqlx semantics).
func (t *dbTx) NamedSelectContext(ctx context.Context, dest any, query string, arg any) error {
	return execute(ctx, t.driver, t.meta, "select", query, arg, func(ctx context.Context, q string, args []any) error {
		return t.tx.SelectContext(ctx, dest, q, args...)
	})
}

// NamedGetContext runs a query expecting a single result row inside the
// transaction and stores it into dest. It returns [sql.ErrNoRows] when the query
// returns no rows.
func (t *dbTx) NamedGetContext(ctx context.Context, dest any, query string, arg any) error {
	return execute(ctx, t.driver, t.meta, "get", query, arg, func(ctx context.Context, q string, args []any) error {
		return t.tx.GetContext(ctx, dest, q, args...)
	})
}

// NamedExecContext runs an INSERT/UPDATE/DELETE inside the transaction and
// returns the result.
func (t *dbTx) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	var res sql.Result
	err := execute(ctx, t.driver, t.meta, "exec", query, arg, func(ctx context.Context, q string, args []any) error {
		var e error
		res, e = t.tx.ExecContext(ctx, q, args...)
		return e
	})
	return res, err
}

// Commit commits the transaction.
func (t *dbTx) Commit() error { return t.tx.Commit() }

// Rollback aborts the transaction.
func (t *dbTx) Rollback() error { return t.tx.Rollback() }

// execute binds the named query, then runs fn against the bound query. Binding
// is pure logic: a failure here returns directly and is never instrumented.
func execute(ctx context.Context, d driverSpec, m meta, op, query string, arg any, fn func(context.Context, string, []any) error) error {
	bound, args, err := bindQuery(d, query, arg)
	if err != nil {
		return err
	}
	return m.instrument(ctx, op, bound, func(ctx context.Context) error {
		return fn(ctx, bound, args)
	})
}

// instrument records one span, one count metric, and one duration histogram
// around fn, which is the actual statement execution.
func (m meta) instrument(ctx context.Context, op, query string, fn func(context.Context) error) error {
	opName := operationName(op, query)

	var tr trace.Tracer
	if m.tracer != nil {
		tr = m.tracer.Tracer(tracerScope)
	} else {
		tr = tracer.Tracer(tracerScope)
	}
	ctx, span := tr.Start(ctx, tracerScope,
		trace.WithSpanKind(trace.SpanKindClient),
	)
	start := time.Now()
	err := fn(ctx)

	// db.response.status_code and error.type are set on every event: to the
	// driver error code / message on failure, and to the empty string on
	// success.
	code, etype := "", ""
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		code, etype = errorInfo(err)
	}

	attrs := m.attrs(opName, query, code, etype)
	span.SetAttributes(tracer.Attrs(attrs)...)
	span.End()

	if m.metrics != nil {
		_ = m.metrics.Count(ctx, metricCount, 1, attrs)
		_ = m.metrics.Histogram(ctx, metricDuration, time.Since(start).Seconds(), attrs)
	} else {
		_ = metrics.Count(ctx, metricCount, 1, attrs)
		_ = metrics.Histogram(ctx, metricDuration, time.Since(start).Seconds(), attrs)
	}
	return err
}

// attrs builds the OTel attributes for a single span or metric event.
func (m meta) attrs(opName, query, code, etype string) map[string]any {
	a := map[string]any{
		"db.system.name":          m.systemName,
		"db.operation.name":       opName,
		"db.query.text":           query,
		"db.response.status_code": code,
		"error.type":              etype,
	}
	if m.namespace != "" {
		a["db.namespace"] = m.namespace
	}
	if m.serverAddr != "" {
		a["server.address"] = m.serverAddr
	}
	if m.serverPort > 0 {
		a["server.port"] = m.serverPort
	}
	return a
}

// operationName returns the leading SQL verb of the query (e.g. "SELECT"),
// falling back to the caller-supplied operation op when the query does not
// start with a recognizable keyword (e.g. a bare named query).
func operationName(op, query string) string {
	if m := sqlOpRE.FindStringSubmatch(query); len(m) > 1 {
		return strings.ToUpper(m[1])
	}
	return op
}
