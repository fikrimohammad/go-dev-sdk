// Package db wraps *sqlx.DB with a small, standardized query API that binds
// named (:name) placeholders internally and records OpenTelemetry traces and
// metrics per executed statement.
//
// Connect opens a database exactly like sqlx.Connect, deriving the
// db.system.name from the driver name and db.namespace from the DSN, and
// returns an instrumented DB. Metrics and tracing clients are injectable via
// WithMetrics / WithTracer and fall back to the package-level defaults.
//
// The exported surface is minimal and consistent: the Queryer, Execer, DB and
// Tx interfaces describe the contracts, backed by unexported implementations.
//
//   - Queryer and Execer group the three query methods: NamedSelectContext,
//     NamedGetContext and NamedExecContext, which run a named query against the
//     connection, and the same three methods run against a transaction.
//   - DB adds transaction management (Begin) and connection lifecycle (Close).
//   - Tx adds Commit and Rollback on top of the query methods.
//
// Each query method takes a named query and a single argument (a struct or a
// map of keys to values) whose keys match the :placeholders. The wrapper
// performs the whole binding pipeline — BindNamed (:name -> ?), sqlx.In
// (expanding IN (?) slices), and Rebind (driver placeholder syntax) — so
// callers just write the named query. Only the actual database execution is
// instrumented; binding is pure logic and failures there are returned without
// emitting any telemetry.
//
// Per executed statement one span "db.query" (client kind) and the
// db.client.operation.{count,duration} metrics are recorded with OTel
// attributes: db.system.name, db.namespace (when known), db.operation.name,
// db.query.text, db.response.status_code (driver error code, empty on
// success) and error.type (the error message, empty on success).
// Transaction begin/commit/rollback are lifecycle operations and are not
// instrumented.
//
// Tracing and metrics use the package-level defaults of the observability
// packages (tracer.Tracer / metrics.Count & metrics.Histogram).
package db

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
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

var sqlOpRE = regexp.MustCompile(`(?i)^\s*(SELECT|INSERT|UPDATE|DELETE|WITH|REPLACE|MERGE|CALL|CREATE|ALTER|DROP|TRUNCATE|SET)\b`)

// binder is the shared named-binding contract implemented by both *sqlx.DB and
// *sqlx.Tx, letting the same binding pipeline serve connection and transaction
// queries alike.
type binder interface {
	BindNamed(query string, arg any) (string, []any, error)
	Rebind(query string) string
}

// meta carries the attributes held by every instrumented execution target.
type meta struct {
	systemName string // db.system.name, from the driver name
	namespace  string // db.namespace, the database/schema name ("" when unknown)

	// metrics and tracer override the package-level defaults when non-nil.
	metrics metrics.Client
	tracer  tracer.Client
}

// Option configures a DB returned by Connect.
type Option func(*options)

type options struct {
	metrics metrics.Client
	tracer  tracer.Client
}

// WithMetrics injects a metrics client for the instrumented executions. A nil
// client falls back to the package-level default (metrics.SetDefault).
func WithMetrics(m metrics.Client) Option {
	return func(o *options) { o.metrics = m }
}

// WithTracer injects a tracer client for the instrumented executions. A nil
// client falls back to the package-level default (tracer.SetDefault).
func WithTracer(t tracer.Client) Option {
	return func(o *options) { o.tracer = t }
}

// Connect applies cfg.SetDefaults and cfg.Validate, opens a database from the
// resulting config (building the data source name), applies the pool settings,
// pings it, and returns an instrumented DB. systemName is derived from the
// driver name; namespace is the database name. Metrics and tracing use the
// package-level defaults unless overridden via WithMetrics/WithTracer.
func Connect(cfg Config, opts ...Option) (DB, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	cfg = cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	raw, err := sqlx.Connect(cfg.Driver, buildDSN(cfg))
	if err != nil {
		return nil, err
	}
	applyPool(raw, cfg)

	return &dbConn{
		DB: raw,
		meta: meta{
			systemName: semconvSystemName(cfg.Driver),
			namespace:  cfg.Database,
			metrics:    o.metrics,
			tracer:     o.tracer,
		},
	}, nil
}

// buildDSN renders a MySQL data source name from cfg.
func buildDSN(cfg Config) string {
	mc := mysql.NewConfig()
	mc.User = cfg.Username
	mc.Passwd = cfg.Password
	mc.Net = "tcp"
	mc.Addr = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	mc.DBName = cfg.Database
	mc.ParseTime = true
	return mc.FormatDSN()
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
	meta
}

// NamedSelectContext binds the named placeholders in query with arg, then runs
// a SELECT storing the result rows into dest (a *[]T, per sqlx semantics).
func (d *dbConn) NamedSelectContext(ctx context.Context, dest any, query string, arg any) error {
	return execute(ctx, d.DB, d.meta, "select", query, arg, func(ctx context.Context, q string, args []any) error {
		return d.SelectContext(ctx, dest, q, args...)
	})
}

// NamedGetContext binds the named placeholders in query with arg, then runs a
// query expecting a single result row into dest.
func (d *dbConn) NamedGetContext(ctx context.Context, dest any, query string, arg any) error {
	return execute(ctx, d.DB, d.meta, "get", query, arg, func(ctx context.Context, q string, args []any) error {
		return d.GetContext(ctx, dest, q, args...)
	})
}

// NamedExecContext binds the named placeholders in query with arg, then runs
// an INSERT/UPDATE/DELETE and returns the result.
func (d *dbConn) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	var res sql.Result
	err := execute(ctx, d.DB, d.meta, "exec", query, arg, func(ctx context.Context, q string, args []any) error {
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
	return &dbTx{tx: tx, meta: d.meta}, nil
}

// Close releases the underlying database connection pool.
func (d *dbConn) Close() error { return d.DB.Close() }

// dbTx implements Tx against a *sqlx.Tx.
type dbTx struct {
	tx *sqlx.Tx
	meta
}

// NamedSelectContext runs a SELECT inside the transaction, storing the result
// rows into dest (a *[]T, per sqlx semantics).
func (t *dbTx) NamedSelectContext(ctx context.Context, dest any, query string, arg any) error {
	return execute(ctx, t.tx, t.meta, "select", query, arg, func(ctx context.Context, q string, args []any) error {
		return t.tx.SelectContext(ctx, dest, q, args...)
	})
}

// NamedGetContext runs a query expecting a single result row inside the
// transaction, storing it into dest.
func (t *dbTx) NamedGetContext(ctx context.Context, dest any, query string, arg any) error {
	return execute(ctx, t.tx, t.meta, "get", query, arg, func(ctx context.Context, q string, args []any) error {
		return t.tx.GetContext(ctx, dest, q, args...)
	})
}

// NamedExecContext runs an INSERT/UPDATE/DELETE inside the transaction and
// returns the result.
func (t *dbTx) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	var res sql.Result
	err := execute(ctx, t.tx, t.meta, "exec", query, arg, func(ctx context.Context, q string, args []any) error {
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
func execute(ctx context.Context, b binder, m meta, op, query string, arg any, fn func(context.Context, string, []any) error) error {
	bound, args, err := bindQuery(b, query, arg)
	if err != nil {
		return err
	}
	return m.instrument(ctx, op, bound, func(ctx context.Context) error {
		return fn(ctx, bound, args)
	})
}

// bindQuery expands the named query end-to-end: :name placeholders become ?, IN
// (?) slices are expanded by sqlx.In, and the result is rebound to the driver's
// placeholder syntax. It performs no database I/O. A nil arg skips binding,
// which is valid for queries without placeholders.
func bindQuery(b binder, query string, arg any) (string, []any, error) {
	if arg == nil {
		return b.Rebind(query), nil, nil
	}
	bound, args, err := b.BindNamed(query, arg)
	if err != nil {
		return "", nil, err
	}
	bound, args, err = sqlx.In(bound, args...)
	if err != nil {
		return "", nil, err
	}
	return b.Rebind(bound), args, nil
}

// instrument records one span and one count + duration histogram around fn,
// which is the actual statement execution.
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
		code = mysqlErrorCode(err)
		etype = errorMessage(err)
	}

	span.SetAttributes(tracer.Attrs(m.attrs(opName, query, code, etype))...)
	span.End()

	attrs := m.attrs(opName, query, code, etype)
	if m.metrics != nil {
		_ = m.metrics.Count(ctx, metricCount, 1, attrs)
		_ = m.metrics.Histogram(ctx, metricDuration, time.Since(start).Seconds(), attrs)
	} else {
		_ = metrics.Count(ctx, metricCount, 1, attrs)
		_ = metrics.Histogram(ctx, metricDuration, time.Since(start).Seconds(), attrs)
	}
	return err
}

// attrs builds the OTel attributes for a single span / metric event.
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

// mysqlErrorCode returns the MySQL driver error number as a string, or the
// empty string when err is not a *mysql.MySQLError.
func mysqlErrorCode(err error) string {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return strconv.Itoa(int(me.Number))
	}
	return ""
}

// errorMessage returns the error message: the driver message when err is a
// *mysql.MySQLError (stripping the "Error NNNN: " prefix), otherwise err's
// Error() text.
func errorMessage(err error) string {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Message
	}
	return err.Error()
}

// semconvSystemName maps a database/sql driver name to the OTel
// db.system.name value, falling back to the raw driver name.
func semconvSystemName(driver string) string {
	switch driver {
	case "mysql":
		return "mysql"
	case "postgres", "pgx", "postgresql":
		return "postgresql"
	case "sqlite", "sqlite3":
		return "sqlite"
	default:
		return driver
	}
}
