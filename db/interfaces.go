package db

import (
	"context"
	"database/sql"
)

// Queryer executes read queries (SELECT).
type Queryer interface {
	// NamedSelectContext binds the named placeholders in query with arg, then
	// runs a SELECT storing the result rows into dest (a *[]T).
	NamedSelectContext(ctx context.Context, dest any, query string, arg any) error
	// NamedGetContext binds the named placeholders in query with arg, then runs
	// a query expecting a single result row into dest.
	NamedGetContext(ctx context.Context, dest any, query string, arg any) error
}

// Execer executes write queries (INSERT/UPDATE/DELETE).
type Execer interface {
	// NamedExecContext binds the named placeholders in query with arg, then
	// runs an INSERT/UPDATE/DELETE and returns the result.
	NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error)
}

// DB is the connection contract: it queries, executes, opens transactions and
// manages the underlying connection pool.
type DB interface {
	// Begin opens a transaction. Call Tx.Commit or Tx.Rollback to finish it.
	Begin(ctx context.Context) (Tx, error)
	// Close releases the underlying database connection pool.
	Close() error
	Queryer
	Execer
}

// Tx is the transaction contract: it queries and executes inside the
// transaction and manages its lifecycle.
type Tx interface {
	// Commit commits the transaction.
	Commit() error
	// Rollback aborts the transaction.
	Rollback() error
	Queryer
	Execer
}
