// This file holds documentation examples for package db. They are
// compile-only because they need a live PostgreSQL server.

package db_test

import (
	"context"
	"log"

	"github.com/fikrimohammad/go-dev-sdk/db"
)

// ExampleConnect opens a PostgreSQL pool and loads one row into a struct
// through a named placeholder.
func ExampleConnect() {
	conn, err := db.Connect(context.Background(), db.Config{
		Driver:   "pgx",
		Host:     "localhost",
		Port:     5432,
		Database: "report_db",
		Username: "app",
		Password: "secret",
		SSLMode:  "disable",
	})
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Println(err)
		}
	}()

	var user struct {
		ID    int64  `db:"id"`
		Email string `db:"email"`
	}
	err = conn.NamedGetContext(context.Background(), &user,
		"SELECT id, email FROM users WHERE id = :id", map[string]any{"id": 42})
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("user %d: %s", user.ID, user.Email)
}

// ExampleDB_Begin moves credits between two accounts in one transaction. The
// deferred Rollback undoes the work on any early return; after a successful
// Commit it is a harmless no-op.
func ExampleDB_Begin() {
	conn, err := db.Connect(context.Background(), db.Config{
		Driver:   "pgx",
		Host:     "localhost",
		Port:     5432,
		Database: "report_db",
		Username: "app",
		Password: "secret",
		SSLMode:  "disable",
	})
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Println(err)
		}
	}()

	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() { _ = tx.Rollback() }()

	const debit = "UPDATE accounts SET balance = balance - :amount WHERE id = :id"
	if _, err := tx.NamedExecContext(ctx, debit, map[string]any{"id": 1, "amount": 100}); err != nil {
		log.Println(err)
		return
	}

	const credit = "UPDATE accounts SET balance = balance + :amount WHERE id = :id"
	if _, err := tx.NamedExecContext(ctx, credit, map[string]any{"id": 2, "amount": 100}); err != nil {
		log.Println(err)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Println(err)
	}
}
