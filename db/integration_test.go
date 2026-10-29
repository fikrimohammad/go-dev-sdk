package db

// Integration tests in this file require a real server and are skipped unless
// the corresponding environment variable is set:
//
//	DB_MYSQL_DSN='app:secret@tcp(127.0.0.1:3306)/report_db'
//	DB_POSTGRES_DSN='postgres://app:secret@127.0.0.1:5432/report_db?sslmode=disable'
//
// Run them with:
//
//	go test -run 'Test(MySQL|Postgres)Integration' ./...

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestMySQLIntegration exercises the real go-sql-driver/mysql driver against a
// MySQL server.
func TestMySQLIntegration(t *testing.T) {
	dsn := os.Getenv("DB_MYSQL_DSN")
	if dsn == "" {
		t.Skip("DB_MYSQL_DSN not set")
	}

	mc, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("mysql.ParseDSN: %v", err)
	}
	// Force parseTime so DATETIME columns scan into time.Time; keep every other
	// option from the caller's DSN.
	mc.ParseTime = true
	dsn = mc.FormatDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := Connect(Config{Driver: "mysql", DSN: dsn, MaxOpenConns: 2})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	// Exercise the discrete-field path too, which builds the DSN itself.
	if host, portStr, err := net.SplitHostPort(mc.Addr); err == nil && mc.Net != "unix" {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			t.Fatalf("port %q: %v", portStr, err)
		}
		sslMode := "disable"
		if mc.TLSConfig != "" && mc.TLSConfig != "false" {
			sslMode = "prefer"
		}
		t.Run("discrete config", func(t *testing.T) {
			discrete, err := Connect(Config{
				Driver:       "mysql",
				Host:         host,
				Port:         port,
				Database:     mc.DBName,
				Username:     mc.User,
				Password:     mc.Passwd,
				SSLMode:      sslMode,
				MaxOpenConns: 1,
			})
			if err != nil {
				t.Fatalf("Connect(discrete): %v", err)
			}
			t.Cleanup(func() { _ = discrete.Close() })

			var one int
			if err := discrete.NamedGetContext(ctx, &one, "SELECT 1", nil); err != nil {
				t.Fatalf("select: %v", err)
			}
			if one != 1 {
				t.Fatalf("one = %d, want 1", one)
			}
		})
	}

	table := fmt.Sprintf("db_sdk_test_%d", time.Now().UnixNano())
	if _, err := pool.NamedExecContext(ctx,
		"CREATE TABLE "+table+" ("+
			"id BIGINT AUTO_INCREMENT PRIMARY KEY, "+
			"name VARCHAR(255) NOT NULL UNIQUE, "+
			"created_at DATETIME(6) NULL)", nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.NamedExecContext(context.Background(), "DROP TABLE IF EXISTS "+table, nil)
	})

	created := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	res, err := pool.NamedExecContext(ctx,
		"INSERT INTO "+table+" (name, created_at) VALUES (:name, :created_at)",
		map[string]any{"name": "first", "created_at": created})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId: %v", err)
	}
	if id == 0 {
		t.Fatal("LastInsertId = 0, want the auto-increment value")
	}

	var row struct {
		ID        int64     `db:"id"`
		Name      string    `db:"name"`
		CreatedAt time.Time `db:"created_at"`
	}
	if err := pool.NamedGetContext(ctx, &row,
		"SELECT id, name, created_at FROM "+table+" WHERE id = :id",
		map[string]any{"id": id}); err != nil {
		t.Fatalf("get: %v", err)
	}
	if row.ID != id || row.Name != "first" {
		t.Fatalf("row = %+v", row)
	}
	if !row.CreatedAt.Equal(created) {
		t.Fatalf("created_at = %v, want %v", row.CreatedAt, created)
	}

	if _, err := pool.NamedExecContext(ctx,
		"INSERT INTO "+table+" (name) VALUES (:name)", map[string]any{"name": "second"}); err != nil {
		t.Fatalf("insert second: %v", err)
	}

	var rows []struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	if err := pool.NamedSelectContext(ctx, &rows,
		"SELECT id, name FROM "+table+" WHERE id IN (:ids) ORDER BY id",
		map[string]any{"ids": []int64{id, id + 1}}); err != nil {
		t.Fatalf("select IN: %v", err)
	}
	if len(rows) != 2 || rows[0].Name != "first" || rows[1].Name != "second" {
		t.Fatalf("rows = %+v", rows)
	}

	var casted int64
	if err := pool.NamedGetContext(ctx, &casted,
		"SELECT CAST(:n AS SIGNED) AS n", map[string]any{"n": int64(42)}); err != nil {
		t.Fatalf("cast select: %v", err)
	}
	if casted != 42 {
		t.Fatalf("casted = %d, want 42", casted)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.NamedExecContext(ctx,
		"INSERT INTO "+table+" (name) VALUES (:name)",
		map[string]any{"name": "rolled back"}); err != nil {
		t.Fatalf("tx insert: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	var count int
	if err := pool.NamedGetContext(ctx, &count, "SELECT count(*) FROM "+table, nil); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2 after rollback", count)
	}

	_, err = pool.NamedExecContext(ctx,
		"INSERT INTO "+table+" (name) VALUES (:name)", map[string]any{"name": "first"})
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1062 {
		t.Fatalf("duplicate insert error = %v, want MySQL error 1062", err)
	}
	if code, _ := errorInfo(err); code != "1062" {
		t.Fatalf("errorInfo = %q, want 1062", code)
	}
}

// TestPostgresIntegration exercises the real pgx driver against a PostgreSQL
// server.
func TestPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("DB_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DB_POSTGRES_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Using the DSN path with the "postgres" alias: driver resolution, DSN
	// parsing, ping, and telemetry identity all run against a real server.
	pool, err := Connect(Config{Driver: "postgres", DSN: dsn, MaxOpenConns: 2})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	// The discrete-field path builds a postgres:// DSN itself. Socket-based
	// DSNs cannot be expressed that way, so only run this for TCP hosts.
	if pc, err := pgx.ParseConfig(dsn); err == nil && !strings.HasPrefix(pc.Host, "/") {
		t.Run("discrete config", func(t *testing.T) {
			discrete, err := Connect(Config{
				Driver:       "postgres",
				Host:         pc.Host,
				Port:         int(pc.Port),
				Database:     pc.Database,
				Username:     pc.User,
				Password:     pc.Password,
				SSLMode:      "prefer",
				MaxOpenConns: 1,
			})
			if err != nil {
				t.Fatalf("Connect(discrete): %v", err)
			}
			t.Cleanup(func() { _ = discrete.Close() })

			var one int
			if err := discrete.NamedGetContext(ctx, &one, "SELECT 1", nil); err != nil {
				t.Fatalf("select: %v", err)
			}
			if one != 1 {
				t.Fatalf("one = %d, want 1", one)
			}
		})
	}

	table := fmt.Sprintf("db_sdk_test_%d", time.Now().UnixNano())
	if _, err := pool.NamedExecContext(ctx,
		"CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, name TEXT NOT NULL)", nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.NamedExecContext(context.Background(), "DROP TABLE IF EXISTS "+table, nil)
	})

	if _, err := pool.NamedExecContext(ctx,
		"INSERT INTO "+table+" (id, name) VALUES (:id, :name)",
		map[string]any{"id": int64(1), "name": "first"}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var rows []struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	if err := pool.NamedSelectContext(ctx, &rows,
		"SELECT id, name FROM "+table+" WHERE id IN (:ids) ORDER BY id",
		map[string]any{"ids": []int64{1, 2}}); err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != 1 || rows[0].Name != "first" {
		t.Fatalf("rows = %+v", rows)
	}

	var n int64
	if err := pool.NamedGetContext(ctx, &n,
		"SELECT :n::bigint AS n WHERE 'ratio: 1' <> ''", map[string]any{"n": int64(42)}); err != nil {
		t.Fatalf("cast select: %v", err)
	}
	if n != 42 {
		t.Fatalf("n = %d, want 42", n)
	}

	var sliceLen int
	if err := pool.NamedGetContext(ctx, &sliceLen,
		"SELECT array_length((ARRAY[1,2,3])[1:2], 1) WHERE :n::int > 0",
		map[string]any{"n": int64(1)}); err != nil {
		t.Fatalf("array slice select: %v", err)
	}
	if sliceLen != 2 {
		t.Fatalf("sliceLen = %d, want 2", sliceLen)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.NamedExecContext(ctx,
		"INSERT INTO "+table+" (id, name) VALUES (:id, :name)",
		map[string]any{"id": int64(2), "name": "rolled back"}); err != nil {
		t.Fatalf("tx insert: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	var count int
	if err := pool.NamedGetContext(ctx, &count, "SELECT count(*) FROM "+table, nil); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1 after rollback", count)
	}

	_, err = pool.NamedExecContext(ctx,
		"INSERT INTO "+table+" (id, name) VALUES (:id, :name)",
		map[string]any{"id": int64(1), "name": "duplicate"})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("duplicate insert error = %v, want SQLSTATE 23505", err)
	}
	if code, _ := errorInfo(err); code != "23505" {
		t.Fatalf("errorInfo = %q, want 23505", code)
	}
}
