package db

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresDSN(t *testing.T) {
	dsn := postgresDSN(Config{
		Host: "db.example.com", Port: 5432, Database: "report_db",
		Username: "user", Password: "p@ss/word", SSLMode: "require",
	})

	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("pgx.ParseConfig(%q): %v", dsn, err)
	}
	if cfg.Host != "db.example.com" || cfg.Port != 5432 {
		t.Fatalf("host/port = %s/%d, want db.example.com/5432", cfg.Host, cfg.Port)
	}
	if cfg.Database != "report_db" {
		t.Fatalf("database = %q, want report_db", cfg.Database)
	}
	if cfg.User != "user" || cfg.Password != "p@ss/word" {
		t.Fatalf("user/password = %q/%q, want escaped credentials to round-trip", cfg.User, cfg.Password)
	}
	if !strings.Contains(dsn, "sslmode=require") {
		t.Fatalf("dsn = %q, want sslmode=require", dsn)
	}
}

func TestPostgresDSN_NoPassword(t *testing.T) {
	dsn := postgresDSN(Config{
		Host: "localhost", Port: 5432, Database: "report_db", Username: "user",
	})
	want := "postgres://user@localhost:5432/report_db"
	if dsn != want {
		t.Fatalf("postgresDSN = %q, want %q", dsn, want)
	}
}

func TestDriverFor(t *testing.T) {
	cases := []struct {
		in         string
		name       string
		systemName string
		port       int
		bind       bindType
	}{
		{"", "mysql", "mysql", 3306, bindQuestion},
		{"mysql", "mysql", "mysql", 3306, bindQuestion},
		{"pgx", "pgx", "postgresql", 5432, bindDollar},
		{"pgx/v5", "pgx", "postgresql", 5432, bindDollar},
		{"postgres", "pgx", "postgresql", 5432, bindDollar},
		{"postgresql", "pgx", "postgresql", 5432, bindDollar},
	}
	for _, c := range cases {
		spec, ok := driverFor(c.in)
		if !ok {
			t.Errorf("driverFor(%q) not supported", c.in)
			continue
		}
		if spec.name != c.name || spec.systemName != c.systemName || spec.port != c.port || spec.bindType != c.bind {
			t.Errorf("driverFor(%q) = %+v", c.in, spec)
		}
	}

	if _, ok := driverFor("sqlmock"); ok {
		t.Fatal("driverFor(sqlmock) without a DSN: got supported, want unsupported")
	}
}

func TestConnInfo(t *testing.T) {
	pg := postgresDriver.connInfo(Config{
		DSN: "postgres://user:pass@db.example.com:5433/report_db?sslmode=disable",
	})
	if pg.host != "db.example.com" || pg.port != 5433 || pg.database != "report_db" {
		t.Fatalf("postgres connInfo = %+v", pg)
	}

	my := mysqlDriver.connInfo(Config{
		DSN: "user:pass@tcp(db.example.com:3307)/report_db?parseTime=true",
	})
	if my.host != "db.example.com" || my.port != 3307 || my.database != "report_db" {
		t.Fatalf("mysql connInfo = %+v", my)
	}

	// Discrete fields are used when no DSN is set.
	cfg := Config{Host: "localhost", Port: 5432, Database: "report_db"}
	if got := postgresDriver.connInfo(cfg); got != (connInfo{"localhost", 5432, "report_db"}) {
		t.Fatalf("discrete connInfo = %+v", got)
	}
}

func TestMySQLDSN(t *testing.T) {
	got := mysqlDSN(Config{
		Host: "db.example.com", Port: 3306, Database: "report_db",
		Username: "user", Password: "p@ss",
	})
	want := "user:p@ss@tcp(db.example.com:3306)/report_db?parseTime=true"
	if got != want {
		t.Fatalf("mysqlDSN = %q, want %q", got, want)
	}
}

func TestMySQLDSN_SSLMode(t *testing.T) {
	cases := map[string]string{
		"":            "",
		"disable":     "false",
		"allow":       "preferred",
		"prefer":      "preferred",
		"require":     "skip-verify",
		"verify-ca":   "true",
		"verify-full": "true",
	}
	for mode, want := range cases {
		dsn := mysqlDSN(Config{
			Host: "localhost", Port: 3306, Database: "report_db",
			Username: "user", SSLMode: mode,
		})
		mc, err := mysql.ParseDSN(dsn)
		if err != nil {
			t.Fatalf("mysql.ParseDSN(%q): %v", dsn, err)
		}
		if mc.TLSConfig != want {
			t.Errorf("SSLMode %q -> tls %q, want %q", mode, mc.TLSConfig, want)
		}
	}
}

func TestErrorInfo(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode string
		wantType string
	}{
		{
			name:     "mysql",
			err:      &mysql.MySQLError{Number: 1062, Message: "duplicate entry"},
			wantCode: "1062",
			wantType: "1062",
		},
		{
			name:     "postgres",
			err:      &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"},
			wantCode: "23505",
			wantType: "23505",
		},
		{
			name:     "wrapped mysql",
			err:      errors.Join(errors.New("x"), &mysql.MySQLError{Number: 1452, Message: "fk"}),
			wantCode: "1452",
			wantType: "1452",
		},
		{
			name:     "wrapped postgres",
			err:      errors.Join(errors.New("x"), &pgconn.PgError{Code: "40001", Message: "serialization failure"}),
			wantCode: "40001",
			wantType: "40001",
		},
		{
			name:     "deadline",
			err:      context.DeadlineExceeded,
			wantType: "timeout",
		},
		{
			name:     "canceled",
			err:      context.Canceled,
			wantType: "canceled",
		},
		{
			name:     "network timeout",
			err:      fmt.Errorf("read: %w", &net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}),
			wantType: "timeout",
		},
		{
			// The message must never leak into error.type.
			name:     "unknown",
			err:      errors.New("connection to 10.0.0.5 refused"),
			wantType: "_OTHER",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, errType := errorInfo(tc.err)
			if code != tc.wantCode {
				t.Errorf("code = %q, want %q", code, tc.wantCode)
			}
			if errType != tc.wantType {
				t.Errorf("error.type = %q, want %q", errType, tc.wantType)
			}
		})
	}
}
