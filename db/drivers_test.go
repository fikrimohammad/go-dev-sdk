package db

import (
	"errors"
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

	if _, ok := driverFor("dbstub"); ok {
		t.Fatal("driverFor(dbstub) should be unsupported without a DSN")
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
	code, msg := errorInfo(&mysql.MySQLError{Number: 1062, Message: "duplicate entry"})
	if code != "1062" || msg != "duplicate entry" {
		t.Fatalf("errorInfo(mysql) = %q, %q", code, msg)
	}

	code, msg = errorInfo(&pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"})
	if code != "23505" || msg != "duplicate key value violates unique constraint" {
		t.Fatalf("errorInfo(postgres) = %q, %q", code, msg)
	}

	code, msg = errorInfo(errors.New("boom"))
	if code != "" || msg != "boom" {
		t.Fatalf("errorInfo(plain) = %q, %q", code, msg)
	}

	wrapped := errors.Join(errors.New("x"), &mysql.MySQLError{Number: 1452, Message: "fk"})
	if code, msg := errorInfo(wrapped); code != "1452" || msg != "fk" {
		t.Fatalf("errorInfo(wrapped mysql) = %q, %q", code, msg)
	}
	wrapped = errors.Join(errors.New("x"), &pgconn.PgError{Code: "40001", Message: "serialization failure"})
	if code, msg := errorInfo(wrapped); code != "40001" || msg != "serialization failure" {
		t.Fatalf("errorInfo(wrapped postgres) = %q, %q", code, msg)
	}
}
