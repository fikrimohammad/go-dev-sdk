package db

import (
	"strings"
	"testing"
	"time"
)

func TestSetDefaults(t *testing.T) {
	got := (Config{}).SetDefaults()
	want := Config{
		Driver:          "mysql",
		MaxOpenConns:    25,
		MaxIdleConns:    25,
		ConnMaxLifetime: 3 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
	}
	if got != want {
		t.Fatalf("SetDefaults() = %+v, want %+v", got, want)
	}
}

func TestSetDefaults_KeepsNegativePoolValues(t *testing.T) {
	got := Config{MaxOpenConns: -1, ConnMaxLifetime: -time.Second}.SetDefaults()
	if got.MaxOpenConns != -1 || got.ConnMaxLifetime != -time.Second {
		t.Fatalf("SetDefaults() masked negative values: %+v", got)
	}
	if err := got.Validate(); err == nil {
		t.Fatal("Validate() = nil, want error for negative pool values")
	}
}

func TestSetDefaults_KeepsExplicit(t *testing.T) {
	in := Config{
		Driver:          "dbstub",
		MaxOpenConns:    5,
		ConnMaxLifetime: time.Minute,
	}
	got := in.SetDefaults()
	if got.Driver != "dbstub" {
		t.Fatalf("Driver = %q, want dbstub", got.Driver)
	}
	if got.MaxOpenConns != 5 {
		t.Fatalf("MaxOpenConns = %d, want 5", got.MaxOpenConns)
	}
	if got.ConnMaxLifetime != time.Minute {
		t.Fatalf("ConnMaxLifetime = %v, want 1m", got.ConnMaxLifetime)
	}
	if got.MaxIdleConns != 5 {
		t.Fatalf("MaxIdleConns = %d, want 5 (carried from MaxOpenConns)", got.MaxIdleConns)
	}
	if got.ConnMaxIdleTime != 5*time.Minute {
		t.Fatalf("ConnMaxIdleTime = %v, want 5m (defaulted)", got.ConnMaxIdleTime)
	}
}

func TestSetDefaults_DriverPorts(t *testing.T) {
	cases := []struct {
		driver string
		want   int
	}{
		{"mysql", 3306},
		{"postgres", 5432},
		{"postgresql", 5432},
		{"pgx", 5432},
	}
	for _, c := range cases {
		got := Config{Driver: c.driver, Host: "localhost"}.SetDefaults()
		if got.Port != c.want {
			t.Errorf("SetDefaults(%q).Port = %d, want %d", c.driver, got.Port, c.want)
		}
	}
}

func TestSetDefaults_NormalizesDriver(t *testing.T) {
	got := Config{Driver: " PostgreSQL ", Host: "localhost"}.SetDefaults()
	if got.Driver != "postgresql" {
		t.Fatalf("Driver = %q, want postgresql", got.Driver)
	}
}

func TestValidate_Valid(t *testing.T) {
	cfg := Config{
		Host: "localhost", Port: 3306, Database: "report_db", Username: "user",
	}.SetDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestValidate_DSN(t *testing.T) {
	cases := []Config{
		{Driver: "mysql", DSN: "user:pass@tcp(localhost:3306)/report_db"},
		{Driver: "pgx", DSN: "postgres://user:pass@localhost:5432/report_db?sslmode=disable"},
		{Driver: "sqlserver", DSN: "sqlserver://user:pass@localhost:1433?database=report_db"},
	}
	for _, cfg := range cases {
		if err := cfg.SetDefaults().Validate(); err != nil {
			t.Errorf("Validate(%q with DSN) = %v, want nil", cfg.Driver, err)
		}
	}

	err := (Config{Driver: "sqlserver"}).SetDefaults().Validate()
	if err == nil || !strings.Contains(err.Error(), "DSN") {
		t.Fatalf("Validate(custom driver without DSN) = %v, want DSN error", err)
	}
}

func TestValidate_SSLMode(t *testing.T) {
	postgres := Config{
		Driver: "postgres", Host: "localhost", Database: "report_db", Username: "user", SSLMode: "require",
	}.SetDefaults()
	if err := postgres.Validate(); err != nil {
		t.Fatalf("Validate(postgres + SSLMode) = %v, want nil", err)
	}

	mysql := Config{
		Driver: "mysql", Host: "localhost", Database: "report_db", Username: "user", SSLMode: "require",
	}.SetDefaults()
	if err := mysql.Validate(); err != nil {
		t.Fatalf("Validate(mysql + SSLMode) = %v, want nil", err)
	}

	bad := Config{
		Driver: "postgres", Host: "localhost", Database: "report_db", Username: "user", SSLMode: "sometimes",
	}.SetDefaults()
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "not a valid SSL mode") {
		t.Fatalf("Validate(postgres + invalid SSLMode) = %v, want sslmode error", err)
	}
}

func TestValidate_MissingConnectionSettings(t *testing.T) {
	err := (Config{}).SetDefaults().Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want error")
	}
	for _, want := range []string{"host", "port", "database", "username"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing mention of %q", err, want)
		}
	}
}

func TestValidate_InvalidPoolValues(t *testing.T) {
	cfg := Config{
		Host: "localhost", Port: 3306, Database: "report_db", Username: "user",
		MaxOpenConns:    10,
		MaxIdleConns:    20,
		ConnMaxLifetime: -time.Second,
		ConnMaxIdleTime: -time.Second,
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want error")
	}
	for _, want := range []string{"MaxIdleConns", "MaxOpenConns", "ConnMaxLifetime", "ConnMaxIdleTime"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing mention of %q", err, want)
		}
	}
}

func TestValidate_PortRange(t *testing.T) {
	for _, port := range []int{-1, 65536} {
		cfg := Config{Host: "localhost", Port: port, Database: "report_db", Username: "user"}.SetDefaults()
		if err := cfg.Validate(); err == nil {
			t.Errorf("Validate() for port %d = nil, want error", port)
		}
	}
	// Port 0 is defaulted from the driver, so it validates for built-in drivers.
	cfg := Config{Host: "localhost", Port: 0, Database: "report_db", Username: "user"}.SetDefaults()
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() for defaulted port = %v, want nil", err)
	}
}
