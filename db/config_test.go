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

func TestValidate_Valid(t *testing.T) {
	cfg := Config{
		Host: "localhost", Port: 3306, Database: "report_db", Username: "user",
	}.SetDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
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
	for _, port := range []int{0, -1, 65536} {
		cfg := Config{Host: "localhost", Port: port, Database: "report_db", Username: "user"}.SetDefaults()
		if err := cfg.Validate(); err == nil {
			t.Errorf("Validate() for port %d = nil, want error", port)
		}
	}
}
