package server

import (
	"testing"
)

func TestConfig(t *testing.T) {
	cfg := Config{
		Prefix: "/admin",
	}
	if cfg.Prefix != "/admin" {
		t.Fatalf("expected prefix /admin, got %s", cfg.Prefix)
	}
}
