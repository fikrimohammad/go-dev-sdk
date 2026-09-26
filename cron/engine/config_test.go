package engine

import (
	"testing"
	"time"
)

func TestConfig_SetDefaults(t *testing.T) {
	cfg := Config{}.SetDefaults()
	if cfg.GlobalTimeout != DefaultGlobalTimeout {
		t.Fatalf("got GlobalTimeout %v, want %v", cfg.GlobalTimeout, DefaultGlobalTimeout)
	}
	if cfg.ShutdownTimeout != DefaultShutdownTimeout {
		t.Fatalf("got ShutdownTimeout %v, want %v", cfg.ShutdownTimeout, DefaultShutdownTimeout)
	}

	custom := Config{GlobalTimeout: 5 * time.Minute, ShutdownTimeout: 20 * time.Second}.SetDefaults()
	if custom.GlobalTimeout != 5*time.Minute {
		t.Fatalf("got GlobalTimeout %v, want 5m", custom.GlobalTimeout)
	}
	if custom.ShutdownTimeout != 20*time.Second {
		t.Fatalf("got ShutdownTimeout %v, want 20s", custom.ShutdownTimeout)
	}
}

func TestConfig_Validate(t *testing.T) {
	if err := (Config{GlobalTimeout: -time.Second}).Validate(); err == nil {
		t.Fatal("expected error on negative GlobalTimeout")
	}

	if err := (Config{ShutdownTimeout: -time.Second}).Validate(); err == nil {
		t.Fatal("expected error on negative ShutdownTimeout")
	}

	if err := (Config{GlobalTimeout: time.Minute, ShutdownTimeout: 5 * time.Second}).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
