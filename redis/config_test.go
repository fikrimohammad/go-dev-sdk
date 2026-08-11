package redis

import (
	"strings"
	"testing"
	"time"
)

func TestConfig_SetDefaults(t *testing.T) {
	c := Config{Host: "localhost"}.SetDefaults()
	if c.Port != defaultPort {
		t.Fatalf("Port = %d, want %d", c.Port, defaultPort)
	}
	if c.ConnectTimeout != defaultConnectTimeout {
		t.Fatalf("ConnectTimeout = %v, want %v", c.ConnectTimeout, defaultConnectTimeout)
	}
}

func TestConfig_SetDefaults_Explicit(t *testing.T) {
	c := Config{
		Host:           "localhost",
		Port:           6380,
		ConnectTimeout: 3 * time.Second,
	}.SetDefaults()
	if c.Port != 6380 {
		t.Fatalf("Port = %d, want 6380", c.Port)
	}
	if c.ConnectTimeout != 3*time.Second {
		t.Fatalf("ConnectTimeout = %v, want 3s", c.ConnectTimeout)
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	c := Config{Host: "localhost", Port: 6379}.SetDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestConfig_Validate_Errors(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"missing host", Config{Port: 6379}, "host is required"},
		{"port too low", Config{Host: "h", Port: -1}, "out of range"},
		{"port too high", Config{Host: "h", Port: 65536}, "out of range"},
		{"negative db", Config{Host: "h", Port: 6379, DB: -1}, "DB must not be negative"},
		{"negative dial timeout", Config{Host: "h", Port: 6379, DialTimeout: -1}, "DialTimeout must not be negative"},
		{"negative pool size", Config{Host: "h", Port: 6379, PoolSize: -1}, "PoolSize must not be negative"},
		{"min idle over pool", Config{Host: "h", Port: 6379, PoolSize: 5, MinIdleConns: 6}, "must not exceed PoolSize"},
		{"max idle over pool", Config{Host: "h", Port: 6379, PoolSize: 5, MaxIdleConns: 6}, "must not exceed PoolSize"},
		{"negative conn max lifetime", Config{Host: "h", Port: 6379, ConnMaxLifetime: -1}, "ConnMaxLifetime must not be negative"},
		{"negative conn max idle time", Config{Host: "h", Port: 6379, ConnMaxIdleTime: -1}, "ConnMaxIdleTime must not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.SetDefaults().Validate()
			if err == nil {
				t.Fatal("Validate: expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %q, want it to contain %q", err.Error(), tc.want)
			}
		})
	}
}
