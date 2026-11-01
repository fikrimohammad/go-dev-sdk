package server

import (
	"strings"
	"testing"
	"time"
)

func TestConfig_SetDefaults(t *testing.T) {
	tests := []struct {
		name string
		in   Config
		want Config
	}{
		{
			name: "zero value gets every default",
			in:   Config{},
			want: Config{Addr: ":3000", ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, ShutdownTimeout: 10 * time.Second},
		},
		{
			name: "only zero fields get defaults",
			in:   Config{Addr: "127.0.0.1:8080", ReadTimeout: time.Second},
			want: Config{Addr: "127.0.0.1:8080", ReadTimeout: time.Second, WriteTimeout: 30 * time.Second, ShutdownTimeout: 10 * time.Second},
		},
		{
			name: "custom values are kept",
			in:   Config{Addr: "127.0.0.1:8080", ReadTimeout: time.Second, WriteTimeout: 2 * time.Second, ShutdownTimeout: 3 * time.Second},
			want: Config{Addr: "127.0.0.1:8080", ReadTimeout: time.Second, WriteTimeout: 2 * time.Second, ShutdownTimeout: 3 * time.Second},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.in.SetDefaults()
			if got != tt.want {
				t.Errorf("SetDefaults(%+v) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name:    "defaulted config is valid",
			cfg:     Config{Addr: ":3000", ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, ShutdownTimeout: 10 * time.Second},
			wantErr: false,
		},
		{
			name:    "addr without port is rejected",
			cfg:     Config{Addr: "nohostport", ReadTimeout: time.Second, WriteTimeout: time.Second, ShutdownTimeout: time.Second},
			wantErr: true,
		},
		{
			name:    "negative read timeout is rejected",
			cfg:     Config{Addr: ":3000", ReadTimeout: -time.Second, WriteTimeout: time.Second, ShutdownTimeout: time.Second},
			wantErr: true,
		},
		{
			name:    "negative write timeout is rejected",
			cfg:     Config{Addr: ":3000", ReadTimeout: time.Second, WriteTimeout: -time.Second, ShutdownTimeout: time.Second},
			wantErr: true,
		},
		{
			name:    "negative shutdown timeout is rejected",
			cfg:     Config{Addr: ":3000", ReadTimeout: time.Second, WriteTimeout: time.Second, ShutdownTimeout: -time.Second},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate(%+v) error = %v, wantErr %v", tt.cfg, err, tt.wantErr)
			}
			if err != nil && !strings.HasPrefix(err.Error(), "rest/server: ") {
				t.Errorf("Validate(%+v) error = %q, want prefix %q", tt.cfg, err, "rest/server: ")
			}
		})
	}
}

func TestConfig_Validate_ReportsEveryProblem(t *testing.T) {
	cfg := Config{Addr: ":3000", ReadTimeout: -time.Second, ShutdownTimeout: -time.Second}
	err := cfg.Validate()
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("Validate(%+v) error = %v, want an errors.Join error with two problems", cfg, err)
	}
	if n := len(joined.Unwrap()); n != 2 {
		t.Errorf("Validate(%+v) reported %d problems, want 2: %v", cfg, n, err)
	}
}
