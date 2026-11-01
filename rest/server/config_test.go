package server

import (
	"strings"
	"testing"
	"time"
)

func TestSetDefaults(t *testing.T) {
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
			got := tt.in
			got.setDefaults()
			if got != tt.want {
				t.Errorf("setDefaults(%+v) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestValidate(t *testing.T) {
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
			err := tt.cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("validate(%+v) error = %v, wantErr %v", tt.cfg, err, tt.wantErr)
			}
			if err != nil && !strings.HasPrefix(err.Error(), "rest/server: ") {
				t.Errorf("validate(%+v) error = %q, want prefix %q", tt.cfg, err, "rest/server: ")
			}
		})
	}
}
