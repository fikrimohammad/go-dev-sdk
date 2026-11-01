package client

import (
	"strings"
	"testing"
	"time"
)

func TestConfig_SetDefaults(t *testing.T) {
	defaults := Config{
		Timeout:        30 * time.Second,
		AttemptTimeout: 10 * time.Second,
		MaxAttempts:    3,
		BackoffBase:    100 * time.Millisecond,
		BackoffMax:     2 * time.Second,
		MaxRetryAfter:  10 * time.Second,
		DialTimeout:    5 * time.Second,
	}
	custom := Config{
		Timeout:        time.Minute,
		AttemptTimeout: 2 * time.Second,
		MaxAttempts:    5,
		BackoffBase:    time.Second,
		BackoffMax:     4 * time.Second,
		MaxRetryAfter:  time.Second,
		DialTimeout:    time.Second,
		Proxy:          "http://egress:3128",
	}
	tests := []struct {
		name string
		in   Config
		want Config
	}{
		{
			name: "zero value gets every default",
			in:   Config{},
			want: defaults,
		},
		{
			name: "only zero fields get defaults",
			in:   Config{Timeout: time.Minute, MaxAttempts: 2},
			want: Config{Timeout: time.Minute, AttemptTimeout: 10 * time.Second, MaxAttempts: 2, BackoffBase: 100 * time.Millisecond, BackoffMax: 2 * time.Second, MaxRetryAfter: 10 * time.Second, DialTimeout: 5 * time.Second},
		},
		{
			name: "custom values are kept",
			in:   custom,
			want: custom,
		},
		{
			name: "MaxAttempts of 1 is kept, so retry is off",
			in:   Config{MaxAttempts: 1},
			want: Config{Timeout: 30 * time.Second, AttemptTimeout: 10 * time.Second, MaxAttempts: 1, BackoffBase: 100 * time.Millisecond, BackoffMax: 2 * time.Second, MaxRetryAfter: 10 * time.Second, DialTimeout: 5 * time.Second},
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
			name:    "zero value is valid after defaults",
			cfg:     Config{},
			wantErr: false,
		},
		{
			name:    "http proxy is valid",
			cfg:     Config{Proxy: "http://egress:3128"},
			wantErr: false,
		},
		{
			name:    "https proxy is rejected",
			cfg:     Config{Proxy: "https://egress:443"},
			wantErr: true,
		},
		{
			name:    "MaxAttempts of 1 is valid",
			cfg:     Config{MaxAttempts: 1},
			wantErr: false,
		},
		{
			name:    "negative timeout is rejected",
			cfg:     Config{Timeout: -time.Second},
			wantErr: true,
		},
		{
			name:    "negative attempt timeout is rejected",
			cfg:     Config{AttemptTimeout: -time.Second},
			wantErr: true,
		},
		{
			name:    "negative backoff base is rejected",
			cfg:     Config{BackoffBase: -time.Second},
			wantErr: true,
		},
		{
			name:    "negative backoff max is rejected",
			cfg:     Config{BackoffMax: -time.Second},
			wantErr: true,
		},
		{
			name:    "negative max retry after is rejected",
			cfg:     Config{MaxRetryAfter: -time.Second},
			wantErr: true,
		},
		{
			name:    "negative dial timeout is rejected",
			cfg:     Config{DialTimeout: -time.Second},
			wantErr: true,
		},
		{
			name:    "negative MaxAttempts is rejected",
			cfg:     Config{MaxAttempts: -1},
			wantErr: true,
		},
		{
			name:    "BackoffBase above BackoffMax is rejected",
			cfg:     Config{BackoffBase: 3 * time.Second, BackoffMax: 2 * time.Second},
			wantErr: true,
		},
		{
			name:    "BackoffBase above the default BackoffMax is rejected",
			cfg:     Config{BackoffBase: 5 * time.Second},
			wantErr: true,
		},
		{
			name:    "proxy that does not parse is rejected",
			cfg:     Config{Proxy: "::bad"},
			wantErr: true,
		},
		{
			name:    "proxy with a non-http scheme is rejected",
			cfg:     Config{Proxy: "ftp://x"},
			wantErr: true,
		},
		{
			name:    "proxy with credentials and an http scheme is valid",
			cfg:     Config{Proxy: "http://user:p%40ss@egress:3128"},
			wantErr: false,
		},
		{
			name:    "proxy with no host is rejected",
			cfg:     Config{Proxy: "http://"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg.SetDefaults()
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate(%+v) error = %v, wantErr %v", tt.cfg, err, tt.wantErr)
			}
			if err != nil && !strings.HasPrefix(err.Error(), "rest/client: ") {
				t.Errorf("Validate(%+v) error = %q, want prefix %q", tt.cfg, err.Error(), "rest/client: ")
			}
		})
	}
}

func TestConfig_Validate_ReportsEveryProblem(t *testing.T) {
	cfg := Config{Timeout: -time.Second, MaxAttempts: -1}.SetDefaults()
	err := cfg.Validate()
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("Validate(%+v) error = %v, want an errors.Join error with two problems", cfg, err)
	}
	if n := len(joined.Unwrap()); n != 2 {
		t.Errorf("Validate(%+v) reported %d problems, want 2: %v", cfg, n, err)
	}
}

func TestConfig_Validate_ProxySchemeMessage(t *testing.T) {
	cfg := Config{Proxy: "https://egress:443"}.SetDefaults()
	err := cfg.Validate()
	if err == nil {
		t.Fatalf("Validate(Proxy: %q) error = nil, want an error", cfg.Proxy)
	}
	if want := "rest/client: proxy scheme must be http"; err.Error() != want {
		t.Errorf("Validate(Proxy: %q) error = %q, want %q", cfg.Proxy, err.Error(), want)
	}
}
