package producer

import (
	"errors"
	"testing"
	"time"
)

func TestConfig_SetDefaults(t *testing.T) {
	t.Parallel()

	c := Config{}.SetDefaults()
	if c.Timeout != DefaultTimeout {
		t.Fatalf("Timeout = %v, want %v", c.Timeout, DefaultTimeout)
	}
	if c.MaxRetryAttempts != DefaultMaxRetryAttempts {
		t.Fatalf("MaxRetryAttempts = %d, want %d", c.MaxRetryAttempts, DefaultMaxRetryAttempts)
	}
}

func TestConfig_SetDefaults_NegativeGetsDefault(t *testing.T) {
	t.Parallel()

	c := Config{Timeout: -time.Second, MaxRetryAttempts: -1}.SetDefaults()
	if c.Timeout != DefaultTimeout {
		t.Fatalf("Timeout = %v, want %v", c.Timeout, DefaultTimeout)
	}
	if c.MaxRetryAttempts != DefaultMaxRetryAttempts {
		t.Fatalf("MaxRetryAttempts = %d, want %d", c.MaxRetryAttempts, DefaultMaxRetryAttempts)
	}
}

func TestConfig_SetDefaults_Explicit(t *testing.T) {
	t.Parallel()

	c := Config{Timeout: 5 * time.Second, MaxRetryAttempts: 5}.SetDefaults()
	if c.Timeout != 5*time.Second {
		t.Fatalf("Timeout = %v, want 5s", c.Timeout)
	}
	if c.MaxRetryAttempts != 5 {
		t.Fatalf("MaxRetryAttempts = %d, want 5", c.MaxRetryAttempts)
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	t.Parallel()

	c := Config{Endpoints: []string{"localhost:9876"}, Topic: "test-topic"}.SetDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestConfig_Validate_Errors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cfg  Config
		want error
	}{
		{"empty endpoints", Config{Topic: "t"}, ErrEndpointsRequired},
		{"nil endpoints", Config{Endpoints: nil, Topic: "t"}, ErrEndpointsRequired},
		{"empty topic", Config{Endpoints: []string{"localhost:9876"}}, ErrTopicRequired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.cfg.SetDefaults().Validate()
			if err == nil {
				t.Fatal("Validate: expected error")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate() = %v, want it to wrap %v", err, tc.want)
			}
		})
	}
}

func TestConfig_Validate_Aggregates(t *testing.T) {
	t.Parallel()

	err := Config{}.SetDefaults().Validate()
	if err == nil {
		t.Fatal("Validate: expected error")
	}
	if !errors.Is(err, ErrEndpointsRequired) {
		t.Fatalf("Validate() = %v, want endpoints error", err)
	}
	if !errors.Is(err, ErrTopicRequired) {
		t.Fatalf("Validate() = %v, want topic error", err)
	}
}
