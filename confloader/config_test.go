package confloader

import (
	"errors"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	base := mockConfig()
	cases := []struct {
		name   string
		mutate func(*Config)
		want   error
	}{
		{"ok", func(*Config) {}, nil},
		{"missing endpoint", func(c *Config) { c.Endpoint = "" }, ErrInvalidEndpoint},
		{"missing namespace", func(c *Config) { c.Namespace = "" }, ErrInvalidNamespace},
		{"missing secret", func(c *Config) { c.AuthClientSecret = "" }, ErrInvalidAuthClientSecret},
		{"infisical needs env", func(c *Config) {
			c.Provider = ProviderInfisical
			c.Environment = ""
		}, ErrInvalidEnvironment},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mutate(&c)
			err := c.validate()
			if tc.want == nil {
				if err != nil {
					t.Fatalf("expected nil, got %v", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
		})
	}
}

func TestConfigApplyDefaults(t *testing.T) {
	var c Config
	c.applyDefaults()
	if c.Watcher.PollingInterval <= 0 {
		t.Fatalf("expected default polling interval > 0")
	}
	if c.Watcher.PollingMaxRetries <= 0 {
		t.Fatalf("expected default max retries > 0")
	}
	if c.Watcher.PollingRetryDelay <= 0 {
		t.Fatalf("expected default retry delay > 0")
	}
	if c.Watcher.PollingRetryBackoff <= 0 {
		t.Fatalf("expected default retry backoff > 0")
	}

	// Explicit values must be preserved.
	c.Watcher.PollingInterval = 7 * c.Watcher.PollingInterval
	c.applyDefaults()
	if c.Watcher.PollingInterval <= 0 {
		t.Fatalf("applyDefaults must not overwrite explicit values")
	}
}
