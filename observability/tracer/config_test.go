package tracer

import (
	"context"
	"maps"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestConfig_Defaults(t *testing.T) {
	t.Run("an empty Config gets sane defaults", func(t *testing.T) {
		cfg := Config{}
		cfg.setDefaults()

		if cfg.Endpoint != "localhost:4317" {
			t.Errorf("setDefaults() Endpoint = %q, want %q", cfg.Endpoint, "localhost:4317")
		}
		if cfg.ExportTimeout != 5*time.Second {
			t.Errorf("setDefaults() ExportTimeout = %v, want %v", cfg.ExportTimeout, 5*time.Second)
		}
		if cfg.BatchTimeout != 5*time.Second {
			t.Errorf("setDefaults() BatchTimeout = %v, want %v", cfg.BatchTimeout, 5*time.Second)
		}
		if cfg.MaxQueueSize != 2048 {
			t.Errorf("setDefaults() MaxQueueSize = %d, want 2048", cfg.MaxQueueSize)
		}
		if cfg.MaxExportBatchSize != 512 {
			t.Errorf("setDefaults() MaxExportBatchSize = %d, want 512", cfg.MaxExportBatchSize)
		}
		if cfg.Sampler != nil {
			t.Errorf("setDefaults() Sampler = %v, want nil", cfg.Sampler)
		}
	})

	t.Run("explicit values are preserved", func(t *testing.T) {
		sampler := sdktrace.NeverSample()
		cfg := Config{
			Endpoint:           "collector:4318",
			ExportTimeout:      3 * time.Second,
			Headers:            map[string]string{"auth": "x"},
			GlobalKV:           map[string]any{"cloud.region": "eu"},
			Sampler:            sampler,
			BatchTimeout:       2 * time.Second,
			MaxQueueSize:       100,
			MaxExportBatchSize: 10,
		}
		cfg.setDefaults()
		if cfg.Endpoint != "collector:4318" {
			t.Errorf("setDefaults() Endpoint = %q, want %q", cfg.Endpoint, "collector:4318")
		}
		if cfg.ExportTimeout != 3*time.Second {
			t.Errorf("setDefaults() ExportTimeout = %v, want %v", cfg.ExportTimeout, 3*time.Second)
		}
		if want := map[string]string{"auth": "x"}; !maps.Equal(cfg.Headers, want) {
			t.Errorf("setDefaults() Headers = %v, want %v", cfg.Headers, want)
		}
		if want := map[string]any{"cloud.region": "eu"}; !maps.Equal(cfg.GlobalKV, want) {
			t.Errorf("setDefaults() GlobalKV = %v, want %v", cfg.GlobalKV, want)
		}
		if cfg.Sampler != sampler {
			t.Errorf("setDefaults() Sampler = %v, want %v", cfg.Sampler, sampler)
		}
		if cfg.BatchTimeout != 2*time.Second {
			t.Errorf("setDefaults() BatchTimeout = %v, want %v", cfg.BatchTimeout, 2*time.Second)
		}
		if cfg.MaxQueueSize != 100 {
			t.Errorf("setDefaults() MaxQueueSize = %d, want 100", cfg.MaxQueueSize)
		}
		if cfg.MaxExportBatchSize != 10 {
			t.Errorf("setDefaults() MaxExportBatchSize = %d, want 10", cfg.MaxExportBatchSize)
		}
	})
}

func TestConfig_Validate(t *testing.T) {
	t.Run("a defaulted Config validates", func(t *testing.T) {
		cfg := Config{}
		cfg.setDefaults()
		if err := cfg.validate(); err != nil {
			t.Errorf("validate() = %v, want nil", err)
		}
	})

	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "non-positive export timeout is rejected", cfg: Config{ExportTimeout: -time.Second}},
		{name: "non-positive batch settings are rejected", cfg: Config{BatchTimeout: -time.Second, MaxQueueSize: -1, MaxExportBatchSize: -1}},
		{name: "max export batch size over the queue size is rejected", cfg: Config{MaxQueueSize: 10, MaxExportBatchSize: 11}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			cfg.setDefaults()
			if err := cfg.validate(); err == nil {
				t.Errorf("validate(%+v) = nil, want non-nil", cfg)
			}
		})
	}
}

func TestConfig(t *testing.T) {
	t.Run("the zero-value Config uses TLS", func(t *testing.T) {
		if got := (Config{}).Insecure; got {
			t.Errorf("Config{}.Insecure = %v, want false", got)
		}
	})

	tests := []struct {
		name     string
		insecure bool
	}{
		{name: "TLS exporter is created", insecure: false},
		{name: "plaintext exporter is created", insecure: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(context.Background(), testInfo(), Config{Insecure: tt.insecure})
			if err != nil {
				t.Fatalf("New(Insecure=%v) error = %v, want nil", tt.insecure, err)
			}
			if err := c.Stop(context.Background()); err != nil {
				t.Errorf("Stop() = %v, want nil", err)
			}
		})
	}
}
