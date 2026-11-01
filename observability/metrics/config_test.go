package metrics

import (
	"maps"
	"strings"
	"testing"
	"time"
)

func TestConfig_Defaults(t *testing.T) {
	t.Run("an empty Config gets sane defaults", func(t *testing.T) {
		cfg := Config{}
		cfg.setDefaults()

		if cfg.Endpoint != "localhost:4317" {
			t.Errorf("setDefaults() Endpoint = %q, want %q", cfg.Endpoint, "localhost:4317")
		}
		if cfg.Timeout != 10*time.Second {
			t.Errorf("setDefaults() Timeout = %v, want %v", cfg.Timeout, 10*time.Second)
		}
		if cfg.ExportInterval != 10*time.Second {
			t.Errorf("setDefaults() ExportInterval = %v, want %v", cfg.ExportInterval, 10*time.Second)
		}
		if cfg.Scope != DefaultInstrumentationScope {
			t.Errorf("setDefaults() Scope = %q, want %q", cfg.Scope, DefaultInstrumentationScope)
		}
		if cfg.HistogramMaxBuckets != 160 {
			t.Errorf("setDefaults() HistogramMaxBuckets = %d, want 160", cfg.HistogramMaxBuckets)
		}
		if cfg.HistogramMaxScale != 20 {
			t.Errorf("setDefaults() HistogramMaxScale = %d, want 20", cfg.HistogramMaxScale)
		}
	})

	t.Run("explicit values are preserved", func(t *testing.T) {
		cfg := Config{
			Endpoint:       "collector:4318",
			Timeout:        2 * time.Second,
			ExportInterval: 1 * time.Minute,
			Scope:          "custom.scope",
			Headers:        map[string]string{"authorization": "Bearer x"},
		}
		cfg.setDefaults()
		if cfg.Endpoint != "collector:4318" {
			t.Errorf("setDefaults() Endpoint = %q, want %q", cfg.Endpoint, "collector:4318")
		}
		if cfg.Timeout != 2*time.Second {
			t.Errorf("setDefaults() Timeout = %v, want %v", cfg.Timeout, 2*time.Second)
		}
		if cfg.ExportInterval != 1*time.Minute {
			t.Errorf("setDefaults() ExportInterval = %v, want %v", cfg.ExportInterval, 1*time.Minute)
		}
		if cfg.Scope != "custom.scope" {
			t.Errorf("setDefaults() Scope = %q, want %q", cfg.Scope, "custom.scope")
		}
		if want := map[string]string{"authorization": "Bearer x"}; !maps.Equal(cfg.Headers, want) {
			t.Errorf("setDefaults() Headers = %v, want %v", cfg.Headers, want)
		}
	})

	t.Run("histogram tuning values are preserved", func(t *testing.T) {
		cfg := Config{HistogramMaxBuckets: 256, HistogramMaxScale: 15}
		cfg.setDefaults()
		if cfg.HistogramMaxBuckets != 256 {
			t.Errorf("setDefaults() HistogramMaxBuckets = %d, want 256", cfg.HistogramMaxBuckets)
		}
		if cfg.HistogramMaxScale != 15 {
			t.Errorf("setDefaults() HistogramMaxScale = %d, want 15", cfg.HistogramMaxScale)
		}
	})
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{name: "a defaulted Config validates", cfg: Config{}},
		{name: "non-positive timeout is rejected", cfg: Config{Timeout: -time.Second}, wantErr: true},
		{name: "non-positive export interval is rejected", cfg: Config{ExportInterval: -time.Second}, wantErr: true},
		{name: "negative histogram max buckets is rejected", cfg: Config{HistogramMaxBuckets: -1}, wantErr: true},
		{name: "histogram max buckets over the cap is rejected", cfg: Config{HistogramMaxBuckets: maxHistogramMaxBuckets + 1}, wantErr: true},
		{name: "histogram max buckets at the cap validates", cfg: Config{HistogramMaxBuckets: maxHistogramMaxBuckets}},
		{name: "histogram max scale above range is rejected", cfg: Config{HistogramMaxScale: 21}, wantErr: true},
		{name: "histogram max scale below range is rejected", cfg: Config{HistogramMaxScale: -11}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			cfg.setDefaults()
			err := cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("validate(%+v) = %v, want error: %t", cfg, err, tt.wantErr)
			}
		})
	}

	t.Run("multiple invalid fields are aggregated", func(t *testing.T) {
		cfg := Config{Timeout: -time.Second, ExportInterval: -time.Second, HistogramMaxBuckets: -1}
		cfg.setDefaults()
		err := cfg.validate()
		if err == nil {
			t.Fatal("validate() = nil, want error")
		}
		for _, want := range []string{"timeout", "export_interval", "histogram_max_buckets"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("validate() = %q, want it to contain %q", err, want)
			}
		}
	})
}

func TestConfig_IsInsecure(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{name: "isInsecure defaults to insecure", cfg: Config{}, want: true},
		{name: "isInsecure honors an explicit false", cfg: Config{Insecure: new(false)}, want: false},
		{name: "isInsecure honors an explicit true", cfg: Config{Insecure: new(true)}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.isInsecure(); got != tt.want {
				t.Errorf("isInsecure(%+v) = %t, want %t", tt.cfg, got, tt.want)
			}
		})
	}
}
