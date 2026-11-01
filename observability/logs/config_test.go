package logs

import (
	"errors"
	"maps"
	"testing"
)

func TestConfig_Defaults(t *testing.T) {
	t.Run("an empty Config gets sane defaults", func(t *testing.T) {
		cfg := Config{}
		cfg.setDefaults()

		if cfg.Format != FormatText {
			t.Errorf("setDefaults() Format = %q, want %q", cfg.Format, FormatText)
		}
		if cfg.Level != "debug" {
			t.Errorf("setDefaults() Level = %q, want %q", cfg.Level, "debug")
		}
	})

	t.Run("explicit values are preserved", func(t *testing.T) {
		cfg := Config{Format: FormatJSON, Level: "error", GlobalKV: map[string]any{"env": "prod"}}
		cfg.setDefaults()

		if cfg.Format != FormatJSON {
			t.Errorf("setDefaults() Format = %q, want %q", cfg.Format, FormatJSON)
		}
		if cfg.Level != "error" {
			t.Errorf("setDefaults() Level = %q, want %q", cfg.Level, "error")
		}
		if want := map[string]any{"env": "prod"}; !maps.Equal(cfg.GlobalKV, want) {
			t.Errorf("setDefaults() GlobalKV = %v, want %v", cfg.GlobalKV, want)
		}
	})
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Config
		wantErrs []error
	}{
		{"a defaulted Config validates", Config{}, nil},
		{"invalid format is rejected", Config{Format: "xml"}, []error{ErrInvalidLogFormat}},
		{"invalid level is rejected", Config{Level: "verbose"}, []error{ErrInvalidLogLevel}},
		{"multiple invalid fields are aggregated", Config{Format: "xml", Level: "verbose"},
			[]error{ErrInvalidLogFormat, ErrInvalidLogLevel}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			cfg.setDefaults()
			err := cfg.validate()

			if len(tt.wantErrs) == 0 && err != nil {
				t.Errorf("validate(%+v) error = %v, want nil", tt.cfg, err)
			}
			for _, want := range tt.wantErrs {
				if !errors.Is(err, want) {
					t.Errorf("validate(%+v) error = %v, want %v", tt.cfg, err, want)
				}
			}
		})
	}
}
