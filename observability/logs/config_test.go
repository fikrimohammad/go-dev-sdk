package logs

import (
	"errors"
	"maps"
	"testing"
)

func TestConfig_SetDefaults(t *testing.T) {
	t.Run("an empty Config gets sane defaults", func(t *testing.T) {
		cfg := Config{}.SetDefaults()

		if cfg.Format != FormatText {
			t.Errorf("SetDefaults() Format = %q, want %q", cfg.Format, FormatText)
		}
		if cfg.Level != "debug" {
			t.Errorf("SetDefaults() Level = %q, want %q", cfg.Level, "debug")
		}
	})

	t.Run("explicit values are preserved", func(t *testing.T) {
		cfg := Config{Format: FormatJSON, Level: "error", GlobalKV: map[string]any{"env": "prod"}}.SetDefaults()

		if cfg.Format != FormatJSON {
			t.Errorf("SetDefaults() Format = %q, want %q", cfg.Format, FormatJSON)
		}
		if cfg.Level != "error" {
			t.Errorf("SetDefaults() Level = %q, want %q", cfg.Level, "error")
		}
		if want := map[string]any{"env": "prod"}; !maps.Equal(cfg.GlobalKV, want) {
			t.Errorf("SetDefaults() GlobalKV = %v, want %v", cfg.GlobalKV, want)
		}
	})
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Config
		wantErrs []error
	}{
		{name: "a defaulted Config validates", cfg: Config{}, wantErrs: nil},
		{name: "invalid format is rejected", cfg: Config{Format: "xml"}, wantErrs: []error{ErrInvalidLogFormat}},
		{name: "invalid level is rejected", cfg: Config{Level: "verbose"}, wantErrs: []error{ErrInvalidLogLevel}},
		{
			name:     "multiple invalid fields are aggregated",
			cfg:      Config{Format: "xml", Level: "verbose"},
			wantErrs: []error{ErrInvalidLogFormat, ErrInvalidLogLevel},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg.SetDefaults()
			err := cfg.Validate()

			if len(tt.wantErrs) == 0 && err != nil {
				t.Errorf("Validate(%+v) error = %v, want nil", tt.cfg, err)
			}
			for _, want := range tt.wantErrs {
				if !errors.Is(err, want) {
					t.Errorf("Validate(%+v) error = %v, want %v", tt.cfg, err, want)
				}
			}
		})
	}
}
