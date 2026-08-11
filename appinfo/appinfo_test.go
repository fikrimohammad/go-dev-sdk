package appinfo

import "testing"

func TestDefault_Defaults(t *testing.T) {
	withVersion(t, DefaultVersion, func() {
		t.Setenv("APP_NAME", "")
		t.Setenv("APP_VERSION", "")
		t.Setenv("APP_ENV", "")

		info := Default()
		if info.Name != DefaultName {
			t.Errorf("Name = %q, want %q", info.Name, DefaultName)
		}
		if info.Version != DefaultVersion {
			t.Errorf("Version = %q, want %q", info.Version, DefaultVersion)
		}
		if info.Environment != DefaultEnvironment {
			t.Errorf("Environment = %q, want %q", info.Environment, DefaultEnvironment)
		}
	})
}

func TestDefault_EnvOverrides(t *testing.T) {
	t.Setenv("APP_NAME", "report-service")
	t.Setenv("APP_VERSION", "1.4.2")
	t.Setenv("APP_ENV", "production")

	info := Default()
	if info.Name != "report-service" {
		t.Errorf("Name = %q, want %q", info.Name, "report-service")
	}
	if info.Version != "1.4.2" {
		t.Errorf("Version = %q, want %q", info.Version, "1.4.2")
	}
	if info.Environment != "production" {
		t.Errorf("Environment = %q, want %q", info.Environment, "production")
	}
}

func TestDefault_StampedVersionFallback(t *testing.T) {
	withVersion(t, "1.4.2", func() {
		t.Setenv("APP_VERSION", "")
		info := Default()
		if info.Version != "1.4.2" {
			t.Errorf("Version = %q, want %q", info.Version, "1.4.2")
		}
	})
}

func TestDefault_PartialOverrides(t *testing.T) {
	withVersion(t, DefaultVersion, func() {
		t.Setenv("APP_NAME", "report-service")
		t.Setenv("APP_VERSION", "")
		t.Setenv("APP_ENV", "staging")

		info := Default()
		if info.Name != "report-service" {
			t.Errorf("Name = %q, want %q", info.Name, "report-service")
		}
		if info.Version != DefaultVersion {
			t.Errorf("Version = %q, want %q", info.Version, DefaultVersion)
		}
		if info.Environment != "staging" {
			t.Errorf("Environment = %q, want %q", info.Environment, "staging")
		}
	})
}

// withVersion sets the stamped version var and restores it after fn.
func withVersion(t *testing.T, v string, fn func()) {
	t.Helper()
	prev := version
	version = v
	t.Cleanup(func() { version = prev })
	fn()
}
