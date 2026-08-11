// Package appinfo provides the standardized service identity attached to
// logs, metrics, and traces. It is intentionally dependency-free so it can be
// reused as part of a standalone development SDK.
package appinfo

import "os"

// SDK-neutral defaults applied when the corresponding environment variable or
// build-time stamped value is unset or empty. Deployments override them via
// environment variables.
const (
	DefaultName        = "app"
	DefaultVersion     = "dev"
	DefaultEnvironment = "development"
)

// version is the fallback service version used when APP_VERSION is unset. Build
// tooling stamps a concrete release at link time, which surfaces as the
// APP_VERSION default:
//
//	-ldflags "-X github.com/fikrimohammad/go-dev-sdk/appinfo.version=1.4.2"
var version = DefaultVersion

// Info is the standardized service identity.
type Info struct {
	// Name is the service name, e.g. "efficient-report-exporter".
	Name string
	// Version is the service version, e.g. "1.4.2".
	Version string
	// Environment is the deployment environment, e.g. "production".
	Environment string
}

// Default resolves the service identity from environment variables:
//
//	APP_NAME       → Name        (default appinfo.DefaultName)
//	APP_VERSION    → Version     (default stamped via -ldflags, else appinfo.DefaultVersion)
//	APP_ENV        → Environment (default appinfo.DefaultEnvironment)
//
// Environment variables are read at call time; set them before the process
// starts to keep the identity stable.
func Default() Info {
	return Info{
		Name:        envOr("APP_NAME", DefaultName),
		Version:     envOr("APP_VERSION", version),
		Environment: envOr("APP_ENV", DefaultEnvironment),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
