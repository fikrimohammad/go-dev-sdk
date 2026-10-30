// Package identity provides the standardized service identity attached to logs,
// metrics, and traces. It is intentionally dependency-free so it can be reused
// as part of a standalone development SDK.
//
// Identity is resolved from environment variables, with the OpenTelemetry
// environment conventions honoured as fallbacks. An identity that names no
// service is invalid; callers reject it with [Info.Validate].
package identity

import (
	"errors"
	"net/url"
	"os"
	"runtime/debug"
	"strings"
)

// Semantic convention resource attribute keys. The package owns the spelling so
// every consumer emits the same attributes.
const (
	nameKey        = "service.name"
	versionKey     = "service.version"
	environmentKey = "deployment.environment.name"

	// legacyEnvironmentKey is the deprecated spelling still read from
	// OTEL_RESOURCE_ATTRIBUTES during migration.
	legacyEnvironmentKey = "deployment.environment"
)

// Placeholder values used when nothing is configured.
const (
	placeholderName = "app"
	fallbackVersion = "dev"
)

// ErrName reports an identity that has no service name.
var ErrName = errors.New("service name is required")

// version holds the build-time stamped version. Build tooling sets it with
// -ldflags; when left empty, [Resolve] falls back to the running binary's build
// information:
//
//	-ldflags "-X github.com/fikrimohammad/go-dev-sdk/identity.version=1.4.2"
var version string

// buildVersion reports the version of the main module recorded in the binary's
// build information. It is a variable so tests can stub the toolchain
// metadata.
var buildVersion = func() string {
	info, ok := debug.ReadBuildInfo()
	return versionFromBuildInfo(info, ok)
}

// versionFromBuildInfo extracts the main module version from build
// information, returning "" when it is unavailable or unversioned.
func versionFromBuildInfo(info *debug.BuildInfo, ok bool) string {
	if !ok || info == nil || info.Main.Version == "" || info.Main.Version == "(devel)" {
		return ""
	}
	return info.Main.Version
}

// Info is the standardized service identity.
type Info struct {
	// Name is the service name, e.g. "checkout".
	Name string
	// Version is the service version, e.g. "1.4.2".
	Version string
	// Environment is the deployment environment, e.g. "production".
	Environment string
}

// Default resolves the service identity from the process environment. It is
// equivalent to Resolve(nil).
func Default() Info { return Resolve(nil) }

// Resolve resolves the service identity, reading variables through lookup. A
// nil lookup reads the process environment with [os.LookupEnv]. Values are
// trimmed; a value that is empty after trimming counts as unset.
//
// Resolution order, highest precedence first:
//
//	Name:        APP_NAME, OTEL_SERVICE_NAME, service.name in
//	             OTEL_RESOURCE_ATTRIBUTES, "app".
//	Version:     APP_VERSION, the -ldflags stamp, service.version in
//	             OTEL_RESOURCE_ATTRIBUTES, the binary's build information,
//	             "dev".
//	Environment: APP_ENV, deployment.environment.name in
//	             OTEL_RESOURCE_ATTRIBUTES (or the deprecated
//	             deployment.environment), unset.
//
// Environment has no fallback: an unresolved environment is left empty so the
// attribute is omitted instead of labelling production data with a placeholder.
func Resolve(lookup func(string) (string, bool)) Info {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	get := func(key string) string {
		value, _ := lookup(key)
		return strings.TrimSpace(value)
	}
	attributes := resourceAttributes(get("OTEL_RESOURCE_ATTRIBUTES"))
	return Info{
		Name:        first(get("APP_NAME"), get("OTEL_SERVICE_NAME"), attributes[nameKey], placeholderName),
		Version:     first(get("APP_VERSION"), version, attributes[versionKey], buildVersion(), fallbackVersion),
		Environment: first(get("APP_ENV"), attributes[environmentKey], attributes[legacyEnvironmentKey]),
	}
}

// Validate reports whether the identity names a service. A non-empty trimmed
// service name is required; version and environment are optional.
func (i Info) Validate() error {
	if strings.TrimSpace(i.Name) == "" {
		return ErrName
	}
	return nil
}

// Attributes returns the identity as OpenTelemetry semantic convention
// resource attributes. Values are trimmed and an empty version or environment
// is omitted, so an unresolved optional field is never emitted. The service
// name is always present; callers should check it with [Info.Validate] before
// emitting.
func (i Info) Attributes() map[string]string {
	attributes := map[string]string{nameKey: strings.TrimSpace(i.Name)}
	if version := strings.TrimSpace(i.Version); version != "" {
		attributes[versionKey] = version
	}
	if environment := strings.TrimSpace(i.Environment); environment != "" {
		attributes[environmentKey] = environment
	}
	return attributes
}

// Placeholder returns the identity used when no environment variable and no
// build information apply. It is intended for detecting an unresolved service
// name, for example info.Name == identity.Placeholder().Name.
func Placeholder() Info {
	return Info{Name: placeholderName, Version: fallbackVersion}
}

// first returns the first non-empty candidate so callers can express their
// precedence order inline.
func first(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// resourceAttributes parses an OTEL_RESOURCE_ATTRIBUTES value. Malformed pairs
// are skipped, values are URL path unescaped like the OpenTelemetry SDK, and
// later duplicates override earlier ones.
func resourceAttributes(value string) map[string]string {
	if value == "" {
		return nil
	}
	attributes := make(map[string]string)
	for pair := range strings.SplitSeq(value, ",") {
		key, raw, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		raw = strings.TrimSpace(raw)
		if decoded, err := url.PathUnescape(raw); err == nil {
			raw = decoded
		}
		attributes[key] = raw
	}
	return attributes
}
