package identity

import (
	"errors"
	"maps"
	"runtime/debug"
	"testing"
)

func TestResolve(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		stamp string
		build string
		want  Info
	}{
		{
			name: "defaults",
			want: Info{Name: "app", Version: "dev"},
		},
		{
			name: "app variables",
			env: map[string]string{
				"APP_NAME":    "checkout",
				"APP_VERSION": "1.4.2",
				"APP_ENV":     "production",
			},
			want: Info{Name: "checkout", Version: "1.4.2", Environment: "production"},
		},
		{
			name: "trims values",
			env: map[string]string{
				"APP_NAME":    "  checkout  ",
				"APP_VERSION": " 1.4.2 ",
				"APP_ENV":     " production ",
			},
			want: Info{Name: "checkout", Version: "1.4.2", Environment: "production"},
		},
		{
			name: "whitespace counts as unset",
			env: map[string]string{
				"APP_NAME":    "   ",
				"APP_VERSION": "\t",
				"APP_ENV":     " ",
			},
			want: Info{Name: "app", Version: "dev"},
		},
		{
			name: "otel service name",
			env:  map[string]string{"OTEL_SERVICE_NAME": "checkout"},
			want: Info{Name: "checkout", Version: "dev"},
		},
		{
			name: "app name wins over otel service name",
			env:  map[string]string{"APP_NAME": "checkout", "OTEL_SERVICE_NAME": "other"},
			want: Info{Name: "checkout", Version: "dev"},
		},
		{
			name: "resource attributes",
			env: map[string]string{
				"OTEL_RESOURCE_ATTRIBUTES": "service.name=checkout,service.version=2.0.0,deployment.environment.name=staging",
			},
			want: Info{Name: "checkout", Version: "2.0.0", Environment: "staging"},
		},
		{
			name: "legacy environment key",
			env:  map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "deployment.environment=staging"},
			want: Info{Name: "app", Version: "dev", Environment: "staging"},
		},
		{
			name: "new environment key wins over legacy",
			env: map[string]string{
				"OTEL_RESOURCE_ATTRIBUTES": "deployment.environment=legacy,deployment.environment.name=staging",
			},
			want: Info{Name: "app", Version: "dev", Environment: "staging"},
		},
		{
			name: "app variables win over resource attributes",
			env: map[string]string{
				"APP_NAME":                 "checkout",
				"APP_VERSION":              "1.4.2",
				"APP_ENV":                  "production",
				"OTEL_RESOURCE_ATTRIBUTES": "service.name=other,service.version=9.9.9,deployment.environment.name=staging",
			},
			want: Info{Name: "checkout", Version: "1.4.2", Environment: "production"},
		},
		{
			name:  "stamped version",
			stamp: "1.4.2",
			want:  Info{Name: "app", Version: "1.4.2"},
		},
		{
			name:  "app version wins over stamp",
			env:   map[string]string{"APP_VERSION": "2.0.0"},
			stamp: "1.4.2",
			want:  Info{Name: "app", Version: "2.0.0"},
		},
		{
			name:  "stamp wins over resource attributes",
			stamp: "1.4.2",
			env:   map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "service.version=2.0.0"},
			want:  Info{Name: "app", Version: "1.4.2"},
		},
		{
			name:  "resource attributes win over build information",
			env:   map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "service.version=2.0.0"},
			build: "v9.9.9",
			want:  Info{Name: "app", Version: "2.0.0"},
		},
		{
			name:  "build information",
			build: "v1.2.3",
			want:  Info{Name: "app", Version: "v1.2.3"},
		},
		{
			name: "malformed resource attributes ignored",
			env:  map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "novalue,service.name=checkout"},
			want: Info{Name: "checkout", Version: "dev"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubVersion(t, tt.stamp)
			stubBuildVersion(t, tt.build)
			if got := Resolve(lookupFrom(tt.env)); got != tt.want {
				t.Fatalf("Resolve() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestDefault(t *testing.T) {
	stubVersion(t, "")
	stubBuildVersion(t, "")
	t.Setenv("APP_NAME", "checkout")
	t.Setenv("APP_VERSION", "2.0.0")
	t.Setenv("APP_ENV", "staging")
	t.Setenv("OTEL_SERVICE_NAME", "ignored")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=ignored,deployment.environment.name=ignored")

	want := Info{Name: "checkout", Version: "2.0.0", Environment: "staging"}
	if got := Default(); got != want {
		t.Fatalf("Default() = %+v, want %+v", got, want)
	}
}

func TestInfoValidate(t *testing.T) {
	tests := []struct {
		name string
		info Info
		want error
	}{
		{name: "valid", info: Info{Name: "checkout"}},
		{name: "valid with surrounding spaces", info: Info{Name: " checkout "}},
		{name: "empty", info: Info{}, want: ErrName},
		{name: "whitespace", info: Info{Name: "   "}, want: ErrName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.info.Validate(); !errors.Is(err, tt.want) {
				t.Fatalf("Validate() = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestInfoAttributes(t *testing.T) {
	tests := []struct {
		name string
		info Info
		want map[string]string
	}{
		{
			name: "all fields",
			info: Info{Name: "checkout", Version: "1.4.2", Environment: "production"},
			want: map[string]string{
				"service.name":                "checkout",
				"service.version":             "1.4.2",
				"deployment.environment.name": "production",
			},
		},
		{
			name: "name only",
			info: Info{Name: "checkout"},
			want: map[string]string{"service.name": "checkout"},
		},
		{
			name: "trims values",
			info: Info{Name: " checkout ", Version: " 1.4.2 ", Environment: " production "},
			want: map[string]string{
				"service.name":                "checkout",
				"service.version":             "1.4.2",
				"deployment.environment.name": "production",
			},
		},
		{
			name: "empty name",
			info: Info{},
			want: map[string]string{"service.name": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.info.Attributes(); !maps.Equal(got, tt.want) {
				t.Fatalf("Attributes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlaceholder(t *testing.T) {
	want := Info{Name: "app", Version: "dev"}
	if got := Placeholder(); got != want {
		t.Fatalf("Placeholder() = %+v, want %+v", got, want)
	}
}

func TestVersionFromBuildInfo(t *testing.T) {
	tests := []struct {
		name string
		info *debug.BuildInfo
		ok   bool
		want string
	}{
		{name: "unavailable", ok: false},
		{name: "nil build info", ok: true},
		{name: "unversioned", info: &debug.BuildInfo{}, ok: true},
		{name: "development build", info: &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}}, ok: true},
		{name: "release build", info: &debug.BuildInfo{Main: debug.Module{Version: "v1.2.3"}}, ok: true, want: "v1.2.3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := versionFromBuildInfo(tt.info, tt.ok); got != tt.want {
				t.Fatalf("versionFromBuildInfo() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildVersion(t *testing.T) {
	if got := buildVersion(); got == "(devel)" {
		t.Fatalf("buildVersion() = %q, want empty for an unversioned build", got)
	}
}

func TestResourceAttributes(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  map[string]string
	}{
		{name: "empty", value: ""},
		{name: "single", value: "service.name=checkout", want: map[string]string{"service.name": "checkout"}},
		{name: "multiple", value: "a=1,b=2", want: map[string]string{"a": "1", "b": "2"}},
		{name: "trims", value: " a = 1 , b = 2 ", want: map[string]string{"a": "1", "b": "2"}},
		{name: "keeps value spaces", value: "a=hello world", want: map[string]string{"a": "hello world"}},
		{name: "keeps equals in value", value: "a=b=c", want: map[string]string{"a": "b=c"}},
		{name: "unescapes", value: "deployment.environment.name=prod%20eu", want: map[string]string{"deployment.environment.name": "prod eu"}},
		{name: "keeps invalid escape", value: "a=100%", want: map[string]string{"a": "100%"}},
		{name: "skips malformed", value: "novalue,a=1", want: map[string]string{"a": "1"}},
		{name: "duplicate wins last", value: "a=1,a=2", want: map[string]string{"a": "2"}},
		{name: "empty value", value: "a=", want: map[string]string{"a": ""}},
		{name: "trailing comma", value: "a=1,", want: map[string]string{"a": "1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resourceAttributes(tt.value); !maps.Equal(got, tt.want) {
				t.Fatalf("resourceAttributes(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

// stubVersion replaces the build-time stamped version for the duration of the
// test.
func stubVersion(t *testing.T, value string) {
	t.Helper()
	previous := version
	version = value
	t.Cleanup(func() { version = previous })
}

// stubBuildVersion replaces the toolchain build information for the duration of
// the test.
func stubBuildVersion(t *testing.T, value string) {
	t.Helper()
	previous := buildVersion
	buildVersion = func() string { return value }
	t.Cleanup(func() { buildVersion = previous })
}

// lookupFrom returns a lookup function backed by values.
func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
