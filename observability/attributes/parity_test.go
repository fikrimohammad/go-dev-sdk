// Package attributes_test contains parity tests that keep this module's
// service identity handling aligned with the OpenTelemetry SDK. The test in
// this file compares identity.Resolve with the SDK's OTEL_RESOURCE_ATTRIBUTES
// detector; it is used by no other test file.
package attributes_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/fikrimohammad/go-dev-sdk/identity"
)

func TestEnvironmentParity(t *testing.T) {
	tests := []struct {
		name string
		env  string
	}{
		{
			name: "all identity attributes",
			env:  "service.name=svc,service.version=1.2.3,deployment.environment.name=prod",
		},
		{
			name: "escaped value",
			env:  "service.name=svc,service.version=1.2.3,deployment.environment.name=prod%20eu",
		},
		{
			name: "trims values",
			env:  " service.name = svc , service.version = 1.2.3 ",
		},
		{
			name: "duplicate wins last",
			env:  "service.name=first,service.name=second,service.version=1.2.3",
		},
		{
			name: "malformed pair ignored",
			env:  "novalue,service.name=svc,service.version=1.2.3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, key := range []string{"APP_NAME", "APP_VERSION", "APP_ENV", "OTEL_SERVICE_NAME"} {
				t.Setenv(key, "")
			}
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", tt.env)

			info := identity.Resolve(nil)
			res, err := resource.New(context.Background(), resource.WithFromEnv())
			if err != nil && !errors.Is(err, resource.ErrPartialResource) {
				t.Fatalf("resource.New() error = %v", err)
			}

			attributes := info.Attributes()
			for _, key := range []string{"service.name", "service.version", "deployment.environment.name"} {
				want, ok := res.Set().Value(attribute.Key(key))
				got, present := attributes[key]
				if ok != present || (ok && got != want.AsString()) {
					t.Fatalf("attribute %q = %q (present %v), want %q (present %v)",
						key, got, present, want.AsString(), ok)
				}
			}
		})
	}
}
