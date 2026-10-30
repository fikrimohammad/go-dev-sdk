package identity_test

import (
	"fmt"

	"github.com/fikrimohammad/go-dev-sdk/identity"
)

func ExampleResolve() {
	env := map[string]string{
		"APP_NAME":    "checkout",
		"APP_VERSION": "1.4.2",
		"APP_ENV":     "production",
	}
	info := identity.Resolve(func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	})
	fmt.Println(info.Name, info.Version, info.Environment)
	// Output: checkout 1.4.2 production
}

func ExampleInfo_Attributes() {
	info := identity.Info{Name: "checkout", Version: "1.4.2", Environment: "production"}
	attributes := info.Attributes()
	fmt.Println(attributes["service.name"], attributes["service.version"], attributes["deployment.environment.name"])
	// Output: checkout 1.4.2 production
}
