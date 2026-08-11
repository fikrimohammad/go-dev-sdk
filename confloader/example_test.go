package confloader

import (
	"context"
	"fmt"
	"log"

	"github.com/fikrimohammad/go-dev-sdk/confloader/client"
)

// ExampleNew shows the intended usage: declare a config struct of Getter[T]
// fields, build the loader with a provider Config, and read values through the
// typed getters' Get method.
func ExampleNew() {
	type DBConfig struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}

	type AppConfig struct {
		DBConfig Getter[DBConfig] `conf:"folder=default,key=db_config"`
		Debug    Getter[bool]     `conf:"folder=settings,key=debug"`
	}

	// In a real program this would connect to etcd/infisical. The example uses
	// a fake client so it runs without external services.
	mc := newMockClient()
	mc.set("default", "db_config", `{"host":"localhost","port":5432}`)
	mc.set("settings", "debug", "true")

	loader, err := New[AppConfig](
		context.Background(),
		Config{
			Provider:         ProviderEtcd,
			Endpoint:         "localhost:2379",
			AuthClientID:     "username",
			AuthClientSecret: "password",
			Namespace:        "my-project",
		},
		WithClient(client.Client(mc)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = loader.Stop() }()

	dbConfig, err := loader.Data().DBConfig.Get(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s:%d\n", dbConfig.Host, dbConfig.Port)

	debug, err := loader.Data().Debug.Get(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(debug)

	// Output:
	// localhost:5432
	// true
}
