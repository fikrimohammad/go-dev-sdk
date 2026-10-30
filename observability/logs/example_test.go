package logs_test

import (
	"context"
	"io"

	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
)

func ExampleNew() {
	logger, err := logs.New(identity.Info{Name: "orders"}, logs.Config{
		Format:      logs.FormatJSON,
		Writer:      io.Discard,
		ErrorWriter: io.Discard,
	})
	if err != nil {
		panic(err)
	}

	// A valid span context in ctx would add trace_id and span_id.
	logger.Info(context.Background(), "order accepted", "order_id", "o-1")

	// Output:
}
