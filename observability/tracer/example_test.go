package tracer_test

import (
	"context"

	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

func ExampleClient_Tracer() {
	client := tracer.Wrap(nil)

	ctx, span := client.Tracer("example").Start(context.Background(), "operation")
	defer span.End()

	_ = tracer.TraceIDFrom(ctx)
	_ = tracer.SpanIDFrom(ctx)

	// Output:
}
