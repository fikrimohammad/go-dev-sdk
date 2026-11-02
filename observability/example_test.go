package observability_test

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"

	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

// Example wires logs, metrics, and tracing as described in the package doc. It
// has no Output comment because it needs a reachable OTLP collector.
func Example() {
	ctx := context.Background()
	info := identity.Default()

	log, err := logs.New(info, logs.Config{Format: "json"})
	if err != nil {
		fmt.Println(err)
		return
	}
	logs.SetDefault(log)

	mc, err := metrics.New(ctx, info, metrics.Config{Endpoint: "collector:4317", Insecure: true})
	if err != nil {
		fmt.Println(err)
		return
	}
	otel.SetMeterProvider(mc.Provider())
	defer func() { _ = mc.Stop(ctx) }()

	tc, err := tracer.New(ctx, info, tracer.Config{Endpoint: "collector:4317", Insecure: true})
	if err != nil {
		fmt.Println(err)
		return
	}
	tracer.SetGlobal(tc)
	defer func() { _ = tc.Stop(ctx) }()

	logs.Info(ctx, "service started", "user_id", 42)
	if err := metrics.Add(ctx, metrics.Instrument{Name: "orders.created", Unit: "{order}"}, 1, map[string]any{"region": "eu"}); err != nil {
		fmt.Println(err)
	}
}
