package metrics_test

import (
	"context"

	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
)

func ExampleClient_Add() {
	client := metrics.Wrap(nil)

	err := client.Add(context.Background(), metrics.Instrument{
		Name:        "jobs.processed",
		Unit:        "{job}",
		Description: "Jobs processed",
	}, 1, map[string]any{"queue": "default"})
	if err != nil {
		panic(err)
	}

	// Output:
}
