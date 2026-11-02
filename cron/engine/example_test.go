package engine_test

import (
	"context"
	"fmt"

	"github.com/fikrimohammad/go-dev-sdk/cron"
	"github.com/fikrimohammad/go-dev-sdk/cron/engine"
)

func ExampleEngine_Trigger() {
	eng, err := engine.New(engine.Config{})
	if err != nil {
		fmt.Println(err)
		return
	}

	err = eng.Register(cron.Job{
		Name:     "nightly-report",
		Schedule: "@every 1h",
		Handler: func(ctx context.Context) error {
			fmt.Println("report generated")
			return nil
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	eng.Start()
	// Trigger runs the job synchronously, so its output is ready before it returns.
	if err := eng.Trigger(context.Background(), "nightly-report"); err != nil {
		fmt.Println(err)
	}
	if err := eng.Stop(context.Background()); err != nil {
		fmt.Println(err)
	}
	// Output: report generated
}
