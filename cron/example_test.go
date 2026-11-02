// This file holds runnable documentation examples for package cron; go
// test checks their output.

package cron_test

import (
	"context"
	"fmt"

	"github.com/fikrimohammad/go-dev-sdk/cron"
)

func ExampleWithFencingToken() {
	ctx := cron.WithFencingToken(context.Background(), "42")

	token, ok := cron.FencingTokenFromContext(ctx)
	fmt.Println(token, ok)

	_, ok = cron.FencingTokenFromContext(context.Background())
	fmt.Println(ok)
	// Output:
	// 42 true
	// false
}
