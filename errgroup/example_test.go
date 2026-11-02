package errgroup_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/fikrimohammad/go-dev-sdk/errgroup"
)

func ExampleGroup() {
	g := errgroup.New(context.Background(), errgroup.WithMaxConcurrency(2))

	// Each task writes only its own index, and Wait orders those writes before the read.
	results := make([]int, 4)
	for i := range results {
		g.Go(func(ctx context.Context) error {
			results[i] = i * i
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(results)
	// Output: [0 1 4 9]
}

func ExampleWithMaxConcurrency() {
	g := errgroup.New(context.Background(), errgroup.WithMaxConcurrency(1))

	// The first task holds the only slot until release is closed, so TryGo must refuse the second.
	release := make(chan struct{})
	fmt.Println(g.TryGo(func(ctx context.Context) error {
		<-release
		return nil
	}))
	fmt.Println(g.TryGo(func(ctx context.Context) error { return nil }))

	close(release)
	_ = g.Wait()
	// Output:
	// true
	// false
}

func ExamplePanicError() {
	g := errgroup.New(context.Background())
	g.Go(func(ctx context.Context) error {
		panic("disk full")
	})

	err := g.Wait()
	if pe, ok := errors.AsType[*errgroup.PanicError](err); ok {
		fmt.Println("panic value:", pe.Value)
	}
	// Output: panic value: disk full
}
