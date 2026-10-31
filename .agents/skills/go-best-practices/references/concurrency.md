# Concurrency and context

Sources: [decisions#goroutine-lifetimes](https://google.github.io/styleguide/go/decisions#goroutine-lifetimes), [decisions#synchronous-functions](https://google.github.io/styleguide/go/decisions#synchronous-functions), [decisions#contexts](https://google.github.io/styleguide/go/decisions#contexts), [best-practices#decl-chan](https://google.github.io/styleguide/go/best-practices#decl-chan), [best-practices#documentation-conventions-contexts](https://google.github.io/styleguide/go/best-practices#documentation-conventions-contexts).

## Contents

- Synchronous functions first
- Goroutine lifetimes
- Channels
- `context.Context` rules
- Documenting concurrency and context behavior

## Synchronous functions first

Prefer functions that return their results directly and finish all callbacks and channel operations before returning. They keep goroutines local to a call, which makes lifetimes easy to reason about, and they avoid leaks and races. They are also trivially testable: pass an input, check the output, with no polling.

A caller who wants concurrency can wrap a synchronous call in a goroutine. Removing concurrency that an API forces on its callers is hard and sometimes impossible. So don't return a channel, take a callback, or start background work when returning a value would do.

## Goroutine lifetimes

When you start a goroutine, make it obvious when (or whether) it exits.

- A goroutine blocked on a channel send or receive is never collected, even when nothing else references the channel. That is a leak.
- In-flight goroutines that outlive their purpose cause races (they modify inputs the caller has moved on from), unbounded memory, panics (sending on a closed channel), and programs that don't shut down cleanly.

Keep synchronization inside one function's scope, and factor the per-item work into synchronous functions:

```go
// Good: every goroutine ends before Run returns, and process stops on ctx.
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for item := range w.q {
		wg.Go(func() { // Go 1.25+; otherwise wg.Add(1) + defer wg.Done()
			process(ctx, item) // returns at latest when ctx is cancelled
		})
	}
	wg.Wait()
	return ctx.Err()
}

// Bad: nobody knows when, or if, these finish.
func (w *Worker) Run() {
	for item := range w.q {
		go process(item)
	}
}
```

Tools for joining goroutines:

- `sync.WaitGroup`, or `wg.Go` in Go 1.25+.
- `errgroup.Group`, or `errgroup.WithContext` when the first error should cancel the rest. Use `g.SetLimit(n)` to bound concurrency instead of building a hand-rolled semaphore.
- A `done chan struct{}` or condition variables when the shape needs them.

Whatever you use, a maintainer should be able to see where each goroutine ends. If that is not evident from the code, document when and why it exits.

Since Go 1.22, each loop iteration has its own loop variable, so closures may capture `item` directly. Don't add `item := item`.

For bounded parallel work that returns results, have each goroutine write to its own index of a pre-sized slice (no mutex needed), or send results on a channel that one collector drains before returning. Return the results synchronously.

## Channels

- Specify direction in signatures (`<-chan T` to receive, `chan<- T` to send). The compiler then catches misuse, such as closing a receive-only channel, and the signature documents ownership:

```go
// sum reads from values until the channel is closed.
func sum(values <-chan int) int
```

- Only the sender closes a channel, and only once. Sending on a closed channel panics.
- A buffered channel can stand in for a mutex if that matches the local style. The guide treats that as a local choice.

## `context.Context` rules

Contexts carry deadlines, cancellation, credentials, and trace data across API boundaries. Go passes them explicitly along the whole call chain.

- **First parameter, always**: `func F(ctx context.Context, ...)`. This also holds in test helpers: `func readTestFile(ctx context.Context, t *testing.T, path string) string`.
  - Exceptions: HTTP handlers (use `r.Context()`), streaming RPC methods (use `stream.Context()`), test functions (use `t.Context()`), and entry points (`main`, `init`) that start from `context.Background()`.
- **Never a struct field.** Add a `ctx` parameter to each method that needs one. The only exception is a method whose signature must match an interface you don't control, and that is very rare. Never put a context in an option struct.
- **Never a custom context type**, and never an interface other than `context.Context` in a signature. No exceptions. Put application data in parameters, receivers, or (if it truly belongs there) context values.
- **Don't create base contexts in the middle of a call chain.** Library code takes its caller's context. `context.Background()` belongs in `main`, `init`, and server frameworks. If work must continue after the caller's context is cancelled (async cleanup, a background flush), detach it with `context.WithoutCancel(ctx)`. This keeps values and trace data but drops cancellation, and you then bound the work with its own timeout.
- **In tests**, use the innermost `t.Context()` (Go 1.24+) rather than `context.Background()`. It is cancelled *before* `t.Cleanup` functions run, so cleanup code uses `context.Background()` or `context.WithoutCancel(ctx)`.
- Contexts are immutable. Passing the same one to several calls with the same deadline and cancellation is fine.
- A function that takes a context should normally return an `error`, conventionally `ctx.Err()` when cancelled.
- Respect cancellation in loops and waits. Prefer `select { case <-ctx.Done(): return ctx.Err(); case <-timer.C: }` over `time.Sleep`, which ignores cancellation.

## Documenting concurrency and context behavior

Readers assume these defaults. Document only the deviations.

- **Context:** cancellation interrupts the function and it returns `ctx.Err()`. Don't restate that. Do document when it returns something else (for example "If the context is cancelled, Run returns a nil error"), when other mechanisms affect its lifetime (a `Stop` method, asynchronous shutdown), or when the context must meet special conditions (no deadline, must carry a value). Avoid designing APIs with such conditions in the first place.
- **Concurrency:** read-only operations are assumed safe for concurrent use, and mutating operations are assumed unsafe. Document when it is unclear whether an operation mutates (an LRU `Lookup` mutates internally), when the API provides synchronization ("safe for simultaneous use by multiple goroutines", stated once on the type), and when an interface you consume has concurrency requirements for implementers.
- **Cleanup:** say what the caller must release and how: "Call Stop to release the Ticker's resources", "Caller should close resp.Body".
