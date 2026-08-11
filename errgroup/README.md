# errgroup

A concurrency-limited, panic-safe wrapper around
[`golang.org/x/sync/errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup).

## Features

- **Concurrency limit** — a `maxConcurrency` cap via functional options; at the
  default this is `20`, pass `WithMaxConcurrency` to change it. `<= 0` means
  unlimited.
- **Panic safety** — a panic inside a task is recovered and turned into an
  error (with the panic stack trace). If the panic value is an `error`, it is
  wrapped with `%w` so `errors.Is` / `errors.As` still work against it.
- **Cancellation-aware scheduling** — a task whose parent context is already
  canceled is never invoked; the cancellation error is reported instead. This
  keeps tasks queued behind the concurrency limit from doing pointless work
  after a sibling has failed.
- **Child groups** — `SubGroup` derives a child group that inherits the parent
  context and default concurrency limit but scopes its own error/panic
  cancellation (a child failure does not tear down the parent).
- **Shared context** — `Context()` exposes the cancellation-linked context for
  plumbing into non-task work.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/errgroup
```

## Step-by-step

### 1. Create a group

```go
g := errgroup.New(ctx)                          // default limit: 20
g := errgroup.New(ctx, errgroup.WithMaxConcurrency(5))
```

The group's internal context is derived from `ctx` and canceled on the first
task error, the first recovered panic, or when `Wait` returns.

### 2. Schedule tasks

```go
for _, item := range items {
    item := item // capture loop variable (Go < 1.22 style)
    g.Go(func(ctx context.Context) error {
        return process(ctx, item)
    })
}
```

`TryGo` behaves like `Go` but returns `false` without scheduling when the
concurrency limit is already reached:

```go
if !g.TryGo(func(ctx context.Context) error { return work(ctx) }) {
    // limit reached — fall back to serial execution
}
```

### 3. Wait for completion

```go
if err := g.Wait(); err != nil {
    // first non-nil error (or recovered panic), group ctx now canceled
    return err
}
```

### 4. Scope a subgroup of related work

```go
func processBatch(ctx context.Context, g *errgroup.Group, batch []Item) error {
    // Fails in here cancel the subgroup's ctx only — the parent keeps running.
    sub := g.SubGroup()
    for _, item := range batch {
        sub.Go(func(ctx context.Context) error { return handle(ctx, item) })
    }
    return sub.Wait()
}
```

### 5. Plumb cancellation into non-task work

```go
g := errgroup.New(ctx)
g.Go(func(ctx context.Context) error { return fetch(ctx) })

// Cleanup that should abort when any task fails:
go func() {
    <-g.Context().Done()
    cleanup() // runs once the group finishes or a task errors
}()
```

## API reference

| Symbol | Description |
| --- | --- |
| `New(ctx, opts...)` | Builds a group; nil `ctx` becomes `context.Background()` |
| `WithMaxConcurrency(n)` | Option; `<= 0` disables the limit |
| `(*Group).Go(f)` | Schedules a task |
| `(*Group).TryGo(f)` | Schedules unless the limit is reached |
| `(*Group).Wait()` | Blocks until tasks finish, returns first error |
| `(*Group).Context()` | The group's cancellation-linked context |
| `(*Group).SubGroup(opts...)` | Child group inheriting ctx + default limit |
| `DefaultMaxConcurrency` | Package default limit (20) |

## Notes

- The zero `Group` value is **not** usable; always construct with `New`.
- The default concurrency (`DefaultMaxConcurrency = 20`) is a package variable
  and can be changed if you prefer a different baseline.
- After `Wait` returns the group must not be reused.
