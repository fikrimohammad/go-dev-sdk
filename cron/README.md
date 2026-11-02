# cron

`cron` provides an observable, resilient, multi-replica-safe recurring job scheduler for Go applications, designed for dedicated cron microservice containers.

## Features

- **Robfig Cron v3 Integration**: Schedule recurring jobs with 5-field standard expressions (`0 * * * *`), 6-field seconds expressions (`*/15 * * * * *`), and descriptors (`@hourly`, `@every 30s`).
- **Fixed Middleware Chain**: every run passes through tracer, logger, overlap guard, toggler, metrics, distributed locker, and panic recovery, in that order, before the handler (see [Middleware order](#middleware-order)).
- **Multi-Replica Safe**: Distributed locking with fencing tokens prevents concurrent job execution across replicas.
- **Skip-on-Overlap Protection**: If a job execution runs past its next trigger, new triggers are skipped cleanly to prevent race conditions and duplicate processing.
- **Fail-Safe Policies**:
  - Lock infrastructure errors: **Fail-closed** (aborts execution to prevent split-brain).
  - Toggler infrastructure errors: **Fail-open** (warns and proceeds to prevent schedule starvation).
- **Two-Phase Graceful Shutdown**: `Stop(ctx)` waits for in-flight jobs up to the context deadline, cancelling active job contexts if the deadline expires. `ctx` is required; without a deadline, `Config.ShutdownTimeout` applies.

---

## Installation

```go
import (
	"github.com/fikrimohammad/go-servicekit/cron"
	"github.com/fikrimohammad/go-servicekit/cron/engine"
)
```

---

## Usage

### 1. Initializing and Registering Jobs

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fikrimohammad/go-servicekit/cron"
	"github.com/fikrimohammad/go-servicekit/cron/engine"
)

func main() {
	// 1. Create the engine
	eng, err := engine.New(engine.Config{
		GlobalTimeout: 2 * time.Minute,
	},
		engine.WithToggler(myToggler), // optional
		engine.WithLocker(myLocker),   // optional
	)
	if err != nil {
		log.Fatalf("failed to create cron engine: %v", err)
	}

	// 2. Register jobs (must be registered before Start)
	err = eng.Register(cron.Job{
		Name:        "cleanup-sessions",
		Schedule:    "0 2 * * *", // Every day at 2:00 AM
		Description: "Purges expired user sessions from database",
		Timeout:     5 * time.Minute,
		Handler: func(ctx context.Context) error {
			if token, ok := cron.FencingTokenFromContext(ctx); ok {
				log.Printf("executing with fencing token: %s", token)
			}
			// Run cleanup logic...
			return nil
		},
	})
	if err != nil {
		log.Fatalf("failed to register job: %v", err)
	}

	// 3. Start scheduler
	eng.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	}()

	// 4. Block until a shutdown signal arrives
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}
```

---

## Middleware order

Each run is wrapped in this chain, outermost first:

1. **Tracer**: starts the `cron.<job name>` span and records the outcome on it. A skipped run leaves the span status unset and records a `cron.job.skipped` event.
2. **Logger**: logs the start, then the outcome. A skip is logged at Info, a failure at Error. The completed and failed lines carry the fencing token.
3. **Overlap guard**: returns `cron.ErrJobRunning` when the same job is already running on this engine.
4. **Toggler**: returns `cron.ErrJobDisabled` when the job is disabled. A toggler error fails open: the job runs, and the error is noted on the span.
5. **Metrics**: records `cron.job.duration` for every run except a skip. A lock-backend failure is measured with `error.type`.
6. **Distributed locker**: takes the lock, sets the fencing token in the context, and releases the lock after the run. A lock error other than `cron.ErrLockHeld` fails closed.
7. **Panic recovery**: turns a handler panic into an error, so the run is traced, logged, and measured as a failure.
8. **Handler**: your `cron.HandlerFunc`.

The engine also recovers a panic from the toggler, the locker, or an unlock, and returns it as an error with the stack logged at Error, so a scheduled run never crashes the process.

---

## Telemetry

### Metrics

| Instrument | Type | Unit | Attributes |
| --- | --- | --- | --- |
| `cron.job.duration` | histogram | `s` | `cron.job.name`, `cron.job.trigger`, `error.type` (failures only) |

- `cron.job.trigger` is `scheduled` or `manual`.
- `error.type` is the `errs` code for an `*errs.Error` (for example `4001`), `timeout` for a deadline or a network timeout, `canceled`, `panic` for a recovered panic, or `_OTHER` for anything else. The error message is never used as a value.
- A skipped run (disabled, lock held, or already running) is not measured. The logger and the span record it instead.
- The fencing token is not a metric attribute, because it changes on every run.

### Spans

Each run is one internal span named `cron.<job name>`. It carries the same `cron.job.name` and `cron.job.trigger` attributes, plus `error.type` on failure and `cron.job.fencing_token` when the run holds a lock. A failed run sets the span status to error.

A skipped run is not a failure. Its span status stays unset, it has no `error.type`, and it records a `cron.job.skipped` event with the attribute `cron.job.skip_reason`: `disabled`, `lock_held`, or `running`.

---

## Testing your code

### Unit tests: call the handler

A job's `Handler` is a plain function. Call it with a context. To simulate the lock, attach a fencing token with `cron.WithFencingToken`:

```go
func TestCleanupSessions(t *testing.T) {
	ctx := cron.WithFencingToken(t.Context(), "42")

	if err := cleanupSessions(ctx); err != nil {
		t.Fatalf("cleanupSessions() = %v, want nil", err)
	}
}
```

### Integration-style tests: trigger through the engine

To run a job through the middleware chain, register it with an engine and call `Trigger`. Two small fakes stand in for the infrastructure. The toggler enables every job:

```go
type fakeToggler struct{}

func (fakeToggler) IsEnabled(context.Context, string) (bool, error) { return true, nil }
```

The locker grants every lock. One type serves as both `cron.Locker` and `cron.LockToken`:

```go
type fakeLock struct{}

func (f fakeLock) Lock(context.Context, string, time.Duration) (cron.LockToken, error) {
	return f, nil
}

func (fakeLock) FencingToken() string        { return "1" }
func (fakeLock) Unlock(context.Context) error { return nil }
```

Then wire the fakes in and trigger the job:

```go
func TestCleanupSessionsJob(t *testing.T) {
	eng, err := engine.New(engine.Config{},
		engine.WithToggler(fakeToggler{}),
		engine.WithLocker(fakeLock{}),
	)
	if err != nil {
		t.Fatalf("engine.New() = %v, want nil", err)
	}
	if err := eng.Register(cron.Job{
		Name:     "cleanup-sessions",
		Schedule: "0 2 * * *",
		Handler:  cleanupSessions,
	}); err != nil {
		t.Fatalf("Register() = %v, want nil", err)
	}
	eng.Start()
	defer func() { _ = eng.Stop(t.Context()) }()

	if err := eng.Trigger(t.Context(), "cleanup-sessions"); err != nil {
		t.Fatalf("Trigger() = %v, want nil", err)
	}
}
```

Use `engine.WithMetrics` and `engine.WithTracer` to inject a test provider when you need to assert on telemetry.

---

## Running this module's tests

```bash
# Run unit tests
go test -v -count=1 ./...

# Run race detector tests
go test -v -race ./...
```
