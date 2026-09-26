# cron

`cron` provides an observable, resilient, multi-replica-safe recurring job scheduler for Go applications, designed for dedicated cron microservice containers.

## Features

- **Robfig Cron v3 Integration**: Schedule recurring jobs with 5-field standard expressions (`0 * * * *`), 6-field seconds expressions (`*/15 * * * * *`), and descriptors (`@hourly`, `@every 30s`).
- **Strict 6-Stage Middleware Chain**:
  $$\text{panic recovery} \longrightarrow \text{tracer} \longrightarrow \text{toggler} \longrightarrow \text{distributed locker (fencing token)} \longrightarrow \text{metrics} \longrightarrow \text{logger}$$
- **Multi-Replica Safe**: Distributed locking with fencing tokens prevents concurrent job execution across replicas.
- **Skip-on-Overlap Protection**: If a job execution runs past its next trigger, new triggers are skipped cleanly to prevent race conditions and duplicate processing.
- **Fail-Safe Policies**:
  - Lock infrastructure errors: **Fail-closed** (aborts execution to prevent split-brain).
  - Toggler infrastructure errors: **Fail-open** (warns and proceeds to prevent schedule starvation).
- **Two-Phase Graceful Shutdown**: Waits for in-flight jobs up to the shutdown context deadline, cancelling active job contexts if the deadline expires.
- **Developer REST API**: Built on `apiserver` (Hertz) to inspect registered jobs (`GET /jobs`, `GET /jobs/:name`) and manually trigger jobs synchronously or asynchronously (`POST /jobs/:name/run`).

---

## Installation

```go
import (
	"github.com/fikrimohammad/go-dev-sdk/cron"
	"github.com/fikrimohammad/go-dev-sdk/cron/engine"
	"github.com/fikrimohammad/go-dev-sdk/cron/server"
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
	"time"

	"github.com/fikrimohammad/go-dev-sdk/cron"
	"github.com/fikrimohammad/go-dev-sdk/cron/engine"
	"github.com/fikrimohammad/go-dev-sdk/cron/server"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
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
				log.Printf("executing with fencing token: %d", token)
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

	// 4. Run developer REST API server
	srv, err := server.New(server.Config{}, eng, metricsClient, tracerClient)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	if err := srv.Run(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
```

---

## Developer REST API

| Method | Endpoint | Query Param | Response Code | Description |
|---|---|---|---|---|
| `GET` | `/healthz` | — | `200 OK` | Health status: `{"status": "ok"}` |
| `GET` | `/jobs` | — | `200 OK` | List of all registered jobs with runtime statistics |
| `GET` | `/jobs/:name` | — | `200 OK` / `404` | Details of a single job |
| `POST` | `/jobs/:name/run` | `?async=true` | `202 Accepted` | Asynchronously triggers job execution in background |
| `POST` | `/jobs/:name/run` | (none) | `200 OK` | Synchronously triggers job execution and waits for result |

### Error Responses for Manual Triggers
- `404 Not Found`: Job name is not registered.
- `409 Conflict`: Job is currently executing (either locked by another replica or running locally).
- `422 Unprocessable Entity`: Job is disabled by toggler.
- `503 Service Unavailable`: Cron engine is stopped or shutting down.
- `504 Gateway Timeout`: Job execution exceeded its configured timeout deadline.
- `500 Internal Server Error`: Job handler failed with an internal error.

---

## Testing

```bash
# Run unit tests
go test -v -count=1 ./...

# Run race detector tests
go test -v -race ./...
```
