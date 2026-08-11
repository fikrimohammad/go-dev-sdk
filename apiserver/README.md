# apiserver

A thin wrapper around the [Hertz](https://github.com/cloudwego/hertz) HTTP
server with a standardized middleware stack pre-wired for request IDs, panic
recovery, tracing, structured logging, and metrics.

## Features

- **Fixed middleware order** registered by `New`:
  1. `RequestID` — extracts or generates `X-Request-ID` (8-char hex), sets it on
     the response header and request context.
  2. `PanicRecovery` — recovers panics, logs the stack, returns a 500 JSON
     response in the standard API format.
  3. `Tracer` — creates an OTel server span per request, extracting the W3C
     `traceparent` so requests join upstream traces.
  4. `Logger` — logs every request as structured attributes (status ≥ 400 at
     Error level with bound errors).
  5. `Metrics` — records `http.server.request.count` and
     `http.server.request.duration` with OTel HTTP semantic-convention
     attributes.
- **Observability-aware** — attributes are consistent across the tracer and
  metrics middlewares; `errs` codes drive `error.type`.
- **Graceful lifecycle** — `Run` blocks until shutdown; `Shutdown` drains
  gracefully.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/apiserver
```

## Step-by-step

### 1. Set up observability clients

```go
info := appinfo.Default()

tc, _ := tracer.New(ctx, info, tracer.Config{Endpoint: "collector:4317"})
tracer.SetDefault(tc)
otel.SetTracerProvider(tc.Provider())

mc, _ := metrics.New(ctx, info, metrics.Config{Endpoint: "collector:4317"})
metrics.SetDefault(mc)

logs.SetDefault(must(logs.New(info, logs.Config{Format: "json"})))
```

### 2. Configure and create the server

```go
srv, err := apiserver.New(apiserver.Config{
    Addr: ":8080", // default ":3000"
}, mc, tc)
if err != nil { /* handle */ }
```

`Config` fields: `Addr`, `ReadTimeout` (30s), `WriteTimeout` (30s),
`ShutdownTimeout` (10s).

### 3. Register routes

```go
h := srv.Hertz()

h.GET("/ping", func(ctx context.Context, c *app.RequestContext) {
    c.String(http.StatusOK, "pong")
})

h.POST("/users", func(ctx context.Context, c *app.RequestContext) {
    var req CreateUserRequest
    if err := c.BindAndValidate(&req); err != nil {
        c.Error(err) // flows into Logger/Metrics as a bound error
        c.JSON(http.StatusBadRequest, map[string]any{"base": map[string]any{
            "code": "1001", "message": "invalid argument",
        }})
        return
    }
    // ...
})
```

### 4. Run and shut down

```go
// Blocks until shutdown.
if err := srv.Run(); err != nil { /* handle */ }

// Graceful shutdown from a signal handler:
srv.Shutdown(shutdownCtx)
```

## Middleware details

| Middleware | What it does |
| --- | --- |
| `RequestID()` | `X-Request-ID` from header or generated; stored in context (`c.Get("X-Request-ID")`) and echoed on the response |
| `PanicRecovery()` | Recovers, logs panic + stack (with `trace_id` when present), returns `5001` JSON |
| `Tracer(tc)` | Server span named `"{METHOD} {route}"`; W3C propagation in, `error.type` from `errs` codes |
| `Logger()` | `method/path/route/status/duration` (+ `trace_id`, `request_id`); Error level with bound errors on status ≥ 400 |
| `Metrics(mc)` | `http.server.request.count` + `http.server.request.duration` with HTTP semantic-convention attributes |

## Telemetry attributes

Request attributes follow the OTel HTTP conventions:
`http.request.method`, `http.route`, `http.response.status_code`,
`url.scheme`, `url.path`, `network.protocol.name/version`, `error.type`.

## API reference

| Symbol | Description |
| --- | --- |
| `Config` | `Addr`, read/write/shutdown timeouts |
| `New(cfg, mc, tc)` | Builds the Hertz server with base middlewares |
| `(*Server).Hertz()` | The underlying `*server.Hertz` for route registration |
| `(*Server).Run()` | Blocks until shutdown |
| `(*Server).Shutdown(ctx)` | Graceful shutdown |
| `RequestID/PanicRecovery/Tracer/Logger/Metrics` | Standalone middleware constructors |
| `RequestIDKey` | Context key type for the request ID |
