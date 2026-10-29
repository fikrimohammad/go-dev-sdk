# observability

OpenTelemetry plumbing for Go services: **logs**, **metrics**, and **traces**,
unified behind a `log/slog`-style global-facade design. Each signal is a
separate subpackage that follows the same pattern: `New` builds a configured,
started client, `SetDefault` installs it, `Default` returns it, and the
package-level functions delegate to the installed default.

## Features

- **logs** — routes structured records to stdout/stderr by severity (debug/info
  → stdout, warn/error → stderr), text or JSON format.
- **metrics** — counters and histograms exported over OTLP gRPC, with
  exponential histogram buckets by default.
- **tracer** — spans exported over OTLP gRPC, plus `Attrs` and `TraceIDFrom`
  helpers; the provider can be registered as the OTel global.
- **attributes** — shared key normalization (`order_id` → `order.id`) and
  resource-attribute helpers, plus `service.*` / `deployment.environment`
  identity from `appinfo`.
- **Safe defaults** — before any `SetDefault`, logs write debug text to stderr
  and metrics/tracer are no-op implementations, so package-level calls are
  always safe.
- **Construction vs. globals** — `New` is a pure constructor with no global
  side effects; installing/owning the lifecycle is the composition root's job.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/observability
```

> Each signal imports `github.com/fikrimohammad/go-dev-sdk/observability/logs`,
> `.../metrics`, `.../tracer`, or `.../attributes` directly.

## Step-by-step

### 1. Resolve the service identity

```go
info := appinfo.Default() // APP_NAME / APP_VERSION / APP_ENV
```

### 2. Configure logs

```go
log, err := logs.New(info, logs.Config{Format: "json"}) // or "text"
if err != nil { /* handle */ }
logs.SetDefault(log)
```

### 3. Configure metrics

```go
mc, err := metrics.New(ctx, info, metrics.Config{
    Endpoint: "collector:4317", // OTLP gRPC
})
if err != nil { /* handle */ }
metrics.SetDefault(mc)
```

### 4. Configure traces

```go
tc, err := tracer.New(ctx, info, tracer.Config{
    Endpoint: "collector:4317",
})
if err != nil { /* handle */ }
tracer.SetDefault(tc)
otel.SetTracerProvider(tc.Provider()) // route otel.* + auto-instrumentation through it
```

### 5. Emit signals

```go
// Logs — keys are normalized (user_id → user.id).
logs.Info(ctx, "request", "method", "GET", "user_id", 42)
logs.Error(ctx, "job failed", errs.SlogAttr(err))

// Metrics — returns errors; treat them as real.
_ = metrics.Count(ctx, "http.server.request.count", 1, attrs)
_ = metrics.Histogram(ctx, "http.server.request.duration", 0.05, attrs)

// Traces.
_, span := tracer.Tracer("my.scope").Start(ctx, "operation")
defer span.End()
```

### 6. Shut down on exit

Ownership lies with the caller who called `New`; `Stop` is idempotent.

```go
defer mc.Stop(ctx)
defer tc.Stop(ctx)
```

## Configuration

### logs.Config

| Field | Default | Meaning |
| --- | --- | --- |
| `Format` | `"text"` | `"text"` or `"json"` |
| `Level` | `"debug"` | Minimum severity |
| `GlobalKV` | — | Fields attached to every record |

### metrics.Config

| Field | Default | Meaning |
| --- | --- | --- |
| `Endpoint` | `localhost:4317` | OTLP gRPC collector endpoint |
| `Timeout` | `10s` | Export connection/operation timeout |
| `ExportInterval` | `10s` | Export period |
| `Insecure` | `true` (nil) | Disable TLS for the exporter |
| `GlobalKV` | — | Resource attributes |
| `HistogramMaxBuckets` | `160` (max `320`) | Exponential histogram buckets |
| `HistogramMaxScale` | `20` | Exponential histogram scale (`-10..20`) |

### tracer.Config

| Field | Default | Meaning |
| --- | --- | --- |
| `Endpoint` | `localhost:4317` | OTLP gRPC collector endpoint |
| `ExportTimeout` | `5s` | Per-export timeout |
| `Insecure` | `true` (nil) | Disable TLS for the exporter |
| `Headers` | — | Extra headers sent with exports |

## Telemetry conventions

- Every signal carries the service identity as resource attributes
  (`service.name`, `service.version`, `deployment.environment`).
- Attribute keys are normalized to OTel semantic-convention style
  (`order_id` → `order.id`).
- Metric instrument names are **not** namespaced by service name; identity
  lives only in the resource attributes.

## API reference

| Package | Key symbols |
| --- | --- |
| `logs` | `New`, `SetDefault`, `Default`, `Debug/Info/Warn/Error`, `Logger` |
| `metrics` | `New`, `SetDefault`, `Default`, `Count`, `Histogram`, `Stop`, `Client`, `Noop` |
| `tracer` | `New`, `Wrap`, `Noop`, `SetDefault`, `Tracer`, `Attrs`, `TraceIDFrom`, `Stop`, `Client` |
| `attributes` | `NormalizeKey`, `ConvertMapsToKVs`, `ConvertAppInfoToMap` |
