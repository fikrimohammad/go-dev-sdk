# observability

OpenTelemetry plumbing for Go services: **logs**, **metrics**, and **traces**.
Each signal is a separate subpackage. `New` builds a configured, started client
and the caller owns its lifecycle. Logs keep a package default (`SetDefault` /
`Default`). Metrics and traces install into the OpenTelemetry globals
(`otel.SetMeterProvider`, `tracer.SetGlobal`), and every SDK package records
through those globals.

## Features

- **logs** — routes structured records to stdout/stderr by severity (debug/info
  → stdout, warn/error → stderr), text or JSON format, and adds `trace_id` /
  `span_id` from a valid span context. Writers are injectable through
  `logs.Config`.
- **metrics** — counters, histograms, up-down counters, and gauges exported
  over OTLP gRPC, with instrument units/descriptions, strict name validation,
  and exponential histogram buckets by default. A stable instrumentation scope
  identifies this library, not the service.
- **tracer** — spans exported over OTLP gRPC with configurable sampler, batch
  settings, resource attributes, and OTLP headers. `SetGlobal` installs the
  provider and the W3C TraceContext/Baggage propagator in one call; `Attrs`,
  `TraceIDFrom`, and `SpanIDFrom` are shared helpers.
- **attributes** — shared key normalization (`order_id` → `order.id`) and
  resource-attribute helpers, plus the `service.name`, `service.version`, and
  `deployment.environment.name` identity attributes resolved by the `identity`
  package. Keys that already contain a dot, and the standard
  `trace_id` / `span_id` correlation keys, keep their spelling.
- **OTel globals** — metrics and traces follow `otel.GetMeterProvider()` and
  `otel.GetTracerProvider()` on every call. Installing a provider is the only
  wiring an app does. Before any install, metrics and traces are no-ops.
- **TLS by default** — exporters use TLS unless `Insecure: true` is set.

## API status and migration

These modules are still in development, so the current API is intentionally
allowed to break between phases. Current contract changes worth noting:

- Metrics are instrument-based only. `Count` and `Histogram` were removed; use
  `Add`, `Record`, `AddUpDown`, or `RecordGauge` with an `Instrument` that
  carries the metric name, UCUM unit, and description. Names are strict,
  lowercase, and never silently sanitized.
- Metric instrumentation scope defaults to `DefaultInstrumentationScope`, not
  the service name. Service identity remains in resource attributes.
- Logs automatically add `trace_id` and `span_id` from a valid span context;
  explicit values win. `Config.Writer`/`ErrorWriter` choose the output streams
  (default `os.Stdout`/`os.Stderr`).
- Metrics and traces no longer have a package-level `SetDefault` or a
  package-level `Stop`. Install the client into the OTel globals instead, and
  stop the client you built with `New`.
- Exporter TLS is on by default. `Config.Insecure` is a plain `bool`; set
  `insecure: true` for a plaintext collector.
- Registered semantic-convention keys and the correlation keys `trace_id`,
  `span_id`, and `trace_flags` keep their spelling. Developer-style keys are
  still normalized (`order_id` → `order.id`).

CI runs `go test -shuffle=on` and a separate `go test -race -shuffle=on` pass
for every module.

## Installation

```bash
go get github.com/fikrimohammad/go-servicekit/observability@v0.1.0
```

> Each signal imports `github.com/fikrimohammad/go-servicekit/observability/logs`,
> `.../metrics`, `.../tracer`, or `.../attributes` directly.

## Step-by-step

### 1. Resolve the service identity

```go
info := identity.Default() // APP_NAME / APP_VERSION / APP_ENV
```

`info.Name` must be non-empty. `Version` and `Environment` are optional; empty
values are omitted from the exported resource attributes.

### 2. Configure logs

```go
log, err := logs.New(info, logs.Config{Format: "json"}) // or "text"
if err != nil { /* handle */ }
logs.SetDefault(log)
```

Pass `Config.Writer` and `Config.ErrorWriter` to send output to specific
streams; nil means `os.Stdout` and `os.Stderr`.

### 3. Configure metrics

```go
mc, err := metrics.New(ctx, info, metrics.Config{
    Endpoint: "collector:4317", // OTLP gRPC, TLS by default
})
if err != nil { /* handle */ }
otel.SetMeterProvider(mc.Provider())
```

### 4. Configure traces

```go
tc, err := tracer.New(ctx, info, tracer.Config{
    Endpoint: "collector:4317", // OTLP gRPC, TLS by default
})
if err != nil { /* handle */ }
tracer.SetGlobal(tc) // installs the provider and W3C TraceContext/Baggage propagator
```

### 5. Emit signals

```go
// Logs — keys are normalized (user_id → user.id); a valid span context in ctx
// adds trace_id and span_id automatically.
logs.Info(ctx, "request", "method", "GET", "user_id", 42)
logs.Error(ctx, "job failed", errs.SlogAttr(err))

// Metrics — returns errors; treat them as real. Instrument metadata carries
// the UCUM unit and description. SDK packages and this call record through
// the OTel global meter provider installed in step 3.
if err := metrics.Record(ctx, metrics.Instrument{
    Name: "http.server.request.duration",
    Unit: "s",
    Description: "HTTP server request duration",
}, 0.05, attrs); err != nil { /* handle */ }

// Traces — recorded through the OTel global tracer provider from step 4.
_, span := tracer.Tracer("my.scope").Start(ctx, "operation")
defer span.End()
```

Metric names are immutable once registered. Reusing a name with a different
unit, description, or kind returns `ErrInstrumentConflict`.

### 6. Shut down on exit

The caller owns each client returned by `New`, so the caller stops it.
`Stop` is idempotent.

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
| `Writer` | `os.Stdout` | Destination for debug/info records |
| `ErrorWriter` | `os.Stderr` | Destination for warn/error records |

### metrics.Config

| Field | Default | Meaning |
| --- | --- | --- |
| `Endpoint` | `localhost:4317` | OTLP gRPC collector endpoint |
| `Timeout` | `10s` | Export connection/operation timeout |
| `ExportInterval` | `10s` | Export period |
| `Scope` | package scope | Instrumentation scope name |
| `Insecure` | `false` (TLS) | Disable TLS for the exporter; `OTEL_EXPORTER_OTLP_INSECURE` can also enable plaintext |
| `Headers` | — | Extra headers sent with exports |
| `GlobalKV` | — | Resource attributes |
| `HistogramMaxBuckets` | `160` (max `320`) | Exponential histogram buckets |
| `HistogramMaxScale` | `20` | Exponential histogram scale (`-10..20`) |

### tracer.Config

| Field | Default | Meaning |
| --- | --- | --- |
| `Endpoint` | `localhost:4317` | OTLP gRPC collector endpoint |
| `ExportTimeout` | `5s` | Per-export timeout |
| `Insecure` | `false` (TLS) | Disable TLS for the exporter; `OTEL_EXPORTER_OTLP_INSECURE` can also enable plaintext |
| `Headers` | — | Extra headers sent with exports |
| `GlobalKV` | — | Resource attributes |
| `Sampler` | SDK default | Programmatic sampler override (`otel/sdk/trace.Sampler`) |
| `BatchTimeout` | `5s` | Maximum batch delay |
| `MaxQueueSize` | `2048` | Maximum queued spans |
| `MaxExportBatchSize` | `512` | Maximum spans per export batch |

## Telemetry conventions

- Every signal carries the service identity as resource attributes
  (`service.name`, `service.version`, `deployment.environment.name`). A non-empty
  `service.name` is required; empty `service.version` and
  `deployment.environment.name` values are omitted.
- Developer-style attribute keys are normalized to OTel semantic-convention
  style (`order_id` → `order.id`). Keys that already contain a dot (such as
  `http.response.status_code`) and the correlation keys `trace_id`, `span_id`,
  and `trace_flags` keep their spelling.
- Metric instrument names are **not** namespaced by service name; identity
  lives only in the resource attributes.
- Instrument names must be lowercase and match the OTel instrument-name
  syntax. Names are case-sensitive and are never sanitized or silently merged;
  reusing a name with different unit, description, or instrument kind returns
  `ErrInstrumentConflict`.
- Instrument units use UCUM (`s`, `1`, `{request}`) and are recorded together
  with the instrument description. The default instrumentation scope is
  `DefaultInstrumentationScope`; `Config.Scope` and `WrapWithScope` override it.

## API reference

| Package | Key symbols |
| --- | --- |
| `logs` | `New`, `SetDefault`, `Default`, `Debug/Info/Warn/Error`, `Logger` |
| `metrics` | `New`, `Wrap`, `WrapWithScope`, `Default`, `Add`, `Record`, `AddUpDown`, `RecordGauge`, `Instrument`, `Client`, `Noop` |
| `tracer` | `New`, `Wrap`, `Noop`, `SetGlobal`, `Default`, `Tracer`, `Attrs`, `TraceIDFrom`, `SpanIDFrom`, `Client` |
| `attributes` | `NormalizeKey`, `ConvertMapsToKVs`, `ConvertAppInfoToMap` |
