# identity

Standardized service identity: the **name**, **version**, and **environment**
attached to every log, metric, and trace emitted by the SDK. Intentionally
dependency-free so it can be reused anywhere.

The package is named `identity` because it owns the OpenTelemetry `service.*`
identity. Every exported identifier is one word — `Info`, `Default`, `Resolve`,
`Info.Validate`, `Info.Attributes`, and `Placeholder` — with the usual Go
convention of an `Err` prefix on the `ErrName` sentinel.

## Features

- One `Info` struct describing who the service is.
- Validation and semantic-convention attribute keys owned by this package, so
  consumers never re-implement them.
- Resolved from environment variables at call time, with OpenTelemetry's
  environment conventions honoured as fallbacks.
- Version stamped at build time with `-ldflags`, or read from the binary's
  build information.
- One-word API surface with no dependencies.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/identity
```

## Resolution

```go
info := identity.Default()
```

`Default()` reads the environment at call time:

| Env var | Field | Fallback |
| --- | --- | --- |
| `APP_NAME` | `Name` | `OTEL_SERVICE_NAME`, then `service.name` in `OTEL_RESOURCE_ATTRIBUTES`, then `app` |
| `APP_VERSION` | `Version` | `-ldflags` stamp, then `service.version` in `OTEL_RESOURCE_ATTRIBUTES`, then build information, then `dev` |
| `APP_ENV` | `Environment` | `deployment.environment.name` (or the deprecated `deployment.environment`) in `OTEL_RESOURCE_ATTRIBUTES`, then unset |

Values are trimmed, and a value that is empty after trimming counts as unset.
`OTEL_RESOURCE_ATTRIBUTES` values are URL path unescaped like the OpenTelemetry
SDK; malformed pairs are ignored.

`Environment` has no fallback: an unresolved environment is left empty so the
attribute is omitted instead of labelling production data as development.

## Step-by-step

### 1. Resolve and validate the identity

```go
info := identity.Default()
if err := info.Validate(); err != nil {
    // APP_NAME (or OTEL_SERVICE_NAME) is required.
}
```

`Validate` is also what the observability clients and the messaging clients
call, so behaviour is identical everywhere.

### 2. Stamp the version at build time

`APP_VERSION`'s build-time fallback is a package variable that build tooling
can replace at link time:

```bash
go build -ldflags "-X github.com/fikrimohammad/go-dev-sdk/identity.version=1.4.2" ./cmd/api
```

Builds that cannot use `-ldflags` (for example `go install`) fall back to the
version recorded in the binary's build information.

### 3. Pass the identity to the SDK

```go
info := identity.Default()

log, err := logs.New(info, logs.Config{Format: "json"})
mc, err := metrics.New(ctx, info, metrics.Config{Endpoint: "collector:4317"})
tc, err := tracer.New(ctx, info, tracer.Config{Endpoint: "collector:4317"})

// Every exported signal now carries:
//   service.name="my-svc" service.version="1.4.2"
//   deployment.environment.name="production"
```

### 4. Emit the identity yourself

```go
attributes := info.Attributes()
// map[string]string{
//     "service.name":                "my-svc",
//     "service.version":             "1.4.2",
//     "deployment.environment.name": "production",
// }
```

Empty `Version` and `Environment` fields are omitted; the service name is
always present.

### 5. (Optional) Build the identity yourself, or provide the environment

```go
info := identity.Info{
    Name:        "efficient-report-exporter",
    Version:     "1.4.2",
    Environment: "production",
}
```

`Resolve` accepts a lookup function, which is useful when identity comes from a
configuration layer rather than the process environment:

```go
info := identity.Resolve(func(key string) (string, bool) {
    value, ok := cfg.Lookup(key)
    return value, ok
})
```

## API reference

| Symbol | Description |
| --- | --- |
| `Info` | `{Name, Version, Environment string}` |
| `Default()` | Resolves from the process environment |
| `Resolve(lookup)` | Resolves through `lookup`; nil reads the process environment |
| `Info.Validate()` | Reports whether the identity names a service |
| `Info.Attributes()` | Semantic-convention attribute map |
| `Placeholder()` | Fallback identity, for detecting an unresolved name |
| `ErrName` | Sentinel for a missing service name |

## Migrating from appinfo

`appinfo` was renamed to `identity`; the module path changed as well:

| Before | After |
| --- | --- |
| `github.com/fikrimohammad/go-dev-sdk/appinfo` | `github.com/fikrimohammad/go-dev-sdk/identity` |
| `appinfo.Info` | `identity.Info` |
| `appinfo.Default()` | `identity.Default()` |
| `appinfo.DefaultName`, `DefaultVersion`, `DefaultEnvironment` | `identity.Placeholder()` |
| `-X .../appinfo.version=...` | `-X .../identity.version=...` |
| `deployment.environment` attribute | `deployment.environment.name` |

Behaviour changes: values are trimmed, `OTEL_SERVICE_NAME` and
`OTEL_RESOURCE_ATTRIBUTES` are honoured, and `Environment` no longer defaults to
`development`.

## Notes

- Environment variables are read on every call; set them before the process
  starts to keep the identity stable for the process lifetime.
- `identity` has **no dependencies** — it can be imported by any module,
  including standalone tools.
- The NATS clients reject a name containing dots for their own subject and
  group naming rules; `identity` itself allows dots because OTel service names
  may contain them.
- `deployment.environment` was deprecated in favour of
  `deployment.environment.name` in OpenTelemetry semantic conventions v1.27.0.
