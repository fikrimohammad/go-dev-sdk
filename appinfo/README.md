# appinfo

Standardized service identity: the **name**, **version**, and **environment**
attached to every log, metric, and trace emitted by the SDK. Intentionally
dependency-free so it can be reused anywhere.

## Features

- One `Info` struct describing who the service is.
- Resolved from environment variables at call time.
- Version can be stamped at build time with `-ldflags`.
- Consumed by the `observability` packages (and anything else that needs the
  identity) as the OTel resource attributes `service.name`,
  `service.version`, and `deployment.environment`.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/appinfo
```

## Step-by-step

### 1. Resolve the identity

```go
info := appinfo.Default()
```

`Default()` reads the environment at call time:

| Env var | Field | Fallback |
| --- | --- | --- |
| `APP_NAME` | `Name` | `app` |
| `APP_VERSION` | `Version` | build-time stamp, else `dev` |
| `APP_ENV` | `Environment` | `development` |

### 2. Stamp the version at build time

`APP_VERSION`'s fallback is a package variable that build tooling can replace
at link time:

```bash
go build -ldflags "-X github.com/fikrimohammad/go-dev-sdk/appinfo.version=1.4.2" ./cmd/api
```

### 3. Pass the identity to the SDK

```go
info := appinfo.Default()

log, err := logs.New(info, logs.Config{Format: "json"})
mc, err := metrics.New(ctx, info, metrics.Config{Endpoint: "collector:4317"})
tc, err := tracer.New(ctx, info, tracer.Config{Endpoint: "collector:4317"})

// Every exported signal now carries:
//   service.name="my-svc" service.version="1.4.2"
//   deployment.environment="production"
```

### 4. (Optional) Build the identity yourself

```go
info := appinfo.Info{
    Name:        "efficient-report-exporter",
    Version:     "1.4.2",
    Environment: "production",
}
```

## API reference

| Symbol | Description |
| --- | --- |
| `Info` | `{Name, Version, Environment string}` |
| `Default()` | Resolves from `APP_NAME` / `APP_VERSION` / `APP_ENV` |
| `DefaultName` / `DefaultVersion` / `DefaultEnvironment` | Fallback constants |

## Notes

- Environment variables are read on every call; set them before the process
  starts to keep the identity stable for the process lifetime.
- `appinfo` has **no dependencies** — it can be imported by any module,
  including standalone tools.
