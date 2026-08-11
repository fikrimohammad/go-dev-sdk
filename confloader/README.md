# confloader

Loads and **watches hot configuration / secrets** from pluggable backends
(etcd, Infisical, ...). Declare your config as a struct of typed `Getter[T]`
fields; the loader fetches every entry, keeps the cache fresh with a
background polling loop, and exposes thread-safe, typed accessors that refresh
on read.

## Features

- **Typed accessors** — declare `Getter[T]` fields and call `.Get(ctx)` to read
  a `T`, with automatic YAML/JSON/`TextUnmarshaler` decoding into the target
  type.
- **Cache-with-polling** — entries are eagerly fetched at `New` (unless lazy)
  and kept fresh on a configurable interval with retry/backoff.
- **Read-through on staleness** — a `Get` never blocks on the polling loop: a
  missing or stale entry is fetched directly from the source and either yields
  the fresh value or the source's own error (never `ErrStale`).
- **Default-folder fallback** — when a key is absent in the requested folder,
  the loader retries the standardized `default` folder in the same namespace.
- **Health signals** — `IsStale()` / `StaleReason()` expose drift for health
  checks and alerting.
- **Pluggable backends** — `ProviderEtcd` and `ProviderInfisical` built in; the
  `client.Client` contract lets you add your own.
- **Panic-safe polling** — a panicking client marks entries stale instead of
  killing the refresh loop.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/confloader
```

## How values are stored

The standardized model is **namespace → folder → key** (no subfolders):

- **etcd**: keys at `<namespace>/<folder>/<key>`.
- **Infisical**: `namespace` is the workspace/project ID, `folder` is the
  secret path (`/<folder>`, `default` → `/`), and an `environment` (e.g.
  `prod`) is required.

## Step-by-step

### 1. Declare your config struct

Every config field is a `Getter[T]` tagged with
`conf:"folder=...,key=..."`. Plain (non-`Getter`) fields are left untouched.

```go
type DBConfig struct {
    Host string `json:"host"`
    Port int    `json:"port"`
}

type AppConfig struct {
    DBConfig Getter[DBConfig] `conf:"folder=default,key=db_config"`
    Debug    Getter[bool]     `conf:"folder=settings,key=debug"`
}
```

Values are decoded with YAML (which also accepts JSON), so `json`-tagged
structs work out of the box. Any `encoding.TextUnmarshaler`
(`time.Duration`, `net.IP`, ...) works too.

### 2. Build the loader

```go
loader, err := confloader.New[AppConfig](ctx, confloader.Config{
    Provider:         confloader.ProviderEtcd,
    Endpoint:         "localhost:2379",
    AuthClientID:     "username",
    AuthClientSecret: "password",
    Namespace:        "my-project",
})
if err != nil { /* handle */ }
defer loader.Stop() // stops polling and closes the client
```

The blocking initial fetch is bounded by `ctx`. For a lazy start that returns
immediately even when the backend is down, pass `WithInitialFetch(false)`.

### 3. Read values

```go
cfg := loader.Data()

dbConfig, err := cfg.DBConfig.Get(ctx)
if err != nil { /* source error, timeout, or ErrNotFound */ }

debug := cfg.Debug.GetWithDefault(ctx, false) // never fails; returns fallback
```

### 4. Watch changes

Values are re-fetched on the polling interval; `Get` also refreshes stale
entries on read. Register an error handler for refresh failures:

```go
loader, err := confloader.New[AppConfig](ctx, cfg,
    confloader.WithErrorHandler(func(folder, key string, err error) {
        logs.Error(ctx, "config refresh failed", "folder", folder, "key", key, errs.SlogAttr(err))
    }),
)
```

### 5. Health checks

```go
if loader.IsStale() {
    // some entry is serving last-known-good data
}
if err := loader.StaleReason("settings", "debug"); err != nil {
    // the underlying refresh error for that entry
}
```

## Configuration

| Field | Meaning |
| --- | --- |
| `Provider` | `"etcd"` or `"infisical"` |
| `Endpoint` | Provider host[:port] (`localhost:2379`) |
| `AuthClientID` | Username / client ID |
| `AuthClientSecret` | Password / client secret |
| `Namespace` | etcd root prefix / Infisical workspace ID |
| `Environment` | Required for Infisical; ignored by etcd |
| `Watcher` | Polling defaults: interval 30s, max retries 3, retry delay 1s, backoff 2x |

## Options

| Option | Effect |
| --- | --- |
| `WithClient(c client.Client)` | Inject a pre-built backend (tests, custom providers) |
| `WithInitialFetch(do bool)` | `false` = lazy start, entries populate on first poll |
| `WithErrorHandler(fn)` | Called for every refresh/parse error |

## Errors

- `Getter.Get` returns the **source's own error** on failure and `ErrNotFound`
  for a definitively absent key (after default-folder fallback).
- Config validation errors: `ErrInvalidProvider`, `ErrInvalidEndpoint`,
  `ErrInvalidAuthClientID/Secret`, `ErrInvalidNamespace`,
  `ErrInvalidEnvironment`, `ErrUnsupportedProvider`.
- Tag errors: `ErrMissingTag`, `ErrInvalidTag`, `ErrTagMissingKey`,
  `ErrTagMissingFolder`, `ErrRootFolder`, `ErrInvalidGetterType`.
- Staleness: `ErrStale` describes the state inspected via `IsStale` /
  `StaleReason` — it is **never returned by `Get`**.

## API reference

| Symbol | Description |
| --- | --- |
| `Config` / `WatcherConfig` | Provider connection + polling settings |
| `New[T](ctx, cfg, opts...)` | Build the loader; `T` must be a struct |
| `(*Loader[T]).Data()` | The populated config struct of `Getter[T]` fields |
| `Getter[T].Get(ctx)` | Latest value; refreshes on miss/stale; source error on failure |
| `Getter[T].GetWithDefault(ctx, def)` | Non-failing read with fallback |
| `(*Loader[T]).Stop()` | Stop polling + close client; idempotent |
| `IsStale()` / `StaleReason(folder, key)` | Drift health signals |
| `WithClient` / `WithInitialFetch` / `WithErrorHandler` | Construction options |
| `ProviderEtcd` / `ProviderInfisical` / `DefaultFolder` | Constants |

## Notes

- Concurrency: a `Loader` is safe for concurrent use; each `Getter` is
  thread-safe. Concurrent cold/stale reads coalesce into a single fetch.
- Always `defer loader.Stop()` to release the polling goroutine and the
  backend connection.
- Polling ticks self-reschedule (a slow refresh never overlaps the next tick).
