# go-dev-sdk

Personal go-to SDK for building Go applications.

## Modules

Each package is an independent Go module, importable directly:

| Module | Import path | Purpose |
| --- | --- | --- |
| `errs` | `github.com/fikrimohammad/go-dev-sdk/errs/v2` | Structured errors carrying an integer code, cause, metadata, and stack |
| `errgroup` | `github.com/fikrimohammad/go-dev-sdk/errgroup` | Concurrency-limited, panic-safe errgroup wrapper |
| `appinfo` | `github.com/fikrimohammad/go-dev-sdk/appinfo` | Standardized service identity (name/version/env) |
| `observability` | `github.com/fikrimohammad/go-dev-sdk/observability` | OpenTelemetry logs, metrics, and traces |
| `confloader` | `github.com/fikrimohammad/go-dev-sdk/confloader` | Layered config: file + dynamic (etcd) + secrets (Infisical) |
| `apiserver` | `github.com/fikrimohammad/go-dev-sdk/apiserver` | Hertz HTTP server wiring and middleware |
| `db` | `github.com/fikrimohammad/go-dev-sdk/db` | sqlx-based MySQL pool with queryer abstraction |
| `redis` | `github.com/fikrimohammad/go-dev-sdk/redis` | Redis client with metrics/tracing |
| `rocketmq` | `github.com/fikrimohammad/go-dev-sdk/rocketmq` | RocketMQ producer/consumer with middleware and telemetry |
| `s3` | `github.com/fikrimohammad/go-dev-sdk/s3` | AWS S3 client with multipart upload and presigned URLs |

## Development

The repository is a Go workspace; all modules are listed in `go.work`.

```bash
# build every module
go build ./...

# test every module (mockey requires inlining disabled)
go test -count=1 -gcflags="all=-N -l" ./...

# run inside a single module
(cd errs && go test ./...)
```

## CI / Releases

GitHub Actions workflows live in `.github/workflows/`:

- **`ci.yml`** — on PR + push to main. Discovers every Go module and runs a
  matrix job per module: `gofmt`, `golangci-lint` (config at repo root,
  `.golangci.yml`), `go vet`, `go test -gcflags="all=-N -l"`, and a
  `go mod tidy -diff` check. Tests must use the inlining-disabled flag because
  of `mockey`.
- **`coverage-report.yml`** — on PR. Runs tests with coverage per module and
  posts a per-module coverage table as a PR comment.
- **`release.yml`** — on push to main. Tags each module independently when
  commits touched it, following Conventional Commits: `feat!:` / `BREAKING
  CHANGE:` → major, `feat:` → minor, otherwise patch. Modules without commits
  since their last tag are skipped.

CI does not rely on `go.work` (it is gitignored); each module builds standalone
via its `replace` directives.

## Versioning

Modules are tagged independently (`errs/v1.0.0`, `observability/v1.0.0`, ...). Cross-module dependencies use `require` + `replace` directives pointing at sibling directories during development.
