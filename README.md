# go-servicekit

Personal go-to SDK for building Go applications.

## Modules

Each package is an independent Go module, importable directly:

| Module | Import path | Purpose |
| --- | --- | --- |
| `errs` | `github.com/fikrimohammad/go-servicekit/errs` | Structured errors carrying an integer code, cause, metadata, and stack |
| `errgroup` | `github.com/fikrimohammad/go-servicekit/errgroup` | Concurrency-limited, panic-safe errgroup wrapper |
| `identity` | `github.com/fikrimohammad/go-servicekit/identity` | Standardized service identity (name/version/env) |
| `observability` | `github.com/fikrimohammad/go-servicekit/observability` | OpenTelemetry logs, metrics, and traces |
| `confloader` | `github.com/fikrimohammad/go-servicekit/confloader` | Layered config: file + dynamic (etcd) + secrets (Infisical) |
| `rest` | `github.com/fikrimohammad/go-servicekit/rest` | Hertz HTTP server and client for gateway services: fixed telemetry, edge-safe defaults, RFC 9457 errors |
| `cron` | `github.com/fikrimohammad/go-servicekit/cron` | Recurring job scheduler with robfig/cron and middleware pipeline |
| `db` | `github.com/fikrimohammad/go-servicekit/db` | sqlx-based MySQL/PostgreSQL pool with named queries and telemetry |
| `nats` | `github.com/fikrimohammad/go-servicekit/nats` | NATS JetStream producer/consumer with middleware and telemetry |
| `redis` | `github.com/fikrimohammad/go-servicekit/redis` | Redis client with metrics/tracing |
| `s3` | `github.com/fikrimohammad/go-servicekit/s3` | AWS S3 client with multipart upload and presigned URLs |

## Development

The repository is a Go workspace; all modules are listed in `go.work`.

```bash
# build every module
go build ./...

# test every module
go test -count=1 ./...

# run inside a single module
(cd errs && go test ./...)
```

## CI / Releases

GitHub Actions workflows live in `.github/workflows/`:

- **`ci.yml`** — on PR + push to main. Discovers every Go module and runs a
  matrix job per module: `gofmt`, `golangci-lint` (config at repo root,
  `.golangci.yml`), `go vet`, `go test` (plus `-race`), and a
  `go mod tidy -diff` check. Every module except those in `LAX_MODULES` (today
  only `confloader`) is linted on the full tree and checked by gostyle.
  gostyle also has its own test job.
- **`coverage-report.yml`** — on PR. Runs tests with coverage per module and
  posts a per-module coverage table as a PR comment.
- **`db-integration`** job in `ci.yml` runs the MySQL and PostgreSQL integration
  tests against service containers.
- **`release-dry-run`** job in `ci.yml` runs the release script's unit tests,
  checks that every sibling `require` matches `VERSION`, and dry-runs the
  release while `VERSION` is unreleased.
- **`release.yml`** — manual only (`workflow_dispatch`); nothing is tagged on
  push. Run `gh workflow run release.yml` on main. It releases the version in
  `VERSION`, tags every module at that version in one atomic push, and then
  checks that each module installs from proxy.golang.org. It refuses a version
  that already exists as a tag or in proxy.golang.org.

CI does not rely on `go.work` (it is gitignored); each module builds standalone
via its `replace` directives.

## Versioning

All modules share one version, set in `VERSION` (currently `v0.1.0`). Every
sibling `require` must equal it, and CI checks this. Each release tags every
module as `<module>/<version>`. All modules are in development (v0), so any
release may break the API. Never reuse a version: fix a bad release forward
with a new patch version. See `AGENTS.md` for the full release steps.
