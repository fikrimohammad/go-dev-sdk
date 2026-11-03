# AGENTS.md

Repository-wide instructions for AI agents and contributors.

This repository is a Go workspace of independent modules (each directory with a
`go.mod`). These instructions apply inside every module. A nested `AGENTS.md`
overrides this file for its subtree.

Apply the rules to new and modified code. When you change an existing package
that does not yet comply, fix the declarations you are already touching; do not
perform unrelated repo-wide rewrites.

## Non-negotiable invariants

- Unit test files mirror their source file: `foo.go` → `foo_test.go`. Tests for
  a declaration live in that declaration's test file. The only cross-cutting
  exceptions are `helpers_test.go`, `mock_test.go` / `mocks_test.go`,
  `example_test.go`, `integration_test.go`, and `parity_test.go`.
- Integration tests live in `integration_test.go` and must call `t.Skip` when
  their required environment variable is unset, so `go test ./...` remains
  hermetic.
- Use modern Go idioms available in each module's `go.mod` version, and
  follow the [Google Go Style Guide](https://google.github.io/styleguide/go/).
  `.golangci.yml` (golangci-lint >= v2.6) enforces its mechanical rules plus
  `modernize`. Every module must pass `golangci-lint run ./...` on the full tree
  and gostyle on `./...`, both with zero findings, except the modules in
  `LAX_MODULES` (see Verification).
- Client packages may export an interface describing their caller-facing
  surface (so users can generate mocks, e.g. with gomock), and constructors may
  return it. This overrides the go-best-practices "return concrete types" rule
  for those interfaces only. `ireturn` allows SDK interfaces for this reason.
- Every package and every exported declaration or struct field follows
  [Go Doc Comments](https://go.dev/doc/comment). Package comments start with
  `Package <name> ...`.
- All tracing and metrics follow the OpenTelemetry
  [trace](https://opentelemetry.io/docs/specs/semconv/general/trace/),
  [metrics](https://opentelemetry.io/docs/specs/semconv/general/metrics/), and
  [naming](https://opentelemetry.io/docs/specs/semconv/general/naming/)
  conventions, plus the relevant domain convention (DB, HTTP, messaging, RPC,
  object stores, exceptions). gostyle's `error-type-message` and
  `metric-high-cardinality` rules enforce a low-cardinality `error.type` and keep
  per-event attributes (query text, message IDs, URLs, user IDs) off metrics.
  Suppress a deliberate case with `// gostyle:ignore <rule> <reason>`.
- Config methods: every package `Config` has exported value-receiver
  `SetDefaults() Config` (fills zero values, returns the copy) and
  `Validate() error`. `Validate` reports every problem with `errors.Join` and
  returns nil when usable. Constructors call `cfg = cfg.SetDefaults()` and then
  `cfg.Validate()`.
- Examples: every package with exported API has `example_test.go` in package
  `<pkg>_test`. Examples are runnable with `// Output:` when no external service
  is needed, and compile-only (no `// Output:`) when they need a database,
  Redis, S3, or NATS. Test-only and internal packages are exempt.
- Errors: a package's exported errors (sentinels, aliases, error types and their
  methods) live in `errors.go`.
- Interfaces: all interface types live in `<pkg>.go`, the file named after the
  package (as in `observability/logs/logs.go`). No `interfaces.go`.

## Load the detailed skill before acting

The detailed procedures live in project skills under `.agents/skills`. Load the
matching skill before writing or reviewing code in these areas. When a task
touches more than one area, load each relevant skill.

| Area | Skill | Load when |
| --- | --- | --- |
| Telemetry | `opentelemetry-semconv` | Adding or changing spans, metrics, attributes, units, or telemetry names |
| Go code, tests, docs | `go-best-practices` | Writing, changing, refactoring, or reviewing any Go code, test files (placement, tables, mocks, integration tests), or doc comments |

If a skill cannot be loaded, follow the invariants above and the linked
specifications.

Skill files:

- [opentelemetry-semconv](.agents/skills/opentelemetry-semconv/SKILL.md)
- [go-best-practices](.agents/skills/go-best-practices/SKILL.md)

## Verification

Run from inside the module you changed:

```bash
gofmt -l .
go vet ./...
go test -count=1 ./...
golangci-lint run ./...
GOWORK=off go run ../.agents/skills/go-best-practices/scripts/gostyle.go ./...
go mod tidy -diff
```

Every module is strict: `golangci-lint run ./...` on the full tree and gostyle
on `./...` must both report zero findings. The exception is `LAX_MODULES` in
`.github/workflows/ci.yml`, today only `confloader`. A lax module passes
`golangci-lint run --new-from-merge-base=main ./...` and skips gostyle until its
redesign. The list may only shrink: remove a module in the commit that makes it
clean. Adding a module needs the owner's approval.

Integration tests are opt-in:

```bash
DB_MYSQL_DSN='app:secret@tcp(127.0.0.1:3306)/report_db' \
  go test -run TestMySQLIntegration ./...

DB_POSTGRES_DSN='postgres://app:secret@127.0.0.1:5432/report_db?sslmode=disable' \
  go test -run TestPostgresIntegration ./...
```

CI runs the MySQL and PostgreSQL integration tests in the `db-integration` job,
against service containers, and fails if either one is skipped.

## Releasing

- One version covers every module. It lives in `VERSION`, and every sibling
  `require` in every `go.mod` must equal it. CI runs
  `.github/scripts/release.sh "$(cat VERSION)" --check` to enforce this.
- Release by running `gh workflow run release.yml` on `main`. The workflow tags
  every module as `<module>/<version>` on the same commit, in one atomic push.
  Then `verify-install` proves that each module installs from proxy.golang.org.
- Never reuse a version. The proxy and checksum database keep a pushed version
  forever. Fix a bad release forward with a new patch version.
- After a release, bump `VERSION` and every sibling `require` together before
  the next release.

## Definition of done

- [ ] New or changed unit tests live in the matching `<source>_test.go`, or
      in a documented exception file.
- [ ] New integration tests live in `integration_test.go` and skip without
      their environment variable.
- [ ] New or changed exported declarations and fields have Go doc comments.
- [ ] New or changed telemetry follows the semantic conventions in the
      `opentelemetry-semconv` skill.
- [ ] New or changed Go code follows the `go-best-practices` skill.
- [ ] New or changed code follows the Config, example, `errors.go`, and
      `<pkg>.go` conventions above.
- [ ] The verification commands above pass, including `modernize`. For a
      strict module, `golangci-lint run ./...` and gostyle `./...` report zero
      findings. A `LAX_MODULES` module passes
      `golangci-lint run --new-from-merge-base=main ./...`.
