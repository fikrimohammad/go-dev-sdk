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
  `modernize`; new and changed code must pass
  `golangci-lint run --new-from-merge-base=main ./...` for each module.
- Every package and every exported declaration or struct field follows
  [Go Doc Comments](https://go.dev/doc/comment). Package comments start with
  `Package <name> ...`.
- All tracing and metrics follow the OpenTelemetry
  [trace](https://opentelemetry.io/docs/specs/semconv/general/trace/),
  [metrics](https://opentelemetry.io/docs/specs/semconv/general/metrics/), and
  [naming](https://opentelemetry.io/docs/specs/semconv/general/naming/)
  conventions, plus the relevant domain convention (DB, HTTP, messaging, RPC,
  object stores, exceptions).

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
go test -count=1 -gcflags="all=-N -l" ./...
golangci-lint run --new-from-merge-base=main ./...
GOWORK=off go run ../.agents/skills/go-best-practices/scripts/gostyle.go .
go mod tidy -diff
```

Existing code predates the style-guide lint rules, so lint only what changed
since `main` (CI uses `only-new-issues`). Fix `gostyle` findings in code you
wrote or touched.

Integration tests are opt-in:

```bash
DB_MYSQL_DSN='app:secret@tcp(127.0.0.1:3306)/report_db' \
  go test -run TestMySQLIntegration ./...

DB_POSTGRES_DSN='postgres://app:secret@127.0.0.1:5432/report_db?sslmode=disable' \
  go test -run TestPostgresIntegration ./...
```

## Definition of done

- [ ] New or changed unit tests live in the matching `<source>_test.go`, or
      in a documented exception file.
- [ ] New integration tests live in `integration_test.go` and skip without
      their environment variable.
- [ ] New or changed exported declarations and fields have Go doc comments.
- [ ] New or changed telemetry follows the semantic conventions in the
      `opentelemetry-semconv` skill.
- [ ] New or changed Go code follows the `go-best-practices` skill.
- [ ] The verification commands above pass, including `modernize` and the
      style-guide rules through `golangci-lint run --new-from-merge-base=main ./...`.
