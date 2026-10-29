---
name: go-test-conventions
description: Use when adding, changing, or reviewing Go tests in this repository, including test file naming, test placement, table-driven cases, shared mocks/helpers, and env-gated integration tests.
whenToUse: Load before creating or modifying any *_test.go file, or when deciding which test file a new test belongs in.
---

# Go test conventions

Use this skill whenever you add or change Go tests. The invariants in the repository `AGENTS.md` still apply; this skill contains the detailed rules.

## File naming and placement

Unit test files mirror the source file they test:

| Source file | Test file |
| --- | --- |
| `foo.go` | `foo_test.go` |
| `drivers.go` | `drivers_test.go` |
| `bind.go` | `bind_test.go` |
| `config.go` | `config_test.go` |
| `db.go` | `db_test.go` |

- Tests for a declaration live in that declaration's source-file test. A test for `driverFor`, `mysqlDSN`, `postgresDSN`, `connInfo`, or `errorInfo` belongs in `drivers_test.go`, not in `db_test.go` or a driver-specific file.
- Do not create feature-named test files such as `postgres_test.go` to hold unit tests for declarations defined in `drivers.go`, `bind.go`, or `db.go`.
- Do not scatter tests for one source file across several test files.
- Tests for a package's public execution surface belong in that package's main test file. For example, `db_test.go` owns `Connect`, `Begin`, and `NamedExecContext`, even when binding is involved.
- Tests for helper declarations belong in the helper's source-file test. For example:
  - `bind_test.go` owns `bindQuery`, `rebind`, `expandSlices`, and `placeholderIndexes`.
  - `drivers_test.go` owns `driverFor`, `mysqlDSN`, `postgresDSN`, `connInfo`, and `errorInfo`.
- Keep unit tests in the same package (`package foo`) when they need unexported access. External `package foo_test` tests are acceptable only for public-API tests.
- If a test spans several source files, put it with the source file that owns the public behavior. If it is genuinely end-to-end, put it in `integration_test.go`.

## Allowed cross-cutting test files

A test file with no matching source file must use one of these documented names:

- `helpers_test.go` — shared test helpers.
- `mock_test.go` or `mocks_test.go` — shared fakes and mocks.
- `example_test.go` — runnable examples.
- `integration_test.go` — tests that need real external services.
- `parity_test.go` — cross-implementation or compatibility tests.

Add a short header comment to every exception file explaining its purpose and which tests use it.

### Integration tests

- Each package has at most one `integration_test.go`.
- It is the only place for tests that need real external services (MySQL, PostgreSQL, NATS, Redis, S3, etc.).
- Integration tests must call `t.Skip` when their required environment variable is unset, so the default `go test ./...` remains hermetic.
- Do not put integration tests in a unit test file.

## Test names and case pattern

- Unit test names: `Test<Identifier>` or `Test<Identifier>_<Behavior>`.
  - Examples: `TestDriverFor`, `TestMySQLDSN_SSLMode`, `TestBindQuery_NilArg`, `TestBeginTx_Commit`.
- Preserve the identifier's spelling in the test name: `mysqlDSN` becomes `TestMySQLDSN`, not `TestMysqlDsn`.
- Prefer table-driven tests when a test has more than one case. Keep the table shape consistent within a package. For new files, use this default:

```go
func TestThing(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty input", in: "", want: ""},
		{name: "trims spaces", in: " x ", want: "x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Thing(tt.in)
			if got != tt.want {
				t.Fatalf("Thing(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
```

- Existing files that use `cases` and `c` may keep that style; do not mix both styles in the same file.
- Keep `name` as the first field of each table entry.
- Subtest names describe behavior, not "case 1" or an input dump.
- Use `t.Fatalf` for setup failures and terminal assertion failures; use `t.Errorf` only when continuing the test is safe.
- One test function should verify one behavior or one small group of related behaviors.

## Reliability rules

- Test helpers must call `t.Helper()`.
- Use `t.Cleanup()` for teardown instead of ad-hoc defers in setup helpers.
- Tests must not depend on execution order. Reset package-level state in setup.
- Do not add `t.Parallel()` when a test uses shared package-level state, global fakes, or a shared database backend.
- Unit tests must not require network access, Docker, credentials, or a running service. Mock or use a stub driver instead.
- Mockey-based tests must be run with `-gcflags="all=-N -l"`.

## Verification

Run from inside the module directory:

```bash
gofmt -l .
go vet ./...
go test -count=1 -gcflags="all=-N -l" ./...
go mod tidy -diff
golangci-lint run ./...
```

Run real-service integration tests explicitly:

```bash
DB_MYSQL_DSN='app:secret@tcp(127.0.0.1:3306)/report_db' \
  go test -run TestMySQLIntegration ./...

DB_POSTGRES_DSN='postgres://app:secret@127.0.0.1:5432/report_db?sslmode=disable' \
  go test -run TestPostgresIntegration ./...
```

Before finishing, confirm:

- [ ] New or changed unit tests live in the matching `<source>_test.go`.
- [ ] Test names and table-driven cases follow the patterns above.
- [ ] Tests do not rely on execution order or hidden external services.
- [ ] New integration tests are in `integration_test.go` and skip without their environment variable.
