---
name: go-best-practices
description: Apply the Google Go Style Guide (guide, style decisions, and best practices) whenever writing, modifying, refactoring, or reviewing Go code. Use for every .go change - new packages, functions, types, error handling, interfaces, option structs, contexts, goroutines, tests and test files, and doc comments - and for Go code reviews, even when the user does not mention style or best practices.
whenToUse: Load before writing or changing any Go code, *_test.go file, or doc comment, and before reviewing Go code or a Go diff.
---

# Go best practices (Google Go Style Guide)

The mechanical rules of the [Google Go Style Guide](https://google.github.io/styleguide/go/) and [Effective Go](https://go.dev/doc/effective_go) are enforced by tools. These include naming, initialisms, receivers, error strings, stop words in wrapping, ctx placement, nil slices, imports, test wording, and assertion libraries. `.golangci.yml` maps each linter to its guide section, and `scripts/gostyle.go` covers the few rules no linter can express. Don't pre-check those by hand. Write the code, run the tools, and fix what they report.

This file holds the rules that need judgment. Apply them while you write.

## Precedence

1. Repository instructions win where they apply: `AGENTS.md` and `opentelemetry-semconv`. This skill includes the repository's test layout and doc-comment conventions.
2. Then the Google guide, then decisions, then best practices, then Effective Go, which they build on. Effective Go dates from 2009, and where its examples differ, the Google guide and this repository win. That covers recovering panics to keep a server running, `init()` that validates the environment or registers flags, labeled `break`, and buffered-channel semaphores. Local consistency settles only what the guides leave open.
3. Apply the rules to code you write or touch. Don't rewrite unrelated code, and don't spread an existing deviation.

## Workflow

1. Read the package you are changing. Note its error style, option patterns, receiver names, and test table shape, and reuse them.
2. Write the code using the design rules below.
3. From the module directory, run:

   ```bash
   golangci-lint run --new-from-merge-base=main ./...
   GOWORK=off go run <repo>/.agents/skills/go-best-practices/scripts/gostyle.go <changed package dirs>
   ```

   For a standalone module outside this repo, use `golangci-lint run -c <repo>/.golangci.yml ./...` instead. Fix every finding in code you wrote. To keep one on purpose, add `//nolint:<linter> // <reason>` or `// gostyle:ignore <rule> <reason>`.
4. Run the rest of the `AGENTS.md` verification commands.

This file plus the tools is all you need to write code. Don't read `references/` or `.golangci.yml` up front: every file you read is re-sent on every later turn, so it costs tokens for the rest of the session. Open one reference only if you are stuck on a specific decision, or when a review needs a guide citation (see the end of this file).

## Rules the tools can't check

**Errors**
- Return `error` from functions that can fail, never `*MyError`. A nil pointer stored in an `error` is non-nil.
- No in-band errors: return `(v, ok bool)` or `(v, error)`, never `-1` or `""` meaning "not found".
- When callers must branch on a failure, give it structure: exported sentinels or error types. Callers use `errors.Is`/`errors.As`, never `err.Error()` text.
- Treat `%w` vs `%v` as an API decision. `%w` exposes the cause as part of your contract. `%v` hides it, which is right at RPC and storage boundaries and for implementation details.
- An annotation must add context the inner error lacks. If there is nothing to add, `return err`.
- Don't log and return the same error. The caller decides.
- Panic only for API misuse and broken invariants, or in `Must*` helpers at init. `main` calls `run() error`, prints the error, and calls `os.Exit(1)`.
- Error strings name where they come from, so they make sense far from the call. Exported sentinels start with the package name: `errors.New("money: currency mismatch")`, like `image: unknown format`.

**Interfaces and API shape**
- Create an interface only for a present need. That means two or more implementations behind the same logic, breaking an import cycle, or a consumer that needs a small part of a big type. "Service", "repository", and "for mocks" are not needs.
- The consumer defines the interface, with only the methods it calls. Return concrete types. The exception is a type that exists only to implement a published interface and has no other exported methods. Export the interface and return it instead (`crc32.NewIEEE` returns `hash.Hash32`).
- When a type must satisfy an interface that no code in the package converts it to, add a compile-time check next to the type: `var _ json.Marshaler = (*Amount)(nil)`. This covers `json.Marshaler`, `fmt.Stringer`, and SDK contracts callers rely on. Skip it where a static conversion already exists.
- Make the zero value useful, as `bytes.Buffer` and `sync.Mutex` do. A method that writes to a map field creates the map lazily (`if l.m == nil { l.m = make(map[string]int) }`), so `var x T` is ready to use. When a type truly needs configuration, document `New` as the only way to get one.
- Embed a type in a struct only when every method of the embedded type belongs in the outer type's API. Embedding `sync.Mutex` in an exported struct exports `Lock` and `Unlock`, so use a named field instead.
- Adapt plain functions to a one-method interface with a function type that implements it, as `http.HandlerFunc` does.
- Test HTTP and gRPC through real transports (`httptest.NewServer`, an in-process server). Put doubles in a `<pkg>test` package, never in production code.
- Use generics only when more than one type is used in practice.
- Many parameters call for an option struct (and `ctx` never goes in it). Use functional options only when most callers pass none and there are many options.
- No package-level mutable state in libraries: no registries, singletons, or lazy clients. Callers construct instances and pass them in.
- Use pointer receivers when a method mutates, the type holds a mutex or other uncopyable field, the type is large, or you are unsure.

**Naming beyond the linter**
- Don't repeat what the call site already shows: the package name (`widget.New`), the receiver (`c.WriteTo`, not `c.WriteConfigTo`), parameter or result types (`Parse`, not `ParseYAMLConfig`), or type words (`users`, not `userSlice`).
- Name length grows with scope. Name a constant for its role, not its value.
- Name one-method interfaces after their method with an *-er* suffix (`Reader`, `Notifier`). Use `Read`, `Write`, `Close`, `Flush`, or `String` only with the standard signature and meaning. When a method has one of those meanings, use the standard name: `String()`, never `ToString()`. Setters are `SetOwner`.

**Concurrency and context**
- Prefer synchronous functions that return results, not channels or callbacks.
- Every goroutine has a visible end before the function returns and stops on `ctx.Done()`. No fire-and-forget `go f()` in libraries.
- For bounded fan-out, use `errgroup.WithContext` with `g.SetLimit(n)`: it joins the goroutines, cancels the rest on the first error, and returns that error. Don't hand-roll channel semaphores, `WaitGroup` plus a mutex-guarded first error, or labeled `break`/`continue` loops (gostyle flags labels).
- To outlive the caller's cancellation, use `context.WithoutCancel(ctx)` with its own timeout.
- Make waits cancellable: `select` on `ctx.Done()` and a timer, never `time.Sleep`.
- Specify channel direction in signatures. Only the sender closes.

**Test layout (repository conventions)**
- Unit tests mirror the source file: tests for a declaration in `foo.go` go in `foo_test.go`. Tests for a package's public API go in its main test file. Don't spread one file's tests across several test files, and don't create feature-named files (`postgres_test.go` for code in `drivers.go`).
- The only test files without a matching source file are `helpers_test.go`, `mock_test.go`/`mocks_test.go`, `example_test.go`, `integration_test.go`, and `parity_test.go`. Each starts with a header comment saying what it holds.
- Use `package foo` when tests need unexported access. Use `package foo_test` only for public-API tests.
- Each package has at most one `integration_test.go`. It is the only place tests may use real services, and it calls `t.Skip` when its environment variable is unset.
- Name tests `Test<Identifier>` or `Test<Identifier>_<Behavior>`, keeping the identifier's spelling (`mysqlDSN` becomes `TestMySQLDSN`).
- Use table-driven tests when there is more than one case. New files use `tests := []struct{ name string; ... }` with `for _, tt := range tests { t.Run(tt.name, ...) }`, with `name` as the first field. Subtest names describe behavior and contain no `/`. Files that already use `cases`/`c` keep that style. Don't mix the two in one file.
- When cases need different checking logic, write separate test functions. Don't branch inside the loop on per-row mode flags.

**Tests**
- Failure messages name the function and its input, and print got before want: `t.Errorf("Parse(%q) = %v, want %v", in, got, want)`. For diffs, state the direction: `(-want +got)`.
- Use `t.Error` for mismatches so one run shows every failure. Use `t.Fatal` only when continuing is meaningless, such as failed setup or an error that makes later checks moot. Inside `t.Run`, `t.Fatalf` ends only that subtest.
- Helpers do setup, not assertions. They fail with context and register teardown with `t.Cleanup`.
- Tests are hermetic and independent of run order. Unit tests use no network, Docker, or credentials (use `httptest` or stubs instead). Reset any package-level state you change. Skip `t.Parallel()` when tests share package-level state, global fakes, or a database.
- Test error semantics (`errors.Is`, presence), not error text. Compare decoded data, not serialized bytes.
- Tests that use mockey need `-gcflags="all=-N -l"`.

**Doc comments** (the linter already checks that exported names are documented, that comments start with the name and end with a period, and that a package comment exists)
- Put the package comment, `Package <name> ...`, directly above the `package` clause in the main file or `doc.go`.
- Every exported struct field gets its own comment, or the type's comment explains all of them. Grouped consts may share one group comment.
- Format: separate paragraphs with a blank `//` line. Write lists as `//   - item`. Headings are `// # Heading` with blank lines around them. Code blocks are indented. Mark deprecations with a `Deprecated:` paragraph.
- There's no inline-code or Markdown syntax: no backticks, emphasis, or HTML. Refer to parameters by name. Link with `[Config]`, `[Config.Close]`, or `[sql.ErrNoRows]`. Unresolved links keep their brackets, so check with `go doc -all .`.
- Avoid writing a literal `''` or a pair of backticks: gofmt turns them into curly quotes.
- Content: describe caller-visible behavior, not internals. Document returned sentinel errors and error types, the zero value if it isn't obvious, panics, side effects, cleanup the caller must do (`Close`), and concurrency or context behavior that differs from the defaults. Boolean functions "report whether". Comments on unexported code explain why, not what.

## Google-internal guidance, translated

| Guide says | Write instead |
| --- | --- |
| `log.Exit` / `log.Fatal` (glog) | `main` calls `run() error`, prints the error, and calls `os.Exit(1)` |
| Internal context detachment | `context.WithoutCancel(ctx)` |
| Bazel `testonly` doubles | a `<pkg>test` package |
| `cmp.Diff` | go-cmp if the module already has it; otherwise `==`, `slices.Equal`, `maps.Equal` |

## Reviewing Go code

Run the tools on the change first and report their findings. Then review the rules above, which the tools can't judge. List correctness problems (leaks, nil-interface errors, ignored errors) before style. Link the guide section for each point, and don't nitpick code outside the change.

## References (only when stuck)

`references/` holds the full guide distillation with good and bad examples. The files are `naming.md`, `errors.md`, `api-design.md`, `concurrency.md`, `language.md`, `testing.md`, and `effective-go.md` (what Effective Go adds, and where it conflicts with the Google guide). Read only the section you need, for example with `grep -n -A20 '## Option struct' references/api-design.md`. Don't read whole files.
