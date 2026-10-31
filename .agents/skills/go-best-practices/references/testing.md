# Testing

Sources: [decisions#useful-test-failures](https://google.github.io/styleguide/go/decisions#useful-test-failures), [decisions#test-structure](https://google.github.io/styleguide/go/decisions#test-structure), [best-practices#tests](https://google.github.io/styleguide/go/best-practices#tests).

In this repository, the Test layout section of SKILL.md governs test file placement, test function names, table field naming (`tests`/`tt`, `name` first), and integration-test gating. This file covers what the style guide adds on top.

The goal: **a maintainer can diagnose a failure from its output alone, without reading the test source.**

## Contents

- Use package `testing`, not assertion libraries
- Failure messages
- Comparing values
- Keep going: `t.Error` vs. `t.Fatal`
- Goroutines in tests
- Table-driven tests and subtests
- Test helpers
- Testing errors
- Test doubles and real transports
- Setup scope, `TestMain`, `sync.Once`
- Contexts in tests
- Acceptance-test APIs

## Use package `testing`

Use the standard `testing` package only: top-level tests, subtests, benchmarks, fuzz tests, examples, logging, and failures. No third-party assertion frameworks (testify `assert`/`require`, gomega, goconvey), and don't write your own:

```go
// Bad
assert.IsNotNil(t, "obj", obj)
assert.StringEq(t, "obj.Type", obj.Type, "blogPost")

// Good
want := BlogPost{Comments: 2, Body: "Hello, world!"}
if !cmp.Equal(got, want) {
	t.Errorf("Blog post = %v, want = %v", got, want)
}
```

Assertion helpers either stop the test too early or drop the context that makes a failure understandable, and they fragment the codebase into competing styles. For a reusable domain check, return a value or an `error` that the test function turns into a failure. Don't take `*testing.T` and fail inside the helper:

```go
// polygonCmp is a cmp.Option, not an assertion: callers decide how to fail.
func polygonCmp() cmp.Option { ... }

if diff := cmp.Diff(want, got, polygonCmp()); diff != "" {
	t.Errorf("Fencepost(tomsDiner, 1m) returned unexpected diff (-want +got):\n%v", diff)
}
```

Existing files that already use an assertion library keep working. Don't migrate unrelated tests, but write new tests with plain `testing`.

## Failure messages

The standard shape is:

```go
t.Errorf("YourFunc(%v) = %v, want %v", input, got, want)
```

- **Identify the function**: `YourFunc(...)`, not just `got %v, want %v`.
- **Identify the input**, if it is short. If the input is large or opaque, give the case a descriptive name and print that instead.
- **Got before want.** Use the words "got" and "want", not "actual" and "expected".
- **Diffs** of large values: print a newline before the diff and state its direction: `"Func(%q) mismatch (-want +got):\n%s"` for `cmp.Diff(want, got)`. Use `(-got +want)` if you pass them the other way.
- **Detail level**: if a test calls the function several times, say which call failed. For big structs, describe the important parts. Setup failures need less detail: `t.Fatalf("Setup: create test database: %v", err)`.
- Use `%q` for strings (empty and odd values stand out), `%+v` for small structs, and a diff for big values.
- Trigger the failure once during development to check the message is actionable.

Test output messages are capitalized normally ("Setup: ..."), unless they begin with an unexported identifier (`arrangeInputs(%q) = ...`).

## Comparing values

- `==` for scalars and comparable structs. Pointers compare by identity, not by pointee.
- `slices.Equal`, `maps.Equal`, `bytes.Equal` for those types.
- `cmp.Equal` / `cmp.Diff` (github.com/google/go-cmp) for complex structures, with `protocmp.Transform()` for protos and `cmpopts` (`IgnoreFields`, `EquateEmpty`, `EquateApprox`, `IgnoreInterfaces`) for semantic equality. Use it when the module already depends on it. Don't add a dependency for a single comparison that `==` or `slices` can do.
- **Not `reflect.DeepEqual`.** It is sensitive to unexported fields and implementation details, and it gives no diff.
- `cmp` is for tests only. It may panic on misuse, which is unacceptable in production.
- Compare whole structs, not field by field. Build the expected value and compare once, unless irrelevant fields would obscure what the test checks. Multiple return values can be compared one by one without wrapping them in a struct.
- **Compare stable, semantic results.** Don't string-compare JSON or other serialized output from packages you don't own (their formatting can change). Unmarshal and compare the data.

## Keep going: `t.Error` vs. `t.Fatal`

- Report mismatches with `t.Error`/`t.Errorf` so one run shows every broken property.
- Use `t.Fatal` when continuing would be meaningless or misleading: setup failed, or a step's output feeds the next step.

```go
gotEncoded := Encode(input)
if gotEncoded != wantEncoded {
	t.Fatalf("Encode(%q) = %q, want %q", input, gotEncoded, wantEncoded) // decoding garbage is pointless
}
gotDecoded, err := Decode(gotEncoded)
if err != nil {
	t.Fatalf("Decode(%q) returned unexpected error: %v", gotEncoded, err)
}
if gotDecoded != input {
	t.Errorf("Decode(%q) = %q, want %q", gotEncoded, gotDecoded, input)
}
```

- In table tests **with** `t.Run`: `t.Fatal` ends only that subtest, which is fine for per-row failures. **Without** subtests: use `t.Error` and `continue`.
- `t.Fatal` before the table loop is for whole-test setup.

## Goroutines in tests

`t.Fatal`, `t.FailNow`, `t.Skip*`, and the like must be called only from the goroutine running the test or subtest. Inside a goroutine you started, use `t.Error` and `return`. `t.Parallel()` doesn't change this.

```go
var wg sync.WaitGroup
for range num {
	wg.Go(func() {
		if err := engine.Vroom(); err != nil {
			t.Errorf("No vroom left on engine: %v", err) // not t.Fatalf
			return
		}
	})
}
wg.Wait()
```

## Table-driven tests and subtests

- Use a table when many cases share the same checking logic. When cases need different logic, write separate test functions (for example one table for valid outputs and one for error cases). Don't branch inside the loop on per-row mode flags. That "data-driven trap" makes rows hard to read:

```go
// Bad: the row picks which codex to build.
switch test.codex {
case fake:
	codex = newFakeCodex()
case prod:
	codex = setupCodex(t)
}
// Good: TestDecode (real codex) and TestDecodeWithFake share a row type.
```

- A simple `wantErr bool` row is fine if it doesn't complicate the loop body.
- Use field names in table literals when the table is long, when adjacent fields share a type, or when you want to omit zero fields. Omit failure-related fields from success rows.
- Never identify a row by its index (`"case #%d"`). Use a name or print the inputs. Even with `t.Run`, failure messages still print the inputs.
- Subtests must not depend on each other's execution or state. Each must run alone with `-run`.
- Subtest names should be short and readable, and typeable on the command line. Avoid `/`, which `-run` treats as a separator. Put long descriptions in a `desc` field that is printed in failures. (In this repo, descriptive names with spaces are allowed.)

## Test helpers

A test helper does **setup or cleanup**. Its failures mean the *environment* is broken, not the code under test.

- Call `t.Helper()` so failures are reported at the caller's line.
- Fail inside the helper with `t.Fatalf` and a message that says what broke ("Setup failed: could not write pak0 asset: %v"). That keeps call sites clean, and is better than returning an error for every test to check.
- Parameter order: `ctx`, then `t`, then the rest.
- Register teardown with `t.Cleanup`.
- If nothing in the helper can fail, drop the `t` parameter.
- Name helpers that fail the test `mustXxx`. They are handy in table literals: `data: mustMarshalAny(t, msg)`.
- `t.Helper` must not be used to build assertion libraries.

## Testing errors

- Test the error's *semantics*, not its text. Message strings change, and tests that compare them become change-detectors.
- Often only presence matters: `if gotErr := err != nil; gotErr != tt.wantErr { t.Errorf("f(%q) = %v, want error presence = %v", tt.input, err, tt.wantErr) }`.
- For specific errors, use `errors.Is(err, ErrX)` or `cmp.Diff` with `cmpopts.EquateErrors()`. If every `wantErr` is nil or "any error", use a `bool` instead (least mechanism).
- A string check is fine for a property of *your own* message, such as "includes the parameter name".

## Test doubles and real transports

- Test through the public API of the real implementation where you can.
- For HTTP or RPC integrations, use the production client against a test server: `httptest.NewServer(handler)`, or an in-process gRPC server with a fake service implementation. Don't hand-write a fake client behind a new interface.
- When a double is needed, put shared ones in a `<pkg>test` package. Name them by behavior (`AlwaysCharges`) or type (`StubService`), and prefix local variables (`spyCC`). See naming.md.
- Prefer the testing library the service's authors provide, if one exists.

## Setup scope, `TestMain`, `sync.Once`

- Scope setup to the tests that need it. Call `mustLoadDataset(t)` in each test that uses it, not in `init()` or a package var that every test (and `-run TestUnrelated`) pays for.
- Use `TestMain` only when *all* tests need an expensive setup that *requires teardown*. Put the work in a `runMain(ctx, m) (int, error)` so defers run before `os.Exit`.
- `sync.Once` amortization fits when the setup is expensive, used by only some tests, and needs no teardown (store the error alongside the data, and `t.Fatal` in each caller). Avoid it if the setup takes a context.
- Keep tests hermetic. Reset any global state a test modifies.

## Contexts in tests

Use `t.Context()` for the test's context, and pass it explicitly to helpers. It is cancelled before `t.Cleanup` callbacks run, so cleanup uses `context.Background()` or `context.WithoutCancel(ctx)`.

## Acceptance-test APIs

When you ship a way for *other* packages to validate their implementations of your interface (like `fstest.TestFS`), put it in `<pkg>test`. Make it accept the implementation and return an `error` describing which invariant broke, either fail-fast or as an aggregated error type. Call `t.Fatal` only for setup failures. End users then write:

```go
if err := chesstest.ExerciseGame(t, chesstest.SimpleGame, player); err != nil {
	t.Errorf("Deep Blue player failed acceptance test: %v", err)
}
```
