# Errors

Sources: [decisions#errors](https://google.github.io/styleguide/go/decisions#errors), [best-practices#error-handling](https://google.github.io/styleguide/go/best-practices#error-handling), [decisions#dont-panic](https://google.github.io/styleguide/go/decisions#dont-panic), [decisions#must-functions](https://google.github.io/styleguide/go/decisions#must-functions).

Errors are values. Code produces them and code consumes them. They are shown to humans, read by maintainers, and interpreted by callers. Every time you create or pass on an error, decide whether this function is the right place to handle it, and what the caller needs to know.

## Contents

- Returning errors
- Error strings
- Handling (never silently drop)
- In-band errors
- Structure: sentinels and types
- Adding information when wrapping
- `%w` vs `%v`
- Placement of `%w`
- Logging errors
- Indent error flow
- Panics, `Must`, and program exit
- Documenting errors

## Returning errors

- `error` is the last result. A `nil` error means success.
- When the error is non-nil, callers must treat the other results as unspecified unless the docs say otherwise.
- Functions that can fail return the `error` interface, not a concrete type:

```go
func Bad() *os.PathError { ... } // a nil *PathError in an error interface is != nil

func Good() error { ... }
```

A constructor whose purpose is to build an error value (for example `errs.New(code, msg) *Error`) may return the concrete type. The rule targets functions whose error result signals failure.

- A function that takes a `context.Context` should usually return an `error`, so the caller can tell whether it was cancelled.

## Error strings

Lowercase, no trailing punctuation. They are usually embedded in more context:

```go
err := fmt.Errorf("something bad happened")  // good
err := fmt.Errorf("Something bad happened.") // bad
```

Exceptions for the first word are exported names, proper nouns, and acronyms. Full displayed messages (log lines, test failures, UI) are capitalized normally: `log.Printf("Operation aborted: %v", err)`.

## Handling

When a function returns an error, do exactly one of these:

- Handle it and address it right there.
- Return it to the caller, usually with added context.
- In exceptional cases, in `main` or init, exit the program.

Discarding with `_` is rarely right. When it is (for example `(*bytes.Buffer).Write` is documented to never fail), leave a comment:

```go
n, _ := b.Write(p) // never returns a non-nil error
```

When orchestrating related operations where only the first failure matters, `errgroup` is the standard tool.

## In-band errors

Don't signal failure with special values (`-1`, `""`, `nil`):

```go
// Bad: Parse(Lookup(missingKey)) blames Parse for Lookup's miss.
func Lookup(key string) int // returns -1 if absent

// Good: the caller can't use the value without checking.
func Lookup(key string) (value string, ok bool)

value, ok := Lookup(key)
if !ok {
	return fmt.Errorf("no value for %q", key)
}
return Parse(value)
```

Use `ok bool` when no explanation is needed, and `error` otherwise. Either way it is the last result.

## Structure: sentinels and types

If callers need to tell failures apart, give the errors structure so they can check programmatically. This applies to tests as well as production code.

```go
var (
	// ErrDuplicate occurs if this animal has already been seen.
	ErrDuplicate = errors.New("duplicate")

	// ErrMarsupial occurs because we're allergic to marsupials outside Australia.
	ErrMarsupial = errors.New("marsupials are not supported")
)

switch err := process(an); {
case errors.Is(err, ErrDuplicate):
	return fmt.Errorf("feed %q: %v", an, err)
case errors.Is(err, ErrMarsupial):
	alternate = an.BackupAnimal()
	return handlePet(..., alternate, ...)
}
```

- Use `==` only when the error is never wrapped. Otherwise use `errors.Is`.
- When callers need data from the error, use a type with fields (like `os.PathError` with `Path`) and `errors.As`.
- Never branch on `err.Error()` text (`strings.Contains`, regexps). It breaks when the wording changes. If a dependency gives you only text, ask the owner for structure rather than parsing it.
- Status-code errors (gRPC `status` and `codes`) are a valid structure at RPC boundaries. Use canonical codes.

## Adding information when wrapping

Add context the caller doesn't have and the inner error doesn't already carry.

```go
// Good: says what this function was doing; os already includes the path.
if err := os.Open("settings.txt"); err != nil {
	return fmt.Errorf("load launch code settings: %v", err)
}
// -> load launch code settings: open settings.txt: no such file or directory

// Bad: duplicates the path.
return fmt.Errorf("could not open settings.txt: %v", err)
// -> could not open settings.txt: open settings.txt: no such file or directory

// Bad: adds nothing. Just return err.
return fmt.Errorf("failed: %v", err)
```

Name the *action* that failed, with its parameters, and leave out stop words ("failed to", "couldn't", "unable to"). Otherwise the chain reads `failed to X: failed to Y: failed to Z: ...`. When one function does several things, each annotation should say which step failed:

```go
func (s *server) setupTLS(certPath string) error {
	data, err := os.ReadFile(certPath)
	if err != nil {
		return fmt.Errorf("load certificate: %v", err) // os.ReadFile already names the path
	}
	if err := s.validateCert(data); err != nil {
		return fmt.Errorf("verify signature from %s: %v", certPath, err)
	}
	if err := s.applyCert(data); err != nil {
		return fmt.Errorf("apply certificate from %s: %v", certPath, err)
	}
	return nil
}
```

Not `"failed to setup TLS: %v"` three times.

## `%w` vs `%v`

The choice decides what callers can do with the error, so make it deliberately.

Use **`%v`** (the cause is flattened to text, so callers can't unwrap it):

- To add context purely for human readers.
- To log or display.
- At system boundaries (RPC, IPC, storage) where you translate into a canonical error space and don't want callers depending on your internals. For example, return a `codes.Internal` status instead of exposing a filesystem error to RPC clients.

Use **`%w`** (callers can `errors.Is`/`errors.As` the cause):

- Inside your application, when a higher layer legitimately needs to check the cause (`errors.Is(err, fs.ErrNotExist)`).
- When your package documents that it returns certain underlying errors. Wrapping makes that cause part of your API contract, so document and test it.

Don't use `errors.Unwrap` to inspect a chain. It doesn't handle multi-errors. Use `errors.Is`/`errors.As`.

## Placement of `%w`

Put `%w` at the end so the printed text reads newest to oldest, matching the chain:

```go
err2 := fmt.Errorf("err2: %w", err1) // "err3: err2: err1"  good
err2 := fmt.Errorf("%w: err2", err1) // "err1: err2: err3"  reversed
err2 := fmt.Errorf("a %w b", err1)   // scrambled
```

The exception is sentinel categorization. When the wrapped error is a category (a "not found" or "invalid argument" class), lead with it so the category is the first thing a reader sees:

```go
var ErrParse = errors.New("parse error")
var ErrParseInvalidHeader = fmt.Errorf("%w: invalid header", ErrParse)

return fmt.Errorf("%w: invalid character in header: %v", ErrParseInvalidHeader, err)
```

## Logging errors

- If you return an error, don't also log it. The caller decides whether to log, rate-limit, retry, or exit. Logging at each layer duplicates reports and creates log spam.
- Log messages should say clearly what went wrong and include what is needed to diagnose it, like a good test failure.
- Keep PII out of logs.
- Use the error level sparingly. It should mean "actionable", not "more serious than a warning". It is also more expensive.
- Don't compute expensive log arguments when the level is disabled. With `slog`, check `logger.Enabled(ctx, slog.LevelDebug)` before building them.

## Indent error flow

Handle errors first and keep the normal path at the left margin. This applies to any `if` that ends in `return`, `continue`, `break`, `panic`, or exit.

```go
// Good
if err != nil {
	return err
}
// normal code

// Bad
if err != nil {
	// handle
} else {
	// normal code that looks abnormal
}
```

If a variable from the `if` initializer is used for more than a few lines, declare it outside:

```go
x, err := f()
if err != nil {
	return err
}
// lots of code using x
```

An unusual condition such as `err == nil` deserves a comment so it isn't misread: `if err := doSomething(); err == nil { // if NO error`.

## Panics, `Must`, and program exit

**Don't panic for normal errors.** Libraries return errors, especially for transient failures.

Panic is acceptable when:

- An API is misused in a way that tests and review should catch (as with the stdlib's `reflect` or an out-of-range index).
- An internal invariant is violated and the state can't be recovered. Prefer to fail loudly at the top (in `main`) rather than deep in a library.
- Inside a package, as an internal control-flow shortcut (deep parsers), *only* when a deferred `recover` at the public API boundary turns it back into an error, and it re-panics on anything it didn't raise itself:

```go
func Parse(in string) (_ *Node, err error) {
	defer func() {
		if p := recover(); p != nil {
			sErr, ok := p.(*syntaxError)
			if !ok {
				panic(p) // not ours
			}
			err = fmt.Errorf("syntax error: %v", sErr.msg)
		}
	}()
	// ...
}
```

- Marking unreachable code after a call the compiler doesn't know never returns: `panic("unreachable")`.

Don't `recover` panics generally to "avoid crashes". The program may be holding locks or corrupted state, and you spread the corruption. Surface the panic and fix the bug.

**`Must` functions** (`MustParse`, `template.Must`, `regexp.MustCompile`) are for program initialization with constant inputs, typically package-level vars. Never call them on user input or in request paths where ordinary error handling works:

```go
func MustParse(version string) *Version {
	v, err := Parse(version)
	if err != nil {
		panic(fmt.Sprintf("MustParse(%q) = _, %v", version, err))
	}
	return v
}

var DefaultVersion = MustParse("1.2.3") // good

v := version.MustParse(o.GetVersionString()) // bad: return the error instead
```

Test helpers may follow the same convention with `t.Fatal` (see testing.md).

**Program initialization errors** (bad flags, bad config) propagate up to `main`. `main` prints a human-actionable message and exits non-zero. A stack trace pointing at a check helps less than "invalid -port: must be 1-65535". Open-source pattern:

```go
func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "mytool: %v\n", err)
		os.Exit(1) // os.Exit skips defers, so all cleanup lives in run
	}
}
```

## Documenting errors

- Document the sentinels and error types a function returns, so callers know what they can handle: "At end of file, Read returns 0, io.EOF."
- Say whether an error type is returned by pointer ("If there is an error, it will be of type *PathError"), because `errors.As` and comparisons depend on it.
- Put conventions that apply to most of a package's errors in the package doc comment.
