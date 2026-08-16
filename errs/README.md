# errs

Structured errors for Go services: an error carries an integer **code** (defined
by the application, not the SDK), a wrapped **cause**, **metadata**, and an
eagerly captured **stack trace**.

## Features

- **Plain integer codes** — `code` is a plain `int`; the SDK defines no error
  taxonomy. Each application owns its code constants (and any HTTP mapping).
- **Cause wrapping** — `Wrap` preserves the root cause; `errors.Is` / `errors.As`
  traversal works through the whole chain (`Unwrap`).
- **Code-based equality** — `Is` matches on equal codes, so a wrapped inner
  error is "the same error" as its unwrapped form.
- **Metadata & debug** — attach arbitrary key/value pairs (`WithMeta`) or a
  short debug detail (`WithDebug`) without mutating the original.
- **Stack traces** — captured at the call site; formatted on demand.
- **Structured logging** — implements `slog.LogValuer`; `SlogAttr` forces the
  structured path so `*Error` fields (code, message, debug, meta, cause, stack)
  show up as real structured fields instead of a plain string.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/errs/v2
```

## Step-by-step

### 1. Define your codes

Codes live in the application. Use whatever numeric scheme fits your domain:

```go
package codes

const (
    OK              = 0
    InvalidArgument = 1001
    NotFound        = 4004
    Internal        = 5001
)
```

### 2. Create an error

```go
// A fresh error with a stack trace.
return errs.New(codes.NotFound, "user not found")

// Wrap an underlying cause.
if err := repo.Get(ctx, id); err != nil {
    return errs.Wrap(codes.Internal, "failed to load user", err)
}
```

### 3. Attach context

`WithMeta` / `WithDebug` return a **new** error; the receiver is never mutated.

```go
return errs.Wrap(codes.Internal, "payroll run failed", err).
    WithDebug("worker id 7 panicked").
    WithMeta("run_id", runID)
```

### 4. Inspect on the way out

```go
var e *errs.Error
if errs.AsError(err) != nil {
    e = errs.AsError(err)
    code := e.Code()        // int
    dbg := e.Debug()        // debug detail or ""
    frames := e.StackFrames() // []string of formatted frames
}
```

Or use the standard library directly:

```go
var e *errs.Error
if errors.As(err, &e) {
    code := e.Code()
}
```

### 5. Log it structurally

```go
// *Error implements slog.LogValuer, but slog resolves plain error values
// before LogValuer, so use SlogAttr to get the structured fields.
logs.Error(ctx, "job failed", errs.SlogAttr(err))

// Handlers wired through errs.SlogAttr receive:
//   code=4004 message="user not found" cause=... stack=...
```

## API reference

| Symbol | Description |
| --- | --- |
| `New(code int, msg)` | New `*Error` with a stack trace |
| `Wrap(code int, msg, cause)` | New `*Error` wrapping a cause |
| `AsError(err)` | Extract the `*Error` in a chain, wrapping plain errors with code `0` |
| `(*Error).Code/Message/Cause/Debug/Meta` | Field accessors |
| `(*Error).WithMeta/WithDebug` | Immutable enrichments |
| `(*Error).Unwrap/Is` | Standard chain semantics; `Is` compares codes |
| `(*Error).StackFrames()` | Formatted stack frames |
| `(*Error).LogValue()` | `slog.LogValuer` implementation |
| `SlogAttr(err)` | `slog.Attr` forcing the structured log path |

## Notes

- Stack traces are captured eagerly at construction (depth capped at 32).
- `AsError` wraps plain (non-`*Error`) errors with code `0` (unknown); the
  application decides how to interpret and map that code.
