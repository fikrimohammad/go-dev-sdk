# errs

Typed, structured errors for Go services: an error carries a category **code**
that maps to an HTTP status and a structured `slog` representation, a wrapped
**cause**, **metadata**, and an eagerly captured **stack trace**.

## Features

- **Typed codes** — `Code` enum with a human-readable name and an HTTP status
  mapping (`InvalidArgument` → 400, `NotFound` → 404, internal codes → 500).
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
go get github.com/fikrimohammad/go-dev-sdk/errs
```

## Step-by-step

### 1. Choose a code

Use the built-ins, or add your own numeric codes. The 4-digit layout is
convention but not enforced:

```go
const (
    CustomValidation Code = 1002 // 1xxx → 400 Bad Request
    CustomNotFound   Code = 4005 // 4xxx → 404 Not Found
    CustomInternal   Code = 5010 // 5xxx → 500 Internal Server Error
)
```

### 2. Create an error

```go
// A fresh error with a stack trace.
return errs.New(errs.NotFound, "user not found")

// Wrap an underlying cause.
if err := repo.Get(ctx, id); err != nil {
    return errs.Wrap(errs.DBInternal, "failed to load user", err)
}

// Convert any error into an *errs.Error (Internal if it isn't one already).
return errs.AsError(err)
```

### 3. Attach context

`WithMeta` / `WithDebug` return a **new** error; the receiver is never mutated.

```go
return errs.Wrap(errs.Internal, "payroll run failed", err).
    WithDebug("worker id 7 panicked").
    WithMeta("run_id", runID)
```

### 4. Inspect on the way out

```go
var e *errs.Error
if errs.As(err, &e) {
    code := e.Code()                 // typed code
    status := e.Code().HTTPStatus()  // http.StatusNotFound
    dbg := e.Debug()                 // debug detail or ""
    frames := e.StackFrames()        // []string of formatted frames
}
```

Or just derive the code in one call:

```go
code := errs.CodeFromError(err) // OK if nil, Internal if not an *Error
```

### 5. Log it structurally

```go
// *Error implements slog.LogValuer, but slog resolves plain error values
// before LogValuer, so use SlogAttr to get the structured fields.
logs.Error(ctx, "job failed", errs.SlogAttr(err))

// Handlers wired through errs.SlogAttr receive:
//   code=NOT_FOUND message="user not found" cause=... stack=...
```

## API reference

| Symbol | Description |
| --- | --- |
| `Code` | Typed error category (`int`); `String()`, `HTTPStatus()` |
| `New(code, msg)` | New `*Error` with a stack trace |
| `Wrap(code, msg, cause)` | New `*Error` wrapping a cause |
| `AsError(err)` | Extract the `*Error` in a chain, else wrap as `Internal` |
| `As(err, &e)` | `errors.As` helper for `*Error` |
| `CodeFromError(err)` | Code of the chain, `Internal` fallback |
| `(*Error).Code/Message/Cause/Debug/Meta` | Field accessors |
| `(*Error).WithMeta/WithDebug` | Immutable enrichments |
| `(*Error).Unwrap/Is` | Standard chain semantics; `Is` compares codes |
| `(*Error).StackFrames()` | Formatted stack frames |
| `(*Error).LogValue()` | `slog.LogValuer` implementation |
| `SlogAttr(err)` | `slog.Attr` forcing the structured log path |

## Notes

- Stack traces are captured eagerly at construction (depth capped at 32).
- `OK` (`0`) is the nil/healthy sentinel and maps to `200`.
