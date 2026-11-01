# Spec: foundations

Module id: `foundations` ([capability map](capability-map.md)). Status: approved
2026-11-01; assumptions 1–6 confirmed by the owner. Amended 2026-11-01: Default()
resolves the OTel global per call (see Design note). Items: X7–X10, O1–O4, O6, E1–E4, G1–G4, I1
([item index](../../ideas/sdk-fix-backlog.md#item-index)).

## Objective

Fix the four modules that every other SDK module imports, so the later PRs build on a
correct base. The headline change: SDK telemetry follows OpenTelemetry's own global
providers, so an app that only calls `otel.SetTracerProvider` / `otel.SetMeterProvider`
gets spans and metrics from every SDK package. Today it gets none unless it also calls
the SDK's parallel `SetDefault`.

All four modules end up clean on full-tree lint and strict in CI, so their legacy
findings can't come back.

### User stories

- As a service author, I install OTel providers once at startup and every SDK package
  (db, redis, nats, cron, s3, rest) records through them, with no SDK-specific wiring.
- As a service author, I get TLS to my collector by default. Plaintext is something I
  ask for.
- As a caller of `errgroup`, a panicking task gives me an `*errgroup.PanicError` I can
  inspect with `errors.As`. The panic value and stack are fields, not text jammed into
  an error string.
- As a caller of `errs`, `errors.Is` never reports two unrelated "unknown" errors as
  equal, and wrapped `*errs.Error` values still log as structured fields.
- As a reviewer, CI fails any PR that adds a lint finding anywhere in these modules, not
  only on changed lines.

## Assumptions (correct any of these before approving)

1. **`errs.Is` (E1):** `(*Error).Is(target)` matches when the target is an `*Error`
   with an equal code **and that code is non-zero**. Code 0 means "unknown"
   (`AsError` of a plain error), so it never matches by code. Pointer identity and the
   normal `Unwrap` chain still work through `errors.Is`.
2. **logs keeps `Default`/`SetDefault`.** Logging has no OTel global equivalent here.
   Only `SetStreams` and its stream variables are deleted (O2).
3. **Package-level `metrics.Stop` and `tracer.Stop` are deleted.** `Default()` now wraps
   the OTel global, which the app owns, so stopping it from the SDK means nothing. The
   app stops the clients it built with `New`.
4. **`errgroup.PanicError` is exported.** Callers need `errors.As` to read the value
   and stack.
5. **logs keeps tolerating a nil ctx.** A log call must never panic. The nil-ctx
   removal (X7) applies to errgroup, metrics, and tracer.
6. **`attributes.ErrMissingServiceName` and `ValidateAppInfo` stay.** They aren't in
   the backlog, and removing them is unrelated churn.

## Tech Stack

- Go 1.26.5 (`go.work`). Modules: `observability`, `errs`, `errgroup`, `identity`, with
  local `replace` directives as today.
- `go.opentelemetry.io/otel` v1.45.0 (global providers, `sdk/metric`, `sdk/trace`,
  `sdk/trace/tracetest`, `sdk/metric` manual reader in tests).
- `golang.org/x/sync/errgroup` (unchanged dependency of `errgroup`).
- golangci-lint v2.15.0 with `.golangci.yml`; gostyle from
  `.agents/skills/go-best-practices/scripts`.
- `github.com/smartystreets/goconvey` is **removed** from `observability` (O4).

## Commands

Run inside each changed module (AGENTS.md verification, with lint run on the full tree):

```bash
gofmt -l .
go vet ./...
go test -count=1 -shuffle=on ./...           # -gcflags dropped in T19: mockey left the repo
golangci-lint run ./...                      # full tree, not --new-from-merge-base
GOWORK=off go run ../.agents/skills/go-best-practices/scripts/gostyle.go $(go list -f '{{.Dir}}' ./...)
go mod tidy -diff
```

The modules changed only to compile (`db`, `redis`, `s3`, `nats`, `rest`, `cron`,
`confloader`) run the same commands, but their lint stays `--new-from-merge-base=main`,
except `rest`, which is already clean and becomes strict here.

## Project Structure

```
observability/doc.go                 → rewritten: OTel-global model, no SetDefault
observability/metrics/{metrics,config}.go  → Default follows OTel global; Insecure bool
observability/tracer/{tracer,config}.go    → same; doc fixes
observability/logs/{logs,config}.go        → SetStreams and stream vars deleted
observability/**/*_test.go           → goconvey → package testing (7 files, ~2k lines)
errs/{doc,error,constructors,slog}.go      → doc.go new (package comment); E1–E3
errgroup/{errgroup,option}.go        → PanicError, const default, docs
identity/identity.go                 → ErrName message prefix
{db,redis,s3,nats,rest}/**           → tests stop calling SetDefault; doc comments updated
.golangci.yml                        → ireturn allows the SDK's own interfaces
.github/workflows/ci.yml             → STRICT_LINT_MODULES ratchet
AGENTS.md                            → exported-interface convention; strict-lint note
docs/ideas/sdk-fix-backlog.md, docs/specs/sdk-fix-backlog/*  → committed with this PR
```

## Public API

Before → after. "Deleted" means removed outright, with no deprecation, per the
no-backward-compat policy.

### observability/metrics

| Before | After |
| --- | --- |
| `SetDefault(Client)` | Deleted. Install a provider with `otel.SetMeterProvider` (or `tracer.SetGlobal` for traces). |
| `Default() Client`, backed by an SDK-owned atomic pointer | `Default() Client` resolves `otel.GetMeterProvider()` on each call and returns a cached `Wrap` of it. The cache is keyed by provider identity, so the instrument cache survives while the provider is unchanged. Instruments created before the first install forward to it through OTel's delegate. |
| `Stop(ctx)` (package-level) | Deleted. |
| `Add`, `Record`, `AddUpDown`, `RecordGauge` (package-level) | Unchanged; they delegate to `Default()`. |
| `Config.Insecure *bool`; nil means insecure | `Config.Insecure bool`; false (the zero value) means TLS. |
| A nil ctx becomes `context.Background()` | No fallback. A nil ctx is a caller bug. |
| `New`, `Wrap`, `WrapWithScope`, `Noop`, `Client`, `Instrument` | Unchanged. |

### observability/tracer

| Before | After |
| --- | --- |
| `SetDefault(Client)` | Deleted. |
| `Default() Client` | Resolves `otel.GetTracerProvider()` on each call, cached by provider identity (same as metrics). |
| `Stop(ctx)` (package-level) | Deleted. |
| `SetGlobal(Client)` | Unchanged: installs the provider and the W3C propagator into the OTel globals. That is now the one way to route SDK spans. |
| `Tracer`, `Attrs`, `TraceIDFrom`, `SpanIDFrom` | Unchanged. The docs drop the reference to the nonexistent `Init` (O3). |
| `Config.Insecure *bool` | `Config.Insecure bool`, TLS by default. |
| A nil ctx is tolerated in `New`/`Stop` | No fallback. |

### observability/logs

| Before | After |
| --- | --- |
| `SetStreams` (deprecated), package `stdout`/`stderr` vars, `streamWriter` | Deleted. A nil `Config.Writer` means `os.Stdout`; a nil `Config.ErrorWriter` means `os.Stderr`. |
| `Default`, `SetDefault`, `New`, `Logger`, the package funcs | Unchanged (assumption 2). |

### errs

```go
// Is reports whether target is an *Error with the same non-zero code.
// Errors with code 0 (unknown) never match by code.
func (e *Error) Is(target error) bool
```

- `LogValue` emits `debug` once (`meta` no longer repeats it) (E2).
- `SlogAttr(err)` finds an `*Error` anywhere in err's chain with `errors.As` (E3).
- A package comment in a new `doc.go` (E4).

### errgroup

```go
// DefaultMaxConcurrency is the concurrency limit New uses when WithMaxConcurrency is not given.
const DefaultMaxConcurrency = 20

// PanicError is returned by Wait when a task panics. Value is the value passed to
// panic, and Stack is the goroutine stack at the point of the panic.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string // "errgroup: task panicked: <value>"
func (e *PanicError) Unwrap() error // Value when it is an error, otherwise nil
```

- `New(nil, …)` no longer substitutes `context.Background()` (X7).
- `Option` and `WithMaxConcurrency` get doc comments (G3).
- `Group` keeps its stored ctx with
  `//nolint:containedctx // tasks receive the group ctx, as in upstream errgroup` (G4).

### identity

- `ErrName` reads `"identity: service name is required"` (I1). Its identifier and its
  behavior don't change.

## Behaviour

### Telemetry routing (X10/O6)

1. When a caller passes no client, SDK packages (db, redis, nats, cron, s3, rest) fall back
   to `metrics.Default()` / `tracer.Default()`. That is unchanged code in those packages.
2. `Default()` now resolves to the OTel global provider, so routing is decided by
   `otel.SetMeterProvider` / `otel.SetTracerProvider` (or `tracer.SetGlobal`), called
   any time.
3. Before the app installs a provider, the OTel global is a no-op, so measurements are
   dropped, exactly as the SDK's old noop default did.
4. An explicitly injected client (`WithMetrics` / `WithTracer`) still wins.

No metric name, span name, unit, or attribute changes in this module. The telemetry
contract is untouched.

### Design note: why Default() resolves per call (amended 2026-11-01)

OTel's global delegate binds to the **first** `otel.Set*Provider` only (an internal
`sync.Once`). A probe on otel v1.45.0 showed that a provider captured at init sent 2/0
metrics and 1/0 spans to the first and second installed providers. Resolving
`otel.Get*Provider()` at call time reached the second. A `Default()` built once at init
would silently lose telemetry for any app that swaps providers, and for any test package
that installs one per test. The per-call resolution with a per-provider cache fixes this
and keeps the spec's intent: the SDK follows whatever provider the app installs.

### Exporter TLS (O1)

`metrics.New` and `tracer.New` call `WithInsecure()` only when `Config.Insecure` is true.
The YAML/JSON key stays `insecure`. Configs that relied on the old nil-means-insecure
default must now set `insecure: true` for a plaintext collector. This is a deliberate
breaking change.

## Code Style

The PanicError recovery shows the target style: a typed error, no fmt-assembled stack
text, and no naked returns (G2/G3):

```go
func (g *Group) adaptTask(fn func(ctx context.Context) error) func() error {
	return func() (err error) {
		defer func() {
			if v := recover(); v != nil {
				err = &PanicError{Value: v, Stack: debug.Stack()}
			}
		}()
		// Skip queued work once a sibling has failed or the parent is cancelled.
		if ctxErr := g.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fn(g.ctx)
	}
}
```

These conventions are enforced by the tools and the go-best-practices skill: doc comments
on every exported identifier; sentinel errors prefixed with the package name; `errors.AsType`
(X8); test messages written as `Func(in) = got, want want` (X9).

## Testing Strategy

- **Framework:** package `testing` with table tests (`tests := []struct{ name string; … }`,
  `t.Run(tt.name, …)`). goconvey is removed (O4), and no assertion library is added.
- **Placement:** tests mirror their source file (`metrics.go` → `metrics_test.go`).
  Converting goconvey files keeps their file names.
- **Global state:** tests that install OTel globals don't call `t.Parallel()`, and they
  restore the previous provider with `t.Cleanup`.
- **Regression tests for every [B] item and behavior change.** Each must fail on `main`
  and pass after the change:

| Test | Proves |
| --- | --- |
| `metrics.TestDefault_FollowsOTelGlobal` | `metrics.Add` after `otel.SetMeterProvider(manual reader)` is collected, including an instrument first used before the install; after a **second** `otel.SetMeterProvider`, new measurements reach the second provider |
| `tracer.TestDefault_FollowsOTelGlobal` | A span from `tracer.Tracer("x")` is recorded after `otel.SetTracerProvider(recorder)`, and after a second install it reaches the second recorder |
| `errs.TestError_Is` | Two code-0 errors don't match; the same non-zero code matches; different codes don't |
| `errs.TestError_LogValue` | `debug` appears exactly once |
| `errs.TestSlogAttr` | A `fmt.Errorf("x: %w", errsErr)` still logs structured fields |
| `errgroup.TestGroup_Go` (panic cases) | Wait returns `*PanicError` with Value and Stack; `errors.Is` reaches an error panic value |
| `metrics.TestConfig` / `tracer.TestConfig` | The zero-value Config isn't insecure |

- **Coverage:** no exported behavior in these modules is left without a test. Statement
  coverage per package must not fall below its value on `main` (measured at the start of
  implementation and recorded in the PR).

## Boundaries

- **Always:**
  - Write the failing regression test before the fix.
  - Run the full verification for every module touched.
  - Keep confloader changes compile-only.
  - Update the doc comments that mention `SetDefault` in every module.
  - Commit the idea doc and these specs with the PR.
- **Ask first:**
  - Adding or removing any dependency other than goconvey's removal.
  - Changing any telemetry name, unit, or attribute.
  - Touching confloader logic.
  - Expanding scope beyond the items listed here.
  - Changing the meaning of any assumption above.
- **Never:**
  - Add deprecation shims or a migration path (there's no backward compat).
  - Push to main or merge without the owner's approval.
  - Silence a lint finding with `//nolint` unless this spec names it (only G4).
  - Delete or skip a failing test to get to green.

## Success Criteria

1. `golangci-lint run ./...` (full tree) and gostyle report **zero** findings in
   `observability`, `errs`, `errgroup`, `identity`, and `rest`.
2. CI lints those five modules with `only-new-issues: false` through a
   `STRICT_LINT_MODULES` list in `ci.yml`. The other modules keep `only-new-issues: true`.
3. All modules in `go.work`, confloader included, pass `go build`, `go vet`,
   `go test -count=1 -shuffle=on`, and `go mod tidy -diff`.
4. Every test in the Testing Strategy table exists and fails against `main`.
5. `grep -rn 'SetDefault\|SetStreams\|contextOrBackground' observability/metrics observability/tracer`
   finds nothing. `grep -rn goconvey .` (excluding `vendor/`) finds nothing.
6. `.golangci.yml`'s ireturn allow-list includes `github.com/fikrimohammad/go-dev-sdk/`.
   AGENTS.md documents that client packages may export and return a caller-facing
   interface so users can generate mocks.
7. `go doc -all` for each changed package shows no unresolved links or stale references
   (`SetDefault`, `Init`, `SetStreams`).

## Open Questions

- Assumptions 1–6 above need the owner's confirmation or correction before
  implementation starts.
