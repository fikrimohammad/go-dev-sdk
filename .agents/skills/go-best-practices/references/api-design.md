# API design

Sources: [decisions#interfaces](https://google.github.io/styleguide/go/decisions#interfaces), [best-practices#interfaces](https://google.github.io/styleguide/go/best-practices#interfaces), [best-practices#funcargs](https://google.github.io/styleguide/go/best-practices#funcargs), [best-practices#globals](https://google.github.io/styleguide/go/best-practices#globals), [decisions#receiver-type](https://google.github.io/styleguide/go/decisions#receiver-type), [decisions#generics](https://google.github.io/styleguide/go/decisions#generics), [best-practices#package-size](https://google.github.io/styleguide/go/best-practices#package-size).

The guiding principle is [least mechanism](https://google.github.io/styleguide/go/guide#least-mechanism). Abstractions remove information from the reader (an interface hides which implementation runs), so each one must earn its place. Adding abstraction later is easy. Removing an unnecessary one is hard.

## Contents

- Interfaces: when, who owns them, what to return
- Testing seams without interfaces
- Function arguments: option structs vs. variadic options
- Receiver type and passing values
- Copying
- Generics
- Global state
- Package size and layout
- Flags and CLIs
- Type aliases

## Interfaces

**Don't create one until a real need exists.**

- Don't confuse the concept with the keyword. Designing a "service" or "repository" doesn't mean you need `type Service interface`. Write the concrete type first.
- Reuse existing interfaces, especially generated ones (RPC clients and servers). Don't wrap them in a new hand-written interface "for abstraction" or "for testing".
- Don't export a test double, or an interface that exists only so tests can swap in a double, from the production package. Exporting the interface, the real implementation, and the double triples what readers must understand.

**Valid reasons to introduce one:**

1. Multiple implementations: two or more concrete types handled by the same logic (for example code that works with both a `json.Encoder` and a `gob.Encoder`). The *consumer* defines it.
2. Decoupling packages to break an import cycle. The producer may define it, but treat the cycle as a smell: the package boundaries are often wrong.
3. Hiding complexity: a function needs one or two methods of a type with a huge surface. The consumer defines a minimal interface.

**Ownership and visibility:**

- The consumer defines the interface, in the package that uses it, with only the methods it calls. The bigger the interface, the weaker the abstraction.
- Keep it unexported if only this package uses it. Exporting commits you to maintaining it.
- The producer exports an interface when the interface *is the product*: a protocol many implementations follow (`io.Writer`, `hash.Hash`, a plugin contract, generated RPC service interfaces). A standalone, implementation-free package works well for such contracts. It also prevents many identical copies of the same interface across a large codebase.

**Design:**

- Keep interfaces small. Compose larger ones from small ones.
- Document the contract: behavior, edge cases, errors, and concurrency expectations. Single-method interfaces document on the type. Multi-method interfaces document every method. Document unexported interfaces too, since they are often the hardest glue to understand later.

**Return concrete types; accept interfaces.** A concrete result gives callers the full API, and they can still pass it to anything that takes an interface. Return an interface only when:

- It is `error`.
- The implementation is chosen at runtime (factory, strategy, command, chaining):

```go
func NewWriter(format string) io.Writer {
	switch format {
	case "json":
		return &jsonWriter{}
	case "xml":
		return &xmlWriter{}
	default:
		return &textWriter{}
	}
}
```

- Deliberate encapsulation, where exposing the concrete type would let callers break invariants (for example, bypass a rate limiter's internal refill). Ask first whether the extra methods would really break integrity. Don't encapsulate by rote.
- Returning the concrete type would create an import cycle.

`database/sql` is the model of restraint here: it has one concrete `*sql.DB`, not a `DB` interface with MySQL and Oracle implementations.

## Testing seams without interfaces

- **Use real transports.** Test code that talks HTTP against `httptest.NewServer`, and gRPC against an in-process server with a test implementation, using the *production* client. Imitating a client by hand is complex and drifts from reality.
- Inject dependencies as parameters or struct fields: a `*http.Client`, a base URL, a `func() time.Time` clock. Callers and tests pass what they need without a package-level hook.
- When the real implementation is impractical in tests, put fakes and stubs in a dedicated `<pkg>test` package (like `spannertest`), not in production code.

## Function arguments

Long signatures blur which argument is which, and same-typed neighbors get swapped. For exported APIs, consider whether one highly configurable function should be several simpler ones that share an unexported implementation. Otherwise, pick a strategy by how the call sites will look:

**Option struct.** The last parameter collects arguments, and callers name only what they set.

```go
type ReplicationOptions struct {
	Config              *replicator.Config
	PrimaryRegions      []string
	ReadonlyRegions     []string
	ReplicateExisting   bool
	OverwritePolicies   bool
	ReplicationInterval time.Duration
	CopyWorkers         int
	HealthWatcher       health.Watcher
}

func EnableReplication(ctx context.Context, opts ReplicationOptions)

storage.EnableReplication(ctx, storage.ReplicationOptions{
	Config:         config,
	PrimaryRegions: []string{"us-east1", "us-central2"},
})
```

Prefer this when callers usually set several options, all callers need some option, or options are shared across functions. The fields are self-documenting and can't be swapped, defaults are simply the zero value, and the struct grows without breaking callers. Contexts never go in option structs. Export the struct only if an exported function uses it.

**Variadic functional options.** Exported constructors return closures that modify an unexported options struct.

```go
type replicationOptions struct {
	readonlyCells     []string
	replicateExisting bool
}

// A ReplicationOption configures EnableReplication.
type ReplicationOption func(*replicationOptions)

// ReadonlyCells adds cells that should hold read-only replicas.
// Passing this option multiple times adds more cells.
func ReadonlyCells(cells ...string) ReplicationOption {
	return func(o *replicationOptions) { o.readonlyCells = append(o.readonlyCells, cells...) }
}

// ReplicateExisting controls whether existing files are replicated.
// Default: false.
func ReplicateExisting(enabled bool) ReplicationOption {
	return func(o *replicationOptions) { o.replicateExisting = enabled }
}

func EnableReplication(ctx context.Context, config *placer.Config, primaryCells []string, opts ...ReplicationOption)
```

Prefer this only when *many* of these hold: most callers pass no options, options are rarely used, there are many of them, options take arguments, options can fail validation, options need long docs, or other packages should define options. It costs much more code, so don't use it for two or three settings.

Rules for functional options:

- Options take a value rather than signalling by presence: `FailFast(enable bool)`, not `EnableFailFast()`; `Format(log.Capacitor)`, not `CapacitorFormat()`. Programmatic callers then just pass values.
- Apply options in order, with the last one winning for non-cumulative settings.
- Document each option's default.

## Receiver type and passing values

Pick a receiver by which method set the method must be in. Correctness first:

- Mutates the receiver: pointer.
- Contains fields that must not be copied (`sync.Mutex`, `bytes.Buffer`, other sync types): pointer.
- Large struct or array: pointer.
- Contains pointers to things that get mutated: prefer a pointer, which makes mutation visible.
- Concurrent callers must see modifications: pointer. They must not see them: value.
- Slice that isn't resliced or reallocated: value (`func (b Buffer) Len() int`).
- Map, func, or chan type: value (`func (h Header) Add(k, v string)`).
- Small plain-old-data value with no mutable fields (`time.Time`): value.
- Built-in type that isn't modified (`type User string`): value.
- When in doubt: pointer.

Keep all methods of a type pointer or all value. Don't choose based on folklore about performance. Benchmark if it matters.

**Pass values.** Don't pass `*string` or `*io.Reader` "to save bytes". A string or interface value is already small and fixed-size. If a function only ever reads `*x`, `x` should not be a pointer. This doesn't apply to large or growing structs, or protobuf messages, which are passed by pointer.

## Copying

Don't copy a value of type `T` whose methods are on `*T`. Don't copy structs from other packages that contain `sync.Mutex` or `bytes.Buffer` (a copied `Buffer`'s slice can alias the original's array). A value receiver or value parameter hides the copy, so if your struct contains such fields, take and return `*T`.

```go
type Record struct {
	buf bytes.Buffer
}

func New() *Record                 // good
func (r *Record) Process(...)      // good
func (r Record) Process(...)       // bad: copies r.buf
func Consumer(r Record)            // bad
```

## Generics

Allowed where they meet a real requirement, but be wary of premature use.

- If only one type is instantiated in practice, write the code for that type. Adding a type parameter later is easy, and removing one is not.
- Don't write generic code just because an algorithm doesn't care about the element type.
- If several types share a useful interface, model the solution with that interface. You may not need generics.
- Prefer generics over `any` plus heavy type switching.
- Don't build DSLs, error-handling frameworks, or assertion libraries with generics.
- Document exported generic APIs and include runnable examples.

## Global state

Libraries must not force clients onto package-level state. That rules out exported or unexported package variables that control behavior, global service locators, callback registries, and lazily built singleton clients:

```go
// Bad
package sidecar

var registry = make(map[string]*Plugin)

func Register(name string, p *Plugin) error { ... }

// Good: clients create instances and pass them as dependencies.
type Registry struct{ plugins map[string]*Plugin }

func New() *Registry { return &Registry{plugins: make(map[string]*Plugin)} }

func (r *Registry) Register(name string, p *Plugin) error { ... }
```

Global state makes tests order-dependent (one test's registration leaks into the next), prevents parallel tests and sharding, and leaves questions with no good answer: two servers in one process, replacing a dependency in a test, name collisions, calls at the wrong startup phase.

**Litmus tests.** Package state is unsafe when independent code interacts through it, tests interact through it, users want to swap it for test doubles, or ordering matters (`init`, flag parsing). It is acceptable only when the state is logically constant, the package's observable behavior is stateless (a private cache callers can't detect), nothing leaks outside the process, or nobody expects predictable behavior.

**Default instance.** If convenience truly demands a package-level API (like `http.Handle` over `http.DefaultServeMux`), it must:

1. Exist alongside an instance API.
2. Be a thin proxy to that API.
3. Be used only by binaries, never by other libraries.
4. Document and enforce its invariants, and offer a way to reset to defaults for tests.

## Package size and layout

- Put types in the same package when client code needs them together, or when their implementations are tightly coupled. Test: if users must import both packages to do anything meaningful, merge them.
- Give conceptually distinct things their own small package. The package name plus the type name should read as one identifier: `bytes.Buffer`, `ring.New`.
- File size is flexible. Avoid files of many thousands of lines, and avoid dozens of tiny files. There is no "one type per file" rule. Group related code by file (as package `bytes` does). An optional `doc.go` holds a long package comment.
- If code is both a library and a binary, keep the CLI separate so it is just another client of the library.

## Flags and CLIs

- Define flags only in `package main`. A library that registers flags on import leaks configuration as a side effect. Configure libraries with parameters, option structs, or fields.
- Flag names use snake_case (`poll_interval`). The variables holding them use mixedCaps (`pollInterval`). Group global flag vars in their own `var` block after the imports.
- For subcommands, the guide recommends `github.com/google/subcommands` for simplicity. `cobra` is common outside Google. With cobra, take the context from `cmd.Context()` instead of building a new root context.

## Type aliases

Use a type definition (`type T1 T2`) to define a new type. Aliases (`type T1 = T2`) are rare. They exist mainly to migrate a type between packages. Don't use them otherwise.
