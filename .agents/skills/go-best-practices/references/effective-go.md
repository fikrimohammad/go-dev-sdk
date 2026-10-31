# Effective Go: what it adds, and where it yields

Source: [Effective Go](https://go.dev/doc/effective_go). It is the baseline the Google guide builds on. It dates from 2009, isn't kept current, and doesn't cover generics or modules. This file keeps only what the Google guide, the linters, and `go vet` don't already enforce.

## Contents

- Naming: interfaces and canonical methods
- The zero value
- Interfaces: generality, compile-time checks, function adapters
- Embedding
- Errors that identify their origin
- Where Effective Go yields

## Naming: interfaces and canonical methods

- One-method interfaces are named for the method, with an *-er* suffix or similar agent noun: `Reader`, `Writer`, `Formatter`, `CloseNotifier`.
- `Read`, `Write`, `Close`, `Flush`, `String`, and similar names carry standard signatures and meanings. Don't give a method one of those names unless it matches both. When your method *does* mean one of them, use the standard name and signature, so the type works with the standard library automatically:

```go
func (a Amount) String() string // fmt prints it; not ToString()
func (b *Buffer) Write(p []byte) (n int, err error) // *Buffer is an io.Writer
```

- Getters drop `Get`: field `owner` gets getter `Owner()`. A setter, if needed, is `SetOwner`.

## The zero value

Design types so their zero value works without further setup. Then callers can write `var x T` or `new(T)` and get straight to work. The property carries through structs:

```go
// The zero value is ready to use: an unlocked mutex and an empty buffer.
type SyncedBuffer struct {
	lock   sync.Mutex
	buffer bytes.Buffer
}
```

The usual obstacle is a map field, because writing to a nil map panics. Create it lazily in the methods that write:

```go
type Ledger struct {
	mu       sync.Mutex
	balances map[string]int64
}

// Add records amount for account. The zero value of Ledger is ready to use.
func (l *Ledger) Add(account string, amount int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.balances == nil {
		l.balances = make(map[string]int64)
	}
	l.balances[account] += amount
}
```

Reads from a nil map are fine and need no guard. When a type genuinely needs configuration (a limit, an endpoint), say in its doc comment that `New` is the only way to get a usable value.

## Interfaces

**Generality.** A type that exists only to implement an interface, with no other exported methods, doesn't need to be exported. Export the interface and have the constructor return it. `crc32.NewIEEE` and `adler32.New` both return `hash.Hash32`, so swapping algorithms changes only the constructor call. This is the narrow exception to the Google guide's "return concrete types". It doesn't justify returning interfaces from types that have their own API.

**Compile-time interface checks.** Most interface satisfaction is checked statically wherever a value is converted. Some is only checked at run time: `encoding/json` asserts `json.Marshaler` on the values it encodes, and a type that silently fails to satisfy it just gets default encoding. Pin the guarantee in the package that defines the type:

```go
var (
	_ json.Marshaler   = Amount{}
	_ json.Unmarshaler = (*Amount)(nil)
	_ fmt.Stringer     = Amount{}
)
```

Add these only when no static conversion already exists in the code. Don't add one for every interface a type happens to satisfy.

**Function adapters.** A function type can implement a one-method interface, so plain functions can be used wherever the interface is expected:

```go
// HandlerFunc adapts an ordinary function to Handler.
type HandlerFunc func(ctx context.Context, msg Message) error

// Handle calls f(ctx, msg).
func (f HandlerFunc) Handle(ctx context.Context, msg Message) error { return f(ctx, msg) }
```

## Embedding

Embedding a type promotes all of its methods to the outer type. The promoted methods still run with the inner value as receiver. That is convenient (`bufio.ReadWriter` embeds `*Reader` and `*Writer` and satisfies `io.ReadWriter` with no forwarding methods). It is also a public-API decision:

- Embedding `sync.Mutex` in an exported struct exports `Lock` and `Unlock`. Use a named field `mu sync.Mutex`.
- Embedding a dependency (`*log.Logger`, a client) exports its whole method set, and you then have to support it. Use a named field and forward only what callers need.
- Embedded fields are addressed by their type name (`job.Logger`). A field or method on the outer type hides one with the same name deeper down.

Interface embedding (`type ReadWriter interface { Reader; Writer }`) is the idiomatic way to compose small interfaces into larger ones.

## Errors that identify their origin

An error should make sense when printed far from the call that caused it. Error types carry the operation and its subject (`os.PathError` holds `Op`, `Path`, `Err` and prints `open /etc/passwx: no such file or directory`). Error strings should name where they came from, usually with the package as a prefix: `image: unknown format`, `money: currency mismatch`. Callers who need details use `errors.As` to reach the typed error. They don't parse the text.

## Where Effective Go yields

| Effective Go | Use instead | Why |
| --- | --- | --- |
| `safelyDo`: `recover` in each server goroutine, log, and keep serving | Let unexpected panics crash. Recover only inside a package to turn its own panics into errors (Effective Go's regexp example) | The Google guide: recovering leaves corrupted state and held locks behind. It calls `net/http`'s handler recovery a historical mistake. |
| `init()` that checks the environment, calls `log.Fatal`, or registers flags | Validate in constructors that return errors. Define flags only in `main` | Google guide: no flags or global setup in libraries. Errors belong to the caller. |
| Labeled `break Loop` to leave a loop from inside a `switch` | Extract a function and `return`, or restructure. gostyle flags labels | This repository's readability rule |
| Buffered channel as a semaphore, hand-rolled worker pools | `errgroup.WithContext` with `g.SetLimit(n)` | This repository's rule: one obvious, cancellable, joined pattern |
| Naked returns using named results (`io.ReadFull`) | Name results for documentation, but return values explicitly beyond a few lines | Google decisions; `nakedret` enforces it |
| `interface{}`, `for i := 0; i < n; i++`, closures that copy loop variables | `any`, `for i := range n`, plain capture (Go 1.22+) | The guide predates these. `modernize` enforces them |
