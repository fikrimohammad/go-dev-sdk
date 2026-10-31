# Language and formatting

Sources: [guide#formatting](https://google.github.io/styleguide/go/guide#formatting), [guide#line-length](https://google.github.io/styleguide/go/guide#line-length), [decisions#language](https://google.github.io/styleguide/go/decisions#language), [decisions#imports](https://google.github.io/styleguide/go/decisions#imports), [decisions#commentary](https://google.github.io/styleguide/go/decisions#commentary), [best-practices#vardecls](https://google.github.io/styleguide/go/best-practices#vardecls), [best-practices#string-concat](https://google.github.io/styleguide/go/best-practices#string-concat).

## Contents

- Formatting and line length
- Function signatures and calls
- Conditionals, loops, switch
- Composite literals
- Variable declarations, nil slices, size hints
- Strings and `%q`
- Imports
- Comments
- Small decisions (`any`, `crypto/rand`, named results, non-decisions)

## Formatting and line length

- All source is `gofmt` output. Run `gofmt` on generated code too (`go/format.Source`).
- There is **no fixed line length**. If a line feels too long, refactor it (extract a local, shorten a name) rather than wrapping it. If it is as short as it reasonably gets, leave it long.
- Never break a line just before an indentation change (a function signature, an `if`), because the continuation then aligns with the body:

```go
// Bad: conditions 3 and 4 look like body code.
if longCondition1 && longCondition2 &&
	longCondition3 && longCondition4 {
	log.Info("all conditions met")
}
```

- Never split a long string literal (URLs, messages) to fit a width.
- Wrap comments for readability (around 80-100 columns is common, not required), and stay consistent within a file.

## Function signatures and calls

Keep a declaration's signature on one line. To shorten long call sites, change the API (option struct) or factor out locals:

```go
local := helper(some, parameters, here)
good := foo.Call(list, of, parameters, local)
```

Don't break calls at arbitrary points just for length, and don't annotate positional arguments with inline comments (`42, // Port`). Use an option struct (`server.Options{Port: 42}`) or better docs. Break lines when it aids understanding, grouping arguments semantically:

```go
canvas.RenderHeptagon(fillColor,
	x0, y0, vertexColor0,
	x1, y1, vertexColor1,
	// ...
)

log.Warningf("Database key (%q, %d, %q) incompatible in transaction started by (%q, %d, %q)",
	currentCustomer, currentOffset, currentKey,
	txCustomer, txOffset, txKey)
```

## Conditionals, loops, switch

- Don't line-break an `if` condition. Extract operands into named locals (when short-circuiting isn't needed), or factor out repeated calls:

```go
inTransaction := db.CurrentStatusIs(db.InTransaction)
keysMatch := db.ValuesEqual(db.TransactionKey(), row.Key())
if inTransaction && keysMatch {
	return db.Error(db.TransactionError, "query failed: row (%v): key does not match transaction key", row)
}
```

- An `if` that contains a closure or a multi-line literal keeps its braces matched:

```go
if err := db.RunInTransaction(func(tx *db.TX) error {
	return tx.Execute(userUpdate, x, y, z)
}); err != nil {
	return fmt.Errorf("user update: %s", err)
}
```

- Don't wrap `for` clauses. Let the line be long, or move a condition into the body with a `break`.
- Keep `switch` and `case` lines on one line. If a case list is excessively long, put every case on its own indented line and add a blank line before the body.
- Put the variable on the left: `if result == "foo"`, not Yoda style `"foo" == result`.
- `switch` cases don't fall through, so a trailing `break` is redundant. Use a comment to explain an empty case. Inside a `for`, a bare `break` in a `switch` leaves only the switch. Use a labeled break to exit the loop:

```go
loop:
	for {
		switch x {
		case "A":
			break loop
		}
	}
```

## Composite literals

Prefer one literal over building a value field by field.

- **Field names** are required for types from other packages. Field order and the full field set aren't part of an API. They are optional for package-local types, but use them whenever the struct has many fields or names add clarity.

```go
r := csv.Reader{Comma: ',', Comment: '#', FieldsPerRecord: 4} // good
r := csv.Reader{',', '#', 4, false, false, false, false}      // bad
```

- **Matching braces.** The closing brace sits on a line indented like the opening line. Don't hang it after the last value:

```go
good := []*Type{
	{Key: "multi"},
	{Key: "line"},
}

bad := []*Type{
	{Key: "multi"},
	{Key: "line"}}
```

- **Cuddling** (`[]*Type{{ ... }, { ... }}`) is allowed only when indentation still matches and every element is a literal, not a variable.
- **Repeated type names** in slice and map literals may be omitted: `[]*Type{{A: 42}}`, not `[]*Type{&Type{A: 42}}`. `gofmt -s` does this.
- **Zero-value fields** may be omitted when clarity isn't lost. Omitting them draws attention to what *is* set. In tables, keep zero fields when the zero value is the point of the case (testing nil or zero inputs).

## Variable declarations

- Non-zero initialization uses `:=` (`i := 42`, not `var i = 42`).
- A zero value that is ready for use is declared with `var`: `var coords Point`, `var buf bytes.Buffer`, and the decode target `var cfg Config; json.Unmarshal(data, &cfg)`. Don't write `Point{X: 0, Y: 0}` or `[]int(nil)`.
- For a pointer to a zero value, both `new(T)` and `&T{}` are fine. `new` signals that a non-zero literal wouldn't work.
- A struct containing a `sync.Mutex` can take a value field (zero value ready), but then the struct must be passed by pointer and its methods need pointer receivers. If a composite is returned or always has its address taken, declare it as a pointer from the start (`c := new(Counter)`).
- Maps must be initialized (`make` or a literal) before writing. Reading a nil map is fine.

**Nil slices.** `nil` and empty behave the same for `len`, `cap`, `range`, and `append`. Declare empty locals as `var t []string`, not `t := []string{}`, unless you need a non-nil value (for example, JSON encodes `nil` as `null` and an empty slice as `[]`). Don't design APIs that make callers distinguish nil from empty. Test emptiness with `len(s) == 0`, never `s == nil`.

**Size hints.** `make([]T, 0, n)` and `make(map[K]V, n)` belong in performance-sensitive code with evidence (a benchmark, or a known final size such as converting a map to a slice). Overallocating wastes memory, so default to zero values or literals.

## Strings and `%q`

| Situation | Use |
| --- | --- |
| Joining a few strings | `"projectid: " + p` |
| Formatting | `fmt.Sprintf("%s [%s:%d]-> %s", src, qos, mtu, dst)`, not a long `+` chain |
| Output going to an `io.Writer` | `fmt.Fprintf(w, ...)`, without building a temporary string |
| Building piecemeal in a loop | `strings.Builder` (linear, where repeated `+` is quadratic) |
| Complex templated output | `text/template` (or a safe HTML template) |
| Constant multi-line text | a backtick raw string, not `"..." + "\n" +` |

Use `%q` when printing strings for humans, especially values that might be empty or contain control characters. `""` is visible, and an empty `%s` is not. Don't hand-quote with `\"%s\"` or `'%s'`.

## Imports

**Grouping, in this order, separated by blank lines:**

1. Standard library.
2. Everything else (project and third-party).
3. Protocol buffer imports (renamed with `pb`/`grpc` suffixes, for example `foopb "path/to/foo_go_proto"`).
4. Side-effect imports (`_ "..."`).

**Renaming.** Don't rename unless you must:

- Rename on a name collision, preferring to rename the most local or project-specific import.
- Rename generated proto packages to remove underscores and add `pb`. Prefer descriptive names (`pushqueueservicepb`) over `xpb`.
- Optionally rename an uninformative name like `v1` (`core "k8s.io/api/core/v1"`).
- When the package name collides with a variable you want (`url`), use a `pkg` suffix: `urlpkg`.
- A local name follows package-name rules (lowercase, no underscores). Use the same local name for the same package everywhere.

**Blank imports** (`import _ "pkg"`) appear only in `package main` or tests. A library that needs a side effect (a database driver, `time/tzdata`) documents that its binary must import it. Exceptions: `_ "embed"` with `//go:embed`, and import-checker bypasses.

**Dot imports** (`import . "pkg"`) are never used, even in tests.

## Comments

- Comments explain *why* (non-obvious language behavior, business rules, performance trade-offs), not *what* the code plainly does. Redundant comments add noise, contradict the code, and go stale.
- When code that looks routine is not (`err == nil`, an intentional `=` rather than `:=`, a hidden `!`), boost the signal with a short comment, or restructure so the important detail is explicit:

```go
// Gregorian leap years aren't just year%4 == 0.
// See https://en.wikipedia.org/wiki/Leap_year#Algorithm.
var (
	leap4   = year%4 == 0
	leap100 = year%100 == 0
	leap400 = year%400 == 0
)
leap := leap4 && (!leap100 || leap400)
```

- Doc comments: every exported name, plus unexported declarations with non-obvious behavior. Write full sentences that begin with the name (an article before it is fine). SKILL.md's Doc comments section has the exact format.
- Don't enumerate every parameter. Document the error-prone and non-obvious ones and say *why* they matter.
- Document returned sentinel errors and error types, the cleanup the caller must do, and context or concurrency behavior that deviates from the defaults (see concurrency.md).
- Struct field end-of-line comments can be phrases: `PageLength int // lines per page (optional; default: 20)`.
- Put runnable examples in `example_test.go` rather than long code blocks in comments.
- File-wide maintainer notes go after the imports, not in the package comment.

## Small decisions

- **`any`**, not `interface{}`, in new code.
- **`crypto/rand`** for keys, tokens, nonces, and salts, even throwaway ones. Encode them with `hex` or `base64`. `math/rand` is predictable.
- **Named result parameters**: use them when several results share a type (`(left, right *Node, err error)`), when the caller must act on a result (`(ctx Context, cancel func())`), or when a deferred closure must set one. Don't name results just to skip declaring a variable, or when the name repeats the type (`(node *Node)`). Naked returns are fine only in small functions. A named function type (`CancelFunc`) often documents better than a name.
- **`Must` helpers** are only for init-time constants and tests (see errors.md).
- **Non-decisions** (pick either, but stay consistent locally): `var i int` vs. `i := 0`, `&T{}` vs. `new(T)`, `map[K]V{}` vs. `make(map[K]V)`, `errors.New("x")` vs. `fmt.Errorf("x")` for unformatted text, and the order of `cmp.Diff(want, got)` (state the direction in the message).
