---
name: go-doc-comments
description: Use when writing, changing, or reviewing Go doc comments in this repository, including package comments, exported API documentation, doc links, lists, headings, deprecations, and gofmt/go doc verification.
whenToUse: Load before adding or changing a package comment, an exported declaration, an exported struct field, or a doc comment block in Go code.
---

# Go doc comment conventions

Use this skill whenever you write or review Go doc comments. Every package and every exported declaration must follow [Go Doc Comments](https://go.dev/doc/comment). The invariants in the repository `AGENTS.md` still apply.

## Required coverage

- Exactly one package comment per package, immediately above the `package` clause with no blank line in between. Put it in the package's main file (`<package>.go`, `doc.go`, or the primary source file) and make the first sentence begin `Package <name> ...`.
- Every exported const, var, type, function, and method must have a doc comment immediately before its declaration, with no blank line.
- Every exported struct field must have its own comment, or the type's doc comment must explain every exported field.
- Grouped const or var declarations may share one group doc comment when the items form one logical group; per-item end-of-line comments can document the members.
- Test functions (`TestX`, `BenchmarkX`, `FuzzX`) and example functions (`ExampleX`) are exempt from doc comments.

Example:

```go
// Package orders coordinates order placement and fulfillment.
package orders

// Config holds the order service settings.
type Config struct {
	// Driver selects the database driver. The zero value means "mysql".
	Driver string

	// Timeout bounds each outbound request.
	Timeout time.Duration
}

// Connect opens a database using cfg and verifies it is reachable.
// It returns an error if cfg is invalid or the connection cannot be
// established.
func Connect(cfg Config) (*DB, error)
```

## Sentence rules

- Every doc comment is a complete sentence and ends with punctuation.
- Package comments start with `Package <name>`.
- Comments for consts, vars, functions, and methods start with the declared name.
- Type comments must name the type in the first sentence; both `Foo ...` and `A Foo ...` are acceptable.
- The first sentence is the synopsis. Keep it short and descriptive.

## Formatting rules

- Separate paragraphs with a blank `//` line.
- Use doc-comment lists, not ad-hoc indentation:

```go
// RetryPolicy controls retry behavior:
//   - MaxAttempts is the total number of attempts.
//   - Backoff is the delay between attempts.
```

- Use headings (`# Heading`) only with blank lines before and after.
- Use indented lines for code blocks, with blank lines around the block.
- Go doc has no inline-code syntax. Do not use backticks for identifiers, parameters, or code; refer to parameters and results directly by name.
- Use doc links:
  - Current package: `[Config]`, `[Config.SetDefaults]`, `[DB.Begin]`.
  - Imported packages: `[sql.ErrNoRows]`, `[metrics.SetDefault]`, `[pkg.Name]`.
  - Stdlib identifiers need a link when referring to them; for example `[sql.ErrNoRows]` after `database/sql` is imported by the package.
  - Unresolved links keep their brackets in rendered docs, so verify them with `go doc -all .`.
- Do not use Markdown emphasis or raw HTML; Go doc does not support them.
- Mark deprecations with a paragraph starting `Deprecated:`.
- Run `gofmt` after editing doc comments. It canonicalizes lists, headings, and links.
- Avoid writing a literal pair of single quotes (`''`) in a doc comment: `gofmt` converts it to a Unicode right quote (`”`). Write "doubled single quotes" instead.
- Avoid a literal pair of backticks as well; Go doc turns it into a Unicode left quote.

## Content rules

- Exported doc comments describe caller-visible behavior: what the declaration returns, what it does, and what the caller must know. Do not describe internal algorithms or implementation details.
- Document error behavior, including sentinel errors such as `sql.ErrNoRows`.
- Document the zero value when it is not obvious.
- Document concurrency safety if it is stronger or weaker than the default assumption (safe for concurrent use only when explicitly stated).
- Document panics, side effects, and lifecycle requirements (for example, `Close` must be called).
- Boolean-returning functions use "reports whether".
- Unexported comments should explain why the code exists, not restate what the code does. Put implementation details in comments inside the function body.

## Verification

Inspect the rendered API in each module:

```bash
go doc -all .
```

Check that:

- [ ] The package synopsis starts with `Package <name>`.
- [ ] Every exported declaration and field has a comment.
- [ ] Doc links render without brackets (unresolved links keep `[...]`).
- [ ] No stray `”`, `“`, or Markdown syntax appears.
- [ ] `gofmt -l .` is empty.
