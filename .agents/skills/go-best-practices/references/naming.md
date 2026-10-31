# Naming

Sources: [guide#naming](https://google.github.io/styleguide/go/guide#naming), [decisions#naming](https://google.github.io/styleguide/go/decisions#naming), [best-practices#naming](https://google.github.io/styleguide/go/best-practices#naming).

A good name doesn't feel repetitive at its use site, takes its context into account, and doesn't repeat concepts that are already clear. Judge a name where it is *called*, not where it is declared.

## Contents

- MixedCaps and underscores
- Package names
- Repetition (package, type, context)
- Function and method names
- Getters
- Receivers
- Constants
- Initialisms
- Variables and scope
- Shadowing
- Test doubles and helper packages

## MixedCaps and underscores

Multi-word names use `MixedCaps` / `mixedCaps`, even where other languages differ. Local variables count as unexported. Underscores are allowed only in:

1. Package names imported only by generated code.
2. `Test`, `Benchmark`, `Example`, and `Fuzz` function names in `_test.go` files (`TestParse_EmptyInput`).
3. Low-level OS or cgo interop that reuses OS identifiers (rare).

File names are not identifiers and may contain underscores.

## Package names

- Use only lowercase letters and digits: `tabwriter`, not `tabWriter`, `TabWriter`, or `tab_writer`. Keep multi-word names unbroken.
- Name the package for what it provides. `util`, `utility`, `common`, `helper`, `model`, `testhelper`, and `base` tell the reader nothing and invite import renaming. Compare `spannertest.NewDatabaseFromFile(...)`, `io.SeekStart`, and `elliptic.Marshal(...)` with `test.NewDatabaseFromFile`, `common.SeekStart`, and `helper.Marshal`. A generic word can be *part* of a name (`stringsutil` is better than `util`).
- Avoid names that commonly appear as local variables, since the variable shadows the package: `usercount` is better than `count`.
- If you must import a package with an underscore in its name, rename it at import to a valid Go name.
- Black-box tests use `<pkg>_test`.

## Repetition

Evaluate repetition from the caller's side.

**Package vs. exported symbol.** The package name is always visible.

| Repetitive | Better |
| --- | --- |
| `widget.NewWidget` | `widget.New` |
| `widget.NewWidgetWithName` | `widget.NewWithName` |
| `db.LoadFromDatabase` | `db.Load` |
| `goatteleportutil.CountGoatsTeleported` | `gtutil.CountGoatsTeleported` or `goatteleport.Count` |
| `sqldb.DBConnection` | `sqldb.Connection` |
| `reporting.AdsTargetingRevenueReport` (in `ads/targeting/revenue/reporting`) | `reporting.Report` |

If a package exports one main type named after the package, its constructor is `New`.

**Variable vs. type.** The compiler and the reader already know the type.

| Repetitive | Better |
| --- | --- |
| `var numUsers int` | `var users int` |
| `var nameString string` | `var name string` |
| `var primaryProject *Project` | `var primary *Project` |

When the value exists in two forms, disambiguate by form: `limitRaw`/`limitStr` and `limit`.

**External context vs. local names.** The package, type, method, and file all qualify the names inside them.

```go
// Bad: everything restates the context.
func (db *DB) UserCount() (userCount int, err error) {
	var userCountInt64 int64
	if dbLoadError := db.LoadFromDatabase("count(distinct users)", &userCountInt64); dbLoadError != nil {
		return 0, fmt.Errorf("load user count: %s", dbLoadError)
	}
	userCount = int(userCountInt64)
	return userCount, nil
}

// Good.
func (db *DB) UserCount() (int, error) {
	var count int64
	if err := db.Load("count(distinct users)", &count); err != nil {
		return 0, fmt.Errorf("load user count: %s", err)
	}
	return int(count), nil
}
```

`(p *Project) Name()`, not `ProjectName()`. Inside package `ads/targeting`, write `id := in.GetAdsTargetingID()`, not `adsTargetingID := ...`.

## Function and method names

Leave out what the call site already shows: input and output types (when there is no collision), the receiver type, pointer-ness, parameter names, and return types.

| Repetitive | Better |
| --- | --- |
| `yamlconfig.ParseYAMLConfig(input)` | `yamlconfig.Parse(input)` |
| `(c *Config) WriteConfigTo(w)` | `(c *Config) WriteTo(w)` |
| `OverrideFirstWithSecond(dest, source)` | `Override(dest, source)` |
| `TransformToJSON(input) *jsonconfig.Config` | `Transform(input) *jsonconfig.Config` |

Add words only to disambiguate: `WriteTextTo` vs. `WriteBinaryTo`.

Conventions:

- Functions that return something get noun-like names: `(c *Config) JobName(key string) (value string, ok bool)`.
- Functions that do something get verb-like names: `WriteDetail(w io.Writer)`.
- Variants that differ only by type end with the type: `ParseInt`/`ParseInt64`, `AppendInt`/`AppendInt64`. If there is a clear primary version, leave the type off it: `Marshal` and `MarshalText`.
- Setup helpers that panic or `t.Fatal` on failure are named `MustXxx`/`mustXxx`. See errors.md.

## Getters

No `Get`/`get` prefix: `Counts()`, not `GetCounts()`. If the work is expensive, remote, or can block or fail, use a word that says so: `Fetch`, `Compute`, `Load`, `Lookup`. Keep `Get` only when the underlying concept is "get" (HTTP GET, an API that is itself named Get). Generated protobuf getters (`GetFoo()`) are out of your control and are fine to call.

## Receivers

- Short (one or two letters), an abbreviation of the type, and the same for every method of the type.
- Not `this`, `self`, or `me`. Not `_`: omit the name if it is unused.

| Long | Better |
| --- | --- |
| `func (tray Tray)` | `func (t Tray)` |
| `func (info *ResearchInfo)` | `func (ri *ResearchInfo)` |
| `func (this *ReportWriter)` | `func (w *ReportWriter)` |
| `func (self *Scanner)` | `func (s *Scanner)` |

## Constants

MixedCaps like everything else. No `ALL_CAPS`, no `k` prefix. Name a constant for what it *means*. If it has no role beyond its value, it should not be a constant.

```go
// Good
const MaxPacketSize = 512

const (
	ExecuteBit = 1 << iota
	WriteBit
	ReadBit
)

// Bad
const MAX_PACKET_SIZE = 512
const kMaxBufferSize = 1024
const Twelve = 12
const (
	UserNameColumn = "username" // named after the value, not its role
)
```

## Initialisms

All letters of an initialism share one case. In names with several initialisms, each initialism is consistent on its own. Initialisms that contain a lowercase letter keep their prose form unless exportedness forces the first letter's case. In that case the whole initialism takes that case.

| Prose | Exported | Unexported | Wrong |
| --- | --- | --- | --- |
| XML API | `XMLAPI` | `xmlAPI` | `XmlApi`, `XMLApi`, `xmlapi` |
| iOS | `IOS` | `iOS` | `Ios`, `IoS`, `ios` |
| gRPC | `GRPC` | `gRPC` | `Grpc`, `grpc` |
| DDoS | `DDoS` | `ddos` | `DDOS`, `Ddos`, `dDoS` |
| ID | `ID` | `id` | `Id`, `iD` |
| DB | `DB` | `db` | `Db`, `dB` |
| URL | `URL`, `ServeURL` | `url`, `urlPath` | `Url`, `ServeUrl` |
| Txn (an abbreviation, not an initialism) | `Txn` | `txn` | `TXN` |

Plurals: `IDs`, `URLs`. Names you must match for an external interface (for example `sql.Result.LastInsertId`) keep the external spelling.

## Variables and scope

Name length should be proportional to scope size and inversely proportional to how often the name is used. Rough guide: small scope (1-7 lines) means one letter or a short word; medium (8-15) means a word; large (15-25) means more words if needed; spanning more than a page means a descriptive name. These are not hard rules.

- Start with a single word (`count`, `options`). Add words to disambiguate (`userCount`, `projectCount`).
- Don't drop letters to save typing: `Sandbox`, not `Sbx`.
- Leave out type words: `userCount`, not `numUsers` or `usersInt`; `users`, not `userSlice`.
- Leave out words the context already gives. Inside `UserCount()`, `count` or `c` is enough.
- Name a local for what it holds *here*, not where it came from. It is often not the same as the struct or proto field name.
- A widely understood short name can stay short even in a large scope if nothing competes with it (`db` when there is one database).

Single letters are fine when the full word is obvious:

- `r` for `io.Reader` or `*http.Request`, `w` for `io.Writer` or `http.ResponseWriter`.
- `i` for an index, `x`/`y` for coordinates.
- Short-scope loop abbreviations: `for _, n := range nodes`.

## Shadowing

- *Stomping* (`ctx, cancel := context.WithTimeout(ctx, d)` in the same scope) is fine when the old value is no longer needed.
- *Shadowing* with `:=` in an inner block creates a new variable. Code after the block sees the outer one. This is a classic bug:

```go
// Bad: ctx after the if is still the caller's context.
if *shortenDeadlines {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
}

// Good: assign with =.
if *shortenDeadlines {
	var cancel func()
	ctx, cancel = context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
}
```

- Don't name variables after standard packages (`url`, `path`, `time`, `errors`) outside tiny scopes, since that hides the package. When picking a package name, avoid names that force callers to rename imports or variables.

## Test doubles and helper packages

- Put doubles for package `creditcard` in package `creditcardtest`.
- With one double for one type, use a plain name: `creditcardtest.Stub`, not `StubService` and certainly not `StubCreditCardService`.
- With several behaviors, name each by its behavior: `AlwaysCharges`, `AlwaysDeclines`.
- With doubles for several types, name each by type: `StubService`, `StubStoredValue`.
- For local variables in tests, prefix the name so the double stands out from production values: `spyCC`, not `cc`.
