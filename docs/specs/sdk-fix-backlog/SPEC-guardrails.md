# Spec: guardrails (PR 4)

Module ids: `guardrails`, `release`, and `test-transports`
([capability map](capability-map.md)). All three ship in PR 4, at the owner's request
(2026-11-02). Status: approved 2026-11-02.

It covers:
- the backlog's PR 4 row;
- the open key assumption about a gostyle telemetry rule;
- two items PR 1 deferred: the release/versioning repair and the real-transport test rewrite
  ([backlog](../../ideas/sdk-fix-backlog.md#not-doing-and-why)).

## Objective

PRs 1–3 fixed the SDK's code. PR 4 makes it releasable, and makes the fixes stay fixed.

- **A. Guardrails.**
  - A gostyle telemetry rule catches the two patterns behind most of PR 2 and PR 3's telemetry
    work: an `error.type` built from an error message, and a per-event attribute on a metric.
  - Every module is strict by default (full-tree lint plus gostyle in CI), except those on a
    short list that can only shrink.
- **B. Release.**
  - The repository is renamed `github.com/fikrimohammad/go-servicekit` (done by the owner on
    2026-11-02). Every module moves to the new path and starts at **v0.1.0**.
  - The new path has no history in the Go proxy, so no version needs to be skipped or
    retracted.
  - Sibling requires use real versions instead of zero pseudo-versions and deleted tags.
  - One workflow tags every module together, then proves each one installs from the proxy.
- **C. Real transports.** The unit tests for db, redis, s3, and nats drive real in-process
  fakes instead of the SDK's unexported transport stubs, and the stubs and seams are deleted.
  The probe on 2026-11-01 showed the stubs hid N18 and N9.

### User stories

- As a reviewer, a PR that puts `err.Error()` into `error.type`, or `db.query.text` onto a
  metric, fails CI with a message that names the rule and its fix.
- As a module author, a new module is strict from its first commit.
- As a service author, `go get github.com/fikrimohammad/go-servicekit/db@v0.1.0` works outside
  this repository, with no replace directives.
- As a maintainer, a client test passes only if the code works against a real protocol
  implementation.

## Assumptions (correct any of these before approving)

### A. Guardrails

1. **Two new gostyle rules:**
   - `error-type-message`: an `error.type` value comes from an error's text. That is a direct
     `x.Error()`, an identifier assigned from one, or a package function whose result is
     `x.Error()`.
   - `metric-high-cardinality`: an attribute map that reaches a metric call contains a
     per-event key.

   Each finding names its fix and links the semconv guidance.
2. **The rules are syntactic and package-scoped**, like the rest of gostyle (no type checking):
   - **Scope:** the rule analyses all non-test files in one directory together.
   - **Metric calls:** a call `.Record`, `.Add`, `.AddUpDown`, or `.RecordGauge` with four
     arguments (the `metrics.Client` shape), and any `metric.WithAttributes(...)` argument.
   - **Flow:** the attribute argument is traced through identifiers in the same function,
     through calls to package functions and methods that build it (followed to a fixpoint),
     and through map literals and `m[k] = v` writes. `maps.Clone` and `slices.Clone` end the
     trace, so a span-only copy is never flagged.
3. **Per-event keys** are matched as string literals or semconv constant names:

   | Key | Semconv constant |
   | --- | --- |
   | `db.query.text` | `DBQueryTextKey` |
   | `messaging.message.id` | `MessagingMessageIDKey` |
   | `messaging.message.conversation_id` | `MessagingMessageConversationIDKey` |
   | `messaging.message.body.size` | `MessagingMessageBodySizeKey` |
   | `exception.message` | `ExceptionMessageKey` |
   | `exception.stacktrace` | `ExceptionStacktraceKey` |
   | `url.full` | `URLFullKey` |
   | `url.query` | `URLQueryKey` |
   | `user.id` | `UserIDKey` |
   | `session.id` | `SessionIDKey` |
   | `error.message` | none in v1.43.0 |
   | `messaging.nats.message.key` | none (SDK-specific) |
   | `cron.job.fencing_token`, `fencing_token` | none (SDK-specific) |

   Bounded keys such as `db.operation.batch.size` and `messaging.nats.subject` are not listed.
   The list lives in one map in gostyle.go.
4. **Proven before it's trusted** (the backlog's open key assumption):
   - **Recall:** the rules must flag every pre-fix pattern on the PR 2 and PR 3 base commits.
     - At `411bb72`: db's `db.query.text` and message `error.type`, and the same in redis.
     - At `216b2aa`: nats producer and consumer `messaging.message.id` with an `err.Error()`
       `error.type`, and cron's `fencing_token` and `err.Error()` `error.type`.
   - **Precision:** they must report 0 findings on the PR 4 code in every module, rest
     included, and on s3 at `411bb72`.
   - The evidence goes in the PR body. If precision can't reach 0 without losing recall, the
     rule is narrowed and the miss is written down.
5. **Suppression** uses the existing `// gostyle:ignore <rule> <reason>`.
6. **Strict by default in CI.**
   - `STRICT_LINT_MODULES` becomes `LAX_MODULES: '["confloader"]'`.
   - A module not on it gets full-tree golangci-lint and gostyle.
   - confloader gets `only-new-issues` lint and no gostyle until its redesign.
   - The ci.yml comment and AGENTS.md say the list may only shrink.
7. **gostyle runs in CI:** a "Style" step for each non-lax module, plus a job that runs
   gostyle's own tests.

### B. Release

8. **Module path:** every module moves to `github.com/fikrimohammad/go-servicekit/<module>`.
   - That covers every go.mod `module`, `require`, and `replace` line, every import, the
     `.golangci.yml` ireturn allow list, the READMEs, AGENTS.md, the skills, and the specs.
   - The old `go-dev-sdk` path stays frozen at its cached versions. GitHub redirects the old
     URL, but a fetch of a new commit there fails with "module declares its path as …", which
     is fine with one in-dev consumer.
   - The local checkout directory doesn't need to move.
9. **Versions:** every module, confloader included (owner decision), starts at v0.1.0.
   - Sibling requires become `v0.1.0`, replacing today's `v0.0.0-00010101000000-000000000000`
     and the deleted `v1.0.0`.
   - The `replace` directives stay for workspace development. Consumers ignore them.
10. **One release for all modules.** `release.yml` is reworked to take one version and tag
    every module at it, as `<module>/vX.Y.Z` on one commit, pushed in a single
    `git push --atomic`. A sibling's required version therefore exists the moment any tag
    does. The existing guards stay: semver shape, major vs path, tag exists, proxy already
    has the version. A new guard requires every sibling `require` to equal the release
    version.
11. **Installability is proven.**
    - A post-tag job in `release.yml` creates a module outside the repository. It runs
      `GOPROXY=https://proxy.golang.org GOFLAGS=-mod=mod go get <every module>@vX.Y.Z` and
      `go build ./...` on an import of each.
    - CI also gets a pre-release check: with replaces dropped (`go mod edit -dropreplace`),
      every sibling require must point at the version in a single `VERSION` file.
    - The owner runs v0.1.0 itself after PR 4 merges. This PR doesn't tag anything.

### C. Real transports

12. **Fakes per client (owner decision: replace the stubs and delete the seams):**

    | Module | Fake |
    | --- | --- |
    | db | `github.com/DATA-DOG/go-sqlmock` through `Config{Driver: "sqlmock", DSN}`. The env-gated MySQL and PostgreSQL integration tests now run in CI against service containers. |
    | redis | `github.com/alicebob/miniredis/v2` |
    | s3 | `github.com/johannesboyne/gofakes3` with `backend/s3mem` on httptest |
    | nats | The embedded `nats-server/v2/test` server, as PR 3 already uses |

    These become test-only requires in each module's go.mod (owner-approved).
13. **Stubs and seams.**
    - Delete the unexported transport seams and stubs they relied on: the redis
      `underlying`/stub clients, s3's API stubs, nats's `jsProducer`/`jsConsumer` factories
      and mocks, and db's in-repo fake driver if sqlmock covers its cases.
    - A stub stays only where no fake can reach a case, such as an injected hung ack or a
      transport error mid-stream. Each kept stub has a one-line comment saying why.
14. **Coverage** may not drop below today's main for any package. The telemetry tests keep
    asserting exact metric attribute sets.

## Tech Stack

- Go 1.26.5. gostyle stays a standard-library-only, single-file `package main`.
- GitHub Actions, with golangci-lint v2.15.0, plus `services:` containers for MySQL and
  PostgreSQL.
- The test-only additions are go-sqlmock v1.5.2, miniredis/v2 v2.40.0, and gofakes3 v1.2.0.
  These are the versions the 2026-11-01 probe verified.

## Commands

```bash
# gostyle self-test (new CI job)
cd .agents/skills/go-best-practices/scripts && GOWORK=off go test -count=1 gostyle.go gostyle_test.go
# rule validation (recorded in the PR)
git worktree add --detach /tmp/x 411bb72 && GOWORK=off go run <gostyle.go> /tmp/x/db/... /tmp/x/redis/... /tmp/x/s3/...
git worktree add --detach /tmp/y 216b2aa && GOWORK=off go run <gostyle.go> /tmp/y/nats/... /tmp/y/cron/...
# release dry check (CI)
for m in */go.mod; do (cd "$(dirname $m)" && go mod edit -dropreplace=... && <check requires == VERSION>); done
# db integration (CI, with service containers)
DB_MYSQL_DSN=... DB_POSTGRES_DSN=... go test -run 'TestMySQLIntegration|TestPostgresIntegration' ./...
```

## Code Style

The gostyle finding:

```
db/db.go:402:2: metric-high-cardinality: "db.query.text" reaches a metric's attributes; keep per-event values on the span (https://opentelemetry.io/docs/specs/semconv/general/metrics/)
```

A real-transport test (redis), replacing a stub:

```go
func TestGet_RecordsDuration(t *testing.T) {
	srv := miniredis.RunT(t)
	reader := sdkmetric.NewManualReader()
	rdb, err := New(t.Context(), Config{Addrs: []string{srv.Addr()}},
		WithMetrics(metrics.Wrap(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))))
	...
}
```

## Testing Strategy

- **gostyle:**
  - `testdata/bad` and `testdata/good` gain cases for every traced shape, and `TestCheck`
    asserts each rule fires on bad and nothing fires on good.
  - A new `TestTelemetryRules_Table` covers each key form.
- **Release:**
  - The `release.yml` logic moves into a small checked-in script,
    `.github/scripts/release.sh`, with a dry-run mode. CI runs the dry run on every PR.
  - The installability job can only run after a real tag. Its first real run is the owner's
    v0.1.0.
- **Transports:** each module's existing test names keep their meaning where they still apply,
  and the regression tests from PRs 1–3 are kept and moved onto the fakes. Timing tests follow
  PR 3's rules: counts and order, tolerances of at least 100 ms, and `-count=3` locally.

## Boundaries

- **Always:**
  - Show recall and precision before turning the rule on in CI.
  - Keep `go test ./...` hermetic. The only env-gated tests are the DB integration tests,
    which CI feeds through service containers.
  - Keep coverage at or above main.
- **Ask first:**
  - Adding a module to `LAX_MODULES`.
  - Making gostyle type-aware.
  - Keeping more than a handful of stubs.
  - Any dependency beyond the three test fakes.
- **Never:**
  - Ship a rule with false positives on main.
  - Tag or push versions from this PR.
  - Reuse a version the proxy has seen.
  - Weaken a test to make it pass on a fake.

## Success Criteria

1. gostyle has `error-type-message` and `metric-high-cardinality`, with bad and good testdata,
   and its tests pass. Recall and precision are shown as in assumption 4.
2. CI uses `LAX_MODULES: '["confloader"]'`, runs gostyle on every other module, and runs the
   gostyle self-test job.
3. Every go.mod, import, and doc uses `github.com/fikrimohammad/go-servicekit`, and no
   `go-dev-sdk` reference remains outside the backlog's history.
4. Every sibling require is `v0.1.0`. `release.yml` tags every module atomically, verifies
   installability after tagging, and its dry run passes in CI.
5. db, redis, s3, and nats tests run against sqlmock, miniredis, gofakes3, and the embedded
   nats-server. The transport stubs and seams are deleted, except the justified ones. CI runs
   the MySQL and PostgreSQL integration tests.
6. Coverage per package is at or above main.
7. AGENTS.md and both skills describe the rules, strict-by-default, the new path, and the
   release flow. The backlog's key assumptions are ticked.
8. Every module passes the module checks, and CI is green.

## Open Questions

- None. The deferred confloader redesign stays in the backlog doc as a gate for v1.0.0, the
  stable release. It doesn't block v0.1.0: in v0, breaking changes are expected, and
  confloader ships its current API.
