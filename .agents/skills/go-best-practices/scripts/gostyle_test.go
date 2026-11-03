package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheck(t *testing.T) {
	bad, err := check([]string{filepath.Join("testdata", "bad")})
	if err != nil {
		t.Fatalf("check(bad) = %v", err)
	}
	fired := map[string]bool{}
	for _, f := range bad.findings {
		fired[f.rule] = true
	}
	for rule := range ruleDocs {
		if !fired[rule] {
			t.Errorf("check(testdata/bad) did not report rule %q", rule)
		}
	}

	good, err := check([]string{filepath.Join("testdata", "good")})
	if err != nil {
		t.Fatalf("check(good) = %v", err)
	}
	for _, f := range good.findings {
		t.Errorf("check(testdata/good) reported %s: %s %s", f.pos, f.rule, f.msg)
	}
}

func TestTelemetryRules_Table(t *testing.T) {
	const (
		metricRule = "metric-high-cardinality"
		errorRule  = "error-type-message"
	)
	tests := []struct {
		name string
		rule string
		src  string
		want int
	}{
		// Metric keys: each form of the key.
		{"metric literal key", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	m.Record(ctx, i, 1, map[string]string{"db.query.text": q})
}`, 1},
		{"metric semconv selector key", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	m.Record(ctx, i, 1, map[string]string{semconv.DBQueryTextKey: q})
}`, 1},
		{"metric string(semconv) key", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	m.Record(ctx, i, 1, map[string]string{string(semconv.DBQueryTextKey): q})
}`, 1},
		{"metric attribute.String in WithAttributes", metricRule, `package p
func f(c C, q string) {
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("db.query.text", q)))
}`, 1},
		{"metric key.String in WithAttributes", metricRule, `package p
func f(c C, q string) {
	c.Add(ctx, 1, metric.WithAttributes(semconv.DBQueryTextKey.String(q)))
}`, 1},
		{"metric bounded key", metricRule, `package p
func f(ctx context.Context, m M, s string) {
	m.Record(ctx, i, 1, map[string]string{"messaging.nats.subject": s})
}`, 0},
		{"per-event key on span only", metricRule, `package p
func f(span S, q string) {
	span.SetAttributes(attribute.String("db.query.text", q))
}`, 0},
		{"per-event key on a non-metric call", metricRule, `package p
func f(m M, q string) {
	m.Record(ctx, i, 1, map[string]string{"db.query.text": q}, extra)
}`, 0},

		// Metric flow shapes.
		{"builder function", metricRule, `package p
func attrs(q string) map[string]string { return map[string]string{"db.query.text": q} }
func f(ctx context.Context, m M, q string) {
	a := attrs(q)
	m.Record(ctx, i, 1, a)
}`, 1},
		{"builder method", metricRule, `package p
func (s *S) attrs(q string) map[string]string { return map[string]string{"db.query.text": q} }
func (s *S) f(ctx context.Context, q string) {
	a := s.attrs(q)
	s.m.Record(ctx, i, 1, a)
}`, 1},
		{"index write", metricRule, `package p
func f(ctx context.Context, m M, tok string) {
	a := map[string]string{}
	a["fencing_token"] = tok
	m.Add(ctx, i, 1, a)
}`, 1},
		{"index write with semconv key", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	a := map[string]string{}
	a[string(semconv.URLFullKey)] = q
	m.Add(ctx, i, 1, a)
}`, 1},
		{"var declaration", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	var a = map[string]string{"url.query": q}
	m.Record(ctx, i, 1, a)
}`, 1},
		{"identifier chain", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	a := map[string]string{"user.id": q}
	b := a
	m.Record(ctx, i, 1, b)
}`, 1},
		{"fixpoint across builders", metricRule, `package p
func a() map[string]string { return b() }
func b() map[string]string { return map[string]string{"session.id": ""} }
func f(ctx context.Context, m M) {
	m.Record(ctx, i, 1, a())
}`, 1},
		{"builder cycle terminates", metricRule, `package p
func a(n int) map[string]string {
	if n > 0 {
		return a(n - 1)
	}
	return b()
}
func b() map[string]string { return map[string]string{"url.full": ""} }
func f(ctx context.Context, m M) {
	m.Record(ctx, i, 1, a(3))
}`, 1},
		{"append of attribute values", metricRule, `package p
func f(ctx context.Context, m M, id string) {
	var a []attribute.KeyValue
	a = append(a, attribute.String("session.id", id))
	m.Add(ctx, i, 1, a)
}`, 1},
		{"clone stops the trace", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	a := map[string]string{"db.query.text": q}
	c := maps.Clone(a)
	m.Record(ctx, i, 1, c)
}`, 0},
		{"slices.Clone stops the trace", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	a := []attribute.KeyValue{attribute.String("db.query.text", q)}
	m.Record(ctx, i, 1, slices.Clone(a))
}`, 0},
		{"call outside the package stops the trace", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	a := external.Attrs(q)
	m.Record(ctx, i, 1, a)
}`, 0},
		{"one key reported once", metricRule, `package p
func a() map[string]string { return map[string]string{"db.query.text": ""} }
func f(ctx context.Context, m M) {
	m.Record(ctx, i, 1, a())
	m.Add(ctx, i, 1, a())
}`, 1},
		{"gostyle:ignore on the line above", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	// gostyle:ignore metric-high-cardinality reviewed: bounded in practice
	m.Record(ctx, i, 1, map[string]string{"db.query.text": q})
}`, 0},
		{"gostyle:ignore on metric call with builder map", metricRule, `package p
func attrs(q string) map[string]string { return map[string]string{"db.query.text": q} }
func f(ctx context.Context, m M, q string) {
	a := attrs(q)
	// gostyle:ignore metric-high-cardinality reviewed: bounded query enum
	m.Record(ctx, i, 1, a)
}`, 0},
		{"gostyle:ignore on one metric call does not hide an un-ignored call", metricRule, `package p
func attrs(q string) map[string]string { return map[string]string{"db.query.text": q} }
func f(ctx context.Context, m M, q string) {
	a := attrs(q)
	// gostyle:ignore metric-high-cardinality reviewed: bounded query enum
	m.Record(ctx, i, 1, a)
	m.Add(ctx, i, 1, a)
}`, 1},
		{"gostyle:ignore on multi-line WithAttributes", metricRule, `package p
func f(c C, q string) {
	// gostyle:ignore metric-high-cardinality reviewed: bounded query enum
	c.Add(ctx, 1, metric.WithAttributes(
		attribute.String("db.query.text", q),
	))
}`, 0},
		{"bare gostyle:ignore does not suppress", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	// gostyle:ignore
	m.Record(ctx, i, 1, map[string]string{"db.query.text": q})
}`, 1},
		{"gostyle:ignore without reason does not suppress", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	// gostyle:ignore metric-high-cardinality
	m.Record(ctx, i, 1, map[string]string{"db.query.text": q})
}`, 1},
		{"gostyle:ignore all does not suppress", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	// gostyle:ignore all reviewed
	m.Record(ctx, i, 1, map[string]string{"db.query.text": q})
}`, 1},
		{"prose mentioning gostyle:ignore does not suppress", metricRule, `package p
func f(ctx context.Context, m M, q string) {
	// Do not add gostyle:ignore metric-high-cardinality without a reason.
	m.Record(ctx, i, 1, map[string]string{"db.query.text": q})
}`, 1},

		// error.type from error text: each key form.
		{"error literal key", errorRule, `package p
func f(ctx context.Context, m M, err error) {
	m.Add(ctx, i, 1, map[string]string{"error.type": err.Error()})
}`, 1},
		{"error semconv selector key", errorRule, `package p
func f(err error) {
	a := map[string]string{semconv.ErrorTypeKey: err.Error()}
	_ = a
}`, 1},
		{"error string(semconv) index write", errorRule, `package p
func f(err error) {
	a := map[string]string{}
	a[string(semconv.ErrorTypeKey)] = err.Error()
	_ = a
}`, 1},
		{"error attribute.String", errorRule, `package p
func f(err error) {
	_ = attribute.String(string(semconv.ErrorTypeKey), err.Error())
}`, 1},
		{"error key.String", errorRule, `package p
func f(err error) {
	_ = semconv.ErrorTypeKey.String(err.Error())
}`, 1},
		{"gostyle:ignore on multi-line attribute.String", errorRule, `package p
func f(err error) {
	// gostyle:ignore error-type-message err is a bounded enum error type
	_ = attribute.String(
		string(semconv.ErrorTypeKey),
		err.Error(),
	)
}`, 0},

		// error.type from error text: each flow shape.
		{"error identifier from err.Error", errorRule, `package p
func f(err error) {
	var etype string
	etype = err.Error()
	_ = map[string]string{"error.type": etype}
}`, 1},
		{"error helper returns err.Error", errorRule, `package p
func errorText(err error) string { return err.Error() }
func f(err error) {
	a := map[string]string{}
	a[string(semconv.ErrorTypeKey)] = errorText(err)
	_ = a
}`, 1},
		{"error helper of a helper", errorRule, `package p
func errorText(err error) string { return inner(err) }
func inner(err error) string { return err.Error() }
func f(err error) {
	_ = map[string]string{"error.type": errorText(err)}
}`, 1},

		// error.type safe shapes.
		{"error coded class", errorRule, `package p
func errorType(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "_OTHER"
}
func f(err error) {
	code := errorType(err)
	_ = map[string]string{"error.type": code}
}`, 0},
		{"error.message is not error.type", errorRule, `package p
func f(err error) {
	_ = map[string]string{"error.message": err.Error()}
}`, 0},
		{"error.type literal string", errorRule, `package p
func f() {
	_ = map[string]string{"error.type": "timeout"}
}`, 0},

		// Parameters: the trace continues at every same-package call site.
		{"param map back-trace through method receiver", metricRule, `package p
func (m meta) record(ctx context.Context, d D, attrs map[string]any) {
	m.Record(ctx, inst, d, attrs)
}
func (m meta) attrs(q string) map[string]any { return map[string]any{"db.query.text": q} }
func (m meta) run(ctx context.Context, q string) {
	m.record(ctx, 1, m.attrs(q))
}`, 1},
		{"param map back-trace in grouped plain function", metricRule, `package p
func record(ctx context.Context, inst, d D, attrs map[string]any) {
	m.Record(ctx, inst, d, attrs)
}
func run(ctx context.Context, u string) {
	record(ctx, i, 1, map[string]any{"user.id": u})
}`, 1},
		{"param receives only safe values", metricRule, `package p
func record(ctx context.Context, attrs map[string]any) {
	m.Record(ctx, i, 1, attrs)
}
func run(ctx context.Context, base map[string]any, s string) {
	record(ctx, map[string]any{"messaging.nats.subject": s})
	record(ctx, maps.Clone(base))
}`, 0},
		{"param without call sites", metricRule, `package p
func record(ctx context.Context, attrs map[string]any) {
	m.Record(ctx, i, 1, attrs)
}`, 0},

		// Tuple assignments: the identifier at index i traces result i.
		{"tuple index 1 of a same-package call", errorRule, `package p
func errorInfo(err error) (string, string) { return "", err.Error() }
func f(err error) {
	code, etype := errorInfo(err)
	_ = code
	_ = map[string]string{"error.type": etype}
}`, 1},
		{"tuple index 1 with plain assignment", errorRule, `package p
func errorInfo(err error) (string, string) { return "", err.Error() }
func f(err error) {
	var code, etype string
	code, etype = errorInfo(err)
	_ = code
	_ = map[string]string{"error.type": etype}
}`, 1},
		{"tuple index 0 is safe", errorRule, `package p
func errorInfo(err error) (string, string) { return "timeout", err.Error() }
func f(err error) {
	code, _ := errorInfo(err)
	_ = map[string]string{"error.type": code}
}`, 0},
		{"tuple metric key from builder", metricRule, `package p
func pair(q string) (map[string]any, int) { return map[string]any{"db.query.text": q}, 1 }
func f(ctx context.Context, m M, q string) {
	a, n := pair(q)
	_ = n
	m.Record(ctx, i, 1, a)
}`, 1},

		// Error value through a parameter.
		{"error value through a parameter", errorRule, `package p
func (m meta) attrs(op, etype string) map[string]string { return map[string]string{"error.type": etype} }
func (m meta) run(err error) {
	m.attrs("op", err.Error())
}`, 1},
		{"error param receives a coded class", errorRule, `package p
func errorType(err error) string { return "_OTHER" }
func (m meta) attrs(op, etype string) map[string]string { return map[string]string{"error.type": etype} }
func (m meta) run(err error) {
	m.attrs("op", errorType(err))
}`, 0},

		// Receiver-type narrowing when several methods share a name.
		{"same method name, composite base picks the right type", metricRule, `package p
type span struct{}
type meta struct{}
func (span) attrs(q string) map[string]any { return map[string]any{"db.query.text": q} }
func (meta) attrs(op string) map[string]any { return map[string]any{"db.operation.name": op} }
func f(ctx context.Context, m M) {
	m.Record(ctx, "d", 1, meta{}.attrs("GET"))
}`, 0},
		{"same method name, receiver identifier of the span type", metricRule, `package p
type span struct{}
type meta struct{}
func (span) attrs(q string) map[string]any { return map[string]any{"db.query.text": q} }
func (meta) attrs(op string) map[string]any { return map[string]any{"db.operation.name": op} }
func (s span) run(ctx context.Context, m M) {
	m.Record(ctx, "d", 1, s.attrs("GET"))
}`, 1},
		{"same method name, pointer composite base", metricRule, `package p
type span struct{}
type meta struct{}
func (span) attrs(q string) map[string]any { return map[string]any{"db.query.text": q} }
func (meta) attrs(op string) map[string]any { return map[string]any{"db.operation.name": op} }
func f(ctx context.Context, m M) {
	m.Record(ctx, "d", 1, (&span{}).attrs("GET"))
}`, 1},
		{"same method name, var declared with the span type", metricRule, `package p
type span struct{}
type meta struct{}
func (span) attrs(q string) map[string]any { return map[string]any{"db.query.text": q} }
func (meta) attrs(op string) map[string]any { return map[string]any{"db.operation.name": op} }
func f(ctx context.Context, m M) {
	var s span
	m.Record(ctx, "d", 1, s.attrs("GET"))
}`, 1},
		{"same method name, ambiguous unknown base is not followed", metricRule, `package p
type span struct{}
type meta struct{}
func (span) attrs(q string) map[string]any { return map[string]any{"db.query.text": q} }
func (meta) attrs(op string) map[string]any { return map[string]any{"db.operation.name": op} }
func f(ctx context.Context, m M) {
	m.Record(ctx, "d", 1, lookup().attrs("GET"))
}`, 0},
		{"plain function is not reached by a selector call", metricRule, `package p
func attrs(q string) map[string]any { return map[string]any{"db.query.text": q} }
func f(ctx context.Context, m M, x X) {
	m.Record(ctx, "d", 1, x.attrs("GET"))
}`, 0},
		{"method is not reached by an unqualified call", metricRule, `package p
type span struct{}
func (span) attrs(q string) map[string]any { return map[string]any{"db.query.text": q} }
func f(ctx context.Context, m M) {
	m.Record(ctx, "d", 1, attrs("GET"))
}`, 0},

		// Parameter back-tracing follows the same narrowing.
		{"param call sites narrowed by receiver type", metricRule, `package p
type span struct{}
type meta struct{}
func (span) record(ctx context.Context, attrs map[string]any) { m.Record(ctx, "d", 1, attrs) }
func (meta) record(ctx context.Context, attrs map[string]any) {}
func (meta) attrs(op string) map[string]any { return map[string]any{"db.query.text": op} }
func f(ctx context.Context) {
	meta{}.record(ctx, meta{}.attrs("GET"))
}`, 0},

		// Plain builder function, called unqualified.
		{"plain builder function", metricRule, `package p
func buildAttrs(q string) map[string]string { return map[string]string{"db.query.text": q} }
func f(ctx context.Context, m M, q string) {
	m.Record(ctx, "d", 1, buildAttrs(q))
}`, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "x.go"), []byte(tt.src), 0o644); err != nil {
				t.Fatal(err)
			}
			c, err := check([]string{dir})
			if err != nil {
				t.Fatalf("check(%s) = %v", dir, err)
			}
			got := 0
			for _, f := range c.findings {
				if f.rule == tt.rule {
					got++
				}
			}
			if got != tt.want {
				t.Errorf("got %d %s findings, want %d", got, tt.rule, tt.want)
			}
		})
	}
}

func TestTelemetryRules_Package(t *testing.T) {
	const metricRule = "metric-high-cardinality"
	tests := []struct {
		name  string
		files map[string]string
		want  int
	}{
		{"builder in one file, metric call in another", map[string]string{
			"a.go": `package p
func buildAttrs(q string) map[string]any { return map[string]any{"db.query.text": q} }
`,
			"b.go": `package p
func run(ctx context.Context, m M, q string) {
	m.Record(ctx, "d", 1, buildAttrs(q))
}
`,
		}, 1},
		{"builder only in a _test.go file is excluded", map[string]string{
			"helpers_test.go": `package p
func buildAttrs(q string) map[string]any { return map[string]any{"db.query.text": q} }
`,
			"x.go": `package p
func run(ctx context.Context, m M, q string) {
	m.Record(ctx, "d", 1, buildAttrs(q))
}
`,
		}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, src := range tt.files {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			c, err := check([]string{dir})
			if err != nil {
				t.Fatalf("check(%s) = %v", dir, err)
			}
			got := 0
			for _, f := range c.findings {
				if f.rule == metricRule {
					got++
				}
			}
			if got != tt.want {
				t.Errorf("got %d %s findings, want %d", got, metricRule, tt.want)
			}
		})
	}
}
