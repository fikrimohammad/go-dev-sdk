// Command gostyle reports Google Go Style Guide rules that golangci-lint (with
// the repository .golangci.yml) cannot express. It is syntactic (no type
// checking), so its findings ask for judgment, not blind fixes.
//
// Usage:
//
//	GOWORK=off go run gostyle.go [path ...]
//
// Each path is a .go file or a directory (walked recursively; a trailing
// "/..." is accepted). The default is ".". Findings print as
//
//	file:line:col: rule: message (guide link)
//
// The exit status is 1 when anything is reported.
//
// Silence a deliberate deviation with a comment on the same line or the line
// above:
//
//	// gostyle:ignore getter-prefix mirrors the HTTP GET verb
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	decisions = "https://google.github.io/styleguide/go/decisions"
	practices = "https://google.github.io/styleguide/go/best-practices"
)

// ruleDocs maps each rule to the style guide section that motivates it.
var ruleDocs = map[string]string{
	"getter-prefix":           decisions + "#getters",
	"error-concrete-return":   decisions + "#returning-errors",
	"error-log-and-return":    practices + "#error-logging",
	"library-panic":           decisions + "#dont-panic",
	"constructor-stutter":     decisions + "#repetitive-with-package",
	"const-k-prefix":          decisions + "#constant-names",
	"receiver-name":           decisions + "#receiver-names",
	"test-message":            decisions + "#got-before-want",
	"labeled-branch":          "https://google.github.io/styleguide/go/guide#simplicity",
	"canonical-method":        "https://go.dev/doc/effective_go#interface-names",
	"error-type-message":      "https://opentelemetry.io/docs/specs/semconv/attributes-registry/error/",
	"metric-high-cardinality": "https://opentelemetry.io/docs/specs/semconv/general/metrics/",
}

type finding struct {
	pos  token.Position
	rule string
	msg  string
}

type checker struct {
	fset     *token.FileSet
	findings []finding
	// ignores maps each file to its line -> suppressed rules.
	ignores map[string]map[int][]string
	// dirs groups the parsed non-test files by directory for package-scoped rules.
	dirs map[string][]*ast.File
}

type fileCtx struct {
	*checker
	file   *ast.File
	pkg    string
	isTest bool
	isMain bool
}

var (
	testFuncRe = regexp.MustCompile(`^(Test|Benchmark|Example|Fuzz)`)
	getterRe   = regexp.MustCompile(`^[Gg]et[A-Z]`)
	kConstRe   = regexp.MustCompile(`^k[A-Z]`)
	logMethods = map[string]bool{"Print": true, "Printf": true, "Println": true, "Error": true, "Errorf": true, "ErrorContext": true, "Warn": true, "Warnf": true, "WarnContext": true, "Info": true, "Infof": true, "Log": true, "Logf": true}
)

func main() {
	flag.Parse()
	paths := flag.Args()
	if len(paths) == 0 {
		paths = []string{"."}
	}
	c, err := check(paths)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gostyle: %v\n", err)
		os.Exit(2)
	}
	sort.Slice(c.findings, func(i, j int) bool {
		a, b := c.findings[i].pos, c.findings[j].pos
		if a.Filename != b.Filename {
			return a.Filename < b.Filename
		}
		return a.Line < b.Line
	})
	for _, f := range c.findings {
		fmt.Printf("%s: %s: %s (%s)\n", f.pos, f.rule, f.msg, ruleDocs[f.rule])
	}
	fmt.Fprintf(os.Stderr, "gostyle: %d finding(s)\n", len(c.findings))
	if len(c.findings) > 0 {
		os.Exit(1)
	}
}

// check runs every rule over paths.
func check(paths []string) (*checker, error) {
	c := &checker{
		fset:    token.NewFileSet(),
		ignores: map[string]map[int][]string{},
		dirs:    map[string][]*ast.File{},
	}
	for _, p := range paths {
		if err := c.walk(strings.TrimSuffix(p, "/...")); err != nil {
			return nil, err
		}
	}
	for _, files := range c.dirs {
		c.checkTelemetry(files)
	}
	return c, nil
}

func (c *checker) walk(root string) error {
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return c.checkFile(root)
	}
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if path != root && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(name, ".go") {
			return c.checkFile(path)
		}
		return nil
	})
}

func (c *checker) checkFile(path string) error {
	f, err := parser.ParseFile(c.fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return err
	}
	if ast.IsGenerated(f) {
		return nil
	}
	ignores := map[int][]string{}
	for _, cg := range f.Comments {
		for _, cm := range cg.List {
			_, rest, ok := strings.Cut(cm.Text, "gostyle:ignore")
			if !ok {
				continue
			}
			line := c.fset.Position(cm.Pos()).Line
			rules := strings.Split(strings.Fields(rest + " all")[0], ",")
			ignores[line] = append(ignores[line], rules...)
			ignores[line+1] = append(ignores[line+1], rules...)
		}
	}
	c.ignores[path] = ignores
	isTest := strings.HasSuffix(path, "_test.go")
	if !isTest {
		dir := filepath.Dir(path)
		c.dirs[dir] = append(c.dirs[dir], f)
	}
	fc := &fileCtx{
		checker: c,
		file:    f,
		pkg:     f.Name.Name,
		isTest:  isTest,
		isMain:  f.Name.Name == "main",
	}
	fc.run()
	return nil
}

func (fc *fileCtx) report(n ast.Node, rule, format string, args ...any) {
	fc.reportAt(n.Pos(), rule, format, args...)
}

// reportAt records a finding at p unless a gostyle:ignore comment for rule is
// on its line or the line above.
func (c *checker) reportAt(p token.Pos, rule, format string, args ...any) {
	pos := c.fset.Position(p)
	for _, r := range c.ignores[pos.Filename][pos.Line] {
		if r == rule || r == "all" {
			return
		}
	}
	c.findings = append(c.findings, finding{pos, rule, fmt.Sprintf(format, args...)})
}

func (fc *fileCtx) run() {
	for _, decl := range fc.file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			fc.checkFuncDecl(d)
		case *ast.GenDecl:
			if d.Tok != token.CONST {
				continue
			}
			for _, spec := range d.Specs {
				for _, id := range spec.(*ast.ValueSpec).Names {
					if kConstRe.MatchString(id.Name) {
						fc.report(id, "const-k-prefix", "%q: do not prefix constants with k; name them for their role in MixedCaps", id.Name)
					}
				}
			}
		}
	}
	ast.Inspect(fc.file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.IfStmt:
			fc.checkLogAndReturn(n)
		case *ast.CallExpr:
			fc.checkTestMessage(n)
		case *ast.LabeledStmt:
			fc.report(n, "labeled-branch", "label %q makes control flow hard to follow; use errgroup.SetLimit for bounded fan-out, or extract a function and return", n.Label.Name)
		}
		return true
	})
}

func (fc *fileCtx) checkFuncDecl(d *ast.FuncDecl) {
	name := d.Name.Name
	isTestFunc := fc.isTest && d.Recv == nil && testFuncRe.MatchString(name)

	if d.Recv != nil && (name == "ToString" || name == "toString") {
		fc.report(d.Name, "canonical-method", "name the string conversion String() so fmt and %%v use it")
	}

	if getterRe.MatchString(name) && !isTestFunc {
		fc.report(d.Name, "getter-prefix", "drop the Get prefix (%s -> %s) unless the concept is literally \"get\" (e.g. HTTP GET); use Fetch/Compute/Load for slow or remote work", name, name[3:])
	}

	// widget.NewWidget should be widget.New. revive's exported rule only
	// catches names that start with the package name.
	if d.Recv == nil && !fc.isMain && !fc.isTest && len(fc.pkg) > 1 && strings.HasPrefix(name, "New") &&
		strings.HasPrefix(strings.ToLower(name[len("New"):]), fc.pkg) {
		fc.report(d.Name, "constructor-stutter", "%s.%s repeats the package name; use %s.New (single primary type) or %s.New<Type>", fc.pkg, name, fc.pkg, fc.pkg)
	}

	if d.Recv != nil && len(d.Recv.List) == 1 && len(d.Recv.List[0].Names) == 1 {
		switch r := d.Recv.List[0].Names[0]; {
		case r.Name == "_":
			fc.report(r, "receiver-name", "omit an unused receiver name instead of using _")
		case len(r.Name) > 3:
			fc.report(r, "receiver-name", "receiver %q is long; use a one- or two-letter abbreviation of the type", r.Name)
		}
	}

	// A function that can fail returns error, not a concrete error type: a nil
	// *T stored in an error interface is non-nil.
	if res := d.Type.Results; res != nil && fieldCount(res) >= 2 {
		last := res.List[len(res.List)-1].Type
		if star, ok := last.(*ast.StarExpr); ok && strings.HasSuffix(typeName(star.X), "Error") {
			fc.report(last, "error-concrete-return", "return the error interface, not *%s; a nil concrete pointer becomes a non-nil error", typeName(star.X))
		}
	}

	if d.Body == nil || fc.isMain || fc.isTest || name == "init" || strings.HasPrefix(name, "Must") || strings.HasPrefix(name, "must") {
		return
	}
	ast.Inspect(d.Body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		if call, ok := n.(*ast.CallExpr); ok {
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "panic" {
				fc.report(call, "library-panic", "library code returns errors; panic only for API misuse or broken invariants, and never across the package boundary")
			}
		}
		return true
	})
}

// checkLogAndReturn reports an if block that both logs err and returns it,
// which reports the same failure twice.
func (fc *fileCtx) checkLogAndReturn(n *ast.IfStmt) {
	var logged, returned bool
	for _, st := range n.Body.List {
		switch st := st.(type) {
		case *ast.ExprStmt:
			if call, ok := st.X.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && logMethods[sel.Sel.Name] && strings.Contains(strings.ToLower(exprString(sel.X)), "log") && mentionsErr(call) {
					logged = true
				}
			}
		case *ast.ReturnStmt:
			if mentionsErr(st) {
				returned = true
			}
		}
	}
	if logged && returned {
		fc.report(n, "error-log-and-return", "error is both logged and returned; return it with context and let the caller decide whether to log")
	}
}

// checkTestMessage reports test failures that print want before got.
// revive's string-format rule already bans expected/actual wording.
func (fc *fileCtx) checkTestMessage(call *ast.CallExpr) {
	if !fc.isTest || len(call.Args) == 0 {
		return
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || (sel.Sel.Name != "Errorf" && sel.Sel.Name != "Fatalf") {
		return
	}
	bl, ok := call.Args[0].(*ast.BasicLit)
	if !ok || bl.Kind != token.STRING {
		return
	}
	s, err := strconv.Unquote(bl.Value)
	if err != nil {
		return
	}
	lower := strings.ToLower(s)
	w, g := strings.Index(lower, "want"), strings.Index(lower, "got")
	if w >= 0 && g > w && !strings.Contains(lower, "-want") && !strings.Contains(lower, "want)") {
		fc.report(bl, "test-message", "print got before want: \"Func(%%v) = %%v, want %%v\"")
	}
}

func mentionsErr(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && id.Name == "err" {
			found = true
		}
		return !found
	})
	return found
}

func typeName(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	case *ast.IndexExpr:
		return typeName(e.X)
	case *ast.IndexListExpr:
		return typeName(e.X)
	case *ast.StarExpr:
		return typeName(e.X)
	}
	return ""
}

func fieldCount(fl *ast.FieldList) int {
	n := 0
	for _, f := range fl.List {
		n += max(len(f.Names), 1)
	}
	return n
}

func exprString(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return exprString(e.X) + "." + e.Sel.Name
	case *ast.CallExpr:
		return exprString(e.Fun) + "()"
	}
	return ""
}

// perEventKeys maps each attribute key whose value differs for every event (a
// query, message, user, URL, or stack) to its semconv constant name, or "" when
// semconv has none. On a metric such a key creates a series per event, so the
// value belongs on the span.
var perEventKeys = map[string]string{
	"db.query.text":                     "DBQueryTextKey",
	"messaging.message.id":              "MessagingMessageIDKey",
	"messaging.message.conversation_id": "MessagingMessageConversationIDKey",
	"messaging.message.body.size":       "MessagingMessageBodySizeKey",
	"exception.message":                 "ExceptionMessageKey",
	"exception.stacktrace":              "ExceptionStacktraceKey",
	"url.full":                          "URLFullKey",
	"url.query":                         "URLQueryKey",
	"user.id":                           "UserIDKey",
	"session.id":                        "SessionIDKey",
	"error.message":                     "",
	"messaging.nats.message.key":        "",
	"cron.job.fencing_token":            "",
	"fencing_token":                     "",
}

// metricMethods are the metrics.Client-shaped calls whose fourth argument is
// the attribute set.
var metricMethods = map[string]bool{"Record": true, "Add": true, "AddUpDown": true, "RecordGauge": true}

// valueMethods are the attribute constructors: attribute.String(k, v) and the
// key's own constructor k.String(v).
var valueMethods = map[string]bool{
	"String": true, "Bool": true, "Int": true, "Int64": true, "Float64": true,
	"StringSlice": true, "BoolSlice": true, "IntSlice": true, "Int64Slice": true, "Float64Slice": true,
}

// pkgIndex is the package-scoped view that the telemetry rules share.
type pkgIndex struct {
	c        *checker
	funcs    map[string][]*funcInfo   // same-package functions and methods by name
	calls    map[*funcInfo][]callSite // same-package call sites of each function
	seen     map[ast.Node]bool        // expressions already traced
	reported map[token.Pos]bool       // key positions already reported
}

// callSite is a call to a same-package function and the function it appears in.
type callSite struct {
	call   *ast.CallExpr
	caller *funcInfo
}

// tupleRef is an identifier assigned from result index of a multi-value call.
type tupleRef struct {
	call  *ast.CallExpr
	index int
}

// funcInfo holds what the telemetry rules need from one function.
type funcInfo struct {
	body     *ast.BlockStmt
	isMethod bool
	recvName string                      // receiver identifier, "" when unnamed
	recvType string                      // receiver base type name
	params   map[string]int              // parameter name -> position, receiver excluded
	varTypes map[string]string           // parameter or var name -> declared base type name
	defs     map[string][]ast.Expr       // identifier -> values assigned to it
	tuples   map[string][]tupleRef       // identifier -> result of a multi-value call
	writes   map[string][]*ast.IndexExpr // identifier -> index writes m[k] = v
	returns  [][]ast.Expr                // results of each return statement
}

// checkTelemetry runs the package-scoped telemetry rules over the non-test
// files of one directory.
//
// Limits: a call through a function literal (func(){...}()) or through a
// variable holding a closure is not traced, and neither is a generic
// instantiation callee such as Gen[int](k). Those calls end the trace, as
// maps.Clone does, so a key set only through them is not reported.
func (c *checker) checkTelemetry(files []*ast.File) {
	idx := &pkgIndex{
		c:        c,
		funcs:    map[string][]*funcInfo{},
		calls:    map[*funcInfo][]callSite{},
		seen:     map[ast.Node]bool{},
		reported: map[token.Pos]bool{},
	}
	var infos []*funcInfo
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			fi := newFuncInfo(fd)
			infos = append(infos, fi)
			idx.funcs[fd.Name.Name] = append(idx.funcs[fd.Name.Name], fi)
		}
	}
	for _, fi := range infos {
		ast.Inspect(fi.body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				for _, callee := range idx.callees(call, fi) {
					idx.calls[callee] = append(idx.calls[callee], callSite{call, fi})
				}
			}
			return true
		})
	}
	for _, fi := range infos {
		ast.Inspect(fi.body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.CompositeLit:
				for _, elt := range n.Elts {
					if kv, ok := elt.(*ast.KeyValueExpr); ok {
						idx.checkErrorType(kv.Key, kv.Value, fi)
					}
				}
			case *ast.AssignStmt:
				if len(n.Lhs) == len(n.Rhs) {
					for i, lhs := range n.Lhs {
						if ix, ok := lhs.(*ast.IndexExpr); ok {
							idx.checkErrorType(ix.Index, n.Rhs[i], fi)
						}
					}
				}
			case *ast.CallExpr:
				idx.checkErrorTypeCall(n, fi)
				if isMetricCall(n) {
					idx.traceAttrs(n.Args[3], fi)
				}
				if isWithAttributes(n) {
					for _, arg := range n.Args {
						idx.traceAttrs(arg, fi)
					}
				}
			}
			return true
		})
	}
}

// newFuncInfo indexes the parameters, assignments, index writes, and returns
// of fd. It includes closures, so an assignment inside a closure is still found.
func newFuncInfo(fd *ast.FuncDecl) *funcInfo {
	fi := &funcInfo{
		body:     fd.Body,
		params:   map[string]int{},
		varTypes: map[string]string{},
		defs:     map[string][]ast.Expr{},
		tuples:   map[string][]tupleRef{},
		writes:   map[string][]*ast.IndexExpr{},
	}
	if fd.Recv != nil && len(fd.Recv.List) == 1 {
		fi.isMethod = true
		fi.recvType = typeName(fd.Recv.List[0].Type)
		for _, name := range fd.Recv.List[0].Names {
			fi.recvName = name.Name
		}
	}
	pos := 0
	for _, field := range fd.Type.Params.List {
		if len(field.Names) == 0 {
			pos++
		}
		for _, name := range field.Names {
			fi.params[name.Name] = pos
			fi.varTypes[name.Name] = typeName(field.Type)
			pos++
		}
	}
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			if len(n.Lhs) == len(n.Rhs) {
				for i, lhs := range n.Lhs {
					switch l := lhs.(type) {
					case *ast.Ident:
						fi.defs[l.Name] = append(fi.defs[l.Name], n.Rhs[i])
					case *ast.IndexExpr:
						if id, ok := l.X.(*ast.Ident); ok {
							fi.writes[id.Name] = append(fi.writes[id.Name], l)
						}
					}
				}
			} else if call, ok := tupleCall(n.Rhs); ok {
				for i, lhs := range n.Lhs {
					if id, ok := lhs.(*ast.Ident); ok {
						fi.tuples[id.Name] = append(fi.tuples[id.Name], tupleRef{call, i})
					}
				}
			}
		case *ast.ValueSpec:
			if n.Type != nil {
				for _, name := range n.Names {
					fi.varTypes[name.Name] = typeName(n.Type)
				}
			}
			if len(n.Names) == len(n.Values) {
				for i, name := range n.Names {
					fi.defs[name.Name] = append(fi.defs[name.Name], n.Values[i])
				}
			} else if call, ok := tupleCall(n.Values); ok {
				for i, name := range n.Names {
					fi.tuples[name.Name] = append(fi.tuples[name.Name], tupleRef{call, i})
				}
			}
		case *ast.ReturnStmt:
			fi.returns = append(fi.returns, n.Results)
		}
		return true
	})
	return fi
}

// allReturns returns every expression fi returns, at any result position.
func (fi *funcInfo) allReturns() []ast.Expr {
	var out []ast.Expr
	for _, rs := range fi.returns {
		out = append(out, rs...)
	}
	return out
}

// returnsAt returns the expression fi returns at result position i.
func (fi *funcInfo) returnsAt(i int) []ast.Expr {
	var out []ast.Expr
	for _, rs := range fi.returns {
		if i < len(rs) {
			out = append(out, rs[i])
		}
	}
	return out
}

// argRef is an argument expression and the function it is written in.
type argRef struct {
	expr   ast.Expr
	caller *funcInfo
}

// paramArgs returns the arguments that bind to parameter name of fi at each
// same-package call site. The receiver is not in the argument list, so the
// positions line up for methods too.
func (idx *pkgIndex) paramArgs(fi *funcInfo, name string) []argRef {
	pos, ok := fi.params[name]
	if !ok {
		return nil
	}
	var out []argRef
	for _, cs := range idx.calls[fi] {
		if pos < len(cs.call.Args) {
			out = append(out, argRef{cs.call.Args[pos], cs.caller})
		}
	}
	return out
}

// tupleValues returns the expressions that the identifier assigned from t can
// take: result t.index of each same-package callee.
func (idx *pkgIndex) tupleValues(t tupleRef, caller *funcInfo) []argRef {
	var out []argRef
	for _, callee := range idx.callees(t.call, caller) {
		for _, r := range callee.returnsAt(t.index) {
			out = append(out, argRef{r, callee})
		}
	}
	return out
}

// tupleCall returns the call when exprs is the single multi-value call on the
// right of a tuple assignment.
func tupleCall(exprs []ast.Expr) (*ast.CallExpr, bool) {
	if len(exprs) != 1 {
		return nil, false
	}
	call, ok := unparen(exprs[0]).(*ast.CallExpr)
	return call, ok
}

// unparen strips parentheses from e.
func unparen(e ast.Expr) ast.Expr {
	if p, ok := e.(*ast.ParenExpr); ok {
		return unparen(p.X)
	}
	return e
}

// callees returns the same-package functions and methods that call, written in
// caller, invokes. It returns nil for maps.Clone, slices.Clone, and any call
// outside the package, which ends a trace.
//
// A plain function matches only an unqualified call f(...), and a method matches
// only a selector call x.f(...). When several methods share the name, the base
// type of x must be known from the syntax (see baseType) to pick one. When it
// is unknown, a single candidate is followed and several are not, so the trace
// stops rather than guess. Methods promoted from an embedded type are not
// followed.
func (idx *pkgIndex) callees(call *ast.CallExpr, caller *funcInfo) []*funcInfo {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		var plain []*funcInfo
		for _, f := range idx.funcs[fn.Name] {
			if !f.isMethod {
				plain = append(plain, f)
			}
		}
		return plain
	case *ast.SelectorExpr:
		if isIdent(fn.X, "maps") || isIdent(fn.X, "slices") {
			return nil
		}
		var methods []*funcInfo
		for _, f := range idx.funcs[fn.Sel.Name] {
			if f.isMethod {
				methods = append(methods, f)
			}
		}
		typ, known := caller.baseType(fn.X)
		if !known {
			if len(methods) == 1 {
				return methods
			}
			return nil
		}
		var out []*funcInfo
		for _, m := range methods {
			if m.recvType == typ {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// baseType returns the named type of x when the syntax shows it: the enclosing
// receiver, a parameter or variable declared with a type, a composite literal,
// or &T{...}. Every assignment to an identifier must agree on the type.
func (fi *funcInfo) baseType(x ast.Expr) (string, bool) {
	x = unparen(x)
	if u, ok := x.(*ast.UnaryExpr); ok && u.Op == token.AND {
		x = unparen(u.X)
	}
	switch e := x.(type) {
	case *ast.CompositeLit:
		t := typeName(e.Type)
		return t, t != ""
	case *ast.Ident:
		if e.Name == fi.recvName && fi.recvName != "" {
			return fi.recvType, true
		}
		var typ string
		for _, v := range fi.defs[e.Name] {
			t, ok := compositeType(v)
			if !ok || (typ != "" && t != typ) {
				return "", false
			}
			typ = t
		}
		if t := fi.varTypes[e.Name]; t != "" {
			if typ != "" && t != typ {
				return "", false
			}
			typ = t
		}
		return typ, typ != ""
	}
	return "", false
}

// compositeType returns the type of a composite literal, or of &T{...}.
func compositeType(e ast.Expr) (string, bool) {
	e = unparen(e)
	if u, ok := e.(*ast.UnaryExpr); ok && u.Op == token.AND {
		e = unparen(u.X)
	}
	lit, ok := e.(*ast.CompositeLit)
	if !ok {
		return "", false
	}
	t := typeName(lit.Type)
	return t, t != ""
}

// traceAttrs follows an attribute expression that reaches a metric call and
// checks each per-event key it sets.
func (idx *pkgIndex) traceAttrs(e ast.Expr, fi *funcInfo) {
	if idx.seen[e] {
		return
	}
	idx.seen[e] = true
	switch e := e.(type) {
	case *ast.ParenExpr:
		idx.traceAttrs(e.X, fi)
	case *ast.CompositeLit:
		for _, elt := range e.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				idx.checkKey(kv.Key)
			} else {
				idx.traceAttrs(elt, fi)
			}
		}
	case *ast.Ident:
		for _, v := range fi.defs[e.Name] {
			idx.traceAttrs(v, fi)
		}
		for _, t := range fi.tuples[e.Name] {
			for _, v := range idx.tupleValues(t, fi) {
				idx.traceAttrs(v.expr, v.caller)
			}
		}
		for _, a := range idx.paramArgs(fi, e.Name) {
			idx.traceAttrs(a.expr, a.caller)
		}
		for _, w := range fi.writes[e.Name] {
			idx.checkKey(w.Index)
		}
	case *ast.CallExpr:
		if key, ok := kvKey(e); ok {
			idx.checkKey(key)
			return
		}
		if isIdent(e.Fun, "append") {
			for _, arg := range e.Args {
				idx.traceAttrs(arg, fi)
			}
			return
		}
		for _, callee := range idx.callees(e, fi) {
			for _, r := range callee.allReturns() {
				idx.traceAttrs(r, callee)
			}
		}
	}
}

// checkKey reports key when it is a per-event key that reaches a metric.
func (idx *pkgIndex) checkKey(key ast.Expr) {
	name, ok := perEventKey(key)
	if !ok || idx.reported[key.Pos()] {
		return
	}
	idx.reported[key.Pos()] = true
	idx.c.reportAt(key.Pos(), "metric-high-cardinality", "%q reaches a metric's attributes; keep per-event values on the span", name)
}

// checkErrorTypeCall checks the value of an attribute.String(k, v) or k.String(v)
// call whose key is error.type.
func (idx *pkgIndex) checkErrorTypeCall(call *ast.CallExpr, fi *funcInfo) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !valueMethods[sel.Sel.Name] {
		return
	}
	switch {
	case isIdent(sel.X, "attribute") && len(call.Args) == 2:
		idx.checkErrorType(call.Args[0], call.Args[1], fi)
	case len(call.Args) == 1:
		idx.checkErrorType(sel.X, call.Args[0], fi)
	}
}

// checkErrorType reports value when key is error.type and value comes from an
// error's text.
func (idx *pkgIndex) checkErrorType(key, value ast.Expr, fi *funcInfo) {
	if !isErrorTypeKey(key) || !idx.fromErrorText(value, fi, map[ast.Node]bool{}) {
		return
	}
	idx.c.reportAt(value.Pos(), "error-type-message", "error.type is set from an error's text; use a low-cardinality class such as a code, \"timeout\", or \"_OTHER\"")
}

// fromErrorText reports whether e takes its value from an error's text: a call
// to x.Error(), an identifier assigned from one, or a same-package call whose
// returned value does. seen stops a cycle.
func (idx *pkgIndex) fromErrorText(e ast.Expr, fi *funcInfo, seen map[ast.Node]bool) bool {
	if seen[e] {
		return false
	}
	seen[e] = true
	switch e := e.(type) {
	case *ast.ParenExpr:
		return idx.fromErrorText(e.X, fi, seen)
	case *ast.Ident:
		for _, v := range fi.defs[e.Name] {
			if idx.fromErrorText(v, fi, seen) {
				return true
			}
		}
		for _, t := range fi.tuples[e.Name] {
			for _, v := range idx.tupleValues(t, fi) {
				if idx.fromErrorText(v.expr, v.caller, seen) {
					return true
				}
			}
		}
		for _, a := range idx.paramArgs(fi, e.Name) {
			if idx.fromErrorText(a.expr, a.caller, seen) {
				return true
			}
		}
	case *ast.CallExpr:
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Error" && len(e.Args) == 0 {
			return true
		}
		for _, callee := range idx.callees(e, fi) {
			for _, r := range callee.allReturns() {
				if idx.fromErrorText(r, callee, seen) {
					return true
				}
			}
		}
	}
	return false
}

// kvKey reports whether call is an attribute constructor, attribute.String(k, v)
// or k.String(v), and returns its key expression.
func kvKey(call *ast.CallExpr) (ast.Expr, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !valueMethods[sel.Sel.Name] {
		return nil, false
	}
	if isIdent(sel.X, "attribute") {
		if len(call.Args) == 0 {
			return nil, false
		}
		return call.Args[0], true
	}
	_, perEvent := perEventKey(sel.X)
	return sel.X, perEvent
}

// isMetricCall reports whether call is a metrics.Client-shaped record with the
// attribute set as its fourth argument.
func isMetricCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && metricMethods[sel.Sel.Name] && len(call.Args) == 4
}

// isWithAttributes reports whether call is metric.WithAttributes(...).
func isWithAttributes(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "WithAttributes" && isIdent(sel.X, "metric")
}

// perEventKey returns the per-event key that e names: a string literal, a
// semconv constant such as semconv.DBQueryTextKey, or string(...) of either.
func perEventKey(e ast.Expr) (string, bool) {
	k := unwrapKey(e)
	if s, ok := stringLit(k); ok {
		_, found := perEventKeys[s]
		return s, found
	}
	if sel, ok := k.(*ast.SelectorExpr); ok {
		for key, constName := range perEventKeys {
			if constName != "" && constName == sel.Sel.Name {
				return key, true
			}
		}
	}
	return "", false
}

// isErrorTypeKey reports whether e names the error.type attribute: a string
// literal, a semconv.ErrorTypeKey selector, or string(...) of either.
func isErrorTypeKey(e ast.Expr) bool {
	k := unwrapKey(e)
	if s, ok := stringLit(k); ok {
		return s == "error.type"
	}
	sel, ok := k.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "ErrorTypeKey"
}

// unwrapKey strips parentheses and string(...) conversions from a key.
func unwrapKey(e ast.Expr) ast.Expr {
	switch x := e.(type) {
	case *ast.ParenExpr:
		return unwrapKey(x.X)
	case *ast.CallExpr:
		if isIdent(x.Fun, "string") && len(x.Args) == 1 {
			return unwrapKey(x.Args[0])
		}
	}
	return e
}

// stringLit returns the value of a string literal.
func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	return s, err == nil
}

// isIdent reports whether e is the identifier name.
func isIdent(e ast.Expr, name string) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == name
}
