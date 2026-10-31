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
	"getter-prefix":         decisions + "#getters",
	"error-concrete-return": decisions + "#returning-errors",
	"error-log-and-return":  practices + "#error-logging",
	"library-panic":         decisions + "#dont-panic",
	"constructor-stutter":   decisions + "#repetitive-with-package",
	"const-k-prefix":        decisions + "#constant-names",
	"receiver-name":         decisions + "#receiver-names",
	"test-message":          decisions + "#got-before-want",
	"labeled-branch":        "https://google.github.io/styleguide/go/guide#simplicity",
	"canonical-method":      "https://go.dev/doc/effective_go#interface-names",
}

type finding struct {
	pos  token.Position
	rule string
	msg  string
}

type checker struct {
	fset     *token.FileSet
	findings []finding
}

type fileCtx struct {
	*checker
	file    *ast.File
	pkg     string
	isTest  bool
	isMain  bool
	ignores map[int][]string
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
	c := &checker{fset: token.NewFileSet()}
	for _, p := range paths {
		if err := c.walk(strings.TrimSuffix(p, "/...")); err != nil {
			return nil, err
		}
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
	fc := &fileCtx{
		checker: c,
		file:    f,
		pkg:     f.Name.Name,
		isTest:  strings.HasSuffix(path, "_test.go"),
		isMain:  f.Name.Name == "main",
		ignores: map[int][]string{},
	}
	for _, cg := range f.Comments {
		for _, cm := range cg.List {
			_, rest, ok := strings.Cut(cm.Text, "gostyle:ignore")
			if !ok {
				continue
			}
			line := c.fset.Position(cm.Pos()).Line
			rules := strings.Split(strings.Fields(rest + " all")[0], ",")
			fc.ignores[line] = append(fc.ignores[line], rules...)
			fc.ignores[line+1] = append(fc.ignores[line+1], rules...)
		}
	}
	fc.run()
	return nil
}

func (fc *fileCtx) report(n ast.Node, rule, format string, args ...any) {
	pos := fc.fset.Position(n.Pos())
	for _, r := range fc.ignores[pos.Line] {
		if r == rule || r == "all" {
			return
		}
	}
	fc.findings = append(fc.findings, finding{pos, rule, fmt.Sprintf(format, args...)})
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
