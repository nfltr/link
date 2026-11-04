// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

// The action map of spec/README.md ("Action map"), checked against the
// code: every trace event the recorder writes is a step of spec/Trace.tla
// or spec/TraceLive.tla and each of theirs has a hook point in the code
// (TestSpecTraceEventsMapped), and every write to a field the models
// stand for is in a function the README's table maps to a spec action
// (TestSpecWritesMapped): a new writer, or one that stopped writing, is a
// change of the model's transitions the table must show.

// recorderEvents returns the event names the code's trace hooks write.
func recorderEvents(t *testing.T) map[string]bool {
	t.Helper()
	evs := map[string]bool{}
	lit := regexp.MustCompile(`ev:\s*"([A-Za-z]+)"|\bev\s*:?=\s*"([A-Za-z]+)"|traceLive\(c, "([A-Za-z]+)"`)
	for _, f := range append(packageFiles(t), "trace_on.go") { // the hooks' bodies: -tags linktrace
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range lit.FindAllStringSubmatch(string(b), -1) {
			evs[m[1]+m[2]+m[3]] = true
		}
	}
	return evs
}

func TestSpecTraceEventsMapped(t *testing.T) {
	code := recorderEvents(t)
	spec := map[string]bool{}
	for _, f := range []string{"spec/Trace.tla", "spec/TraceLive.tla"} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range regexp.MustCompile(`\bev = "([A-Za-z]+)"`).FindAllStringSubmatch(string(b), -1) {
			spec[m[1]] = true
		}
	}
	if len(code) < 20 {
		t.Fatalf("found only %d trace events in the code: the pattern no longer matches the hooks", len(code))
	}
	for e := range code {
		if !spec[e] {
			t.Errorf("the recorder writes %q, which no step of spec/Trace.tla or TraceLive.tla maps", e)
		}
	}
	for e := range spec {
		if !code[e] {
			t.Errorf("spec/Trace.tla or TraceLive.tla maps %q, but no hook point of the code writes it", e)
		}
	}
}

// packageFiles are the non-test files of package link in a default build.
func packageFiles(t *testing.T) []string {
	t.Helper()
	p, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	return p.GoFiles
}

// modelled are the fields that stand for a model's variables
// (spec/README.md "Action map"), by type.
var modelled = map[string][]string{
	"sessionCredit": {"n", "pend", "held", "consumed", "answered"},
	"Credit":        {"sessions", "opened", "cut"},
	"Session":       {"closed", "peerClosed", "halfClosed", "peerHalf", "finSent", "ans"},
	"Mux":           {"sess", "done", "leaving", "left", "cutting"},
	"Conn":          {"goingAway", "goAwayAt", "rxAt", "lastPing", "closing"},
	"Pool":          {"conns", "dials", "changed", "growing", "refused"},
}

// writes returns, per modelled field (Type.field), the functions
// (Type.method or func) that write it: an assignment, ++/--, a map store
// or delete, or an atomic's Store, Add, Swap or CompareAndSwap.
func writes(t *testing.T) map[string][]string {
	t.Helper()
	fset := token.NewFileSet()
	var files []*ast.File
	for _, name := range packageFiles(t) {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
	if _, err := conf.Check("github.com/nfltr/link", fset, files, info); err != nil {
		t.Fatal(err)
	}
	// field returns "Type.field" when e is a modelled field.
	field := func(e ast.Expr) string {
		for {
			switch x := e.(type) {
			case *ast.IndexExpr: // a map store: m[k] = v
				e = x.X
				continue
			case *ast.ParenExpr:
				e = x.X
				continue
			}
			break
		}
		sel, ok := e.(*ast.SelectorExpr)
		if !ok {
			return ""
		}
		s, ok := info.Selections[sel]
		if !ok || s.Kind() != types.FieldVal {
			return ""
		}
		r := s.Recv()
		if p, ok := r.(*types.Pointer); ok {
			r = p.Elem()
		}
		n, ok := r.(*types.Named)
		if !ok || !slices.Contains(modelled[n.Obj().Name()], sel.Sel.Name) {
			return ""
		}
		return n.Obj().Name() + "." + sel.Sel.Name
	}
	out := map[string][]string{}
	add := func(f, fn string) {
		if f != "" && !slices.Contains(out[f], fn) {
			out[f] = append(out[f], fn)
		}
	}
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			fn := fd.Name.Name
			if fd.Recv != nil {
				rt := fd.Recv.List[0].Type
				if s, ok := rt.(*ast.StarExpr); ok {
					rt = s.X
				}
				fn = rt.(*ast.Ident).Name + "." + fn
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					if x.Tok != token.DEFINE {
						for _, l := range x.Lhs {
							add(field(l), fn)
						}
					}
				case *ast.IncDecStmt:
					add(field(x.X), fn)
				case *ast.CallExpr:
					if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "delete" && len(x.Args) > 0 {
						add(field(x.Args[0]), fn)
					}
					if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
						switch sel.Sel.Name {
						case "Store", "Add", "Swap", "CompareAndSwap":
							add(field(sel.X), fn)
						}
					}
				}
				return true
			})
		}
	}
	for _, v := range out {
		sort.Strings(v)
	}
	return out
}

// readmeWriters parses spec/README.md's table "Writes to modelled state":
// | `Type.field` | `F1`, `F2` | action |.
func readmeWriters(t *testing.T) map[string][]string {
	t.Helper()
	b, err := os.ReadFile("spec/README.md")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	i := strings.Index(s, "<!-- writes: TestSpecWritesMapped -->")
	if i < 0 {
		t.Fatal("spec/README.md has no table of writes to modelled state")
	}
	out := map[string][]string{}
	tick := regexp.MustCompile("`([A-Za-z.]+)`")
	for _, line := range strings.Split(s[i:], "\n")[1:] {
		if !strings.HasPrefix(line, "|") {
			if len(out) > 0 {
				break
			}
			continue
		}
		cells := strings.Split(line, "|")
		if len(cells) < 4 || strings.HasPrefix(strings.TrimSpace(cells[1]), "---") {
			continue
		}
		f := tick.FindStringSubmatch(cells[1])
		if f == nil {
			continue
		}
		var fns []string
		for _, m := range tick.FindAllStringSubmatch(cells[2], -1) {
			fns = append(fns, m[1])
		}
		sort.Strings(fns)
		out[f[1]] = fns
	}
	return out
}

func TestSpecWritesMapped(t *testing.T) {
	got := writes(t)
	if os.Getenv("SPEC_MAP_DUMP") != "" {
		var keys []string
		for k := range got {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			t.Logf("| `%s` | `%s` |", k, strings.Join(got[k], "`, `"))
		}
	}
	want := readmeWriters(t)
	for typ, fs := range modelled {
		for _, f := range fs {
			k := typ + "." + f
			if !slices.Equal(got[k], want[k]) {
				t.Errorf("%s is written by %v; spec/README.md's table of writes says %v", k, got[k], want[k])
			}
		}
	}
}
