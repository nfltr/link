// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bufio"
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// docPkgs are the public packages by the name other packages' docs
// qualify them with, and their directories.
var docPkgs = map[string]string{
	"link":         ".",
	"peerlink":     "peerlink",
	"peerlinktest": "peerlink/peerlinktest",
	"quiclink":     "peerlink/quiclink",
	"pathswitch":   "pathswitch",
}

// docPkg is what a package's docs say and the names it exports.
type docPkg struct {
	text  map[string]string   // doc comment by what it documents: the package, its exported names, struct fields
	names map[string]bool     // exported top-level names and every exported member name
	args  map[string]bool     // parameter names of exported functions and methods: a doc may name them
	types map[string][]string // exported type -> its exported members
}

func loadDocPkg(t *testing.T, dir string) docPkg {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi fs.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	p := docPkg{text: map[string]string{}, names: map[string]bool{}, args: map[string]bool{}, types: map[string][]string{}}
	params := func(fd *ast.FuncDecl) {
		for _, f := range fd.Type.Params.List {
			for _, n := range f.Names {
				p.args[n.Name] = true
			}
		}
	}
	for _, ap := range pkgs {
		d := doc.New(ap, dir, 0)
		p.text["package"] += d.Doc
		values := func(vs []*doc.Value) {
			for _, v := range vs {
				p.text[v.Names[0]] += v.Doc
				for _, n := range v.Names {
					p.names[n] = true
				}
				for _, s := range v.Decl.Specs {
					if vs := s.(*ast.ValueSpec); vs.Names[0].IsExported() {
						p.text[vs.Names[0].Name] += vs.Doc.Text() + vs.Comment.Text()
					}
				}
			}
		}
		values(d.Consts)
		values(d.Vars)
		for _, f := range d.Funcs {
			p.names[f.Name] = true
			p.text[f.Name] += f.Doc
			params(f.Decl)
		}
		for _, ty := range d.Types {
			p.names[ty.Name] = true
			p.text[ty.Name] += ty.Doc
			values(ty.Consts)
			values(ty.Vars)
			member := func(n string) {
				p.names[n] = true
				p.types[ty.Name] = append(p.types[ty.Name], n)
			}
			for _, f := range ty.Funcs {
				p.names[f.Name] = true
				p.text[f.Name] += f.Doc
				params(f.Decl)
			}
			for _, m := range ty.Methods {
				member(m.Name)
				p.text[ty.Name+"."+m.Name] += m.Doc
				params(m.Decl)
			}
			var fields *ast.FieldList
			switch tt := ty.Decl.Specs[0].(*ast.TypeSpec).Type.(type) {
			case *ast.StructType:
				fields = tt.Fields
			case *ast.InterfaceType:
				fields = tt.Methods
			}
			if fields == nil {
				continue
			}
			for _, f := range fields.List {
				for _, n := range f.Names {
					p.text[ty.Name+"."+n.Name] += f.Doc.Text() + f.Comment.Text()
				}
				if ft, ok := f.Type.(*ast.FuncType); ok { // a func field or an interface method
					for _, a := range ft.Params.List {
						for _, n := range a.Names {
							p.args[n.Name] = true
						}
					}
				}
				for _, n := range f.Names {
					if n.IsExported() {
						member(n.Name)
					}
				}
				if len(f.Names) == 0 { // embedded
					if id, ok := ast.Unparen(f.Type).(*ast.Ident); ok && id.IsExported() {
						member(id.Name)
					}
				}
			}
		}
	}
	return p
}

// removedNames are the names api/*.txt records as removed, by package
// (apidiff's "- (*T).M: removed", "- X: removed", "- ./peerlink.T.M: removed").
func removedNames(t *testing.T) map[string]map[string]bool {
	t.Helper()
	files, err := filepath.Glob("api/*.txt")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]bool{}
	for _, f := range files {
		root := "link"
		if strings.HasPrefix(filepath.Base(f), "quiclink-") {
			root = "quiclink"
		} else if !strings.HasPrefix(filepath.Base(f), "link-") {
			continue
		}
		fh, err := os.Open(f)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(fh)
		for sc.Scan() {
			l, ok := strings.CutSuffix(sc.Text(), ": removed")
			if !ok || !strings.HasPrefix(l, "- ") || strings.HasPrefix(l, "- package ") {
				continue
			}
			l = strings.TrimPrefix(l, "- ")
			pkg := root
			if rest, ok := strings.CutPrefix(l, "./"); ok {
				dir, name, _ := strings.Cut(rest, ".")
				pkg, l = filepath.Base(dir), name
			}
			l = strings.NewReplacer("(*", "", "(", "", ")", "").Replace(l)
			parts := strings.Split(l, ".")
			if out[pkg] == nil {
				out[pkg] = map[string]bool{}
			}
			out[pkg][parts[len(parts)-1]] = true
		}
		fh.Close()
	}
	return out
}

var (
	docToken = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*`)
	camelLow = regexp.MustCompile(`^[a-z][a-z0-9]*[A-Z][A-Za-z0-9]*$`)
	// camelWord are words of the docs that look like unexported names:
	// prose, and an example's variable (quiclink.Config.Dial's tlsConf).
	camelWord = map[string]bool{"macOS": true, "mTLS": true, "gRPC": true, "tlsConf": true}
)

// TestDocsNameNoGhosts: the doc comments of the public packages name
// only what exists: no unexported identifier (closeAckErr), no member a
// type lacks (Mux.OpenChain), and no name api/*.txt records as removed
// unless the package exports it again (Pump, FeatureChain). A rename or
// an unexport that leaves its docs behind fails here (lane LINKAPI round
// 2 found 30 such references the v0.13.0 cut left).
func TestDocsNameNoGhosts(t *testing.T) {
	pkgs := map[string]docPkg{}
	for name, dir := range docPkgs {
		pkgs[name] = loadDocPkg(t, dir)
	}
	removed := removedNames(t)
	for name, p := range pkgs {
		for where, text := range p.text {
			seen := map[string]bool{}
			for _, tok := range docToken.FindAllString(text, -1) {
				if seen[tok] {
					continue
				}
				seen[tok] = true
				if why := ghost(name, tok, pkgs, removed); why != "" {
					t.Errorf("%s %s's doc: %q %s", name, where, tok, why)
				}
			}
		}
	}
}

// ghost says why tok, a word of package pkg's docs, names nothing; ""
// when it does or is not a Go name of these packages.
func ghost(pkg, tok string, pkgs map[string]docPkg, removed map[string]map[string]bool) string {
	parts := strings.Split(tok, ".")
	if q, ok := pkgs[parts[0]]; ok && len(parts) > 1 {
		pkg, parts = parts[0], parts[1:]
		if !q.names[parts[0]] {
			return "is not an exported name of " + pkg
		}
	}
	p := pkgs[pkg]
	if len(parts) > 1 {
		if members, ok := p.types[parts[0]]; ok {
			for _, m := range members {
				if m == parts[1] {
					return ""
				}
			}
			return "is no exported member of " + parts[0]
		}
		return ""
	}
	w := parts[0]
	if camelLow.MatchString(w) && !camelWord[w] && !p.args[w] {
		return "is unexported: name what the caller can use"
	}
	if removed[pkg][w] && !p.names[w] {
		return "was removed (api/*.txt)"
	}
	return ""
}
