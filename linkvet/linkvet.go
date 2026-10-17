// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package linkvet is a set of go vet analyzers for code that uses
// github.com/nfltr/link sessions, and for gRPC clients (apps that reach
// their services through the connect sidecar, `nfltr dial`, are gRPC
// apps). Each check reports only what it can see in one function, so a
// report is a bug or a missing cleanup, not a style note (README.md lists
// every check with an example and its fix).
//
// Run them with go vet:
//
//	go install github.com/nfltr/link/linkvet/cmd/linkvet@latest
//	go vet -vettool=$(command -v linkvet) ./...
package linkvet

import (
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/ast/inspector"
	"golang.org/x/tools/go/types/typeutil"
)

// Analyzers is every check, link's and gRPC's.
var Analyzers = []*analysis.Analyzer{
	SessionClose, ReadLoop, CloseWrite, OpenCtx,
	GRPCRecvSelect, GRPCDrain, GRPCCloseSend, GRPCDeadline, GRPCDial,
}

const linkPath = "github.com/nfltr/link"

func inLink(p *types.Package) bool {
	return p != nil && (p.Path() == linkPath || strings.HasPrefix(p.Path(), linkPath+"/"))
}

// isSession: *link.Session, peerlink.Session or ChainSession, or such a
// type of another link package (quiclink, grpclink).
func isSession(t types.Type) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := types.Unalias(t).(*types.Named)
	if !ok {
		return false
	}
	o := n.Obj()
	return inLink(o.Pkg()) && (o.Name() == "Session" || o.Name() == "ChainSession")
}

// isOpen: call is an Open or OpenChain whose first result is a session
// (Mux.Open, Pool.Open, peerlink's Node.Open and Link.Open).
func isOpen(info *types.Info, call *ast.CallExpr) bool {
	sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr)
	if !ok || (sel.Sel.Name != "Open" && sel.Sel.Name != "OpenChain") {
		return false
	}
	sig, ok := info.TypeOf(call.Fun).(*types.Signature)
	return ok && sig.Results().Len() > 0 && isSession(sig.Results().At(0).Type())
}

// funcName is fn's "pkgpath.Name", or "" when call has no static callee.
func funcName(info *types.Info, call *ast.CallExpr) string {
	fn, ok := typeutil.Callee(info, call).(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Signature().Recv() != nil {
		return ""
	}
	return fn.Pkg().Path() + "." + fn.Name()
}

// localVar is the local variable (or parameter) e names, else nil.
func localVar(info *types.Info, e ast.Expr) *types.Var {
	id, ok := ast.Unparen(e).(*ast.Ident)
	if !ok {
		return nil
	}
	v, ok := info.ObjectOf(id).(*types.Var)
	if !ok || v.IsField() || v.Parent() == nil || v.Parent() == v.Pkg().Scope() {
		return nil
	}
	return v
}

// method is the method name when call is v.Name(...), else "".
func method(info *types.Info, call *ast.CallExpr, v *types.Var) string {
	if sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr); ok && localVar(info, sel.X) == v {
		return sel.Sel.Name
	}
	return ""
}

// hasMethod: t (or *t) has an exported method name.
func hasMethod(t types.Type, names ...string) bool {
	for _, name := range names {
		obj, _, _ := types.LookupFieldOrMethod(t, true, nil, name)
		if _, ok := obj.(*types.Func); !ok {
			return false
		}
	}
	return true
}

func isTestFile(pass *analysis.Pass, n ast.Node) bool {
	return strings.HasSuffix(pass.Fset.Position(n.Pos()).Filename, "_test.go")
}

// isBackground: e is context.Background() or context.TODO(), or a local
// variable body only ever sets to one of them.
func isBackground(info *types.Info, e ast.Expr, body *ast.BlockStmt) bool {
	if call, ok := ast.Unparen(e).(*ast.CallExpr); ok {
		n := funcName(info, call)
		return n == "context.Background" || n == "context.TODO"
	}
	v := localVar(info, e)
	if v == nil || body == nil {
		return false
	}
	sets, other := 0, false
	ast.Inspect(body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			for i, l := range n.Lhs {
				if localVar(info, l) != v {
					continue
				}
				if len(n.Lhs) == len(n.Rhs) && isBackground(info, n.Rhs[i], nil) {
					sets++
				} else {
					other = true
				}
			}
		case *ast.ValueSpec:
			for i, name := range n.Names {
				if info.Defs[name] == v {
					if len(n.Values) == len(n.Names) && isBackground(info, n.Values[i], nil) {
						sets++
					} else {
						other = true
					}
				}
			}
		case *ast.UnaryExpr:
			if n.Op == token.AND && localVar(info, n.X) == v {
				other = true
			}
		}
		return !other
	})
	return sets > 0 && !other
}

// funcs calls f with each function of the package: its node
// (*ast.FuncDecl or *ast.FuncLit), its type and its body.
func funcs(pass *analysis.Pass, ins *inspector.Inspector, f func(fn ast.Node, typ *ast.FuncType, body *ast.BlockStmt)) {
	ins.Preorder([]ast.Node{(*ast.FuncDecl)(nil), (*ast.FuncLit)(nil)}, func(n ast.Node) {
		switch n := n.(type) {
		case *ast.FuncDecl:
			if n.Body != nil {
				f(n, n.Type, n.Body)
			}
		case *ast.FuncLit:
			f(n, n.Type, n.Body)
		}
	})
}

// inspectOwn is ast.Inspect of n without descending into function
// literals (their bodies are functions of their own).
func inspectOwn(n ast.Node, f func(ast.Node) bool) {
	ast.Inspect(n, func(m ast.Node) bool {
		if m == nil {
			return false
		}
		if _, ok := m.(*ast.FuncLit); ok && m != n {
			return false
		}
		return f(m)
	})
}

// hasCtxParam: the function takes a context.Context.
func hasCtxParam(info *types.Info, typ *ast.FuncType) bool {
	for _, f := range typ.Params.List {
		if isContext(info.TypeOf(f.Type)) {
			return true
		}
	}
	return false
}

func isContext(t types.Type) bool {
	n, ok := types.Unalias(t).(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == "context" && n.Obj().Name() == "Context"
}
