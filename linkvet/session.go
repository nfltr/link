// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package linkvet

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/ctrlflow"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
	"golang.org/x/tools/go/cfg"
	"golang.org/x/tools/go/types/typeutil"
)

// SessionClose reports a session that some path leaves open: one Open
// returns, or one an accept handler (Serve's) is given.
var SessionClose = &analysis.Analyzer{
	Name:     "sessionclose",
	Doc:      "report link sessions (Open, OpenChain, accept handlers) not closed on every path",
	URL:      "https://github.com/nfltr/link/tree/main/linkvet#sessionclose",
	Requires: []*analysis.Analyzer{inspect.Analyzer, ctrlflow.Analyzer},
	Run:      runSessionClose,
}

// ReadLoop reports a loop reading a session that has a context at hand
// but neither a read deadline nor anything that closes the session from
// elsewhere: a peer that stops sending holds the loop.
var ReadLoop = &analysis.Analyzer{
	Name:     "readloop",
	Doc:      "report loops reading a link session with no deadline and no cancel path",
	URL:      "https://github.com/nfltr/link/tree/main/linkvet#readloop",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runReadLoop,
}

// CloseWrite reports a write to a session after its CloseWrite.
var CloseWrite = &analysis.Analyzer{
	Name:     "closewrite",
	Doc:      "report writes to a link session after its CloseWrite",
	URL:      "https://github.com/nfltr/link/tree/main/linkvet#closewrite",
	Requires: []*analysis.Analyzer{ctrlflow.Analyzer, inspect.Analyzer},
	Run:      runCloseWrite,
}

// OpenCtx reports an Open given context.Background() or context.TODO()
// outside tests: it waits as long as the peer stays unreachable.
var OpenCtx = &analysis.Analyzer{
	Name:     "openctx",
	Doc:      "report link Opens with a context that never ends, outside tests",
	URL:      "https://github.com/nfltr/link/tree/main/linkvet#openctx",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runOpenCtx,
}

// handlers is the accept functions passed to a link Serve in the
// package: literals, and functions named.
func handlers(pass *analysis.Pass, ins *inspector.Inspector) (map[*ast.FuncLit]bool, map[*types.Func]bool) {
	lits, fns := map[*ast.FuncLit]bool{}, map[*types.Func]bool{}
	ins.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call := n.(*ast.CallExpr)
		fn, ok := typeutil.Callee(pass.TypesInfo, call).(*types.Func)
		if !ok || fn.Name() != "Serve" || !inLink(fn.Pkg()) {
			return
		}
		for _, a := range call.Args {
			switch a := ast.Unparen(a).(type) {
			case *ast.FuncLit:
				lits[a] = true
			case *ast.Ident, *ast.SelectorExpr:
				var id *ast.Ident
				if sel, ok := a.(*ast.SelectorExpr); ok {
					id = sel.Sel
				} else {
					id = a.(*ast.Ident)
				}
				if f, ok := pass.TypesInfo.Uses[id].(*types.Func); ok && f.Pkg() == pass.Pkg {
					fns[f] = true
				}
			}
		}
	})
	return lits, fns
}

// openSite is a session variable set from an Open: the node, its block
// and index, and the error Open returned with it.
type openSite struct {
	v, err *types.Var
	call   *ast.CallExpr
	b      *cfg.Block
	i      int
}

// openSites is every v[, err] := x.Open(...) in g.
func openSites(info *types.Info, g *cfg.CFG) []openSite {
	var sites []openSite
	for _, b := range g.Blocks {
		for i, n := range b.Nodes {
			var lhs []ast.Expr
			var rhs []ast.Expr
			switch n := n.(type) {
			case *ast.AssignStmt:
				lhs, rhs = n.Lhs, n.Rhs
			case *ast.ValueSpec:
				for _, id := range n.Names {
					lhs = append(lhs, id)
				}
				rhs = n.Values
			}
			if len(rhs) != 1 || len(lhs) == 0 {
				continue
			}
			call, ok := ast.Unparen(rhs[0]).(*ast.CallExpr)
			if !ok || !isOpen(info, call) {
				continue
			}
			v := localVar(info, lhs[0])
			if v == nil {
				continue
			}
			s := openSite{v: v, call: call, b: b, i: i}
			if len(lhs) == 2 {
				s.err = localVar(info, lhs[1])
			}
			sites = append(sites, s)
		}
	}
	return sites
}

func runSessionClose(pass *analysis.Pass) (any, error) {
	ins := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	cfgs := pass.ResultOf[ctrlflow.Analyzer].(*ctrlflow.CFGs)
	info := pass.TypesInfo
	lits, fns := handlers(pass, ins)

	// A session dropped where it is opened.
	ins.Preorder([]ast.Node{(*ast.AssignStmt)(nil), (*ast.ExprStmt)(nil)}, func(n ast.Node) {
		var call *ast.CallExpr
		switch n := n.(type) {
		case *ast.ExprStmt:
			call, _ = ast.Unparen(n.X).(*ast.CallExpr)
		case *ast.AssignStmt:
			if id, ok := n.Lhs[0].(*ast.Ident); ok && id.Name == "_" && len(n.Rhs) == 1 {
				call, _ = ast.Unparen(n.Rhs[0]).(*ast.CallExpr)
			}
		}
		if call != nil && isOpen(info, call) {
			pass.Reportf(call.Pos(), "the session this Open returns is dropped: it stays open, holding one of the connection's sessions, until the peer or the connection ends it")
		}
	})

	funcs(pass, ins, func(fn ast.Node, typ *ast.FuncType, body *ast.BlockStmt) {
		var g *cfg.CFG
		handler := false
		switch fn := fn.(type) {
		case *ast.FuncDecl:
			g = cfgs.FuncDecl(fn)
			obj, _ := info.Defs[fn.Name].(*types.Func)
			handler = fns[obj]
		case *ast.FuncLit:
			g = cfgs.FuncLit(fn)
			handler = lits[fn]
		}
		if g == nil || len(g.Blocks) == 0 {
			return
		}
		for _, s := range openSites(info, g) {
			if leaks(info, cfgs, s.b, s.i, s.v, s.err) {
				pass.Reportf(s.call.Pos(), "session %s is not closed on every path: Close it (defer %s.Close()), or CloseWrite, Reset or Abort it", s.v.Name(), s.v.Name())
			}
		}
		if !handler {
			return
		}
		// The accept handler owns its session: the last session parameter.
		params := typ.Params.List
		if len(params) == 0 || !isSession(info.TypeOf(params[len(params)-1].Type)) {
			return
		}
		last := params[len(params)-1]
		var v *types.Var
		if len(last.Names) > 0 {
			v, _ = info.Defs[last.Names[len(last.Names)-1]].(*types.Var)
		}
		if v == nil || v.Name() == "_" {
			pass.Reportf(last.Pos(), "the accept handler owns its session and never closes it: name it and Close it")
			return
		}
		if leaks(info, cfgs, g.Blocks[0], -1, v, nil) {
			pass.Reportf(last.Pos(), "the accept handler owns session %s and does not close it on every path: defer %s.Close()", v.Name(), v.Name())
		}
	})
	return nil, nil
}

// writes is how a node writes to a session: its method, or the helper
// given it as writer (its first argument).
var writeHelpers = map[string]bool{
	"io.WriteString": true, "io.Copy": true, "io.CopyN": true, "io.CopyBuffer": true,
	"fmt.Fprint": true, "fmt.Fprintf": true, "fmt.Fprintln": true, "encoding/binary.Write": true,
}

// writeTo is the call in n (not in a function literal) that writes to v.
func writeTo(info *types.Info, n ast.Node, v *types.Var) *ast.CallExpr {
	var w *ast.CallExpr
	inspectOwn(n, func(m ast.Node) bool {
		call, ok := m.(*ast.CallExpr)
		if !ok || w != nil {
			return w == nil
		}
		switch method(info, call, v) {
		case "Write", "WriteBuffers":
			w = call
		}
		if writeHelpers[funcName(info, call)] && len(call.Args) > 0 && localVar(info, call.Args[0]) == v {
			w = call
		}
		return w == nil
	})
	return w
}

func runCloseWrite(pass *analysis.Pass) (any, error) {
	ins := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	cfgs := pass.ResultOf[ctrlflow.Analyzer].(*ctrlflow.CFGs)
	info := pass.TypesInfo
	reported := map[*ast.CallExpr]bool{}
	funcs(pass, ins, func(fn ast.Node, _ *ast.FuncType, _ *ast.BlockStmt) {
		var g *cfg.CFG
		switch fn := fn.(type) {
		case *ast.FuncDecl:
			g = cfgs.FuncDecl(fn)
		case *ast.FuncLit:
			g = cfgs.FuncLit(fn)
		}
		if g == nil {
			return
		}
		for _, b := range g.Blocks {
			for i, n := range b.Nodes {
				switch n.(type) {
				case *ast.DeferStmt, *ast.GoStmt:
					continue
				}
				inspectOwn(n, func(m ast.Node) bool {
					call, ok := m.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "CloseWrite" {
						return true
					}
					v := localVar(info, sel.X)
					if v == nil || !isSession(v.Type()) {
						return true
					}
					at := pass.Fset.Position(call.Pos()).Line
					reach(info, b, i, v, func(n ast.Node) bool {
						w := writeTo(info, n, v)
						if w == nil {
							return true
						}
						if !reported[w] {
							reported[w] = true
							pass.Reportf(w.Pos(), "write to session %s after its CloseWrite (line %d): it fails with io.ErrClosedPipe; write first, then CloseWrite", v.Name(), at)
						}
						return false
					})
					return true
				})
			}
		}
	})
	return nil, nil
}

// readFrom is the call in n (not in a function literal) that reads v.
func readFrom(info *types.Info, n ast.Node, v *types.Var) *ast.CallExpr {
	var r *ast.CallExpr
	inspectOwn(n, func(m ast.Node) bool {
		call, ok := m.(*ast.CallExpr)
		if !ok || r != nil {
			return r == nil
		}
		switch funcName(info, call) {
		case "io.ReadFull", "io.ReadAtLeast", "encoding/binary.Read", "fmt.Fscan", "fmt.Fscanf", "fmt.Fscanln":
			if len(call.Args) > 0 && localVar(info, call.Args[0]) == v {
				r = call
			}
		}
		if method(info, call, v) == "Read" {
			r = call
		}
		return r == nil
	})
	return r
}

func runReadLoop(pass *analysis.Pass) (any, error) {
	ins := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	info := pass.TypesInfo
	funcs(pass, ins, func(fn ast.Node, typ *ast.FuncType, body *ast.BlockStmt) {
		// The sessions opened here.
		var opened []*types.Var
		ctxAtHand := hasCtxParam(info, typ)
		inspectOwn(body, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok || len(as.Rhs) != 1 {
				return true
			}
			call, ok := ast.Unparen(as.Rhs[0]).(*ast.CallExpr)
			if !ok || !isOpen(info, call) {
				return true
			}
			if v := localVar(info, as.Lhs[0]); v != nil {
				opened = append(opened, v)
				for _, a := range call.Args {
					ctxAtHand = ctxAtHand || isContext(info.TypeOf(a))
				}
			}
			return true
		})
		if !ctxAtHand {
			return
		}
		for _, v := range opened {
			if cancelPath(info, body, v) {
				continue
			}
			inspectOwn(body, func(n ast.Node) bool {
				var loopBody *ast.BlockStmt
				switch n := n.(type) {
				case *ast.ForStmt:
					loopBody = n.Body
				case *ast.RangeStmt:
					loopBody = n.Body
				default:
					return true
				}
				var r *ast.CallExpr
				inspectOwn(loopBody, func(m ast.Node) bool {
					if r == nil {
						r = readFrom(info, m, v)
					}
					return r == nil
				})
				if r != nil {
					pass.Reportf(r.Pos(), "read of session %s in a loop with no deadline and no cancel path: a context does not end a blocked Read; set %s.SetReadDeadline, or close %s when the context ends (context.AfterFunc)", v.Name(), v.Name(), v.Name())
				}
				return r == nil
			})
		}
	})
	return nil, nil
}

// cancelPath: something may end or bound a blocked Read of v: a
// deadline, v in a closure or goroutine, or v handed to code not seen
// here. A Close in this function's own flow, or deferred, runs after the
// loop: it is no cancel path.
func cancelPath(info *types.Info, body *ast.BlockStmt, v *types.Var) bool {
	found := false
	var stack []ast.Node
	ast.Inspect(body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		stack = append(stack, n)
		if id, ok := n.(*ast.Ident); ok && info.Uses[id] == v && cancels(info, stack) {
			found = true
		}
		return !found
	})
	return found
}

func cancels(info *types.Info, stack []ast.Node) bool {
	id := stack[len(stack)-1]
	var call *ast.CallExpr
	var name string
	if len(stack) >= 3 {
		if sel, ok := stack[len(stack)-2].(*ast.SelectorExpr); ok && sel.X == id {
			if c, ok := stack[len(stack)-3].(*ast.CallExpr); ok && c.Fun == sel {
				call, name = c, sel.Sel.Name
			}
		}
	}
	for _, a := range stack[:len(stack)-1] {
		switch a := a.(type) {
		case *ast.FuncLit, *ast.GoStmt:
			return true
		case *ast.DeferStmt:
			return a.Call != call // defer s.Close() is fine, anything else is not seen
		}
	}
	if call != nil {
		return name == "SetDeadline" || name == "SetReadDeadline"
	}
	switch p := stack[len(stack)-2].(type) {
	case *ast.CallExpr:
		return !keepsNoReference[funcName(info, p)]
	case *ast.BinaryExpr:
		return false
	case *ast.AssignStmt:
		for _, l := range p.Lhs {
			if l == id {
				return false
			}
		}
	}
	return true
}

func runOpenCtx(pass *analysis.Pass) (any, error) {
	ins := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	info := pass.TypesInfo
	funcs(pass, ins, func(fn ast.Node, _ *ast.FuncType, body *ast.BlockStmt) {
		if isTestFile(pass, fn) {
			return
		}
		inspectOwn(body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isOpen(info, call) {
				return true
			}
			for _, a := range call.Args {
				if isContext(info.TypeOf(a)) && isBackground(info, a, body) {
					pass.Reportf(a.Pos(), "Open with a context that never ends: it waits as long as the peer is unreachable; give it a deadline (context.WithTimeout)")
				}
			}
			return true
		})
	})
	return nil, nil
}
