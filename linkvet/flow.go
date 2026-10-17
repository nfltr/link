// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package linkvet

import (
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis/passes/ctrlflow"
	"golang.org/x/tools/go/cfg"
	"golang.org/x/tools/go/types/typeutil"
)

// use is how a node uses a variable, weakest first.
type use uint8

const (
	useNone    use = iota
	useBenign      // a method call that keeps it open, a nil test, a read or write helper
	useRelease     // a release method, or the value leaves sight (returned, stored, captured, passed on)
	useAssign      // the variable is set again
)

// sessionRelease ends a session's need for a Close (CloseWrite too: the
// session ends once the peer closes its side; Pump hands it on).
var sessionRelease = map[string]bool{
	"Close": true, "CloseWrite": true, "Reset": true, "Abort": true, "AbortWith": true, "Refuse": true, "Pump": true,
}

// keepsNoReference takes a reader or writer and neither closes nor keeps it.
var keepsNoReference = map[string]bool{
	"io.Copy": true, "io.CopyN": true, "io.CopyBuffer": true, "io.ReadAll": true, "io.ReadFull": true,
	"io.ReadAtLeast": true, "io.WriteString": true, "fmt.Fprint": true, "fmt.Fprintf": true,
	"fmt.Fprintln": true, "fmt.Fscan": true, "fmt.Fscanf": true, "fmt.Fscanln": true,
	"encoding/binary.Read": true, "encoding/binary.Write": true,
}

// useOf is the strongest use node n makes of v; release names the
// methods that release it.
func useOf(info *types.Info, n ast.Node, v *types.Var, release map[string]bool) use {
	u := useNone
	var stack []ast.Node
	ast.Inspect(n, func(m ast.Node) bool {
		if m == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		stack = append(stack, m)
		if id, ok := m.(*ast.Ident); ok && info.ObjectOf(id) == v {
			u = max(u, classify(info, stack, release))
		}
		return true
	})
	return u
}

func classify(info *types.Info, stack []ast.Node, release map[string]bool) use {
	id := stack[len(stack)-1]
	for _, a := range stack[:len(stack)-1] {
		switch a.(type) {
		case *ast.FuncLit, *ast.DeferStmt, *ast.GoStmt:
			return useRelease // captured, deferred or handed to a goroutine
		}
	}
	if len(stack) < 2 {
		return useRelease
	}
	switch p := stack[len(stack)-2].(type) {
	case *ast.SelectorExpr:
		if len(stack) >= 3 {
			if call, ok := stack[len(stack)-3].(*ast.CallExpr); ok && call.Fun == p {
				if release[p.Sel.Name] {
					return useRelease
				}
				return useBenign
			}
		}
	case *ast.CallExpr:
		if keepsNoReference[funcName(info, p)] {
			return useBenign
		}
	case *ast.BinaryExpr:
		if p.Op == token.EQL || p.Op == token.NEQ {
			return useBenign
		}
	case *ast.AssignStmt:
		for _, l := range p.Lhs {
			if l == id {
				return useAssign
			}
		}
	case *ast.ValueSpec:
		for _, l := range p.Names {
			if l == id {
				return useAssign
			}
		}
	}
	return useRelease
}

// nilTest: cond is v == nil or v != nil (either order); deadIfTrue says
// which branch v is nil in (for an error: which branch it is not).
func nilTest(info *types.Info, cond ast.Node, v *types.Var, isErr bool) (dead int, ok bool) {
	b, ok := cond.(*ast.BinaryExpr)
	if !ok || (b.Op != token.EQL && b.Op != token.NEQ) {
		return 0, false
	}
	x, y := b.X, b.Y
	if localVar(info, x) != v {
		x, y = y, x
	}
	if localVar(info, x) != v || !info.Types[y].IsNil() {
		return 0, false
	}
	// The branch where the session is nil: err != nil's true branch,
	// s == nil's true branch.
	if (b.Op == token.NEQ) == isErr {
		return 0, true
	}
	return 1, true
}

// isIf: b ends with an if statement's condition.
func isIf(b *cfg.Block) bool {
	return len(b.Succs) == 2 && b.Succs[0].Kind == cfg.KindIfThen
}

// isExit: a path through b leaves the function (a return or the body's
// end), not a panic or other call that does not return.
func isExit(info *types.Info, cfgs *ctrlflow.CFGs, b *cfg.Block) bool {
	// go/cfg models a select as a chain of tests; without a default the
	// last test's false branch is a dead end no run takes.
	if !b.Live || len(b.Succs) > 0 || b.Kind == cfg.KindSelectAfterCase {
		return false
	}
	return len(b.Nodes) == 0 || !noReturn(info, cfgs, b.Nodes[len(b.Nodes)-1])
}

// noReturn: n is a call statement that does not return: panic, a function
// ctrlflow knows never returns (log.Fatal, os.Exit), or testing's Fatal
// and Skip (whose facts a vet run may lack, and which testing.TB hides).
func noReturn(info *types.Info, cfgs *ctrlflow.CFGs, n ast.Node) bool {
	es, ok := n.(*ast.ExprStmt)
	if !ok {
		return false
	}
	call, ok := ast.Unparen(es.X).(*ast.CallExpr)
	if !ok {
		return false
	}
	if id, ok := ast.Unparen(call.Fun).(*ast.Ident); ok {
		if _, ok := info.Uses[id].(*types.Builtin); ok && id.Name == "panic" {
			return true
		}
	}
	if fn := typeutil.StaticCallee(info, call); fn != nil && cfgs.NoReturn(fn) {
		return true
	}
	if fn, ok := typeutil.Callee(info, call).(*types.Func); ok && fn.Pkg() != nil && fn.Pkg().Path() == "testing" {
		switch fn.Name() {
		case "Fatal", "Fatalf", "FailNow", "Skip", "Skipf", "SkipNow":
			return true
		}
	}
	return false
}

// leaks: from node i+1 of block b, some path leaves the function without
// releasing v (err, the error Open returned with it, nil where v is set).
// A path stops at a release or a new value of v; a nil test of v or err
// prunes the branch where v is nil. A use of err it cannot follow ends
// the search with false: it reports only paths it understands.
func leaks(info *types.Info, cfgs *ctrlflow.CFGs, b *cfg.Block, i int, v, err *types.Var) bool {
	type item struct {
		b       *cfg.Block
		i       int
		errLive bool
	}
	type state struct {
		b       int32
		errLive bool
	}
	seen := map[state]bool{}
	work := []item{{b, i + 1, err != nil}}
	for len(work) > 0 {
		it := work[len(work)-1]
		work = work[:len(work)-1]
		dead := -1
		stopped := false
		for j := it.i; j < len(it.b.Nodes) && !stopped; j++ {
			n := it.b.Nodes[j]
			if noReturn(info, cfgs, n) {
				stopped = true
				break
			}
			cond := j == len(it.b.Nodes)-1 && isIf(it.b)
			if it.errLive {
				switch u := useOf(info, n, err, nil); {
				case u == useAssign:
					it.errLive = false
				case u == useNone:
				case cond:
					d, ok := nilTest(info, n, err, true)
					if !ok {
						return false
					}
					dead = d
				default:
					return false
				}
			}
			switch useOf(info, n, v, sessionRelease) {
			case useRelease, useAssign:
				stopped = true
			case useBenign:
				if cond {
					if d, ok := nilTest(info, n, v, false); ok {
						dead = d
					}
				}
			}
		}
		if stopped {
			continue
		}
		if isExit(info, cfgs, it.b) {
			return true
		}
		for k, s := range it.b.Succs {
			if k == dead {
				continue
			}
			if st := (state{s.Index, it.errLive}); !seen[st] {
				seen[st] = true
				work = append(work, item{s, 0, it.errLive})
			}
		}
	}
	return false
}

// reach calls f for each node reachable from node i+1 of block b, along
// paths that do not set v again; f returns false to stop that path.
func reach(info *types.Info, b *cfg.Block, i int, v *types.Var, f func(ast.Node) bool) {
	seen := map[int32]bool{}
	type item struct {
		b *cfg.Block
		i int
	}
	work := []item{{b, i + 1}}
	for len(work) > 0 {
		it := work[len(work)-1]
		work = work[:len(work)-1]
		stopped := false
		for j := it.i; j < len(it.b.Nodes) && !stopped; j++ {
			n := it.b.Nodes[j]
			stopped = !f(n) || useOf(info, n, v, nil) == useAssign
		}
		if stopped {
			continue
		}
		for _, s := range it.b.Succs {
			if !seen[s.Index] {
				seen[s.Index] = true
				work = append(work, item{s, 0})
			}
		}
	}
}
