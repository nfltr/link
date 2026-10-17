// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package linkvet

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// The gRPC checks look at types by their methods and at grpc's
// functions by their names, so they need no gRPC dependency here.
const grpcPath = "google.golang.org/grpc"

// GRPCRecvSelect reports a stream's Recv in a select that has a default
// case: Recv blocks, so the select's other cases (ctx.Done()) go unseen
// while it waits.
var GRPCRecvSelect = &analysis.Analyzer{
	Name:     "grpcrecvselect",
	Doc:      "report a gRPC stream's Recv in a select with a default case",
	URL:      "https://github.com/nfltr/link/tree/main/linkvet#grpcrecvselect",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runRecvSelect,
}

// GRPCDrain reports a client stream on a context that never ends and is
// not received until an error: its resources stay until the ClientConn
// closes.
var GRPCDrain = &analysis.Analyzer{
	Name:     "grpcdrain",
	Doc:      "report gRPC client streams neither received to io.EOF nor canceled",
	URL:      "https://github.com/nfltr/link/tree/main/linkvet#grpcdrain",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runDrain,
}

// GRPCCloseSend reports a client stream that sends and never calls
// CloseSend (or CloseAndRecv): the server never reads io.EOF.
var GRPCCloseSend = &analysis.Analyzer{
	Name:     "grpcclosesend",
	Doc:      "report gRPC client streams that Send without CloseSend",
	URL:      "https://github.com/nfltr/link/tree/main/linkvet#grpcclosesend",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runCloseSend,
}

// GRPCDeadline reports a gRPC call or stream given context.Background()
// or context.TODO() outside tests.
var GRPCDeadline = &analysis.Analyzer{
	Name:     "grpcdeadline",
	Doc:      "report gRPC calls with a context that has no deadline, outside tests",
	URL:      "https://github.com/nfltr/link/tree/main/linkvet#grpcdeadline",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runDeadline,
}

// GRPCDial reports grpc.Dial, DialContext or NewClient in a function that
// runs per request: an HTTP handler, a gRPC service method or a link
// accept handler.
var GRPCDial = &analysis.Analyzer{
	Name:     "grpcdial",
	Doc:      "report gRPC dials in HTTP, gRPC and link handlers (a connection per request)",
	URL:      "https://github.com/nfltr/link/tree/main/linkvet#grpcdial",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runDial,
}

func isStream(t types.Type) bool { return t != nil && hasMethod(t, "Recv", "RecvMsg") }
func isClientStream(t types.Type) bool {
	return t != nil && hasMethod(t, "CloseSend", "RecvMsg", "SendMsg")
}

func runRecvSelect(pass *analysis.Pass) (any, error) {
	ins := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	info := pass.TypesInfo
	ins.Preorder([]ast.Node{(*ast.SelectStmt)(nil)}, func(n ast.Node) {
		sel := n.(*ast.SelectStmt)
		hasDefault := false
		for _, c := range sel.Body.List {
			hasDefault = hasDefault || c.(*ast.CommClause).Comm == nil
		}
		if !hasDefault {
			return
		}
		for _, c := range sel.Body.List {
			for _, st := range c.(*ast.CommClause).Body {
				inspectOwn(st, func(m ast.Node) bool {
					if _, ok := m.(*ast.SelectStmt); ok {
						return false // its own select
					}
					call, ok := m.(*ast.CallExpr)
					if !ok {
						return true
					}
					if s, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr); ok && s.Sel.Name == "Recv" && isStream(info.TypeOf(s.X)) {
						pass.Reportf(call.Pos(), "Recv blocks: the select's default case does not make it non-blocking, and the select's other cases go unseen while it waits; end it by canceling the stream's context")
					}
					return true
				})
			}
		}
	})
	return nil, nil
}

// streamSite is a local client stream from a call: st, err := c.M(ctx, ...).
type streamSite struct {
	v    *types.Var
	call *ast.CallExpr
}

func streamSites(info *types.Info, body *ast.BlockStmt) []streamSite {
	var sites []streamSite
	inspectOwn(body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Rhs) != 1 {
			return true
		}
		call, ok := ast.Unparen(as.Rhs[0]).(*ast.CallExpr)
		if !ok {
			return true
		}
		sig, ok := info.TypeOf(call.Fun).(*types.Signature)
		if !ok || sig.Results().Len() == 0 || !isClientStream(sig.Results().At(0).Type()) {
			return true
		}
		if v := localVar(info, as.Lhs[0]); v != nil {
			sites = append(sites, streamSite{v, call})
		}
		return true
	})
	return sites
}

// streamUses is the methods called on v in body, whether one of them
// is in a loop, and whether v is used any other way (it escapes).
func streamUses(info *types.Info, body *ast.BlockStmt, v *types.Var) (calls map[string]bool, inLoop map[string]bool, escapes bool) {
	calls, inLoop = map[string]bool{}, map[string]bool{}
	var stack []ast.Node
	ast.Inspect(body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		stack = append(stack, n)
		id, ok := n.(*ast.Ident)
		if !ok || info.Uses[id] != v {
			return true
		}
		loop := false
		for _, a := range stack[:len(stack)-1] {
			switch a.(type) {
			case *ast.FuncLit:
				escapes = true
			case *ast.ForStmt, *ast.RangeStmt:
				loop = true
			}
		}
		if len(stack) >= 3 {
			if sel, ok := stack[len(stack)-2].(*ast.SelectorExpr); ok && sel.X == id {
				if call, ok := stack[len(stack)-3].(*ast.CallExpr); ok && call.Fun == sel {
					calls[sel.Sel.Name] = true
					inLoop[sel.Sel.Name] = inLoop[sel.Sel.Name] || loop
					return true
				}
			}
		}
		switch p := stack[len(stack)-2].(type) {
		case *ast.BinaryExpr:
		case *ast.AssignStmt:
			isLHS := false
			for _, l := range p.Lhs {
				isLHS = isLHS || l == id
			}
			escapes = escapes || !isLHS
		default:
			escapes = true
		}
		return true
	})
	return calls, inLoop, escapes
}

// ctxArg is call's first context.Context argument.
func ctxArg(info *types.Info, call *ast.CallExpr) ast.Expr {
	for _, a := range call.Args {
		if isContext(info.TypeOf(a)) {
			return a
		}
	}
	return nil
}

func runDrain(pass *analysis.Pass) (any, error) {
	ins := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	info := pass.TypesInfo
	funcs(pass, ins, func(_ ast.Node, _ *ast.FuncType, body *ast.BlockStmt) {
		for _, s := range streamSites(info, body) {
			ctx := ctxArg(info, s.call)
			if ctx == nil || !isBackground(info, ctx, body) {
				continue // a context canceled here or by the caller ends the stream
			}
			calls, inLoop, escapes := streamUses(info, body, s.v)
			if escapes || calls["CloseAndRecv"] || inLoop["Recv"] || inLoop["RecvMsg"] {
				continue
			}
			pass.Reportf(s.call.Pos(), "client stream %s is neither received until an error (io.EOF) nor canceled: it holds its resources until the ClientConn closes; receive in a loop until err != nil, or use a context you cancel", s.v.Name())
		}
	})
	return nil, nil
}

func runCloseSend(pass *analysis.Pass) (any, error) {
	ins := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	info := pass.TypesInfo
	funcs(pass, ins, func(_ ast.Node, _ *ast.FuncType, body *ast.BlockStmt) {
		for _, s := range streamSites(info, body) {
			calls, _, escapes := streamUses(info, body, s.v)
			if escapes || !calls["Send"] || calls["CloseSend"] || calls["CloseAndRecv"] {
				continue
			}
			pass.Reportf(s.call.Pos(), "client stream %s sends but never calls CloseSend: the server never reads io.EOF; call %s.CloseSend() after the last Send", s.v.Name(), s.v.Name())
		}
	})
	return nil, nil
}

// isGRPCCall: call's function takes a context first and grpc.CallOptions
// last (a generated client method, ClientConn.Invoke and NewStream).
func isGRPCCall(info *types.Info, call *ast.CallExpr) bool {
	sig, ok := info.TypeOf(call.Fun).(*types.Signature)
	if !ok || !sig.Variadic() || sig.Params().Len() < 1 || !isContext(sig.Params().At(0).Type()) {
		return false
	}
	last, ok := sig.Params().At(sig.Params().Len() - 1).Type().(*types.Slice)
	if !ok {
		return false
	}
	n, ok := types.Unalias(last.Elem()).(*types.Named)
	return ok && n.Obj().Name() == "CallOption" && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == grpcPath
}

func runDeadline(pass *analysis.Pass) (any, error) {
	ins := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	info := pass.TypesInfo
	funcs(pass, ins, func(fn ast.Node, _ *ast.FuncType, body *ast.BlockStmt) {
		if isTestFile(pass, fn) {
			return
		}
		inspectOwn(body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isGRPCCall(info, call) || !isBackground(info, call.Args[0], body) {
				return true
			}
			sig := info.TypeOf(call.Fun).(*types.Signature)
			if sig.Results().Len() > 0 && isClientStream(sig.Results().At(0).Type()) {
				pass.Reportf(call.Args[0].Pos(), "gRPC stream on a context with no deadline and no cancel: it runs until the server ends it; use a context you cancel (context.WithCancel, WithTimeout)")
			} else {
				pass.Reportf(call.Args[0].Pos(), "gRPC call with a context that has no deadline: a server that does not answer holds it forever; use context.WithTimeout")
			}
			return true
		})
	})
	return nil, nil
}

func runDial(pass *analysis.Pass) (any, error) {
	ins := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	info := pass.TypesInfo
	lits, fns := handlers(pass, ins)
	ins.WithStack([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node, push bool, stack []ast.Node) bool {
		if !push {
			return true
		}
		call := n.(*ast.CallExpr)
		name := funcName(info, call)
		if name != grpcPath+".Dial" && name != grpcPath+".DialContext" && name != grpcPath+".NewClient" {
			return true
		}
		for i := len(stack) - 1; i >= 0; i-- {
			if what := perRequest(info, stack[i], lits, fns); what != "" {
				pass.Reportf(call.Pos(), "grpc.%s in %s, which runs per request: a connection (and its handshake) per request; dial once and share the *grpc.ClientConn", name[len(grpcPath)+1:], what)
				return true
			}
		}
		return true
	})
	return nil, nil
}

// perRequest names what n is when it is a function that runs per
// request, else "".
func perRequest(info *types.Info, n ast.Node, lits map[*ast.FuncLit]bool, fns map[*types.Func]bool) string {
	var typ *ast.FuncType
	switch n := n.(type) {
	case *ast.FuncLit:
		if lits[n] {
			return "a link accept handler"
		}
		typ = n.Type
	case *ast.FuncDecl:
		if obj, ok := info.Defs[n.Name].(*types.Func); ok {
			if fns[obj] {
				return "a link accept handler"
			}
			if n.Recv != nil && obj.Exported() && grpcService(obj.Signature().Recv().Type()) {
				return "a gRPC service method"
			}
		}
		typ = n.Type
	default:
		return ""
	}
	w, r := false, false
	for _, f := range typ.Params.List {
		t := info.TypeOf(f.Type)
		w = w || named(t, "net/http", "ResponseWriter")
		if p, ok := t.(*types.Pointer); ok {
			r = r || named(p.Elem(), "net/http", "Request")
		}
	}
	if w && r {
		return "an HTTP handler"
	}
	return ""
}

func named(t types.Type, pkg, name string) bool {
	n, ok := types.Unalias(t).(*types.Named)
	return ok && n.Obj().Name() == name && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == pkg
}

// grpcService: t's struct embeds a generated Unimplemented...Server.
func grpcService(t types.Type) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	st, ok := t.Underlying().(*types.Struct)
	if !ok {
		return false
	}
	for i := range st.NumFields() {
		f := st.Field(i)
		ft := f.Type()
		if p, ok := ft.(*types.Pointer); ok {
			ft = p.Elem()
		}
		if n, ok := types.Unalias(ft).(*types.Named); ok && f.Embedded() &&
			strings.HasPrefix(n.Obj().Name(), "Unimplemented") && strings.HasSuffix(n.Obj().Name(), "Server") {
			return true
		}
	}
	return false
}
