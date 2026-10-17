# linkvet

`github.com/nfltr/link/linkvet` is a `go vet` tool for code that uses
[link](../README.md) sessions, and for gRPC clients: apps that reach their
services through the connect sidecar (`nfltr dial`, a `connect:` mapping)
are gRPC apps, and the gRPC checks are for them. It is a module of its own,
so link itself keeps importing the standard library only.

Each check reports only what it can see in one function: precision over
recall. A report is a leak or a bug, or code that relies on something no
reader of the function can see; code it cannot follow (a session handed to
another function, an error tested in a way it does not model) is not
reported. Every check has `analysistest` cases, the reports it must make
and the code it must not report (`testdata/src/<check>/`).

## Install and run

```
go install github.com/nfltr/link/linkvet/cmd/linkvet@latest
go vet -vettool=$(command -v linkvet) ./...
```

Or alone: `linkvet ./...` (`-test=false` leaves out test files). One check
off: `-grpcdeadline=false`; only some: `-sessionclose -closewrite`.
`linkvet help <check>` prints a check's documentation. In a program, the
analyzers are `linkvet.Analyzers` (each an `*analysis.Analyzer`).

## The link checks

### sessionclose

A session some path leaves open: one `Open` or `OpenChain` returns
(`Mux`, `Pool`, `peerlink.Node`, any `peerlink.Link`), or the one an accept
handler passed to `Serve` is given (the handler owns it). It follows each
path from the `Open` to the function's end; `Close`, `CloseWrite`,
`Reset`, `Abort`, `Refuse`, `Pump`, a `defer`, or handing the session on
(returning, storing, passing it to a function, `Join`, a goroutine) ends
the path; the branch of `if err != nil` (or `s == nil`) where there is no
session is not followed; a panic, `log.Fatal` or `t.Fatal` is not an end.

```
s, err := m.Open(hdr) // session s is not closed on every path
if err != nil {
	return err
}
if _, err := s.Write(req); err != nil {
	return err // s stays open
}
return s.Close()
```

Fix: `defer s.Close()` right after the error check. An unclosed session
holds one of the connection's sessions (`MaxSessions`) and its window
until the peer or the connection ends it. Also reported: an `Open` whose
session is dropped (`m.Open(h)`, `_, err := m.Open(h)`), and a handler
whose session parameter is `_`.

### readloop

A loop reading a session (`Read`, `io.ReadFull`, `io.ReadAtLeast`,
`binary.Read`, `fmt.Fscan`) that has a context at hand, the function's or
the one `Open` took, and nothing that ends a blocked `Read`: no read
deadline, no closure or goroutine that could close the session. A
context bounds the `Open` only; a peer that stops sending holds the loop.

```
s, err := node.Open(ctx, peer, hdr)
...
for {
	n, err := s.Read(buf) // read of session s in a loop with no deadline and no cancel path
```

Fix: `stop := context.AfterFunc(ctx, func() { s.Close() }); defer stop()`,
or `s.SetReadDeadline` in the loop. A `Close` after the loop or deferred
is not a cancel path (it runs after the loop).

### closewrite

A write (`Write`, `WriteBuffers`, `io.WriteString`, `io.Copy`,
`fmt.Fprint*`, `binary.Write`) that some path reaches after the same
session's `CloseWrite`, the variable not set again in between. It fails
with `io.ErrClosedPipe`.

```
_ = s.CloseWrite()
_, err := s.Write(trailer) // write to session s after its CloseWrite (line 41)
```

Fix: write everything, then `CloseWrite`; reads after it are fine.

### openctx

`Open` given `context.Background()` or `context.TODO()` (or a variable
only ever set to one of them), outside `_test.go` files. `Node.Open` waits
for the peer to connect until its context ends; with one that never ends
it waits as long as the peer is away.

```
s, err := node.Open(context.Background(), "b", hdr) // Open with a context that never ends
```

Fix: `ctx, cancel := context.WithTimeout(ctx, 5*time.Second); defer cancel()`.

## The gRPC checks

They know gRPC by its shapes (a stream is a type with `Recv` and
`RecvMsg`; a client stream has `CloseSend`, `SendMsg`, `RecvMsg`; a call
takes a `context.Context` first and `...grpc.CallOption` last), so linkvet
does not depend on grpc-go.

### grpcrecvselect

A stream's `Recv` in a `select` that has a `default` case.

```
for {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		msg, err := stream.Recv() // Recv blocks: the select's default case does not make it non-blocking
```

`Recv` blocks; while it waits nobody looks at `ctx.Done()` (or a timer).
Fix: drop the select and receive in a plain loop, and end a blocked `Recv`
by canceling the stream's context (the stream's own, from
`context.WithCancel`, canceled when the timer fires or ctx ends).

### grpcdrain

A client stream opened with a context that never ends
(`context.Background()`, `TODO()`) that is neither received until an
error (`Recv` or `RecvMsg` in a loop, `CloseAndRecv`) nor handed on. Such a
stream holds its goroutine and buffers until the `ClientConn` closes (the
rules in grpc-go's `ClientConn.NewStream` documentation).

```
st, err := client.Watch(context.Background(), req) // client stream st is neither received until an error (io.EOF) nor canceled
...
first, err := st.Recv()
return first, err
```

Fix: a context you cancel (`ctx, cancel := context.WithCancel(ctx);
defer cancel()`), or receive until `err != nil`. A stream on the caller's
context is not reported: the caller may cancel it.

### grpcclosesend

A client stream that `Send`s and never calls `CloseSend` (or
`CloseAndRecv`) in the function, and is not handed on. The server never
reads `io.EOF`; one that waits for the client's end waits until the
context ends.

```
st, err := client.Chat(ctx) // client stream st sends but never calls CloseSend
...
_ = st.Send(req)
for {
	if _, err := st.Recv(); err != nil {
```

Fix: `st.CloseSend()` after the last `Send`.

### grpcdeadline

A unary call or stream given `context.Background()` or `context.TODO()`
outside `_test.go` files (best effort: a context from elsewhere may lack a
deadline too, and is not reported). A server that does not answer holds
the call forever; a stream on such a context has no way to end it from the
client.

```
resp, err := client.Get(context.Background(), req) // gRPC call with a context that has no deadline
```

Fix: `ctx, cancel := context.WithTimeout(ctx, 2*time.Second); defer cancel()`.

### grpcdial

`grpc.Dial`, `grpc.DialContext` or `grpc.NewClient` inside a function that
runs per request: an HTTP handler (a function taking an
`http.ResponseWriter` and an `*http.Request`), an exported method of a
gRPC service (a type embedding an `Unimplemented...Server`), or a link
accept handler (a function passed to `Serve`). A connection, and its
handshake, per request. Conservative: a dial in a helper the handler calls
is not reported.

```
func (s *server) Get(ctx context.Context, in *pb.Req) (*pb.Res, error) {
	cc, err := grpc.NewClient(s.backend) // grpc.NewClient in a gRPC service method, which runs per request
```

Fix: dial once at startup, keep the `*grpc.ClientConn` (it is safe for
concurrent use and reconnects itself), close it at shutdown.

## Measured on link, relay and nfltr

`go vet -vettool=linkvet ./...` on 2026-10-17 (tests included):

| code | reports | in non-test code | false positives |
|---|---:|---:|---:|
| link (its modules: link, peerlink, grpclink, quiclink, soak) | 27 | 3, in `peerlinktest` | 0 |
| relay (`client`, `proto`) and relay-server | 7 | 0 | 0 |
| nfltr | 4 | 3 | 0 |

Every report is true as stated. Those in tests are sessions a test leaves
to the peer's refusal or abort, `MaxSessions` fills and handlers that let
the opener end the session: true, and harmless where they are.
`peerlinktest`'s three are on purpose (a test that a Write after
`CloseWrite` fails; an `Open` on `context.Background()` whose session is
dropped, which must fail with `ErrDraining`). nfltr's
three outside tests: `cmd/samples/grpc-agent` receives in a select with a
default (its 60 s timer and `ctx.Done()` go unseen while `Recv` waits) and
never calls `CloseSend`, two real bugs in a sample; `tests/connect/grpc-liveness`
watches a health stream on `context.Background()` on purpose (it runs
until killed). `make check-linkvet` runs linkvet over link's own packages
(tests and `peerlinktest` aside) and fails on a report.

## Debug mode

The checks see one function. For what happens at run time, link has a
debug mode (`-tags linkdebug`): sessions collected unclosed, concurrent
Reads or Writes on one session, sessions idle past `LINKDEBUG_IDLE`, each
with the stacks involved ([README](../README.md#finding-misuse-linkvet-and-debug-mode)).
