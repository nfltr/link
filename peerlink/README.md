# peerlink

`github.com/nfltr/link/peerlink` is a small, transport-neutral API for
byte-stream sessions between peers, a conformance suite that checks an
implementation keeps its contract, and implementations over
`github.com/nfltr/link` and over gRPC.

New here? The [quick start](../README.md#quick-start) and the
[recipes](../docs/recipes.md) use `Node`; every Example in this package
runs as written (`go doc -all github.com/nfltr/link/peerlink`).

```go
type Link interface {
	// Open opens a session to peer, a name the implementation's dialer
	// resolves, with header: the bytes the peer's accept receives. ctx
	// bounds the open, not the session. A peer's refusal surfaces as
	// Open's error or as the session's first Read error (never io.EOF).
	Open(ctx context.Context, peer string, header []byte) (net.Conn, error)
	// Serve takes the sessions peers open until Drain. accept runs in a
	// goroutine of its own for each, with the opener's name as the
	// implementation authenticated it and the header; it owns the session
	// and closes it (it may return first). Serve returns nil once Drain
	// stopped it, else the error that did.
	Serve(accept func(peer string, header []byte, s net.Conn)) error
	// Drain stops the Link: no new sessions either way, open ones run to
	// their end, then everything is released; at ctx's end the sessions
	// still open are cut and Drain returns ctx's error. It is the only way
	// to release a Link (a done ctx closes at once), and the caller
	// chooses when: until then Serve and Open run as usual.
	Drain(ctx context.Context) error
}
```

## When to use it

When two or more processes exchange many independent byte streams (one per
request, tunnel or forwarded connection) and the code doing so should not
depend on how they are carried: a multiplexer over TLS today, gRPC or QUIC
tomorrow, an in-process pair in tests. Code written against `Link` swaps
implementations without changes; the suite says whether a new one behaves
the same, failures included.

If you need one stream to one peer, use a `net.Conn`; if you need request
and response messages with schemas, use an RPC framework.

## The contract

The package documentation (`go doc github.com/nfltr/link/peerlink`) states
it in full. In short:

- A session is a `net.Conn`, with `CloseWrite() error` (half-close: the
  peer reads `io.EOF` after every byte written and can still write back)
  and `Reset() error` (the peer reads an error): `s.(peerlink.Session)`.
- `io.EOF` means the peer ended its writes on purpose. A session cut any
  other way (reset, broken or closed connection, dead peer, a drain's
  deadline) reads an error that is not `io.EOF`.
- A dead peer (nothing closed, nothing arriving) surfaces as an error on
  every session with it, on both ends, within a bound the implementation
  states.
- A slow or stuck reader stalls only its own session; on a slow path every
  session keeps getting its turn.
- `Drain` refuses new sessions (`ErrDraining` locally; the peer's opens
  fail) while open ones run to their end, then releases everything; at
  its context's end it cuts what is left. The caller decides when to call
  it; until then `Serve` and `Open` run as usual.
- Headers of up to 4 KiB arrive as sent.
- The reverse direction, where an implementation supports it: `Open` on
  the connections a peer dialled in (a peer behind NAT, which the opener
  cannot dial). With none up, `Open` waits for the peer to connect until
  its context ends, then fails with `ErrPeerUnreachable` (and the
  context's error); once the peer dials again, sessions open on the new
  connection. Every other rule holds that way too.
- Policy is the caller's: which peer, retries, who may connect, timeouts.

## Running the suite against your implementation

```go
func TestConformance(t *testing.T) {
	peerlinktest.Run(t, func(t *testing.T) peerlinktest.Pair {
		pc := listenUDP(t)
		px := peerlinktest.NewUDPProxy(t, pc.LocalAddr().String())
		b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: link.LivenessFast})
		a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", px.Addr()), Liveness: link.LivenessFast})
		return peerlinktest.Pair{
			A: a, B: b, AName: "a", BName: "b",
			DeadAfter: a.DeadAfter(),
			BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle,
		}
	})
}
```

That is quiclink's (`peerlink/quiclink/quiclink_test.go`); `mustNew` is
its `New` failing the test on an error. Your implementation needs only to
accept a custom dialer: the suite's
`Proxy` relays TCP and black-holes (nothing crosses, nothing closes),
resets, or throttles the path; `UDPProxy` does the same for UDP (a cut
there drops the flows for good, as UDP has no reset to send) and also
rebinds them (the dialler's address changes). A Pair's faults are its own
functions: an implementation on another transport brings its own. The suite calls `Serve` on B and `Drain` on
both. With `Pair.Reverse`, B dials A and A opens its sessions on B's
connections (the reverse direction); the suite then calls `Serve` on A
too, and runs every case that way, plus `Unreachable`. Every time bound comes from your `DeadAfter` or is a generous bound
for work that must finish at all (`peerlinktest.Within`); none is tied to a
transport. Cases: `Echo`, `Header`, `HalfCloseOpenerFirst`,
`HalfCloseAccepterFirst`, `Large` (64 MiB each way, checksummed),
`Concurrent` (32 sessions), `StuckReader` (8 sessions beside a stuck one),
`Fairness` (8 sessions on a 1 MiB/s path), `DeadPeer`, `Cut`, `Reset`,
`DrainAccepter`, `DrainOpener`, `DrainDeadline`, `Reconnect` (opens work
again within the bound after a cut, the opener retrying), `Unreachable`
(reverse only: `Open` toward a peer with no connection waits until its
context ends, then `ErrPeerUnreachable`).

## The reverse direction (peers behind NAT)

A `peerlink.Node` without `Config.Dial` opens its sessions on the
connections its peers dialled in: the one with the fewest sessions among
that peer's connections not going away (as a `link.Pool` picks), so a
connection going away moves new opens to the others. The peer is the name
`Serve` gives its accept (`Config.ServedPeer`, else the authenticated
`Identity.Subject`). The dialling side lists the peer in `Config.Connect`:
its `Serve` dials it and keeps `Pool.Min` connections (at least one) up,
redialling as they end, and takes the sessions opened on them.

```go
func ExampleNode_reverse() {
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	server := must(peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("server")}}))
	go server.Serve(echo)
	defer drain(server)

	client := must(peerlink.New(peerlink.Config{
		Link:    link.Config{Auth: link.NameAuth("client")},
		Connect: []string{"server"},
		Dial:    dialTCP(ln.Addr().String()),
	}))
	go client.Serve(echo)
	defer drain(client)

	// Open waits (up to its ctx) for the client's connection to come up.
	fmt.Println(call(server, "client", "hello"))
	// Output: server echo hello
}
```

Liveness and drain are as in the other direction: pings on every
connection, a dead peer surfacing on both ends within `DeadAfter`, the
listener's `Drain` sending GOAWAY, the dialler's `Drain` refusing (a reset)
the sessions opened toward it from then on. Every connection carries
sessions both ways, so two peers need only one connection between them
(the quick start). A Node that dials some peers and also opens sessions
toward other peers behind NAT, over the connections they dialled in,
uses two Nodes: one with `Dial`, one without. How many connections the dialler keeps is its `Pool` setting;
the listener cannot ask for more.

## Implementations

| | `peerlink.Node` (this package) | `grpclink.Node` (`peerlink/grpclink`) | `quiclink.Node` (`peerlink/quiclink`) |
|---|---|---|---|
| carries sessions over | `link.Pool` per peer (dialling), `link.Mux` per accepted connection | one gRPC bidirectional stream per session, a `grpc.ClientConn` per peer | one QUIC stream per session, a QUIC connection per peer |
| dependencies | standard library | google.golang.org/grpc (a module of its own) | github.com/quic-go/quic-go (a module of its own) |
| who the opener is | the `Identity.Subject` of the caller's `link.Authenticator` | the caller's `Identify(ctx)` (metadata, credentials) | the caller's `ServedPeer(conn)`, else the client certificate's CommonName |
| dead-peer bound (`DeadAfter`) | Interval + Timeout of the liveness (`LivenessFast`: 4.5 s) | max(Interval, 10 s) + Timeout + 5 s (`LivenessFast`: 18 s): gRPC clients ping no more often than every 10 s, and a client whose writes are stuck waits 5 s to send GOAWAY before closing | Interval + Timeout (`LivenessFast`: 4.5 s): keep-alive after Interval, QUIC's idle timeout of Timeout, and a check that closes a connection silent for Interval + Timeout/2 (QUIC's idle timer alone runs late on a side that cannot send) |
| deadlines | read deadlines | none (`errors.ErrUnsupported`) | read and write deadlines |
| reverse direction (`Open` on connections the peer dialled) | yes, without `Config.Dial` | no: a gRPC stream starts at the client (`TestReverseUnsupported`) | yes, without `Config.Dial` |
| survives the dialler's address changing | no (TCP) | no (TCP) | yes (QUIC connection migration, `TestMigration`) |
| suite | every case, both directions | every case (forward) | every case, both directions (through `peerlinktest.UDPProxy`) |

Suite results on loopback (an arm64 laptop, `-race`, three runs each): both pass
every case; link in both directions (reverse: the same dead-peer and
fairness figures, opens working again within 30 ms of a cut, once the
dialler redialled). Dead peer: link surfaces it on all six sessions at 4.50 s
(bound 4.5 s); gRPC at 4.5 s on the listener and 13.0 s on the dialer, 18.0 s when its
writes were stuck (bound 18 s).
Fairness at 1 MiB/s, 8 sessions: link 448-544 KiB each in 4 s (longest
wait for a turn under 0.5 s, 20 runs under load; before v0.8.0, when
senders waiting for room raced for it, 0.8-3.2 s), gRPC 480-512 KiB
(under 0.3 s). The case's bound is 2 s (with -race on a loaded 4-CPU
Linux host, link's longest wait reached 1.3 s).
QUIC (quiclink, both directions): dead peer at 3.0 s with a session
busy, up to 4.5 s idle (bound 4.5 s); fairness 499-505 KiB each (under
0.1 s); opens working again 3.0-5.0 s after a cut (on UDP a cut is a
path gone silent: the old connection must time out first).

`grpclink` runs with `make check-grpclink` (build, vet, the suite with
`-race`), `quiclink` with `make check-quiclink`.

`grpclink/example_test.go` (`Example`) is application code written against
`peerlink.Link` only (a service and a caller over `net.Conn` sessions),
wired to gRPC in one function; swapping in `peerlink.Node` changes that
function alone.

## What shaped the API

The link-backed `Node` was shaped by a server-to-server link between
replicas of one service, which needed:

- a pool of connections per peer with growth, a cap and dial backoff
  (`Config.Pool`: link's `PoolConfig` knobs);
- the caller's own dialing (TLS, mutual TLS or plaintext) and
  authentication (`Config.Dial`, `Config.Link.Auth`);
- the session header as the caller's routing bytes, opaque to the link;
- metrics per peer: connections up and down, sessions, credit waits, ping
  round trips (`Config.Observer`, a `link.Observer` per peer), with an
  accepted connection's peer named by the caller where peers share one
  credential (`Config.ServedPeer`: its remote address, say);
- failing fast toward a peer the caller considers unreachable: the
  caller's `Dial` sees every dial and its latency and can refuse at once;
  its error returns from `Open` (with no `Pool.Backoff`);
- a drain the caller starts when it is ready (after its own shutdown
  steps), with serving going on until then.

These live in the implementation's `Config`, not in `Link`, which stays
three methods.
