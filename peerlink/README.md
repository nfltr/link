# peerlink

`github.com/nfltr/link/peerlink` is a small, transport-neutral API for
byte-stream sessions between peers, a conformance suite that checks an
implementation keeps its contract, and implementations over
`github.com/nfltr/link` and over gRPC.

```go
// Link carries byte-stream sessions between peers.
type Link interface {
	Open(ctx context.Context, peer string, header []byte) (net.Conn, error)
	Serve(accept func(peer string, header []byte, s net.Conn)) error
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
  and `Reset() error` (the peer reads an error), reached by type assertion.
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
- Policy is the caller's: which peer, retries, who may connect, timeouts.

## Running the suite against your implementation

```go
func TestConformance(t *testing.T) {
	peerlinktest.Run(t, func(t *testing.T) peerlinktest.Pair {
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		px := peerlinktest.NewProxy(t, ln.Addr().String()) // injects the faults
		b := yourlink.New(ln)                              // listens
		a := yourlink.New(nil, dialVia(px.Addr()))         // dials "b" through the proxy
		return peerlinktest.Pair{
			A: a, B: b, AName: "a", BName: "b",
			DeadAfter: a.DeadAfter(), // your stated dead-peer bound
			BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle,
		}
	})
}
```

Your implementation needs only to accept a custom dialer: the suite's
`Proxy` relays TCP and black-holes (nothing crosses, nothing closes),
resets, or throttles the path. The suite calls `Serve` on B and `Drain` on
both. Every time bound comes from your `DeadAfter` or is a generous bound
for work that must finish at all (`peerlinktest.Within`); none is tied to a
transport. Cases: `Echo`, `Header`, `HalfCloseOpenerFirst`,
`HalfCloseAccepterFirst`, `Large` (64 MiB each way, checksummed),
`Concurrent` (32 sessions), `StuckReader` (8 sessions beside a stuck one),
`Fairness` (8 sessions on a 1 MiB/s path), `DeadPeer`, `Cut`, `Reset`,
`DrainAccepter`, `DrainOpener`, `DrainDeadline`.

## Implementations

| | `peerlink.Node` (this package) | `grpclink.Node` (`peerlink/grpclink`) |
|---|---|---|
| carries sessions over | `link.Pool` per peer (dialling), `link.Mux` per accepted connection | one gRPC bidirectional stream per session, a `grpc.ClientConn` per peer |
| dependencies | standard library | google.golang.org/grpc (a module of its own) |
| who the opener is | the `Identity.Subject` of the caller's `link.Authenticator` | the caller's `Identify(ctx)` (metadata, credentials) |
| dead-peer bound (`DeadAfter`) | Interval + Timeout of the liveness (`LivenessFast`: 4.5 s) | max(Interval, 10 s) + Timeout + 5 s (`LivenessFast`: 18 s): gRPC clients ping no more often than every 10 s, and a client whose writes are stuck waits 5 s to send GOAWAY before closing |
| deadlines | read deadlines | none (`errors.ErrUnsupported`) |
| suite | every case | every case |

Suite results on loopback (an arm64 laptop, `-race`, three runs each): both pass
every case. Dead peer: link surfaces it on all six sessions at 4.50 s
(bound 4.5 s); gRPC at 4.5 s on the listener and 13.0 s on the dialer, 18.0 s when its
writes were stuck (bound 18 s).
Fairness at 1 MiB/s, 8 sessions: link 416-576 KiB each in 4 s (longest
wait for a turn under 1 s), gRPC 480-512 KiB (under 0.3 s).

`grpclink` runs with `make check-grpclink` (build, vet, the suite with
`-race`).

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
  round trips (`Config.Observer`, a `link.Observer` per peer);
- failing fast toward a peer the caller considers unreachable: the
  caller's `Dial` sees every dial and its latency and can refuse at once;
  its error returns from `Open` (with no `Pool.Backoff`);
- a drain the caller starts when it is ready (after its own shutdown
  steps), with serving going on until then.

These live in the implementation's `Config`, not in `Link`, which stays
three methods.
