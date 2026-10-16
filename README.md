# link

`github.com/nfltr/link` multiplexes many byte-stream sessions, each a
`net.Conn` with its own flow control, over connections you dial and secure
(plain TCP, TLS, mutual TLS). A slow reader stalls only its own session,
a dead peer is noticed within a bound you pick, and the main module
imports only the Go standard library (Apache-2.0).

## Quick start

Two peers in one program: `a` dials `b` over plaintext TCP, then each
opens a session to the other on that one connection. It needs Go 1.27 or
later (`go.mod`). Run it with
`go run github.com/nfltr/link/examples/quickstart@latest`, or save it as
`main.go` in a module of your own (`go get github.com/nfltr/link@latest`):

```go
// Command quickstart runs two peers, a and b, in one process: a dials b
// over plaintext TCP, then each opens a session to the other on that one
// connection.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

func main() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}

	// b listens. NameAuth: each peer sends its name and is known by it
	// (a trusted network only; see docs/recipes.md for mutual TLS).
	b, err := peerlink.New(peerlink.Config{
		Listener: ln,
		Link:     link.Config{Auth: link.NameAuth("b")},
	})
	if err != nil {
		log.Fatal(err)
	}
	go b.Serve(reply("b"))

	// a dials b, and keeps a connection to it (Connect) so that b can
	// open sessions to a as well.
	a, err := peerlink.New(peerlink.Config{
		Link:    link.Config{Auth: link.NameAuth("a")},
		Connect: []string{"b"},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", ln.Addr().String()) // peer is "b"
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	go a.Serve(reply("a"))

	fmt.Println(ask(a, "b")) // a opens a session to b
	fmt.Println(ask(b, "a")) // b opens one to a, on the connection a dialled

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = a.Drain(ctx)
	_ = b.Drain(ctx)
}

// reply serves each session a peer opens: it reads the request and
// answers it.
func reply(me string) func(peer string, header []byte, s peerlink.Session) {
	return func(peer string, header []byte, s peerlink.Session) {
		defer s.Close()
		req, _ := io.ReadAll(s) // until the opener's CloseWrite
		fmt.Fprintf(s, "%s got %q (%s) from %s", me, req, header, peer)
	}
}

// ask opens a session to peer, sends a request and reads the answer.
func ask(l peerlink.Link, peer string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := l.Open(ctx, peer, []byte("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	if _, err := io.WriteString(s, "hello"); err != nil {
		log.Fatal(err)
	}
	// The peer reads io.EOF after "hello"; this side can still read.
	if err := s.CloseWrite(); err != nil {
		log.Fatal(err)
	}
	answer, err := io.ReadAll(s)
	if err != nil {
		log.Fatal(err)
	}
	return string(answer)
}
```

It prints:

```
b got "hello" (greeting) from a
a got "hello" (greeting) from b
```

In two processes, one runs `b`'s half (`net.Listen`, a Node with
`Listener`, `Serve`) and the other `a`'s (a Node whose `Dial` reaches
`b`'s address, with `Connect: []string{"b"}`, and `Serve`: `b` opens
sessions to `a` only on the connection `Connect` keeps up, and only
`Serve` takes them); either then opens sessions to the other.
[examples/twoproc](examples/twoproc/main.go) does that:

```
go run github.com/nfltr/link/examples/twoproc@latest -role b &   # listens on -addr (127.0.0.1:7000)
go run github.com/nfltr/link/examples/twoproc@latest -role a     # dials it; exits once both sessions ran
kill %1
```

For mutual TLS ([examples/mtls](examples/mtls/main.go)), gRPC or HTTP
over a link, liveness, draining, reconnects and metrics, see the
[recipes](docs/recipes.md).

## Chaining

Since v0.10.0 sessions join end to end: a path `origin -> hop -> ... ->
far end`, each arrow a connection of its own (its own TLS, liveness and
credit). `link.Forward(route, cfg)` is an accept function that makes a
`Mux` (or `Pool`) a hop: your `Route` opens the next leg (on any `Mux`,
`Pool` or peer you choose), Forward passes the far end's answer back and
splices the two sessions with `link.Join`, holding no goroutine while
they are idle. Where both ends of a connection announce `FeatureChain`
(every v0.10.0 side does; WIRE.md section 12), each open is answered:
`Session.Answered` returns nil only once the far end accepted
(`Accept`, or its first write), or the `*link.Abort` of a refusal
(`Refuse(code, msg)`) or reset (`Abort(code, msg)`): its `Code` (link's
below `CodeApp`, yours from it), its `Msg`, and `Hops`, which hop it came
from (0: your peer). A dead hop reaches the origin as `CodeConnLost` from
the hop before it; a draining hop refuses new opens (`CodeDraining`)
while what it joined finishes, and cuts the rest at its deadline
(`CodeDrainDeadline`); a routing loop stops at `ForwardConfig.MaxHops`
(`CodeHopLimit`). A connection to a v0.9 peer is unchanged: its sessions
do not answer (`Answers()` false, `Answered` returns `ErrNoAnswers`),
and a hop in front of one accepts once its Route returned.

```go
// ExampleForward joins two links end to end: the origin opens through a
// forwarding hop to a far end, which answers each open; the origin reads
// the answer (accepted, with the path's length, or a typed refusal
// saying which hop and why) before it trusts the session.
func ExampleForward() {
	o, h1 := bind(link.NoAuth) // origin -> hop
	h2, f := bind(link.NoAuth) // hop -> far end

	// The far end: an echo, or a refusal with the application's code.
	go link.NewMux(f).Serve(func(header []byte, s *link.Session) {
		if string(header) != "echo" {
			_ = s.Refuse(link.CodeApp+1, "no such service")
			return
		}
		_ = s.Accept()
		_, _ = io.Copy(s, s)
		_ = s.CloseWrite()
	})

	// The hop: every session goes on to the far end (the Route decides
	// where; Forward answers, joins and holds no goroutine while idle).
	onward := link.NewMux(h2)
	go onward.Serve(nil)
	go link.NewMux(h1).Serve(link.Forward(func(ctx context.Context, in *link.Session) (*link.Session, error) {
		c, _ := link.ChainFromContext(ctx)
		return onward.OpenChain(in.Header(), c)
	}, link.ForwardConfig{}))

	// The origin.
	m := link.NewMux(o)
	go m.Serve(nil)
	defer m.Close()
	s, _ := m.Open([]byte("echo"))
	_, _ = io.WriteString(s, "hello") // before the answer: no round trip per hop
	_ = s.CloseWrite()
	if err := s.Answered(context.Background()); err != nil {
		panic(err)
	}
	reply, _ := io.ReadAll(s)
	fmt.Printf("%s, %d hop(s) beyond the peer\n", reply, s.PathLen())

	r, _ := m.Open([]byte("ftp"))
	var a *link.Abort
	if errors.As(r.Answered(context.Background()), &a) {
		fmt.Printf("refused %d hop(s) beyond the peer: code %d, %s\n", a.Hops, a.Code-link.CodeApp, a.Msg)
	}
	// Output:
	// hello, 1 hop(s) beyond the peer
	// refused 1 hop(s) beyond the peer: code 1, no such service
}
```

Over any `peerlink.Link` (a `Node`, quiclink, a mix):
`node.Serve(peerlink.Forward(route, cfg))` for a hop, `peerlink.Join(a,
b)` to splice any two conns (a session and the TCP connection to its
target at the far end, or a client's local connection), and the
`peerlink.ChainSession` interface for the answers (quiclink v0.4.0
negotiates them with ALPN `peerlink-chain`). Every hop's `Observer`
gets `Hop` events (opened, answered with its latency, ended with bytes
each way) carrying the origin's trace id.

Bytes in flight per session and direction are bounded by the windows
of the links plus one 256 KiB write batch per hop (3.5 MiB over three
links with the default 1 MiB windows), so a stuck far reader stalls only
its own session. Hops are transports: a hop sees the header, the bytes
and the answers and can forge them; encrypt end to end inside the
session (TLS with a pinned certificate) where that matters
([SECURITY.md](SECURITY.md)). The design is
[docs/design/chaining.md](docs/design/chaining.md).

Measured (Apple M5, TCP loopback, in process):

| Forward hops | links | MB/s, 1 session | MB/s, 32 sessions | 64 B echo round trip, µs | OpenChain -> Answered, µs |
|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 1,812 | 1,922 | 55 | 27 |
| 1 | 2 | 2,433 | 1,708 | 205 | 55 |
| 2 | 3 | 1,256 | 2,434 | 321 | 112 |
| 4 | 5 | 1,725 | 1,173 | 505 | 220 |

Medians of three runs (`go test -run '^$' -bench BenchmarkChain -benchmem -count 3`;
8 MiB per session; a shared host, load average 14-26 on 10 CPUs, so the
spread between runs is wide). A joined session at rest holds
0 goroutines and about 7.0 KiB of heap for its three
sessions and joint (origin, one Forward hop, far end; `TestChainIdleCost`,
200 sessions). In-flight bytes for a stalled far reader stay within the
links' windows plus 256 KiB per hop (`TestChainStalledFarReader`).

## Which API

| you want | use |
|---|---|
| sessions between named peers over TCP or TLS: **start here** | `peerlink.Node` ([peerlink](peerlink/README.md)) |
| the same over QUIC (UDP), surviving address changes | `quiclink.Node` ([peerlink/quiclink](peerlink/quiclink/README.md), a module of its own) |
| the same over a network that passes only gRPC | `grpclink.Node` (`peerlink/grpclink`, a module of its own) |
| gRPC or HTTP served over a link | `peerlink.Listener` ([recipes](docs/recipes.md#grpc-over-a-link)) |
| code that works with any of these, and tests that check them | the `peerlink.Link` interface; `peerlinktest.Run` (the conformance suite) |
| advanced: your own connection management | `link.Pool` (connections to one peer), `link.Mux` (sessions on one connection), `link.Client`/`link.Server` (the handshake) |
| advanced: your own receive loop or transport | `link.Conn` (frames), `link.Credit` (per-session flow control) |

Constructors check their configuration and name the field to fix
(`peerlink.New`, `link.NewPool`, `link.Config.Validate`); a zero
`link.Config` is valid for a dialer, and a listener needs `Auth`. Each package's documentation (`go doc`, or
[pkg.go.dev](https://pkg.go.dev/github.com/nfltr/link)) has an Example for
each common use.

## Errors

Test errors with `errors.Is` (or `errors.As` for `*link.VersionError`);
the messages name what to change.

| error | where you see it | what it means | what to change |
|---|---|---|---|
| `link.ErrHandshake` (wraps the network's error) | `Open` (dialing), `Client`, `Server`, `peerlink.Config.HandshakeError` (`func(net.Conn, error)`) | the hello or its reply did not arrive whole: the other end closed or timed out, is not link, speaks TLS where this end does not, or TLS refused a certificate | TLS on both ends or neither; the address of a link listener; the certificates (the CA each end trusts, the names, expiry), as the message says |
| `link.ErrRefused` | `Open` (dialing), `Client` | the listener's Authenticator refused the dialer's credential | the dialer's `Config.Auth`: what the listener accepts (`CertAuth`'s certificate, the token) |
| `link.ErrAuth` (wraps `Verify`'s error) | `Server`, `peerlink.Config.HandshakeError` (`func(net.Conn, error)`) | this listener refused a dialer | the reason it wraps; the dialer's credential or this `Config.Auth` |
| `link.ErrVersion`, `*link.VersionError` | `Client`, `Server` | the peer speaks another wire version | upgrade the older end (every release since v0.5.0 speaks version 2) |
| `link.ErrDeadPeer` (a `net.Error` with `Timeout()` true) | session reads, `Mux.Serve`, `Observer.ConnDown` | nothing arrived for the liveness bound: the peer or the path died | nothing, if it did; else a preset with a longer bound for a path that goes silent (`LivenessNAT`, `LivenessMobile`) |
| `link.ErrGoAway` | `Mux.Open`, `Conn.SendOpen` | the connection is going away (a drain) | open on another connection (`Pool` and `Node` do) |
| `link.ErrFull` | `Open` | the peer's most sessions at once are open | close sessions sooner, raise the peer's `Config.MaxSessions`, or allow more connections (`Pool.Max`) |
| `peerlink.ErrDraining` | `Node.Open` | this Node is draining | nothing: it is shutting down |
| `peerlink.ErrPeerUnreachable` (and the context's error) | `Node.Open` without `Dial` | no connection from that peer before the context ended | the peer's name (the error lists the connected ones); that peer's `Connect` and `Dial`; a longer context |
| `link.ErrReset` | session reads | the peer reset the session | the peer's reason |
| `*link.Abort` (`errors.As`), `link.ErrOpenRefused`, `link.ErrReset` | `Session.Answered`, session reads (`FeatureChain`) | the far end (or the hop `Hops` says) refused the open or reset the session, with a `Code` and a `Msg` | what the code says: `CodeDraining` open elsewhere, `CodeHopLimit` a routing loop, `CodeConnLost` a hop's connection died, `CodeApp`+ the application's own |
| `link.ErrNoAnswers` | `Session.Answered` | the connection's peer does not answer opens (link before v0.10.0, or no `FeatureChain`) | nothing: use the session without an answer, or refuse such a leg in your Route |
| `link.ErrConnClosed` (wraps why: `ErrDeadPeer`, the network's error) | session reads; writes return `io.ErrClosedPipe` wrapping it | the connection closed under the session | open a new session (`Pool` and `Node` redial) |
| `link.ErrFrame`, `link.ErrCredit` | `Conn.Recv`, `Mux.Serve` | the peer broke the wire protocol | a peer that is not link, or a bug: please report it |
| a message naming a field | `peerlink.New`, `link.NewPool`, `Validate`, `Client`, `Server` | a configuration that cannot work | the field it names, as it says |
| `io.EOF` | session reads | the peer ended its writes on purpose (`CloseWrite`, `Close`) | nothing: the normal end |
| `os.ErrDeadlineExceeded` | session reads | the read deadline passed | nothing, or a later deadline |

quiclink adds its own codes (`quiclink.Code*`, in the QUIC errors its
sessions read); grpclink's are gRPC statuses.

## Packages

| package | what |
|---|---|
| `link` | `Client`/`Server` (the hello, an `Authenticator`'s credential, `Config`, `Observer`), `Conn` (frames, write coalescing, OPEN/CLOSE, ping liveness, GOAWAY), `Credit` (per-session flow control, also over a caller's own transport), `Mux` and `Session` (sessions as `net.Conn`s), `Pool` (several connections per peer, growth, backoff, make-before-break drain), `Liveness` and its presets |
| `link/peerlink` | a transport-neutral API for sessions between peers (`Link`: `Open`, `Serve`, `Drain`), its contract, `Node` (an implementation over `Pool` and `Mux`) and `Listener` ([peerlink/README.md](peerlink/README.md)) |
| `link/peerlink/peerlinktest` | the contract's conformance suite (`Run`) and the fault-injecting `Proxy` (TCP) and `UDPProxy`, for any implementation |
| `link/examples/quickstart` | the quick start above |

`peerlink/grpclink` (a module of its own, `make check-grpclink`) implements
`peerlink.Link` over gRPC streams and passes the same suite (one
direction: the dialer opens). `peerlink/quiclink` (a module of its own,
`make check-quiclink`) implements it over QUIC streams and passes the
suite in both directions; its connections survive the dialler's address
changing ([peerlink/quiclink/README.md](peerlink/quiclink/README.md)).

Wire format version 2 is specified in [WIRE.md](WIRE.md), with test
vectors for every message (`testdata/wire_vectors.json`);
`testdata/v2_session.golden` holds a recorded session (and
`testdata/v2_chain_session.golden` one with `FeatureChain`). Every release is
tested against them, byte for byte.

## Checks

`make check`: build, vet, tests with `-race` (the Examples, and
`TestDocCodeBlocksAreCompiled`: every Go block in the Markdown here is a
verbatim excerpt of a Go file, so a doc cannot rot), `fuzz-short` (every
decoder, `FUZZTIME` each, default 10s), gosec, semgrep (docker),
govulncheck. `make check-api`: the exported API against the latest
release, with [apidiff](https://pkg.go.dev/golang.org/x/exp/cmd/apidiff);
the changes must be the ones listed in `api/` (an API change is never
an accident). `make check-grpclink`, `make check-quiclink`: the gRPC and
QUIC implementations' modules; `make check-soak`: the soak module builds
and vets. `make check-all` runs every one of them, as CI does. GitHub Actions
(`.github/workflows/check.yml`) runs them on every push and pull request,
and every fuzz target for 5 minutes weekly. `make fuzz-long FUZZTIME=30m`
is the long fuzz run ([FUZZING.md](FUZZING.md)).

## Status and compatibility

The latest release is v0.10.0; [CHANGELOG.md](CHANGELOG.md) lists every
release. link is pre-1.0 and follows [semantic versioning](https://semver.org)
as Go modules do for v0:

- **The wire** is the stronger promise: version 2 ([WIRE.md](WIRE.md)) is
  held byte for byte by its test vectors and golden recording. A change
  to it is a new wire version, negotiated in the hello, and announced in
  the changelog; feature bits add optional frames without one.
- **The Go API** may change in a minor release (v0.x.0), with the change
  and its migration in the changelog; a patch release (v0.x.y) never
  removes or changes an exported name. Removals are announced a minor
  release ahead where possible.
- **Behaviour** that the docs state (bounds, errors to test with
  `errors.Is`, the peerlink contract) changes only in a minor release,
  except to fix a bug, which the changelog flags.
- `peerlink/grpclink` and `peerlink/quiclink` are modules of their own,
  versioned on their own (`peerlink/<name>/vX.Y.Z`). They stay v0 until
  their transport's module is v1: quiclink's API exposes quic-go's types.
- **Interfaces** that link calls (`Observer`) may gain methods in minor
  releases; implementations embed `NopObserver`, which the compiler
  requires. Interfaces that callers call (`peerlink.Link`,
  `peerlink.Session`, `Authenticator`) never change: new capabilities
  arrive as optional interfaces.
- `peerlinktest` may add checks in minor releases.

v1.0.0 will freeze the API once the maturity criteria below hold: from
then on the Go API follows the Go 1 compatibility rules, and a breaking
change is a `/v2` module.

## Maturity

Evidence that link works, provable from this repository alone:

| criterion | evidence | status |
|---|---|---|
| wire specified, with test vectors | WIRE.md, `testdata/wire_vectors.json` (`TestWireVectors`), `testdata/v2_session.golden` | met |
| every decoder fuzzed, corpus checked in, 24 h cumulative without a crasher | FUZZING.md (hours per target, crashers), `testdata/fuzz/` | 4.85 h, no crasher in the library (target: 24 h) |
| security review of the hello and the Authenticator contract | SECURITY.md (threat model, review record) | met (2026-10-14) |
| standard library only | `TestModuleBoundary` | met |
| CI green on every change, with history | GitHub Actions (`check`) | since 2026-10-14 |
| cumulative soak hours under faults, with leak and integrity checks | SOAK.md (`make soak`), summaries in `soak/results/` | 2.62 h (macOS, Linux CI; chain-3hop since v0.10.0), clean (target: 24 h) |
| benchmarks published in the repository | `compare/README.md` (link vs yamux), `peerlink/quiclink/README.md` | met |
| API reviewed for v1.0 | the checklist below; `make check-api` (apidiff against the latest release, in CI); `TestExportedNamesDocumented` | open (v0.9.0: the review against yamux, smux, quic-go and libp2p applied; the peerlink contract changed in v0.9.0) |

v1.0 API checklist: every exported name documented with an example
where it is not obvious; errors to test with `errors.Is` listed per call;
no exported name that exists only for one caller; option structs instead
of positional growth; the peerlink contract unchanged for two minor
releases; deprecated names removed. Use in production by its first
user (an HTTP and TCP relay) is supporting evidence, not a criterion.
