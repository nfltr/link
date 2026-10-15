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
func reply(me string) func(peer string, header []byte, s net.Conn) {
	return func(peer string, header []byte, s net.Conn) {
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
	if err := s.(peerlink.Session).CloseWrite(); err != nil {
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
`testdata/v2_session.golden` holds a recorded session. Every release is
tested against both, byte for byte.

## Checks

`make check`: build, vet, tests with `-race` (the Examples, and
`TestDocCodeBlocksAreCompiled`: every Go block in the Markdown here is a
verbatim excerpt of a Go file, so a doc cannot rot), `fuzz-short` (every
decoder, `FUZZTIME` each, default 10s), gosec, semgrep (docker),
govulncheck. `make check-api`: the exported API against the latest
release, with [apidiff](https://pkg.go.dev/golang.org/x/exp/cmd/apidiff);
the changes must be the ones listed in `api/` (an API change is never
an accident). `make check-grpclink`, `make check-quiclink`: the gRPC and
QUIC implementations' modules. GitHub Actions
(`.github/workflows/check.yml`) runs them on every push and pull request,
and every fuzz target for 5 minutes weekly. `make fuzz-long FUZZTIME=30m`
is the long fuzz run ([FUZZING.md](FUZZING.md)).

## Status and compatibility

The latest release is v0.8.0; [CHANGELOG.md](CHANGELOG.md) lists every
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
  versioned on their own (`peerlink/<name>/vX.Y.Z`).

v1.0.0 will freeze the API under Go's compatibility rules once the
maturity criteria below hold.

## Maturity

Evidence that link works, provable from this repository alone:

| criterion | evidence | status |
|---|---|---|
| wire specified, with test vectors | WIRE.md, `testdata/wire_vectors.json` (`TestWireVectors`), `testdata/v2_session.golden` | met |
| every decoder fuzzed, corpus checked in, 24 h cumulative without a crasher | FUZZING.md (hours per target, crashers), `testdata/fuzz/` | 4.5 h, no crasher in the library (target: 24 h) |
| security review of the hello and the Authenticator contract | SECURITY.md (threat model, review record) | met (2026-10-14) |
| standard library only | `TestModuleBoundary` | met |
| CI green on every change, with history | GitHub Actions (`check`) | since 2026-10-14 |
| cumulative soak hours under faults, with leak and integrity checks | SOAK.md (`make soak`), summaries in `soak/results/` | 2.35 h (macOS, Linux CI), clean (target: 24 h) |
| benchmarks published in the repository | `compare/README.md` (link vs yamux), `peerlink/quiclink/README.md` | met |
| API reviewed for v1.0 | the checklist below; `make check-api` (apidiff against the latest release, in CI); `TestExportedNamesDocumented` | open (v0.8.0: API trimmed, constructors checked, every name documented) |

v1.0 API checklist: every exported name documented with an example
where it is not obvious; errors to test with `errors.Is` listed per call;
no exported name that exists only for one caller; option structs instead
of positional growth; the peerlink contract unchanged for two minor
releases; deprecated names removed. Use in production by its first
user (an HTTP and TCP relay) is supporting evidence, not a criterion.
