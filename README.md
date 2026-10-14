# link

`github.com/nfltr/link` is the nfltr endpoint-to-endpoint link layer: many
sessions, each a byte stream with its own flow control, multiplexed over one
connection the caller dials and secures (TLS, a proxy, a pipe). A session
whose reader is slow stalls only itself; one write to the network carries
every frame queued at the time.

- Standard library only (`TestModuleBoundary`).
- Apache-2.0 (`LICENSE`, `NOTICE`; every source file carries an SPDX header).
- Wire format version 2 is specified in [WIRE.md](WIRE.md), with test
  vectors for every message (`testdata/wire_vectors.json`);
  `testdata/v2_session.golden` holds a recorded session. Every release is
  tested against both, byte for byte. Version 1 was removed in v0.5.0.

## Packages

| package | what |
|---|---|
| `link` | `Client`/`Server` (the hello, an `Authenticator`'s credential, `Config`, `Observer`), `Conn` (frames, write coalescing, OPEN/CLOSE, ping liveness, GOAWAY), `Credit` (per-session flow control, also over a caller's own transport), `Mux` and `Session` (sessions as `net.Conn`s), `Pool` (several connections per peer, growth, backoff, make-before-break drain), `Liveness` and its presets |
| `link/bytequeue` | a FIFO of byte slices bounded by total bytes; `PushWithin` for a pusher that serves other queues too |
| `link/peerlink` | a transport-neutral API for sessions between peers (`Link`: `Open`, `Serve`, `Drain`), its contract, and `Node`, an implementation over `Pool` and `Mux` ([peerlink/README.md](peerlink/README.md)) |
| `link/peerlink/peerlinktest` | the contract's conformance suite (`Run`) and the fault-injecting `Proxy` (TCP) and `UDPProxy`, for any implementation |

`peerlink/grpclink` (a module of its own, `make check-grpclink`) implements
`peerlink.Link` over gRPC streams and passes the same suite (one
direction: the dialer opens). `peerlink/quiclink` (a module of its own,
`make check-quiclink`) implements it over QUIC streams and passes the
suite in both directions; its connections survive the dialler's address
changing ([peerlink/quiclink/README.md](peerlink/quiclink/README.md)).

## Use

`example_test.go` (`Example`, run by `go test`, shown by `go doc`) is a
complete program: a TLS listener that authenticates dialers and serves
sessions, and a dialer's `Pool` opening sessions on it. In short:

```go
c, err := link.Client(ctx, nc, link.Config{Window: 4 << 20, Liveness: link.LivenessFast, Auth: auth}) // dialer
m := link.NewMux(c)
go m.Serve(nil)
s, err := m.Open(func(string) ([]byte, error) { return header, nil })

c, id, err := link.Server(ctx, nc, link.Config{Auth: auth}) // listener
go link.NewMux(c).Serve(func(header []byte, s *link.Session) { /* ... */ })

p := link.NewPool(link.PoolConfig{Link: cfg, Dial: dial, Max: 8, Backoff: backoff})
s, err := p.Open(ctx, header)
```

## Checks

`make check`: build, vet, tests with `-race`, `fuzz-short` (every decoder,
`FUZZTIME` each, default 10s), gosec, semgrep (docker), govulncheck.
`make check-grpclink`, `make check-quiclink`: the gRPC and QUIC
implementations' modules.
GitHub Actions (`.github/workflows/check.yml`) runs all three on every
push and pull request, and every fuzz target for 5 minutes weekly.
`make fuzz-long FUZZTIME=30m` is the long fuzz run ([FUZZING.md](FUZZING.md)).

## Status and compatibility

The latest release is v0.7.2; [CHANGELOG.md](CHANGELOG.md) lists every
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
| every decoder fuzzed, corpus checked in, 24 h cumulative without a crasher | FUZZING.md (hours per target, crashers), `testdata/fuzz/` | in progress |
| security review of the hello and the Authenticator contract | SECURITY.md (threat model, review record) | met (2026-10-14) |
| standard library only | `TestModuleBoundary` | met |
| CI green on every change, with history | GitHub Actions (`check`) | since 2026-10-14 |
| cumulative soak hours under faults, with leak and integrity checks | SOAK.md (`make soak`), summaries in `soak/results/` | 1.08 h, clean (target: 24 h) |
| benchmarks published in the repository | `compare/README.md` (link vs yamux), `peerlink/quiclink/README.md` | met |
| API reviewed for v1.0 | the checklist below | open |

v1.0 API checklist: every exported name documented with an example
where it is not obvious; errors to test with `errors.Is` listed per call;
no exported name that exists only for one caller; option structs instead
of positional growth; the peerlink contract unchanged for two minor
releases; deprecated names removed. Use in production by its first
user (an HTTP and TCP relay) is supporting evidence, not a criterion.
