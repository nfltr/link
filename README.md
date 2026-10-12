# link

`github.com/nfltr/link` is the nfltr endpoint-to-endpoint link layer: many
sessions, each a byte stream with its own flow control, multiplexed over one
connection the caller dials and secures (TLS, a proxy, a pipe). A session
whose reader is slow stalls only itself; one write to the network carries
every frame queued at the time.

- Standard library only (`TestModuleBoundary`).
- Apache-2.0 (`LICENSE`, `NOTICE`; every source file carries an SPDX header).
- Wire format version 2 is specified in `doc.go`;
  `testdata/v2_session.golden` holds a recorded session every release is
  tested against, byte for byte. Version 1 was removed in v0.5.0.

## Packages

| package | what |
|---|---|
| `link` | `Client`/`Server` (the hello, an `Authenticator`'s credential, `Config`, `Observer`), `Conn` (frames, write coalescing, OPEN/CLOSE, ping liveness, GOAWAY), `Credit` (per-session flow control, also over a caller's own transport), `Mux` and `Session` (sessions as `net.Conn`s), `Pool` (several connections per peer, growth, backoff, make-before-break drain), `Liveness` and its presets |
| `link/bytequeue` | a FIFO of byte slices bounded by total bytes; `PushWithin` for a pusher that serves other queues too |
| `link/peerlink` | a transport-neutral API for sessions between peers (`Link`: `Open`, `Serve`, `Drain`), its contract, and `Node`, an implementation over `Pool` and `Mux` ([peerlink/README.md](peerlink/README.md)) |
| `link/peerlink/peerlinktest` | the contract's conformance suite (`Run`) and the fault-injecting `Proxy`, for any implementation |

`peerlink/grpclink` (a module of its own, `make check-grpclink`) implements
`peerlink.Link` over gRPC streams and passes the same suite.

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
`make check-grpclink`: the gRPC implementation's module.

## Status

v0.1.0: the version-1 wire, extracted unchanged from nfltr's TCP data
plane. v0.2.0: wire version 2 (negotiated in the hello; version 1 served
byte for byte), `Client`/`Server` with an `Authenticator`, ping liveness
driven by the `Liveness` presets, GOAWAY, `Pool`, `Observer`, `Credit` for
callers with a transport of their own. v0.3.0: `Session.Reset` (CLOSE
code 2, announced by feature bit `FeatureReset`; sent as a plain close to
v0.2 peers), a session cut with its connection reads the connection's
error (never `io.EOF`), `Session.Done`, session read deadlines (net/http
serves sessions). Write deadlines are not supported. v0.4.0:
`Session.Pump` (a session's bytes go to a writer from a goroutine that
runs only while bytes are queued: an idle session holds none),
`Session.WriteTo` returns received buffers to the pool after a writev
(it dropped them), a drained `bytequeue` reuses its array,
`bytequeue.TryPopBatch` and `Ready`. No wire change.

**v0.5.0 (breaking): wire version 1 removed.** Version 2 is the only wire
version: `Hello`, `ReadHello`, `Reply`, `Version`, `TokenLen`, `NewConn`,
`Codec` and `Message` are gone, `NewMux` takes only the connection, and
`Config.Version` is 0 or 2. A version-1 hello (a dialer of v0.4 or older)
gets the version reply, which such a dialer reads as a refusal, and
`Server` returns `ErrV1` (`errors.Is` `ErrVersion`). A v0.5 dialer to a
listener that speaks only version 1 is refused after the listener read 37
bytes. Version 2 on the wire is unchanged (`testdata/v2_session.golden`).

v0.6.0: `peerlink` (the `Link` API, its contract, `Node` over `Pool` and
`Mux`), `peerlink/peerlinktest` (the conformance suite) and the
`peerlink/grpclink` module. No wire change.
