# link

`github.com/nfltr/link` is the nfltr endpoint-to-endpoint link layer: many
sessions, each a byte stream with its own flow control, multiplexed over one
connection the caller dials and secures (TLS, a proxy, a pipe). A session
whose reader is slow stalls only itself; one write to the network carries
every frame queued at the time.

- Standard library only (`TestModuleBoundary`).
- Apache-2.0 (`LICENSE`, `NOTICE`; every source file carries an SPDX header).
- Wire formats version 1 and 2 are specified in `doc.go`;
  `testdata/v1_session.golden` and `testdata/v2_session.golden` hold recorded
  sessions every release is tested against, byte for byte.

## Packages

| package | what |
|---|---|
| `link` | `Client`/`Server` (the hello of either version, an `Authenticator`'s credential, version negotiation, `Config`, `Observer`), `Conn` (frames, write coalescing; version 2: OPEN/CLOSE, ping liveness, GOAWAY), `Credit` (per-session flow control, also over a caller's own transport), `Mux` and `Session` (sessions as `net.Conn`s), `Pool` (several connections per peer, growth, backoff, make-before-break drain), `Liveness` and its presets; `Hello`/`ReadHello`/`Reply` (the version-1 handshake by hand) |
| `link/bytequeue` | a FIFO of byte slices bounded by total bytes; `PushWithin` for a pusher that serves other queues too |

## Use

```go
c, err := link.Client(ctx, nc, link.Config{Window: 4 << 20, Liveness: link.LivenessFast, Auth: auth}) // dialer
m := link.NewMux(c, nil)
go m.Serve(nil)
s, err := m.Open(func(string) ([]byte, error) { return header, nil })

c, id, err := link.Server(ctx, nc, link.Config{Auth: auth}) // listener
go link.NewMux(c, nil).Serve(func(header []byte, s *link.Session) { /* ... */ })

p := link.NewPool(link.PoolConfig{Link: cfg, Dial: dial, Max: 8, Backoff: backoff})
s, err := p.Open(ctx, header)
```

## Checks

`make check`: build, vet, tests with `-race`, `fuzz-short` (every decoder,
`FUZZTIME` each, default 10s), gosec, semgrep (docker), govulncheck.

## Status

v0.1.0: the version-1 wire, extracted unchanged from nfltr's TCP data
plane. v0.2.0: wire version 2 (negotiated in the hello; version 1 served
byte for byte), `Client`/`Server` with an `Authenticator`, ping liveness
driven by the `Liveness` presets, GOAWAY, `Pool`, `Observer`, `Credit` for
callers with a transport of their own. v0.3.0: `Session.Reset` (CLOSE
code 2, announced by feature bit `FeatureReset`; sent as a plain close to
v0.2 peers), a session cut with its connection reads the connection's
error (never `io.EOF`), `Session.Done`, session read deadlines (net/http
serves sessions). Write deadlines are not supported.
