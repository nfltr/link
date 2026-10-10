# link

`github.com/nfltr/link` is the nfltr endpoint-to-endpoint link layer: many
sessions, each a byte stream with its own flow control, multiplexed over one
connection the caller dials and secures (TLS, a proxy, a pipe). A session
whose reader is slow stalls only itself; one write to the network carries
every frame queued at the time.

- Standard library only (`TestModuleBoundary`).
- Apache-2.0 (`LICENSE`, `NOTICE`; every source file carries an SPDX header).
- Wire format version 1 is specified in `doc.go`; `testdata/v1_session.golden`
  holds a recorded session every release is tested against, byte for byte.

## Packages

| package | what |
|---|---|
| `link` | `Hello`/`ReadHello`/`Reply` (the version-1 handshake), `Conn` (frames, per-session credit, write coalescing), `Mux` and `Session` (sessions as `net.Conn`s; the caller's `Codec` encodes opens and closes), `Liveness` and its presets |
| `link/bytequeue` | a FIFO of byte slices bounded by total bytes |

## Use

```go
window, err := link.Hello(nc, token, 16<<20)  // dialer
c := link.NewConn(nc, 16<<20, window)
m := link.NewMux(c, codec)
go m.Serve(nil)
s, err := m.Open(func(sid string) ([]byte, error) { return codec.Open(sid, header) })
```

## Checks

`make check`: build, vet, tests with `-race`, `fuzz-short` (every decoder,
`FUZZTIME` each, default 10s), gosec, semgrep (docker), govulncheck.

## Status

Phase 1 of the link package (version-1 wire, extracted unchanged from the
nfltr's TCP data plane). Planned: wire version 2 (OPEN, CLOSE, PING,
GOAWAY frames, link-chosen session ids), credit on every path, `Pool`
(several connections per peer, make-before-break drain), `Authenticator`,
`Observer`.
