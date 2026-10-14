# Changelog

Releases of `github.com/nfltr/link` (tags `vX.Y.Z`) and of the modules in
it (`peerlink/grpclink/vX.Y.Z`, `peerlink/quiclink/vX.Y.Z`). The wire
format is [WIRE.md](WIRE.md); every release since v0.2.0 speaks the same
version 2 unless an entry says otherwise.

## v0.7.2 (2026-10-14)

Fixes, docs and checks; no wire change, no API change.

- A peer that pings without reading no longer grows this side's write
  buffer: a waiting pong answers the newer ping in its place.
- `Pool` redials `Min` with `Backoff` when the peer refuses connections
  (GOAWAY, or a close within the liveness interval, before any session);
  it redialled such a peer in a tight loop. Other redials are as before.
- Senders waiting for room in the write buffer take turns in order; on a
  slow path one could wait seconds for a turn while the others moved.
- Malformed hellos and replies (a field out of bounds) are `ErrFrame`
  (`errors.Is`); they were untyped errors.
- `peerlinktest.Proxy` relays at its rate on Linux (it relayed a seventh
  of it).
- WIRE.md with test vectors (`testdata/wire_vectors.json`), SECURITY.md,
  FUZZING.md; fuzz targets for `ParseWindow` and quiclink's stream
  preamble; `make fuzz-long`; GitHub Actions CI; examples for `Mux`,
  `Conn`, `peerlink.Node` (both directions) and quiclink; the errors to
  test with `errors.Is` in the package docs.
- `make soak` (the `soak/` module, SOAK.md): link over TCP and quiclink
  over UDP, both directions, under cuts, throttling, black holes and
  rebinds, with integrity, dead-peer and leak checks and a JSON summary.

## peerlink/quiclink v0.1.1 (2026-10-14)

- The stream preamble is read by one function, fuzzed (`FuzzPreamble`);
  a runnable `Example`. No behaviour or wire change.

## v0.7.1 (2026-10-14)

- `peerlinktest.UDPProxy`: black hole, cut, throttle and rebind for
  implementations over UDP.
- The `peerlink/quiclink` module (`peerlink/quiclink/v0.1.0`): a
  `peerlink.Link` over QUIC streams, both directions, passing the suite;
  its connections survive the dialler's address changing.
- No change to link or its wire.

## v0.7.0 (2026-10-12)

The reverse direction.

- `peerlink.Node` without `Config.Dial` opens sessions on the connections
  its peers dialled in (peers behind NAT); `Open` toward a peer with none
  up waits until its context ends, then fails with
  `peerlink.ErrPeerUnreachable`.
- `Config.Connect` keeps a dialler's connections up so that its peers
  open sessions on them; `Serve` takes sessions on dialled connections too
  (it no longer needs a `Listener`).
- `PoolConfig.Accept` takes the sessions a peer opens on a `Pool`'s
  connections (they were refused).
- `peerlinktest`: `Pair.Reverse`, cases `Reconnect` and `Unreachable`.
  `grpclink` does not support the reverse direction.
- No wire change.

## v0.6.1 (2026-10-12)

- `peerlink.Config.ServedPeer` names an accepted connection's peer. No
  wire change.

## v0.6.0 (2026-10-12), peerlink/grpclink v0.1.0

- `peerlink`: the `Link` API (`Open`, `Serve`, `Drain`), its contract, and
  `Node`, an implementation over `Pool` and `Mux`.
- `peerlink/peerlinktest`: the contract's conformance suite (`Run`) and
  the fault-injecting TCP `Proxy`.
- The `peerlink/grpclink` module: `peerlink.Link` over gRPC streams.
- No wire change.

## v0.5.0 (2026-10-11), breaking

Wire version 1 removed; version 2 is the only wire version.

- Removed: `Hello`, `ReadHello`, `Reply`, `Version`, `TokenLen`,
  `NewConn`, `Codec`, `Message`. `NewMux` takes only the connection;
  `Config.Version` is 0 or 2.
- A version-1 hello (a dialer of v0.4 or older) gets the version reply,
  which such a dialer reads as a refusal, and `Server` returns `ErrV1`
  (`errors.Is` `ErrVersion`). A v0.5 dialer to a listener that speaks
  only version 1 is refused after the listener read 37 bytes.
- Version 2 on the wire is unchanged (`testdata/v2_session.golden`).

## v0.4.2 (2026-10-11)

- Liveness detects a dead peer within Interval + Timeout, idle or busy.
- GOAWAY drains keep their tails, and opens that crossed the GOAWAY run
  like any other session (they were refused with a close).
- `Pool` works past going-away connections; background growth
  (`GrowWait`); `Observer.ConnDown` gets the cause.
- No wire format change.

## v0.4.1 (2026-10-11)

- Sessions on a busy connection take turns; a sender writes once.

## v0.4.0 (2026-10-11)

- `Session.Pump`: a session's bytes go to a writer from a goroutine that
  runs only while bytes are queued (an idle session holds none).
- `Session.WriteTo` returns received buffers to the pool after a writev.
- A drained `bytequeue` reuses its array; `bytequeue.TryPopBatch`,
  `Ready`.
- No wire change.

## v0.3.0 (2026-10-11)

- `Session.Reset` (CLOSE code 2, announced by feature bit `FeatureReset`;
  sent as a plain close to v0.2 peers).
- A session cut with its connection reads the connection's error, never
  `io.EOF`; `Session.Done`; session read deadlines (net/http serves
  sessions); `WriteBuffers`. Write deadlines are not supported.

## v0.2.1 (2026-10-11)

- A race at liveness start fixed; credit returns in quarter-window steps.

## v0.2.0 (2026-10-11)

- Wire version 2, negotiated in the hello (version 1 served byte for
  byte): `Client`/`Server` with an `Authenticator`, ping liveness driven
  by the `Liveness` presets, GOAWAY, `Pool`, `Observer`, `Credit` for
  callers with a transport of their own.

## v0.1.0 (2026-10-11)

- Wire version 1, extracted unchanged from an existing TCP data plane.
