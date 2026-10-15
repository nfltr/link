# Changelog

Releases of `github.com/nfltr/link` (tags `vX.Y.Z`) and of the modules in
it (`peerlink/grpclink/vX.Y.Z`, `peerlink/quiclink/vX.Y.Z`). The wire
format is [WIRE.md](WIRE.md); every release since v0.2.0 speaks the same
version 2 unless an entry says otherwise.

## v0.8.1 (2026-10-15)

- Errors say more; `errors.Is` matches what it matched before. A
  handshake TLS refused for a certificate (an unknown CA, a name, expiry,
  the peer's alert) points at the certificates, not at TLS on one end
  only. `ErrRefused` names the dialer's `Config.Auth`. A session cut with
  its connection reads `ErrConnClosed` wrapping why (`ErrDeadPeer`, the
  network's error; the peer's close in its text, never as `io.EOF`) and
  saying to open a new session; a write past that end returns
  `io.ErrClosedPipe` wrapping the same error.
- `examples/mtls` (mutual TLS, a throwaway CA) and `examples/twoproc`
  (the quick start in two processes); `ExampleClient`, `ExampleNewPool`.
- No change to the existing API (the examples are new packages); no wire
  change.

## peerlink/quiclink v0.2.1 (2026-10-14)

- quic-go v0.63.1-0.20261014084224-4b404ee15e68 (its main branch; no
  release has it yet): a reordered packet no longer counts as lost for
  good. v0.63.0 took a packet overtaken by three later ones for a loss and
  cut its congestion window, never restoring it when the packet's ACK
  came: on a long path that reorders a few datagrams a session fell to
  ~1 MB/s and kept falling (200 ms RTT, every 100th datagram 2 ms late:
  1.2 MB/s, now ~54 MB/s). quic-go now disables packet-threshold loss
  detection after a spurious loss, restores the congestion state and
  widens the time threshold. Move to the first quic-go release with them.
- `TestThroughputReorderedPath`: a throughput floor on that path.
- No API or wire change.

## v0.8.0 (2026-10-14)

Developer experience: a quick start, recipes, constructors that check
their configuration, errors that say what to change. No wire change.

Breaking changes, and the migration:

- `link.NewPool` returns `(*Pool, error)`: it refuses a `PoolConfig`
  without `Dial`, with `Min` over `Max`, or with an invalid `Link`.
  Migration: `p, err := link.NewPool(cfg); if err != nil { ... }`.
- `peerlink.New` returns `(*Node, error)`: it refuses a `Config` with
  neither `Dial` nor `Listener`, a `Listener` without `Link.Auth` (every
  handshake failed, silently), `Connect` without `Dial` (was an error from
  `Serve`), or an invalid `Link` or `Pool`. Migration as above.
- `link.Config.Version` removed: version 2 is the only one, and any other
  value was refused. Migration: delete the field.
- `link.StatusBound`, `StatusRefused`, `StatusVersion`, `FeatureReset` and
  `(*Conn).PeerFeatures` unexported: wire details (WIRE.md) no caller
  needs; a reset is sent as one to every peer that reads it, as before.
  Migration: delete the uses.
- `bytequeue.New` panics on a limit under 1 (such a queue blocked its
  first `Push` for good).

Added:

- `link.NameAuth(name)` (a trusted network: the dialer's name is its
  `Identity.Subject`; method `MethodName`) and `link.CertAuth` (mutual
  TLS: the verified client certificate's CommonName, else its first DNS
  name). `NoAuth`'s docs say that every peer it accepts is named `""`.
- `link.Config.Validate` and `link.PoolConfig.Validate`: errors name the
  field and what to set; `Client`, `Server` and `NewPool` use them.
- `link.ErrHandshake`: a hello or reply that did not arrive whole (a TLS
  mismatch either way, a peer that is not link's), on both ends, wrapping
  the network's error and saying to check TLS; it was a bare `io.EOF` or
  `ErrFrame`.
- `peerlink.Listener` (`NewListener`, `Handle`): a `net.Listener` of
  sessions, to serve gRPC or HTTP over a link.
- `peerlink.Session`: the contract's `CloseWrite` and `Reset`, for
  `s.(peerlink.Session)` in place of an anonymous interface.
- `peerlink.Config.HandshakeError`: hears of each accepted connection
  whose handshake failed (they were closed silently).
- `peerlink.ErrPeerUnreachable`'s message lists the peers that are
  connected, so a misspelt peer name shows.
- `peerlink.Node.Open`'s errors toward a peer it dials name the peer
  (`peerlink: open to "b": ...`, wrapping the dial's error).
- The README's quick start (`examples/quickstart`, also
  `go run github.com/nfltr/link/examples/quickstart@latest`), a table of
  the errors with what to change, docs/recipes.md (plaintext and mutual
  TLS, liveness presets, gRPC and HTTP over a link, drain, reconnects,
  metrics), each a runnable Example; every Example self-contained.
- `TestDocCodeBlocksAreCompiled`: every Go block in the Markdown is a
  verbatim excerpt of a Go file. `make check-api` (CI): the API against
  the latest release with apidiff must match what `api/` lists.
- Package docs start with what the package is for, and say where to
  start; lint markers no longer show in them; every exported name is
  documented (`TestExportedNamesDocumented`).

## peerlink/quiclink v0.2.0 (2026-10-14)

- Fix: a dead peer is noticed within `DeadAfter` (Interval + Timeout)
  again. QUIC's idle timer starts at the first packet sent after the last
  one received; a side that could not send when the path went silent
  (writes stalled, congestion window full) timed out late by as long as
  it could not send (the soak measured 4.99 s and 5.22 s against 4.5 s;
  `TestDeadPeerSendStalled`: 5.4 s). Each connection now also closes
  itself once no packet arrived for Interval + Timeout/2, sampled every
  Timeout/4 (`CodeDeadPeer`).
- Breaking: `New` returns `(*Node, error)`: it refuses neither `Dial`
  nor `PacketConn`, a `PacketConn` without `TLS`, `Connect` without
  `Dial` (was an error from `Serve`), and an invalid `Liveness`, `Window`
  or `MaxSessions`. Migration: `n, err := quiclink.New(cfg)`.
- `*Session` implements `peerlink.Session`; the Example is
  self-contained. Requires link v0.8.0.

## peerlink/grpclink v0.2.0 (2026-10-14)

- Breaking: `New` returns `(*Node, error)`: it refuses neither `Dial` nor
  `Listener` and an invalid `Liveness`. Migration:
  `n, err := grpclink.New(cfg)`.
- `Example_grpcOverLink`: a gRPC service served over a link
  (`peerlink.Listener`), the other way round from this package. Requires
  link v0.8.0.

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
