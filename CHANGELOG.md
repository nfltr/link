# Changelog

Releases of `github.com/nfltr/link` (tags `vX.Y.Z`) and of the modules in
it (`peerlink/grpclink/vX.Y.Z`, `peerlink/quiclink/vX.Y.Z`). The wire
format is [WIRE.md](WIRE.md); every release since v0.2.0 speaks the same
version 2 unless an entry says otherwise.

## v0.10.2 (2026-10-16)

- Bug fix: a caller of `Conn` (not `Mux`) on a `FeatureChain` connection
  had no way to answer a session the peer opened, so its first data
  frame on it was `ErrFrame` at the opener, which ended the connection:
  since v0.10.0 every side announces `FeatureChain`, which broke
  frame protocols built on `Conn` between two v0.10 sides (nfltr's relay
  and agents' data connections). The first data or close (`SendData`,
  `SendClose`, `SendReset`) a side sends on a session the peer opened and
  nothing answered now follows an ACCEPT link sends itself, as a
  `Session`'s first `Write` answers; an explicit answer (a `Mux`
  session's `Accept`, `Refuse`) is never repeated. No wire change.
  Regression test: `TestConnSendsAnswerPeerOpens` (`ErrFrame` at the
  opener before the fix).

## peerlink/grpclink v0.3.1 (2026-10-16)

- Requires link v0.10.0. No change of its own: its sessions do not answer
  opens (no `peerlink.ChainSession`); `peerlink.Join` and
  `peerlink.Forward` splice them as plain conns, and the conformance
  suite's chain cases pass on them.

## peerlink/quiclink v0.4.0 (2026-10-16)

- Chaining (`peerlink.ChainSession`), negotiated per connection by ALPN:
  a v0.4.0 listener offers `ALPNChain` (`peerlink-chain`) before `ALPN`
  (`peerlink`) when its TLS names no protocols; a dialler offers
  `NextProtos: []string{quiclink.ALPNChain, quiclink.ALPN}`. On such a
  connection an open carries its `link.Chain` (hops and trace, before
  the header preamble), the accepter answers with a reverse preamble
  (accept with the path's length, or a refusal with code, hops and
  message, then FIN), and a reset carries its code and hops in the stream
  error code (QUIC carries no message). A connection that negotiated
  `ALPN` (a v0.3 peer, or a dialler offering only it) is unchanged:
  `Answers()` is false.
- Resets deliver the bytes written before them where both ends support
  RESET_STREAM_AT (`quic.Config.EnableStreamResetPartialDelivery`, now on
  in a Node's QUIC config).
- A write after the peer's `Close` returns an error matching
  `io.ErrClosedPipe` (still wrapping the `*quic.StreamError`).
- `Open` reads the `link.Chain` its ctx carries (`link.ContextWithChain`).
- New: `FuzzAnswer` (the answer's decoder); tests: paths of 2 to 5 links
  over QUIC and over QUIC and TCP in turn, with refusals at each hop, a
  killed middle hop and aborts (`TestChainQUIC*`), the conformance suite
  with and without `ALPNChain`.
- API: `ALPNChain`; `Session` gains the `peerlink.ChainSession` methods
  (`Answers`, `Accept`, `AcceptPath`, `Refuse`, `Abort`, `AbortWith`,
  `Answered`, `Chain`, `PathLen`) and its own `Read` and `Write`. The
  `Session` struct is no longer comparable (apidiff's one incompatible
  change: sessions are used as pointers, never compared by value).
- Requires link v0.10.0.

## v0.10.0 (2026-10-16)

Chaining ([docs/design/chaining.md](docs/design/chaining.md), README
"Chaining"): sessions joined end to end, with answers and typed aborts.

- Wire: feature bit 1, `FeatureChain`, in version 2 (WIRE.md section
  12): frames accept (8), abort (9: refuse or reset, code, hops,
  message of at most 1024 bytes) and chain-open (10: hops, a 16-byte
  trace, the header). Every v0.10.0 side announces it; a connection
  where either side lacks it is byte for byte v0.9.4's
  (`testdata/v2_session.golden` unchanged). New:
  `testdata/v2_chain_session.golden`, 25 wire vectors.
- New API: `Join(a, b *Session)`; `Forward(Route, ForwardConfig)`,
  `Route`, `ForwardConfig` (`MaxHops`, `AnswerTimeout`, `Observer`),
  `DefaultMaxHops`; `Mux.OpenChain`; `Session.Answered`, `Answers`,
  `Chain`, `PathLen`, `Accept`, `AcceptPath`, `Refuse`, `Abort`,
  `AbortWith`; `Abort` (the error type), `ErrOpenRefused`,
  `ErrNoAnswers`, the `Code*` constants, `MaxAbortMsg`; `Trace`, `Chain`,
  `ContextWithChain`, `ChainFromContext`; `FeatureChain`; `HopEvent`,
  `HopKind` (`HopOpened`, `HopAnswered`, `HopEnded`); `Frame.Chain`,
  `Frame.Accept`, `Frame.Abort`. `Observer` gains `Hop(HopEvent)`
  (implementations embed `NopObserver` and compile unchanged).
- Behaviour on a `FeatureChain` connection: every open is a chain-open
  (`Mux.Open` picks a random trace); the accepter answers before its
  first frame (`Accept`, or its first `Write`, `WriteBuffers`,
  `CloseWrite` or `Close`): an accepter that only reads and never
  answers leaves the opener's `Answered` waiting (call `Accept`). A `Mux`
  refuses an open it does not take with an abort refuse
  (`CodeRefused`; `CodeDraining` after a GOAWAY), which the opener reads
  as an `*Abort` matching both `ErrOpenRefused` and `ErrReset` (v0.9
  callers testing `ErrReset` keep working). A drain's deadline aborts
  the sessions still open with `CodeDrainDeadline` before it closes the
  connection. `Pool.Open` and `peerlink.Node.Open` open with the
  `Chain` their ctx carries.
- `Session.WriteTo` and `Pump` into a writer with `WriteBuffers` (a
  `Session`) write each batch with one flush.
- peerlink: `ChainSession` (an optional interface: `link.Session` and a
  Node's sessions implement it), `Join(a, b net.Conn)` (any two conns:
  `link.Join` for two native sessions, else copies with `Pump` where it
  can, half closes passed on, an error passed on as `AbortWith`, `Reset`
  or a TCP RST), `Route`, `Forward`. `Node` refuses sessions peers open
  on its dialled connections while draining with `CodeDraining` (before
  `Serve`: `CodeRefused`), and passes its `Drain` ctx's deadline to its
  accepted connections, which abort their sessions with
  `CodeDrainDeadline` at it.
- peerlinktest: the chain cases (`ChainAccept`, `ChainRefuse`,
  `ChainAbortFar`, `ChainAbortOrigin`, `ChainHalfClose`,
  `ChainDrainMiddle`) run by `Run` on a path of two Pairs and a
  `peerlink.Forward` hop (`NewChainPath`, `ChainPath`); typed outcomes
  are checked where the sessions answer.
- Tests: N-hop paths (1 to 5 links) over TCP, TLS and both with a
  killed hop, a black-holed link, a stalled far reader (the in-flight
  bound), half closes both ways, refusals at each hop (Route, MaxHops
  loop, drain), resets with codes, a drain at a middle hop, legs without
  `FeatureChain`, a random property test (`TestChain*`); against a
  v0.9.4 peer built from the module cache, both directions
  (`TestChainAgainstV094`); fuzz: `FuzzRecvV2` and `FuzzMuxServeV2`
  also on `FeatureChain` connections, new `FuzzForward`; benchmarks
  `BenchmarkChainThroughput`, `BenchmarkChainLatency`,
  `BenchmarkChainOpen`, `TestChainIdleCost` (README "Chaining").
- soak: the `chain-3hop` config (origin, two Forward hops, an echo;
  cut, in-flight bound, then a hop killed, drained or black-holed in
  rotation) and `cmd/linkhop`, the same roles as separate processes for
  a run across machines or pods (SOAK.md, "Chains").

## peerlink/quiclink v0.3.5 (2026-10-15)

- Requires link v0.9.4. No change of its own.

## v0.9.4 (2026-10-15)

- Bug fix: `Observer.SessionClose` now comes for each session still open
  when its connection ends (with ConnDown's error, before `ConnDown`); it
  came only for a session's own end, so an observer's open-session count
  (`SessionOpen` minus `SessionClose`) kept the sessions of every
  connection that died. Regression test:
  `TestObserverSessionsClosedWithConn`.
- No API or wire change.

## peerlink/quiclink v0.3.4 (2026-10-15)

- Requires link v0.9.3. No change of its own.

## v0.9.3 (2026-10-15)

- Bug fix: `Pool.Open`, when the connection it picked fails the open
  because its writes failed (the peer reset it and its reader has yet to
  see the end), picks again: another connection, or a new one, since a
  connection whose writes failed takes no session and does not count
  against `Max`. It returned the write error (the same defect as v0.9.2's
  in `peerlink.Node.Open`). Regression test:
  `TestPoolOpenPassesABrokenConnection`.
- No API or wire change.

## peerlink/quiclink v0.3.3 (2026-10-15)

- Requires link v0.9.2. No change of its own.

## v0.9.2 (2026-10-15)

- Bug fix: `peerlink.Node.Open` without `Config.Dial`, when the
  connection it picked fails the open because its writes failed (the
  peer reset it and its reader has yet to see the end), passes on to
  another connection, or waits for the peer's next one until ctx ends,
  as for a connection going away. It returned the write error: a peer
  that closed its connection and dialled again got the open on the old
  one (relay client `TestLazyAgentLink`, 29 runs in 40 on Linux with 2
  CPUs). Regression test: `TestOpenServedPassesABrokenConnection`.
- No API or wire change.

## peerlink/quiclink v0.3.2 (2026-10-15)

- Requires link v0.9.1 (its v0.3.1 fix is unchanged), so a build that
  selects link v0.9.1 has no module requiring an older link.

## peerlink/quiclink v0.3.1 (2026-10-15)

- Bug fix: `Node.Open` on a Node without `Config.Dial`, toward a peer
  whose every connection is going away (the peer drains: it sent GOAWAY
  on the connections it dialled in), fails at once with an error that is
  `link.ErrGoAway`. It waited for another connection until its ctx ended,
  and a session the caller had open toward the draining peer could reach
  the peer's drain deadline meanwhile (peerlinktest `DrainAccepter`, about
  1 run in 130 under load). Regression test: `TestOpenTowardDrainingPeer`.
- No API or wire change.

## v0.9.1 (2026-10-15)

- Bug fix: `Pool.Close` closes a connection that carries no session once
  the frames already written on it went out (`CloseWrite`, then the
  peer's close, as a drained connection closes); the others still close
  at once. It closed every connection at once, cutting the tails and
  closes of sessions that had ended: their `Write` and `Close` had
  returned, the bytes still in the link's buffer. `peerlink.Node.Drain`
  closes its pools so once its sessions ended: a peer read the last
  sessions' bytes cut short (peerlinktest `DrainAccepter` on a Node that
  dials, about 1 run in 60 under load). Regression test:
  `TestPoolCloseKeepsEndedSessionsTails`.
- Docs: `Client` and `Server` state that nc should have `CloseWrite`
  (a wrapper of a `*net.TCPConn` or `*tls.Conn` passes it on): without
  it a graceful close closes nc, and a TCP connection closed with bytes
  still arriving resets, losing the bytes in flight to the peer.
- No API or wire change.

## peerlink/quiclink v0.3.0 (2026-10-15)

- Requires link v0.9.0. `Node.Open` returns `peerlink.Session`, and
  `Node.Serve`'s accept takes one (link v0.9.0's peerlink contract).
  Migration: as for link v0.9.0, below.
- No wire change.

## peerlink/grpclink v0.3.0 (2026-10-15)

- Requires link v0.9.0. `Node.Open` returns `peerlink.Session`, and
  `Node.Serve`'s accept takes one (link v0.9.0's peerlink contract).
  Migration: as for link v0.9.0, below.
- No wire change.

## v0.9.0 (2026-10-15)

The API cleanup ahead of v1.0, from a review against yamux, smux,
quic-go, libp2p, x/net/http2 and net. No wire change: the test vectors
and the golden recording are byte for byte those of v0.8.1.

Breaking changes:

- peerlink: `Link.Open` returns `peerlink.Session`, and `Serve`'s accept
  and `Listener.Handle` take one, so `CloseWrite` and `Reset` need no
  type assertion; a `Session` is still a `net.Conn`.
- `Mux.Open(header []byte)`: the header itself; no caller used the
  session id the callback form passed.
- `Mux.GoAway(deadline time.Time)` is `Mux.Drain(ctx) error`, as
  `Pool.Drain` and `Node.Drain`: it blocks until the connection closed,
  and at ctx's end closes it and returns ctx's error (ctx's deadline goes
  in the GOAWAY).
- `Conn.Credit() *Credit` in place of the embedded `*Credit`: `Conn`'s
  method set no longer carries every `Credit` method (and `Credit.Close`
  no longer hides behind `Conn.Close`).
- `ErrV1` deleted: a version-1 hello reads as `ErrVersion` (not a
  `*VersionError`, which is a hello of a later version).
- `Observer` has an unexported method, which `NopObserver` provides:
  implementations embed `NopObserver`, and `Observer` may gain events in
  minor releases.
- `bytequeue` moved to `internal/bytequeue`: it is the receive path's
  internals.

Fixed and added:

- `Session.SetWriteDeadline` bounds writes (it was a no-op, and
  `SetDeadline` set only the read deadline): a Write returns
  `os.ErrDeadlineExceeded`, with the bytes it sent, once the deadline
  passed while it waited for credit or for room in the connection's
  write buffer; while a deadline is set, the network write runs from a
  goroutine, so no Write waits on the network itself.
- `ErrDeadPeer` is a `net.Error` with `Timeout()` true; `errors.Is` still
  matches it.
- The stability policy (README, package doc): `Observer` may gain
  methods, the interfaces callers call never change, `peerlinktest` may
  add checks, the submodules stay v0 until their transport is v1.
- Godoc no longer names the unexported `FeatureReset` and
  `StatusVersion`; `NewMux` says to run `Serve`.

### Migration

```
// peerlink accept: the session is a peerlink.Session
before: node.Serve(func(peer string, h []byte, s net.Conn) { s.(peerlink.Session).CloseWrite() })
after:  node.Serve(func(peer string, h []byte, s peerlink.Session) { s.CloseWrite() })
// Link.Open: (peerlink.Session, error); a net.Conn variable still takes it
before: var s net.Conn; s, err = node.Open(ctx, peer, h); s.(peerlink.Session).Reset()
after:  s, err := node.Open(ctx, peer, h); s.Reset()
// Listener.Handle has the accept's signature: no change where it is passed to Serve
// Mux.Open
before: s, err := m.Open(func(string) ([]byte, error) { return header, nil })
after:  s, err := m.Open(header)
// Mux.GoAway
before: err := m.GoAway(time.Now().Add(30 * time.Second))
after:  ctx, cancel := context.WithTimeout(ctx, 30*time.Second); defer cancel(); err := m.Drain(ctx) // blocks: go it for the old behaviour
// Conn's credit
before: c.Received(sid, n); cr := c.Credit
after:  c.Credit().Received(sid, n); cr := c.Credit()
// ErrV1
before: errors.Is(err, link.ErrV1)
after:  var ve *link.VersionError; errors.Is(err, link.ErrVersion) && !errors.As(err, &ve)
// Observer
before: type obs struct{}; func (obs) ConnUp(link.Identity) {} // ... all seven
after:  type obs struct{ link.NopObserver }; func (obs) ConnUp(link.Identity) {}
// bytequeue
before: import "github.com/nfltr/link/bytequeue"
after:  keep a copy of the package (it is internal now)
```

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
