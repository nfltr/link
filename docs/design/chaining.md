# Chaining links (design, 2026-10-16)

Status: design, decided; not implemented. Target: link v0.10.0,
peerlink/quiclink v0.4.0.

A link connects two points. A longer path is links joined end to end:
`origin -> hop 1 -> hop 2 -> ... -> far end`, each arrow its own
connection (its own TLS, liveness and credit). Today the only user that
does this, nfltr's relay, splices sessions itself, three times over
(client data connection -> replica link -> agent link -> target), and it
has paid for it:

- **Half closes lost in the splice.** relay-server v0.19.1 fixed conn
  wrappers that hid `CloseWrite`; the relay keeps `passCloseWrite`,
  `countedSession`, `ctxSession` and the client's `copyThenCloseWrite`
  so a half close survives each wrapper and copy loop. Two splice loops
  (`bridgeSession` in `agent_link.go`, `bridgeToLink` in
  `peer_link_tcp.go`) and a third on the agent each restate the same
  rules (half close on a clean end, reset both ways on an error).
- **No way to say no.** An open is fire-and-forget: link has no answer
  and a reset carries no reason. relay-server v0.21.0 added *session
  answers* (`dataplane.WriteAnswer`/`ReadAnswer`, `pb.TCPData.answer`,
  `peerTCPOpen.n`): a 2-byte length and a protobuf as the first session
  bytes, which every hop must know to pass through as data, and which
  cannot say why a session was cut after it opened.

This document makes chaining native to link: one splice primitive with
the whole session semantics, a forwarding hop that leaves routing to the
caller, and three wire frames, behind a feature bit, that carry the
answer, a typed abort and the path's hop count back and forth.

## 1. Decisions in one table

| # | question | decision | rejected |
|---|---|---|---|
| 1 | primitive | `link.Join(a, b *Session)` and `peerlink.Join(a, b net.Conn)` splice two sessions; `link.Forward(route, cfg)` / `peerlink.Forward(route, cfg)` return an accept function that opens onward through a caller's `Route` and joins | a `Relay`/`Router` type that owns a routing table (link would decide routing) |
| 2 | answers and typed aborts on the wire | **feature bit 1 (`FeatureChain`) in the v2 hello**, three frames: ACCEPT (8), ABORT (9), CHAIN-OPEN (10) | wire v3; in-band answer bytes (today); a reason in CLOSE code 2 |
| 3 | loops | hop count in every CHAIN-OPEN, refused at `ForwardConfig.MaxHops` | per-hop in-flight trace sets |
| 4 | tracing | a 16-byte trace id chosen by the origin, carried unchanged; one `Observer.Hop` event | a path of hop names (grows with every hop, leaks topology) |
| 5 | end-to-end security | none in link: hops are transports; TLS inside the session (connect phase 2's pins) | link-level onion or E2E encryption |
| 6 | versions | link v0.10.0 (API additions, `Observer` gains `Hop`); wire stays version 2 | — |

## 2. The primitives

### Root package (`link`)

```
// FeatureChain: hello/reply bit 1. The side reads ACCEPT, ABORT and
// CHAIN-OPEN, answers every open, and opens with CHAIN-OPEN.

type Trace [16]byte

// Chain is where an open is on its path.
type Chain struct {
	Trace Trace // the origin's; zero on OpenChain: link picks one (crypto/rand)
	Hops  uint8 // joins the open crossed: 0 from the origin
}

func ContextWithChain(ctx context.Context, c Chain) context.Context
func ChainFromContext(ctx context.Context) (Chain, bool)

// Abort is a session's typed end: a refusal (before its answer) or a
// reset (after). errors.Is(a, ErrReset) for resets, ErrOpenRefused for
// refusals.
type Abort struct {
	Refused bool
	Code    uint32 // link's below CodeApp, the application's from CodeApp
	Hops    uint8  // joins the abort crossed: 0 = this side's peer itself
	Msg     string // at most 1024 bytes of UTF-8
}
func (a *Abort) Error() string
func (a *Abort) Is(target error) bool

var ErrOpenRefused = errors.New("link: session refused")
var ErrNoAnswers = errors.New("link: the peer does not answer opens (no FeatureChain)")

const (
	CodeUnspecified   uint32 = 0
	CodeRefused       uint32 = 1 // not taking sessions: no accept function
	CodeDraining      uint32 = 2 // GOAWAY or Drain: open elsewhere
	CodeHopLimit      uint32 = 3 // MaxHops reached: a loop, or a path too long
	CodeNoRoute       uint32 = 4 // the Route failed with an error that is no *Abort
	CodeConnLost      uint32 = 5 // a hop's connection ended (dead peer, network)
	CodeTimeout       uint32 = 6 // no answer within ForwardConfig.AnswerTimeout, or a write deadline
	CodeDrainDeadline uint32 = 7 // a draining hop's deadline cut the session
	CodeApp           uint32 = 1024
)

// Opener side.
func (m *Mux) OpenChain(header []byte, c Chain) (*Session, error)
func (s *Session) Answered(ctx context.Context) error // nil: accepted; *Abort; ErrNoAnswers
func (s *Session) Answers() bool                       // the connection negotiated FeatureChain
func (s *Session) Chain() Chain
func (s *Session) PathLen() uint8                      // from the ACCEPT: hops to the far end

// Accepter side. The first Accept, Write, WriteBuffers, CloseWrite or
// Close answers "accepted"; Refuse before that answers no.
func (s *Session) Accept() error
func (s *Session) Refuse(code uint32, msg string) error

// Either side, any time: Reset with a reason.
func (s *Session) Abort(code uint32, msg string) error

// Join splices a and b until both directions ended; see section 3.
func Join(a, b *Session) error

// Route opens the session in continues on (a Mux, Pool or anything
// else), or returns why not (*Abort: its Code and Msg; any other error:
// CodeNoRoute). ctx carries in's Chain with Hops+1 (Pool.Open reads it),
// and ends when in ends or AnswerTimeout passes. A Route may wait for
// the onward answer itself (Answered) to try another next hop.
type Route func(ctx context.Context, in *Session) (*Session, error)

type ForwardConfig struct {
	MaxHops       uint8         // 0: 8
	AnswerTimeout time.Duration // 0: wait while in is open
}

// Forward returns an accept function for Mux.Serve: refuse at MaxHops,
// call route, wait for the onward answer, pass it back, Join.
func Forward(route Route, cfg ForwardConfig) func(header []byte, s *Session)
```

`Pool.Open(ctx, header)` reads `ChainFromContext(ctx)`; nothing else in
the existing API changes.

### peerlink

`peerlink.Link` and `peerlink.Session` are frozen (README, "Status and
compatibility"), so chaining arrives as an optional interface and two
functions; the Chain rides in `Open`'s ctx, which needs no new method:

```
// ChainSession is a Session on a Link that answers opens (Node over a
// FeatureChain connection, quiclink v0.4.0). Assert it; a Session
// without it is spliced like a net.Conn.
type ChainSession interface {
	Session
	Accept() error
	Refuse(code uint32, msg string) error
	Abort(code uint32, msg string) error
	Answered(ctx context.Context) error
	Chain() link.Chain
}

// Join splices any two conns: *link.Session pairs go through link.Join;
// otherwise it copies both ways (Pump and WriteBuffers when present),
// passes CloseWrite, and turns an error into Abort (ChainSession), Reset,
// or for a *net.TCPConn an RST (SetLinger(0)).
func Join(a, b net.Conn) error

type Route func(ctx context.Context, peer string, header []byte, in Session) (Session, error)
func Forward(route Route, cfg link.ForwardConfig) func(peer string, header []byte, s Session)
```

`peerlink.Forward(...)` is a `Serve` accept function as it stands, so a
forwarding hop is `node.Serve(peerlink.Forward(myRoute, cfg))`.

### Why these and not others

- **Provide, don't decide.** `Route` opens the onward session itself, on
  whatever it chooses: Pool, Node, quiclink, another account's Link, a
  plain TCP dial for the last leg. Retries, the next hop after a
  refusal, authorization and header rewriting are all the Route's.
  Rejected: `Forward(table map[string]Link)` or a resolver that returns a
  peer name, both of which make link pick connections and retry policy.
- **Join is public, not only Forward's internals.** The far end of a
  path (the agent joining a session to its TCP target) and a client
  sidecar (a local TCP conn to a session) need the splice without a
  Route.
- **Two Joins.** `link.Join` is the zero-goroutine, typed path for two
  native sessions; `peerlink.Join` covers every mix (TCP conn, quiclink
  stream, grpclink stream) and uses the first when it can. One
  `Join(a, b net.Conn)` in root was rejected: root would then carry
  TCP-RST and quiclink knowledge it does not need.

## 3. Semantics end to end

```
origin            hop 1 (Forward)          hop 2 (Forward)          far end (Join to target)
  |--CHAIN-OPEN t,h=0-->|                        |                         |
  |--DATA (no wait)---->| route(ctx,in): Open    |                         |
  |                     |--CHAIN-OPEN t,h=1----->| route: Open             |
  |                     |--DATA----------------->|--CHAIN-OPEN t,h=2------>| dials target
  |                     |                        |--DATA------------------>|
  |                     |                        |<--ACCEPT h=0------------|  (or ABORT refuse code,h=0)
  |                     |<--ACCEPT h=1-----------|  Join(in, out)          |
  |<--ACCEPT h=2--------|  Join(in, out)         |                         |
  |<========== DATA both ways; credit per link, never end to end ========>|
  |--CLOSE half-------->|--CLOSE half----------->|--CLOSE half------------>| target reads EOF
  |                     |                        X hop 2's link to the far end dies
  |                     |<--ABORT reset ConnLost h=0                       |
  |<--ABORT reset ConnLost h=1                   |                         |
```

**Bytes and order.** Each direction is the bytes written, in order,
intact, across any number of hops; Join never adds, drops or reorders
one. The origin may write before the answer (no extra round trip per
hop); a hop forwards those bytes as soon as its Route returned. Bytes
toward a refused session are discarded within the windows they used.

**Half close.** A direction's clean end (EOF after CloseWrite, or a CLOSE
code 1) becomes `CloseWrite` on the other side once every byte before it
was written there. Each direction ends on its own: the far end can keep
sending after the origin half-closed and vice versa (the relay's
`TestConnectAnswers` cases, now link's). A full CLOSE (code 0) from one
side becomes `Close` on the other after its tail. Join returns once both
directions ended.

**Aborts both ways.** An error on either side (Abort, Reset, the
connection's end, a write deadline, a failed write) ends the other side
with ABORT reset: the incoming `*Abort` with `Hops+1`, or for a local
cause a new Abort with `Hops: 0` and `CodeConnLost`, `CodeTimeout` or
`CodeDrainDeadline`. The origin reads `*Abort{Code, Hops}`: `Hops` says
which hop (0 its peer, k the k-th beyond it), `Code` why. A peer without
FeatureChain gets a CLOSE code 2 (no reason), as today.

**Answers.** The accepter of a CHAIN-OPEN answers exactly once: ACCEPT,
or ABORT refuse. Forward answers its inbound session with its onward
session's answer (`Hops+1`), or with its own refusal (`MaxHops`, the
Route's `*Abort` or `CodeNoRoute`, `AnswerTimeout`). So an origin's
`Answered` returns nil only once the far end accepted: no hop reports a
session open before the target is reached, which is what v0.21.0's
in-band answers bought, minus the in-band bytes. The ACCEPT's hop count
gives the origin the path's length (`PathLen`). An onward leg without
FeatureChain (`Answers() == false`) cannot answer; Forward then accepts
inbound when the Route returns, and the Route, which sees
`out.Answers()`, refuses instead when it wants answers end to end (the
relay's FEATURE_UNSUPPORTED).

**Backpressure.** Join takes a batch from one side's receive queue only
when it can write it to the other, and the queue returns credit as bytes
leave it; a hop's buffering is its own windows and nothing else. Per
session and direction, bytes in flight between origin and far end are at
most

    sum over the n links of W_i  +  (n - 1) x 256 KiB

(W_i the receiver's window on link i; 256 KiB the write batch a Join holds
between popping and writing). Defaults (1 MiB windows) over 3 links:
3.5 MiB. A stalled far reader stalls only its session: the origin's
Write waits for credit after those bytes, every other session on every
link keeps moving (per-session credit composes; there is no
connection-wide credit to exhaust). Memory per spliced session at one
hop is at most `W_in + W_out + 2 x 256 KiB`, and each link keeps its own
`MaxSessions x Window` bound.

**Deadlines.** Join sets none and pumps, so read deadlines do not apply
to it; a write deadline set on either session bounds Join's writes into
it and, passed, aborts both sides with `CodeTimeout`. The open is bounded
by `AnswerTimeout` and by the Route's ctx. Idle limits are the caller's
(it holds both sessions and may `Abort` them).

**Headers.** Forward hands the Route the inbound header and the Route
opens with whatever header it chooses (pass it, rewrite it, wrap it).
link's own path data (trace, hops) is fixed at 17 bytes outside the
header and never grows; each hop's header is bounded by that hop's
limit (64 KiB in a Mux, `quiclink.MaxHeader`).

**Liveness.** Each link keeps its own liveness; nothing end to end. When
hop k dies, the hops beside it see their session with it fail within
that link's dead-peer bound (`interval + timeout`; immediately on a
reset or close) and abort their other side. The origin reads
`*Abort{Code: CodeConnLost, Hops: k-2}`: the abort originates at hop
k-1, the hop before the dead one, and `Hops` counts the origin's peer
(hop 1) as 0. It arrives within that link's bound plus one trip back. The origin's own
connection is unaffected and its other sessions keep running.

**Drain at a middle hop.** A hop that drains (GOAWAY, `Mux.Drain`,
`Node.Drain`) refuses new opens with ABORT refuse `CodeDraining`; the
hop before it gets that as its onward answer and its Route may open
elsewhere (another connection of a Pool, another peer). Sessions already
joined through the draining hop run to their end; at its drain deadline
it aborts them both ways with `CodeDrainDeadline`. A hop that received
GOAWAY from its next hop opens new sessions elsewhere as today (Pool and
Node already skip connections going away).

## 4. The wire: feature bit 1

**Decision: a feature bit in version 2, not a version 3.** WIRE.md
already reserves bits 1-31 for exactly this ("feature bits add optional
frames without [a new version]"). Released nfltr agents speak link
version 2 to the hosted relay, and one external account runs them; a v3
would make the relay speak both versions anyway, while a bit is
negotiated per connection at no cost: a connection where either side
lacks it is byte-identical to v0.9.4 (`testdata/v2_session.golden`
unchanged proves it). With the bit on both sides:

| type | name | body | rules |
|---:|---|---|---|
| 8 | accept | sid, hops(1) | body exactly 2 + sid_len; sent once, by the side that received the open, before any data of its own on the session; a second accept, or data from the accepter before it, is `ErrFrame`; unknown sid: ignored |
| 9 | abort | sid, kind(1), code(4), hops(1), msg_len(2), msg | kind 0 refuse (accepter, before its accept) or 1 reset (either side, any time); msg UTF-8, at most 1024; body exactly 9 + sid_len + msg_len; ends the session as CLOSE code 2 does; unknown sid: ignored |
| 10 | chain-open | sid, hops(1), trace(16), header | header at most 65536; replaces OPEN (type 3) on the connection: a type 3 there is `ErrFrame`; same max_sessions and restart rules as OPEN |

Frames 8-10 from a peer to which this side did not announce the bit stay
unknown types (`ErrFrame`), as today. Credit is unchanged: a chain-open
starts it on both sides like an open, data before an accept uses it.

Rejected:

- **v3 wire.** As clean as the bit, but forces a second handshake path in
  the relay and old agents for nothing a bit cannot do.
- **In-band answers** (today's `dataplane` answers). Every hop must know
  which leading bytes are an answer and pass them as data; a reset can
  carry no reason (it carries no bytes); the opener must parse before
  using the stream, which makes TLS-inside-the-session and plain
  splicing need special cases; and each user reinvents it.
- **A reason in CLOSE code 2.** The CLOSE body is fixed-length today
  (decoders rely on it); the open answer would still be missing.

## 5. Loops and abuse

- **Hop limit.** `Forward` refuses a chain-open whose `Hops >= MaxHops`
  with `CodeHopLimit` (default 8; the wire bounds it at 255). A routing
  loop costs at most `MaxHops` sessions, then the origin reads the
  refusal with `Hops` pointing at the hop that stopped it.
- **No header growth by link.** Trace and hops are fixed size; hops is a
  counter, not a list. A Route that wraps headers is bounded by every
  hop's header limit (the next Open fails, the origin gets `CodeNoRoute`
  with the Route's message).
- **Bounded reasons.** `msg` at most 1024 bytes, cut by `Abort`/`Refuse`
  at a UTF-8 boundary; decoders refuse more.
- **Cost per session** at a hop is two sessions, each counted against its
  own connection's `MaxSessions`, and the memory bound of section 3.
- Rejected: per-hop sets of in-flight traces to detect loops exactly.
  State on every hop, wrong on deliberate hairpins, and the hop limit
  already bounds the cost.

## 6. Observability

One event, so `Observer` grows by one method (allowed in a minor
release; implementations embed `NopObserver`):

```
type HopKind uint8 // HopOpened, HopAnswered, HopEnded

type HopEvent struct {
	Kind    HopKind
	Chain   Chain         // trace and this hop's Hops
	Latency time.Duration // HopAnswered: Route called -> onward answer
	Err     error         // HopAnswered: the refusal; HopEnded: the abort, nil on a clean end
	In, Out int64         // HopEnded: bytes origin->far and far->origin
}
Hop(HopEvent)
```

Forward emits all three; Join alone emits `HopEnded` (with the sessions'
Chain). Per-hop latency: each hop's `Latency` is the rest of the path
seen from it, so hop k's own share is `Latency_k - Latency_{k+1}`, joined
by trace id across hops' logs; the origin's total is `OpenChain` ->
`Answered`. The trace id is for correlation only: not a secret, not an
authenticator. Existing `SessionOpen`/`SessionClose` keep counting
sessions per connection.

## 7. Security

Hops are transports. Each link keeps its own authentication (the
`Authenticator`, `CertAuth`, mTLS in quiclink) and its own TLS; a hop
knows its neighbours' identities, never the origin's unless the header
says so (signed by the application if it matters).

A middle hop sees: the header, trace, hop count, sizes and timing, and
the bytes unless the ends encrypt. It can: refuse, delay, reset with any
code, forge an ACCEPT or ABORT, rewrite the header, and alter bytes.
ACCEPT and ABORT are unauthenticated: a forged refusal is a denial of
service, a forged accept is caught by end-to-end TLS.

End-to-end confidentiality is layered on top, unchanged: TLS 1.3 inside
the session with a pinned certificate (connect phase 2). Join is byte
transparent, so it works through any number of hops, and since answers
are frames rather than leading bytes, the client starts TLS on the first
byte instead of after parsing an answer. Rejected: encryption in link
(onion layers or a link-level E2E handshake): it duplicates TLS, needs
keys link does not have, and the answer and abort frames would still be
visible to hops.

## 8. Performance

- **Zero goroutines per idle spliced session.** `link.Join` uses
  `Session.Pump` both ways: a goroutine runs only while bytes are
  queued. Today `bridgeToLink` keeps one blocked in `io.CopyBuffer` per
  visitor session.
- **One copy per hop, and it is the coalescer.** A batch of received
  slices goes to the other session with one `WriteBuffers` (one flush);
  the copy into the connection's 256 KiB write buffer is what puts many
  sessions' frames into one TLS write. No intermediate `io.Copy` buffer.
  Handing received slices to the writer by reference (writev without the
  copy) was considered and deferred: it loses cross-session coalescing
  into one TLS record, and memcpy is not the bottleneck next to TLS.
- **Benchmarks** (root, in process, TCP loopback; results in
  `compare/README.md`):
  - `BenchmarkChainThroughput/hops={0,1,2,4}`: MB/s, 8 MiB per session,
    1 and 32 sessions;
  - `BenchmarkChainLatency/hops={0,1,2,4}`: 64 B echo round trip;
  - `BenchmarkChainOpen/hops={0,1,2,4}`: OpenChain -> Answered;
  - `TestChainIdleCost`: heap and goroutines per idle joined session
    (goroutines asserted 0), and the in-flight bound of section 3 under a
    stuck far reader.
  The first run is the baseline; later runs gate on regression, as the
  relay's `BenchmarkTCPTunnelThroughput` does.

## 9. Migration

Order: link v0.10.0 and quiclink v0.4.0, then relay proto/client, then
relay-server, then nfltr; each step tagged with CI green.

1. **link.** Root API and frames (sections 2, 4); `Mux` refuses opens
   with ABORT (`CodeRefused`, `CodeDraining`) on FeatureChain
   connections; `peerlink.Node` announces the bit and returns
   `ChainSession`s. **quiclink v0.4.0** implements `ChainSession`: the
   answer is a reverse preamble (1 byte accept, or refuse with code,
   hops and message, then FIN); a reset packs kind, hops and code into
   the 62-bit `StreamErrorCode` (no message: QUIC carries none). grpclink
   requires v0.10.0 and does not answer (`peerlink.Join` treats it as a
   plain conn).
2. **relay proto and client.** `dataplane.Session` (a `link.Mux` with
   `pb.TCPData` headers) exposes the root methods. The agent's tcp
   sessions answer with `Accept`/`Refuse(CodeApp + ErrorReason, msg)`
   and splice with `peerlink.Join`; `copyThenCloseWrite` and the
   agent's in-band `WriteAnswer` go (kept only on gRPC frame streams,
   which are not link).
3. **relay-server.** `serveConnectSession` becomes a `Route`: its checks
   return `*link.Abort{Code: CodeApp + reason}`; it opens onward on the
   agent link or the replica link with the Chain in ctx; `Forward` does
   the rest. The replica's `serveLinkTCP` becomes `node.Serve(Forward)`.
   Deleted: `bridgeSession`, `bridgeToLink`, the relay's own
   `WriteAnswer` calls, `peerTCPOpen.n`, `countedSession`/`ctxSession`
   (bytes come from `HopEnded`). `passCloseWrite` stays: it is about the
   conns under a link, not sessions.
4. **nfltr connect sidecar.** `dial`, mappings, `tcp-connect` open with
   `OpenChain` and wait `Answered` instead of `ReadAnswer`; TLS with the
   pin starts on byte one; local conn to session is `peerlink.Join`.
5. **In-band answers removed** (`AnswerHeader`, `WriteAnswer`,
   `ReadAnswer`, `pb.TCPData.answer`, `FuzzReadAnswer`) once no released
   client or agent needs them: the relay raises
   `security.min_client_generation` to the generation that speaks
   FeatureChain. Until then the relay answers in-band only on legs whose
   peer did not announce the bit.

**API vs test-only.** API: everything in section 2 and `Observer.Hop`.
peerlinktest (public, may add checks in minor releases) gains the chain
cases of section 10 and a `ChainPair` builder over `Proxy`. Test-only:
the in-process N-hop builder in root `_test.go` files.

## 10. Test plan

- **Property tests over N-hop chains** (root, `-race`): for random
  `n` in 1..5 and one random fault per run, assert bytes intact and in
  order both ways (SHA-256), the origin's error (`*Abort` with the
  expected `Code` and `Hops`), the bound it arrived within, and no
  leak (every hop's `Mux.Sessions() == 0`, goroutines back to baseline).
  Faults: kill hop k's process (close) and black-hole link k (silent,
  via `peerlinktest.Proxy`); stall the far reader (origin blocks at the
  section 3 bound, other sessions keep moving); half-close each
  direction first; refuse at hop k (Route `*Abort`, `MaxHops`, Drain);
  reset at hop k with a code; drain hop k with sessions running
  (finish intact, new opens refused `CodeDraining`, at the deadline
  `CodeDrainDeadline`); a two-hop routing loop (`CodeHopLimit`).
- **Mixed legs:** a chain with one leg lacking FeatureChain (v0.9.4 peer,
  grpclink): today's semantics on that leg, `Answers() == false`,
  `Answered` returns `ErrNoAnswers` on it.
- **Wire:** vectors for frames 8-10, valid and invalid (lengths, msg over
  1024, invalid UTF-8, header over 65536, a second accept, accepter data
  before accept, type 3 on a FeatureChain connection, frames 8-10
  without the bit); `testdata/v2_chain_session.golden` recorded from a
  2-hop session with a refusal and a mid-session abort;
  `testdata/v2_session.golden` byte-identical. Fuzz: `FuzzRecvV2` and
  `FuzzMuxServeV2` seeded with the new frames on a FeatureChain
  connection; a new `FuzzForward` (random frames into a Forward hop
  whose Route opens to an echo) checks the invariants above; hours in
  FUZZING.md before the tag.
- **Cross-version:** v0.10.0 against v0.9.4 both directions
  (`TestChainAgainstV094`, using the module cache); in nfltr `make
  test-relay-cross-version` and `make smoke-test-client-generation` with
  an old agent.
- **Conformance:** peerlinktest `Chain*` cases (accept, refuse with
  reason, abort both ways, half close through 2 hops, drain at the middle)
  run by Node, quiclink, and through them relay-server's
  `TestPeerLinkConformance` and agent-link conformance; the relay's
  `TestConnectAnswers` and the sidecar's conformance suite keep passing
  unchanged in behaviour.
- **Soak:** `soak/` gains a 3-hop mode (origin, two Forward hops, echo)
  with middle-hop kills, drains and black holes in rotation; integrity,
  leak and in-flight checks per window; hours recorded in SOAK.md.

## 11. Versioning

- **link v0.10.0** (minor): new API (section 2), `Observer.Hop`
  (implementations that embed `NopObserver` compile unchanged), Mux
  refusals typed on FeatureChain connections. No removal.
- **peerlink/quiclink v0.4.0**: `ChainSession`; the answer travels as a
  reverse preamble, negotiated per connection by ALPN (a v0.4.0 Node
  offers `peerlink-chain` before `peerlink`), so a v0.3.x peer
  interoperates without answers. grpclink: a patch requiring link v0.10.0.
- **Wire:** version 2 unchanged. Bit 1 defined; frames 8-10 only between
  peers that both announced it; everything else is v0.9.4's bytes.
- **v1.0:** the peerlink contract is unchanged (an optional interface,
  ctx values), so chaining does not restart the "contract unchanged for
  two minor releases" clock in the README's maturity table.
