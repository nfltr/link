# link v1.0 API review

Lane LINKAPI, 2026-11-20, at link v0.12.18 (`83d3c0e`), quiclink
v0.5.25. A proposal for the lead: **no exported name changed in this
lane**. Each change below is a decision to take before the v1.0.0 tag;
the inventory at the end lists every exported identifier of the modules
with who uses it and the verdict.

**Outcome (lane LINKV013, link v0.13.0):** every item below is done as
proposed (CHANGELOG.md "v0.13.0"), except item 6's `CloseWriteSync`,
which stays exported: the relay SDK (`client/nfnet`, `client/tlsend`,
`client/agent`) and relay-core's metrics call it through an interface,
so "only peerlink calls it" was wrong, and hiding it would have ended
their clean closes early without a compile error. The inventory below is
as of v0.12.18.

## Round 2 (lane LINKAPI, 2026-11-20, on v0.13.0 + v0.13.1 and quiclink v0.7.0 unreleased)

A second pass over every exported identifier of `link`, `peerlink`,
`peerlinktest`, `pathswitch` and `peerlink/quiclink` (383 names) at link
`cb6053e` (land/QUICGO3: ENDACKTRUTH, ENDACKQUIC, LINKFAULTS), with the
uses resolved by type, as above, in relay `cb01697`, relay-core
`32a2c77`, relay-server `6cf06e3f`, nfltr `57786a6`, send `7b6c86a`
and pipe `e39832e` at their mains (send and pipe import link only
through the relay SDK), plus quiclink's own uses of link (a module of
its own, so they count). Questions per name: does a consumer need it,
is it named well, are its failures and ownership documented, is it safe
to misuse (zero value, second Close, concurrent use)?

Findings: 1 breaking API change (applied), 27 doc references to names
that no longer exist (fixed, and guarded), 6 wrong or missing failure
docs (fixed), 6 safety statements missing (added, one guard), 0 wire
changes. No consumer changes: relay (`go vet ./...` of its modules,
client/agent's direct and datagram suite) and relay-core (`go vet
./...`, `go test ./core/session/ ./frontend/... ./platform/metrics/`)
pass against the branch unchanged; relay-server, nfltr, send and pipe
reach quiclink only through the relay SDK (no `*quiclink.Session` in
their own code).

| identifier | problem | decision | consumers affected |
|---|---|---|---|
| `quiclink.Session` (embeds `*quic.Stream`) | the embedding makes the quic-go fork's whole stream API quiclink's: the `Stream` field and `CancelRead`, `CancelWrite`, `Context`, `Peek`, `SetPriority`, `SetReceiveFinalSizeCallback`, `SetReliableBoundary`, `SetSendWindowCallback`, `StreamID`, `TryWriteAll`, `WriteWithLimit`. A quic-go upgrade changes quiclink's API, and `CancelWrite`/`Stream.Close` bypass the answer and end acknowledgement the session keeps | unexported (field `stream`); `SetDeadline`, `SetReadDeadline`, `SetWriteDeadline` are the session's own methods; api/quiclink-v0.6.0.txt; CHANGELOG quiclink v0.7.0 | none: relay `client/agent` uses `*quiclink.Session` for `SendDatagram`/`OnDatagram` and as a net.Conn; its direct suite passes against the branch |
| package docs of all five | 23 doc comments named what v0.13.0 removed or unexported: `Pump`, `Sync`, `WriteBuffers`/`writeBuffers`, `Flush`, `Conn.SendMsg`, `Mux.OpenChain`, `Mux.leave`, `link.Join`, `FeatureChain`, `FeatureEndAck`, `Config.DefaultMaxSessions`, `closeAckErr`, `noAuth{}`, `livenessDefault`, `Session.Abort`/`ChainSession.Abort`, peerlinktest's `Within`, quiclink's `errNoDatagrams` and `tagLen`; `peerlink.Session.CloseWriteSync` (a `link.Session` method), `Link.Auth` (the `Config.Link` field) | rewritten to name what a caller can use (chaining and end acknowledgements by WIRE.md section, `peerlink.Join`, `AbortWith`); `TestDocsNameNoGhosts` fails on a doc comment naming an unexported identifier, a member its type lacks, or a name api/*.txt records as removed | none |
| README.md, SECURITY.md | `Abort`, `Join`, bare `MaxSessions` (4 lines) | `AbortWith`, `peerlink.Join`, `Config.MaxSessions` | none |
| `DefaultMaxSessions` | "Config.DefaultMaxSessions's default" (the v0.13.0 rename's leftover) | "Config.MaxSessions's default" | none |
| `quiclink.Session.SendDatagram` | said a caller's largest p is `MaxDatagramPayloadSize` minus the tag, but the error it returns already took the tag off; named an unexported error | says what it returns: the largest p that fits; "an error" when datagrams are off (the caller carries p on the stream, as relay does) | none |
| `peerlink.SyncsEnd`, `EndAcker` | said only a `*link.Session` syncs its end; quiclink sessions do since v0.7.0 | name quiclink sessions | none |
| `Session.CloseWriteSync` | its doc was the implementation's notes (Sync's ping, `Mux.leave`, `closeAckErr`) and did not list `ErrEndAckIdle` or `ErrConnClosed` | the guarantee and every error: nil once the far end took every byte and the end (end acknowledgements) or the next hop read the CLOSE (older peer); `ErrReset`, `ErrEndAckIdle` (the session reset), `ErrConnClosed`, CloseWrite's | none |
| package doc "Errors", README errors table | missing `Session.Answered` (`ErrNoAnswers`, `ErrOpenRefused`), `Session.CloseWriteSync` (`ErrEndAckIdle`) and `ErrDrainDeadline` | listed | none |
| `Conn` | "One goroutine Receives; the sends, Flush ..." describes the frame API v0.13.0 unexported | "a connection the handshake bound, for NewMux; its methods are safe from any goroutine" | none |
| `Mux`, `Pool`, `peerlink.Node`, `quiclink.Node` | no statement of concurrent use | "safe from any goroutine" (Pool: Drain one at a time) | none |
| `Mux.Close`, `Conn.Close`, `Pool.Close`, `peerlink.Node.Drain`, `Session` ends | what a second call returns was undocumented | documented and guarded (`TestCloseTwice`): a Session's ends after it ended return nil; a second `Pool.Close` and `Node.Drain` nil; a second `Mux.Close`/`Conn.Close` the connection's close error (net.ErrClosed on TCP and TLS), as net.Conn's | none |
| `Session.HoldEndAck` | a caller that holds and never calls `AckEnd` leaves the peer's `CloseWriteSync` hanging to its idle bound: undocumented | said, with the error the peer then gets (`ErrEndAckIdle`) | none |
| `quiclink.Node.QUICConfig` | could be read as the Node's own settings to mutate | "a copy" (it was `Clone`d already) | none |
| `ForwardConfig.MaxHops` | "0: 8" where every other default names its constant | "0: DefaultMaxHops" | none |

Kept although no consumer at the mains uses them, each for a stated
reason (an export stays when a consumer uses it, or a used API's
documented use needs it: an error to test, a bound of an input, a
method an interface names):

- Sentinels the docs tell callers to test (`ErrHandshake`, `ErrAuth`,
  `ErrVersion`, `VersionError`, `ErrCredit`, `ErrDrainDeadline`,
  `ErrEndAckIdle`, `ErrOpenRefused`, `pathswitch.ErrProtocol`,
  `ErrStaleEpoch`, `ErrNotClient`); quiclink uses `ErrDrainDeadline`,
  `ErrEndAckIdle`, `ErrNoAnswers`.
- The authenticators and their methods (`NameAuth`, `CertAuth`,
  `MethodNone`, `MethodName`): the README's quick start and recipes.
- Bounds of inputs: `MaxHeader` (Open's header), `MaxSID` (a Credit's
  sid), `MaxAbortMsg` (Refuse's msg), `MaxSessionsWire`
  (Config.MaxSessions), `MaxVersion` (`VersionError.Max`'s scale);
  `MaxFrame`, `MaxData`, `MaxReason` bound no input since the frame API
  went, but each names a row of WIRE.md section 10's table (added in
  v0.13.0 for that): kept, a constant is never a misuse.
- `DefaultWindow`, `DefaultMaxHops`: the defaults the field docs name.
- `ContextWithChain`, `ChainFromContext`, `Route`: the one way to put a
  Chain on an Open, and Forward's argument; quiclink reads
  `ChainFromContext`.
- `Credit.PeerWindow`: `Acquire`'s precondition (n at most the peer's
  window) is stated in its terms.
- `Mux.Conn`, `Mux.Sessions`, `Conn.Done`, `Conn.Stats`,
  `Conn.Version`, `Pool.ConnStats`, `Pool.Drain`, `PoolConfig.Accept`,
  `PoolConfig.Validate`, `Liveness.Validate`: a Mux or Pool user's
  metrics, shutdown and validation (none at the org's mains, which reach
  link through peerlink and the relay SDK; a third-party Mux or Pool
  user needs them).
- `peerlink.Listener`, `NewListener`: serving an http.Server or a
  grpc.Server on a Link (docs/recipes.md); its last in-org user,
  grpclink, was deleted in v0.13.0.
- `peerlink.EndAcker`, `EndNotifier`: the contracts relay's conns
  (`nfnet`, `tlsend`) and relay-core's `CountConn` implement by method.
- Methods that only satisfy an interface (`NopObserver`'s,
  `ChainSession`'s on `*link.Session` and `*quiclink.Session`,
  net.Conn's): the interface needs them.
- quiclink's `Code*`: wire values a peer reads.

Style: every configuration is one plain struct whose zero value is the
default and whose constructor refuses what cannot work naming the field
(`link.Config`/`PoolConfig` also have `Validate`, which quiclink calls);
`ForwardConfig` and `pathswitch.Config` have nothing to refuse. Types
with a constructor (`NewMux`, `NewPool`, `NewCredit`, `Client`/`Server`,
`peerlink.New`, `quiclink.New`, `NewListener`) have no usable zero
value, as is Go's convention.

Not changed, for the lead: pathswitch's and a few link doc comments
cite private paths (nfltr `docs/decisions/...`, relay lane names): true
history, but a public reader cannot follow them; a docs pass before the
v1.0.0 tag. relay main's `browser/e2e/harness` module does not build
(it pins relay-core v0.14.7 with `dataplane.Conn` and an agent
`Config.APIKeyFile` that are gone), independent of link.

## What "v1 compatible" means for link

| surface | v1 promise | guard |
|---|---|---|
| Go API of module `github.com/nfltr/link` (packages `link`, `peerlink`, `peerlinktest`, `pathswitch` unless #5 moves it) | Go 1 compatibility rules: no exported name removed or changed in v1.x; a breaking change is a `/v2` module | `make check-api` (apidiff against the latest tag, `api/`), `TestExportedNamesDocumented` |
| wire version 2 (WIRE.md) | frozen byte for byte: hello, reply, frame types 0-10, the credit, liveness and GOAWAY rules; new behaviour only behind a feature bit (bits 2-31) or a new version negotiated in the hello | `testdata/wire_vectors.json` (`TestWireVectors`), `testdata/v2_session.golden`, `testdata/v2_chain_session.golden` (`TestV2Golden`) |
| abort codes 0-7 (`Code*`) | frozen meanings; 8-1023 reserved for link; 1024 and up the application's (`CodeApp`) | WIRE.md section 12, vectors |
| Liveness presets | the five names (`LivenessPresets`) and their values frozen: a preset's numbers are behaviour callers size timeouts by; a new preset is a minor release | `ExampleParseLiveness` output, `TestLiveness*` |
| Observer events | the event set may grow in minor releases (unexported method, embed `NopObserver`); an event's meaning and arguments never change | `TestObserver*`, the package doc's "Stability" |
| interfaces callers call (`Authenticator`, `peerlink.Link`, `peerlink.Session`, `peerlink.ChainSession`) | never change; new capabilities as optional interfaces | apidiff |
| errors | every error the package doc's "Errors" lists stays testable with `errors.Is` at the call it is listed for | the doc list, `errors_test.go` |
| `peerlink/quiclink`, `peerlink/grpclink` | outside the v1 promise: own modules, v0 until their transport is v1 (README "Status and compatibility") | `make check-api` per module |
| `linkvet`, `soak`, `compare` | outside: tools, not imported by users | - |

## Ranked changes proposed for v1

Ranked by how much they would freeze that should not be frozen. Each
breaking one lands in a v0.13.0 with its migration in CHANGELOG.md,
callers bumped in the promote chain, then v1.0.0 follows with no change
(the README's "peerlink contract unchanged for two minor releases").

1. **Decide the frame-level `Conn` API before v1** (`Conn.SendData`,
   `SendOpen`, `SendClose`, `SendReset`, `SendMsg`, `Recv`, `Flush`,
   `Frame`; wire frame type 1, message; `_` session ids left to the
   caller). Its one caller is relay `proto/wire/dataplane` (the
   `pb.TCPData` adapter of data connections, `x-nfltr-tcp-data`, served
   by relay-core `core/session/dispatcher_tcp_dataplane.go`), the path
   for relays before the agent link. Recommended: retire data
   connections in the relay repositories first, then unexport these
   (and `Conn.Credit`) so `Conn` is a handshake result for `NewMux`; type
   1 and `_` ids stay in WIRE.md as "a Mux drops it". If they cannot
   retire before v1: reshape `Frame` first (seven bools for one kind:
   `Open`, `Close`, `Half`, `Reset`, `Accept`, plus `Abort`) into a
   `Kind` field, which can never change after v1. `Credit` itself stays
   (relay `client/rpcclient`, `client/agent` and relay-core run it over
   gRPC streams, `x-nfltr-tcp-credit`), with `AppendWindow`,
   `ParseWindow`, `CheckWindow`.
2. **Rename `MaxSessions` to `DefaultMaxSessions`.** It is the default
   (1024), not the maximum (the wire's is 2^20, which has no constant);
   `DefaultWindow` is the precedent. Users: relay, quiclink. A name that
   says the wrong thing is frozen for good at v1.
3. **Delete `Conn.Identity`.** `Server` already returns the `Identity`;
   no caller in any repository uses the method (zero on the dialer).
4. **Unexport the `Conn` methods nobody outside link calls**: `Drained`,
   `PeerGoAway`, `PeerMaxSessions`, `GoAway` (the Conn-level drain;
   `Mux.Drain` is the API), `Err`; `GoingAway` is used by peerlink only.
   Folds into #1 if the frame API goes.
5. **Move `pathswitch` to a module of its own (v0)**, as quiclink. It is
   in the root module, so v1 would freeze its 36 names and its wire,
   which has no version field and no test vectors (`pathswitch/doc.go`
   "Wire"). Its one consumer is relay `client/agent` (direct paths), plus
   quiclink. Alternative: keep it, and add a version byte to OPEN and
   vectors to its spec before v1.
6. **Hide the `Session` methods only peerlink calls**: `Pump`,
   `WriteBuffers`, `CloseWriteSync` (and `Sync`, unless its clean-end
   guarantee, `spec/Join.tla`, is wanted public). peerlink reaches them
   through an `internal/` hook (a function variable the root package
   sets), the pattern that keeps a cross-package call out of the API.
   `OnGrant` stays: relay-core calls it through its own interface.
7. **One `Join`.** `link.Join(*Session, *Session)` is called only by
   `peerlink.Join(net.Conn, net.Conn)`, which covers it: unexport it
   (with #6's hook). `link.Forward` stays (the Mux layer's hop; its one
   outside caller is an nfltr test), `peerlink.Forward` is the Link
   form.
8. **One way to carry a `Chain`, and a ctx on `Mux.Open`.** Today:
   `Mux.OpenChain(header, Chain)` (nfltr only), `ContextWithChain` for
   `Pool.Open` and `Node.Open`, and Forward's ctx. `Mux.Open(header)` is
   the only Open without a ctx. Proposed: `Mux.Open(ctx, header)` reading
   the Chain from ctx; delete `OpenChain`. Mechanical for every caller
   (nfltr, relay, relay-core, quiclink, compare), and the last chance:
   after v1 a ctx can only come as a second method.
9. **Feature bits consistently unexported**: `FeatureChain` is exported
   and `FeatureReset` is not (v0.9.0); no code reads `FeatureChain`, only
   doc comments name it. Unexport it (`Session.Answers` is the API) and
   let comments cite WIRE.md section 6.
10. **Mutable preset vars.** `LivenessDefault` (read at run time by
    `Client` for a zero `Config.Liveness`, `handshake.go`), the other
    presets, `LivenessPresets` (a slice), `NoAuth` and `CertAuth` are
    `var`s any importer can reassign. Go has no struct constants; keep
    them, and document "do not assign" on each, or make
    `LivenessPresets` a function returning a copy. No caller assigns one
    today.
11. **Method pairs on `Session`** (`Accept`/`AcceptPath`,
    `Abort`/`AbortWith`/`Refuse`/`Reset`): fixed by
    `peerlink.ChainSession`, an interface callers call, so they freeze
    with it. `Session.Abort(code, msg)` has no outside caller
    (`AbortWith` has); dropping it from `Session` and `ChainSession` is
    the only cut worth considering. Otherwise keep.
12. **`peerlink.Config.HandshakeError`** has no caller in any
    repository. Keep if a consumer adopts it before v1, else delete.
13. **`peerlink/grpclink` has no consumer** in relay, relay-core,
    relay-server, nfltr, send or pipe. It is outside v1 (own module); it
    is also peerlink's only `NewListener`/`Listener.Handle` caller.
    Delete the module or keep it as a tested example; either way not a
    v1 blocker.
14. **quiclink (outside v1)**: unused exports to revisit at its own v1:
    `MaxHeader`, `ErrNoDatagrams`, `Probe`, `Node.DatagramsDropped`,
    `Session.TagLen`, and the QUIC codes `CodeRefused`, `CodeReset`,
    `CodeClosed`, `CodeDrainDeadline`, `CodeUnnamed` (wire values: keep).
    Its wire (ALPN `peerlink`/`peerlink-chain`, the stream header, the
    17-byte chain preamble, datagrams) is described only in its README:
    a WIRE section and vectors are owed before quiclink v1.
15. **peerlinktest**: `Within`, `ChainPath`, `NewChainPath` have no
    caller outside the kit; unexport them, or state that peerlinktest's
    helpers (not `Run` and `Pair`) are outside the v1 promise.

Not proposed: the wire limits and sentinels used by one consumer
(`MinWindow`, `MaxAuth`, `MaxChunk`, `MaxSID`, `Magic`, `ErrCredit`, ...)
stay: they name the wire, not a convenience. `Conn.Version` stays though
it can only return 2: it is the hook for version 3.

## WIRE.md: items not yet versioned or open

- **Version negotiation above 2 is unspecified.** The hello carries
  `version` and an "informational" `max_version`; a listener answers an
  unspoken version with status 2. What a dialer that speaks 3 sends
  first, and how it falls back on status 2 (one more round trip, or
  `version=2` with `max_version=3` and an upgrade in the reply), is not
  written. "A change is a new version negotiated in the hello" needs
  that procedure frozen with v1, or the first v3 has no compatible path.
- **Section 11 names `ErrV1`** as a vector's expected error; the Go name
  was deleted in v0.9.0 (`errV1` wraps `ErrVersion`). Rename the label in
  `testdata/wire_vectors.json` (1 vector) and WIRE.md, or say the labels
  are the vectors' own names.
- **Limits without constants**: the wire's max_sessions (2^20), frame
  body (1048832), data per frame (2^20), open header (65536; quiclink has
  `MaxHeader`), goaway reason (1024). Constants or not, WIRE.md is the
  promise; decide which are API.
- **`FeatureReset` (bit 0) is always set** by every v2 implementation
  (v2 exists since v0.5.0, the bit since v0.3.0), so the "send a reset
  as code 0 to a peer without bit 0" rule only matters to another
  implementation. Keep it (it is the extension pattern), and say so.
- **auth_method 2** is `MethodName` (`NameAuth`) in the code but
  "reserved" in WIRE.md section 2's table (0 none, 1 token, 2-127
  reserved): the wire doc and the constant disagree.
- **Message frames (type 1) and `_` session ids** exist for `Conn`
  callers only (#1).
- **doc.go's "Wire format, version 2"** repeats WIRE.md without
  sections 6 and 12 (frame types 8-10, `FeatureChain`) and says `_` ids
  "name control frames": replace it with a pointer to WIRE.md.
- No TODO, TBD or FIXME in WIRE.md.

## Done in this lane (no API change)

- Doc comments on the fields that had none: `Liveness.Interval`,
  `Liveness.Timeout`, `Abort.Refused`, `HopEvent.Kind`,
  `OwnedWriter.WriteOwned` (who owns the slice), `pathswitch.Open.Epoch`
  and `Token`.
- Examples: `ExampleNewCredit` (Credit on a caller's transport, with
  `AppendWindow`/`ParseWindow`), `ExampleIsHello`,
  `ExampleLiveness_DeadAfter`.
- `FuzzParseLiveness`: 19 more seeds (bounds, spaces, repeated keys, a
  preset after a key, overflow, empty values, control bytes).

## How the inventory was made

Every exported identifier (go/doc over each package's non-test files)
joined with its uses, resolved by type (golang.org/x/tools/go/packages,
tests included, selections and composite-literal keys) in relay
`0e8f46e`, relay-core `92f20cc`, relay-server `63d1039b`, nfltr
`06c918665` and link's own other modules; send and pipe import link only
indirectly. Methods called through a consumer's own interface are found
by name (`OnGrant`) and the standard interfaces (`net.Conn`,
`io.WriterTo`) are marked; an interface method's users are its callers
(implementers of `Observer` and `Authenticator` count on the type).

Users: R relay, C relay-core, S relay-server, N nfltr (tests, the
relay-soak tool, `metrics_agent.go`), q quiclink, g grpclink, k soak,
m compare; p, w, t: peerlink, pathswitch, peerlinktest (same module).
## Inventory

### link

| kind | identifier | users | verdict | note |
|---|---|---|---|---|
| const | `CodeUnspecified` | C p q | keep |  |
| const | `CodeRefused` | C R p q | keep |  |
| const | `CodeDraining` | C N R S p q t | keep |  |
| const | `CodeHopLimit` | C R p q | keep |  |
| const | `CodeNoRoute` | C R S p | keep |  |
| const | `CodeConnLost` | C N R S p q | keep |  |
| const | `CodeTimeout` | C R p | keep |  |
| const | `CodeDrainDeadline` | C N R S p | keep |  |
| const | `CodeApp` | C R p q t | keep |  |
| const | `MethodNone` | - | keep | wire auth_method values (WIRE.md 2) |
| const | `MethodToken` | C R | keep | wire auth_method values (WIRE.md 2) |
| const | `MethodName` | - | keep | wire auth_method values (WIRE.md 2) |
| const | `MinWindow` | R | keep |  |
| const | `MaxWindow` | R | keep |  |
| const | `MinLivenessInterval` | R | keep |  |
| const | `MaxLivenessInterval` | R | keep |  |
| const | `DefaultMaxHops` | p | keep |  |
| const | `DefaultWindow` | k q | keep |  |
| const | `FeatureChain` | - | unexport | #9 doc links only; FeatureReset is unexported |
| const | `Magic` | C R | keep |  |
| const | `MaxAbortMsg` | q | keep |  |
| const | `MaxAuth` | R | keep |  |
| const | `MaxChunk` | R p | keep |  |
| const | `MaxSID` | R | keep |  |
| const | `MaxSessions` | R q | rename | #2 DefaultMaxSessions |
| const | `MaxVersion` | C | keep |  |
| var | `ErrDeadPeer` | C N S k m | keep | errors.Is contract (doc.go "Errors") |
| var | `ErrGoAway` | C N R S p q | keep | errors.Is contract (doc.go "Errors") |
| var | `ErrHandshake` | p | keep | errors.Is contract (doc.go "Errors") |
| var | `ErrAuth` | - | keep | errors.Is contract (doc.go "Errors") |
| var | `ErrVersion` | C | keep | errors.Is contract (doc.go "Errors") |
| var | `LivenessDefault` | C N R S g p q | keep, document | #10 mutable vars read at run time |
| var | `LivenessFast` | R S g k p q | keep, document | #10 mutable vars read at run time |
| var | `LivenessNAT` | R | keep, document | #10 mutable vars read at run time |
| var | `LivenessDatacenter` | R | keep, document | #10 mutable vars read at run time |
| var | `LivenessMobile` | R | keep, document | #10 mutable vars read at run time |
| var | `ErrConnClosed` | N p | keep | errors.Is contract (doc.go "Errors") |
| var | `ErrCredit` | R | keep | errors.Is contract (doc.go "Errors") |
| var | `ErrDrainDeadline` | p q | keep | errors.Is contract (doc.go "Errors") |
| var | `ErrFrame` | R | keep | errors.Is contract (doc.go "Errors") |
| var | `ErrFull` | R p | keep | errors.Is contract (doc.go "Errors") |
| var | `ErrNoAnswers` | C q | keep | errors.Is contract (doc.go "Errors") |
| var | `ErrOpenRefused` | C S t | keep | errors.Is contract (doc.go "Errors") |
| var | `ErrRefused` | R | keep | errors.Is contract (doc.go "Errors") |
| var | `ErrReset` | C R S p | keep | errors.Is contract (doc.go "Errors") |
| var | `LivenessPresets` | R | keep, document | #10 mutable vars read at run time |
| func | `AppendWindow` | R | keep |  |
| func | `CheckWindow` | R | keep |  |
| func | `ContextWithChain` | p | keep | #8 the one way to carry a Chain |
| func | `Forward` | N | keep | #7 the Mux layer's hop; only an nfltr test calls it |
| func | `IsHello` | C | keep |  |
| func | `Join` | p | unexport | #7 peerlink.Join is the API |
| func | `ParseWindow` | R | keep |  |
| func | `Server` | C N R m p q w | keep |  |
| type | `Abort` | C N R S k p q t | keep |  |
| method | `Abort.Error` | k | keep | errors.Is / errors.As |
| method | `Abort.Is` | - | keep | errors.Is / errors.As |
| field | `Abort.Refused` | C N R S k p q t | keep |  |
| field | `Abort.Code` | C N R S k p q t | keep |  |
| field | `Abort.Hops` | C N R S k p q t | keep |  |
| field | `Abort.Msg` | C N R S p q t | keep |  |
| type | `Authenticator` | R | keep |  |
| var | `CertAuth` | k p | keep |  |
| var | `NoAuth` | C N R m p q w | keep |  |
| func | `NameAuth` | g k p q t | keep |  |
| imethod | `Authenticator.Hello` | - | keep | implemented by callers |
| imethod | `Authenticator.Verify` | - | keep | implemented by callers |
| type | `Chain` | C p q | keep |  |
| func | `ChainFromContext` | N p q | keep |  |
| field | `Chain.Trace` | C R S p q | keep |  |
| field | `Chain.Hops` | p q | keep |  |
| type | `Config` | C N R S g k m p q t w | keep |  |
| method | `Config.Validate` | g p q | keep | names the field to fix |
| field | `Config.Window` | C R S m p q | keep |  |
| field | `Config.MaxSessions` | R q | keep |  |
| field | `Config.Liveness` | C N R S g k m p q | keep |  |
| field | `Config.Auth` | C N R S g k m p q t w | keep |  |
| field | `Config.Observer` | C R m p | keep |  |
| type | `Conn` | N R m p q w | keep |  |
| func | `Client` | C N R S m p q w | keep |  |
| method | `Conn.Close` | C R S | keep |  |
| method | `Conn.Credit` | C R | unexport | #1 (keep if #1 keeps the frame API) |
| method | `Conn.Done` | m p | keep | peerlink, compare |
| method | `Conn.Drained` | - | unexport | #4 no caller outside link |
| method | `Conn.Err` | - | unexport | #4 no caller outside link |
| method | `Conn.Flush` | C R p | unexport | #1 |
| method | `Conn.GoAway` | - | unexport | #4 no caller outside link |
| method | `Conn.GoingAway` | p | unexport | #4 peerlink only |
| method | `Conn.Identity` | - | delete | #3 Server returns it |
| method | `Conn.PeerGoAway` | - | unexport | #4 no caller outside link |
| method | `Conn.PeerMaxSessions` | - | unexport | #4 no caller outside link |
| method | `Conn.Recv` | R | unexport | #1 |
| method | `Conn.SendClose` | R | unexport | #1 |
| method | `Conn.SendData` | R | unexport | #1 |
| method | `Conn.SendMsg` | R | unexport | #1 |
| method | `Conn.SendOpen` | R | unexport | #1 |
| method | `Conn.SendReset` | R | unexport | #1 |
| method | `Conn.Stats` | C R | keep |  |
| method | `Conn.Version` | C S | keep |  |
| type | `ConnStats` | C R q | keep |  |
| field | `ConnStats.BytesIn` | C R q | keep |  |
| field | `ConnStats.BytesOut` | C R q | keep |  |
| field | `ConnStats.SessionsOpened` | R q | keep |  |
| field | `ConnStats.SessionsAborted` | R | keep |  |
| field | `ConnStats.CreditWaits` | C R | keep |  |
| field | `ConnStats.CreditWait` | C R | keep |  |
| field | `ConnStats.QueuedBytes` | C R | keep |  |
| type | `Credit` | C R | keep |  |
| func | `NewCredit` | C R | keep |  |
| method | `Credit.Acquire` | C R | keep |  |
| method | `Credit.Close` | C R | keep |  |
| method | `Credit.Consumed` | C R | keep |  |
| method | `Credit.Forget` | C R | keep |  |
| method | `Credit.Grant` | C R | keep |  |
| method | `Credit.Open` | C R | keep |  |
| method | `Credit.PeerWindow` | R | keep |  |
| method | `Credit.Received` | C R | keep |  |
| method | `Credit.SetObserver` | C R | keep |  |
| method | `Credit.Window` | C R | keep |  |
| type | `ForwardConfig` | C N k p q t | keep |  |
| field | `ForwardConfig.MaxHops` | p | keep |  |
| field | `ForwardConfig.AnswerTimeout` | p | keep |  |
| field | `ForwardConfig.Observer` | C k p q | keep |  |
| type | `Frame` | - | unexport | #1 |
| field | `Frame.SID` | R | unexport | #1 |
| field | `Frame.Data` | R | unexport | #1 |
| field | `Frame.Msg` | R | unexport | #1 |
| field | `Frame.Open` | R | unexport | #1 |
| field | `Frame.Header` | R | unexport | #1 |
| field | `Frame.Close` | R | unexport | #1 |
| field | `Frame.Half` | R | unexport | #1 |
| field | `Frame.Reset` | R | unexport | #1 |
| field | `Frame.Chain` | - | unexport | #1 |
| field | `Frame.Accept` | R | unexport | #1 |
| field | `Frame.Abort` | - | unexport | #1 |
| type | `HopEvent` | C R k p q | keep |  |
| field | `HopEvent.Kind` | C k p q | keep |  |
| field | `HopEvent.Chain` | C p q | keep |  |
| field | `HopEvent.Latency` | C p | keep |  |
| field | `HopEvent.Err` | C k p q | keep |  |
| field | `HopEvent.In` | C k p q | keep |  |
| field | `HopEvent.Out` | C k p q | keep |  |
| field | `HopEvent.FirstOut` | C p | keep |  |
| field | `HopEvent.Moved` | C p | keep |  |
| type | `HopKind` | - | keep | the type of an argument or field |
| const | `HopOpened` | k p | keep |  |
| const | `HopAnswered` | C k p | keep |  |
| const | `HopEnded` | C k p q | keep |  |
| type | `Identity` | C R S k m p | keep |  |
| field | `Identity.Method` | C R S k | keep |  |
| field | `Identity.Subject` | C R S k p | keep |  |
| type | `Liveness` | R S g m p q | keep |  |
| func | `ParseLiveness` | R S | keep |  |
| method | `Liveness.DeadAfter` | N R | keep |  |
| method | `Liveness.String` | N S | keep |  |
| method | `Liveness.Validate` | - | keep | names the field to fix |
| field | `Liveness.Interval` | N R S g m p q | keep |  |
| field | `Liveness.Timeout` | S g m p q | keep |  |
| type | `Mux` | N R m p q w | keep |  |
| func | `NewMux` | C N R m p q w | keep |  |
| method | `Mux.Close` | C R m p q w | keep |  |
| method | `Mux.Conn` | m p | keep |  |
| method | `Mux.Drain` | R m p | keep |  |
| method | `Mux.Open` | C N R m p q w | change | #8 take ctx (Pool.Open, Node.Open do) |
| method | `Mux.OpenChain` | N p | merge | #8 into Mux.Open(ctx, header) |
| method | `Mux.Serve` | C N R m p q w | keep |  |
| method | `Mux.Sessions` | p | keep |  |
| type | `NopObserver` | C N R S k m p q | keep | embedded by every Observer |
| method | `NopObserver.ConnDown` | - | keep | embedded by every Observer |
| method | `NopObserver.ConnUp` | - | keep | embedded by every Observer |
| method | `NopObserver.CreditWait` | - | keep | embedded by every Observer |
| method | `NopObserver.GoAway` | - | keep | embedded by every Observer |
| method | `NopObserver.Hop` | - | keep | embedded by every Observer |
| method | `NopObserver.PingRTT` | - | keep | embedded by every Observer |
| method | `NopObserver.SessionAbort` | - | keep | embedded by every Observer |
| method | `NopObserver.SessionClose` | - | keep | embedded by every Observer |
| method | `NopObserver.SessionOpen` | - | keep | embedded by every Observer |
| type | `Observer` | C N R S k p q | keep |  |
| imethod | `Observer.ConnUp` | R p | keep |  |
| imethod | `Observer.ConnDown` | R p | keep |  |
| imethod | `Observer.SessionOpen` | R p | keep |  |
| imethod | `Observer.SessionClose` | R p | keep |  |
| imethod | `Observer.CreditWait` | C R p | keep |  |
| imethod | `Observer.PingRTT` | N R p | keep |  |
| imethod | `Observer.GoAway` | R p | keep |  |
| imethod | `Observer.Hop` | R p | keep |  |
| imethod | `Observer.SessionAbort` | R | keep |  |
| type | `OwnedWriter` | R | keep |  |
| imethod | `OwnedWriter.WriteOwned` | - | keep |  |
| type | `Pool` | R m p | keep |  |
| func | `NewPool` | R m p | keep |  |
| method | `Pool.Close` | R m p | keep |  |
| method | `Pool.ConnStats` | - | keep | Pool's documented surface (make-before-break) |
| method | `Pool.Drain` | - | keep | Pool's documented surface (make-before-break) |
| method | `Pool.Open` | R m | keep |  |
| method | `Pool.Stats` | R | keep |  |
| type | `PoolConfig` | C R S k m p q | keep |  |
| method | `PoolConfig.Validate` | p | keep | names the field to fix |
| field | `PoolConfig.Link` | R m p | keep |  |
| field | `PoolConfig.Dial` | R m p | keep |  |
| field | `PoolConfig.Min` | C R p | keep |  |
| field | `PoolConfig.Max` | R S m p | keep |  |
| field | `PoolConfig.Grow` | S m p | keep |  |
| field | `PoolConfig.GrowWait` | S m | keep |  |
| field | `PoolConfig.Backoff` | R k p q | keep |  |
| field | `PoolConfig.Accept` | p | keep |  |
| type | `PoolStats` | S m p | keep |  |
| field | `PoolStats.Conns` | R S m p | keep |  |
| field | `PoolStats.Sessions` | S m p | keep |  |
| type | `Route` | - | keep | the type of an argument or field |
| type | `Session` | C N R m p q w | keep |  |
| method | `Session.Abort` | - | keep | #11 or drop for AbortWith |
| method | `Session.AbortWith` | C N R p | keep |  |
| method | `Session.Accept` | C N R p | keep |  |
| method | `Session.AcceptPath` | - | keep | #11 ChainSession needs it |
| method | `Session.Answered` | C N R S p | keep |  |
| method | `Session.Answers` | R S | keep |  |
| method | `Session.Chain` | R S | keep |  |
| method | `Session.Close` | C N R S p | keep |  |
| method | `Session.CloseWrite` | C N R S p | keep |  |
| method | `Session.CloseWriteSync` | p | unexport | #6 peerlink only (internal hook) |
| method | `Session.Done` | C R p w | keep |  |
| method | `Session.Err` | p | keep |  |
| method | `Session.Header` | N | keep |  |
| method | `Session.LocalAddr` | (net.Conn) | keep |  |
| method | `Session.OnGrant` | C | keep |  |
| method | `Session.PathLen` | N S | keep |  |
| method | `Session.Pump` | p | unexport | #6 peerlink only (internal hook) |
| method | `Session.Read` | C N R S | keep |  |
| method | `Session.Refuse` | p | keep |  |
| method | `Session.RemoteAddr` | (net.Conn) | keep |  |
| method | `Session.Reset` | R p | keep |  |
| method | `Session.SetDeadline` | N S | keep |  |
| method | `Session.SetReadDeadline` | C S | keep |  |
| method | `Session.SetWriteDeadline` | (net.Conn) | keep |  |
| method | `Session.Sync` | - | keep | #6 the clean end Join.tla states; or unexport with Pump |
| method | `Session.Write` | C N R S p w | keep |  |
| method | `Session.WriteBuffers` | p | unexport | #6 peerlink only (internal hook) |
| method | `Session.WriteTo` | (io.Copy) | keep |  |
| type | `Trace` | S q | keep |  |
| type | `VersionError` | C | keep | errors.Is / errors.As |
| method | `VersionError.Error` | - | keep | errors.Is / errors.As |
| method | `VersionError.Is` | - | keep | errors.Is / errors.As |
| field | `VersionError.Max` | - | keep | errors.Is / errors.As |

### pathswitch

| kind | identifier | users | verdict | note |
|---|---|---|---|---|
| var | `ErrReset` | R | own module (v0) | #5 |
| var | `ErrDetached` | R | own module (v0) | #5 |
| var | `ErrProtocol` | - | own module (v0) | #5 |
| var | `ErrStaleEpoch` | - | own module (v0) | #5 |
| var | `ErrNotClient` | - | own module (v0) | #5 |
| func | `Refuse` | R q | own module (v0) | #5 |
| type | `Config` | R q | own module (v0) | #5 |
| field | `Config.Window` | R | own module (v0) | #5 |
| field | `Config.Detach` | R | own module (v0) | #5 |
| field | `Config.Linger` | R | own module (v0) | #5 |
| field | `Config.OnStreamEnd` | R q | own module (v0) | #5 |
| type | `Conn` | R q | own module (v0) | #5 |
| func | `Client` | R q | own module (v0) | #5 |
| func | `Server` | R q | own module (v0) | #5 |
| method | `Conn.Abort` | R | own module (v0) | #5 |
| method | `Conn.Attach` | R q | own module (v0) | #5 |
| method | `Conn.Close` | R | own module (v0) | #5 |
| method | `Conn.CloseRead` | R | own module (v0) | #5 |
| method | `Conn.CloseWrite` | R q | own module (v0) | #5 |
| method | `Conn.Current` | R | own module (v0) | #5 |
| method | `Conn.Done` | R | own module (v0) | #5 |
| method | `Conn.Epoch` | R | own module (v0) | #5 |
| method | `Conn.Move` | R q | own module (v0) | #5 |
| method | `Conn.Path` | R q | own module (v0) | #5 |
| method | `Conn.Read` | R | own module (v0) | #5 |
| method | `Conn.WaitAcked` | R | own module (v0) | #5 |
| method | `Conn.Write` | R q | own module (v0) | #5 |
| method | `Conn.Written` | R | own module (v0) | #5 |
| type | `Open` | R | own module (v0) | #5 |
| func | `ReadOpen` | R q | own module (v0) | #5 |
| field | `Open.Epoch` | R | own module (v0) | #5 |
| field | `Open.Token` | R q | own module (v0) | #5 |
| field | `Open.Abort` | - | own module (v0) | #5 |
| type | `Stream` | R q | own module (v0) | #5 |
| imethod | `Stream.CloseWrite` | - | own module (v0) | #5 |
| imethod | `Stream.Reset` | q | own module (v0) | #5 |

### peerlink

| kind | identifier | users | verdict | note |
|---|---|---|---|---|
| var | `ErrDraining` | C S g q t | keep |  |
| var | `ErrPeerUnreachable` | g q t | keep |  |
| func | `Forward` | C k q t | keep |  |
| func | `Join` | C R | keep |  |
| type | `ChainSession` | C R S k q t | keep |  |
| imethod | `ChainSession.Answers` | C R k q t | keep |  |
| imethod | `ChainSession.Accept` | R k q t | keep |  |
| imethod | `ChainSession.AcceptPath` | - | keep |  |
| imethod | `ChainSession.Refuse` | q t | keep |  |
| imethod | `ChainSession.Abort` | q t | keep |  |
| imethod | `ChainSession.AbortWith` | R | keep |  |
| imethod | `ChainSession.Answered` | C R S k q t | keep |  |
| imethod | `ChainSession.Chain` | q | keep |  |
| imethod | `ChainSession.PathLen` | q t | keep |  |
| type | `Config` | C R S g k q t | keep |  |
| field | `Config.Dial` | C R S g k q t | keep |  |
| field | `Config.Connect` | C R k | keep |  |
| field | `Config.Listener` | C R S g k q t | keep |  |
| field | `Config.Link` | C R S g k q t | keep |  |
| field | `Config.Pool` | C R S k q | keep |  |
| field | `Config.Observer` | C R S k | keep |  |
| field | `Config.ServedPeer` | C S k | keep |  |
| field | `Config.HandshakeError` | - | keep | #12 no caller yet; drop if none by v1 |
| type | `Link` | C R g k q t | keep |  |
| imethod | `Link.Open` | C R g k q t | keep |  |
| imethod | `Link.Serve` | R g k q t | keep |  |
| imethod | `Link.Drain` | R k q t | keep |  |
| type | `Listener` | - | keep | net.Listener over a Link (http.Serve); grpclink |
| func | `NewListener` | g | keep | net.Listener over a Link (http.Serve); grpclink |
| method | `Listener.Accept` | - | keep | net.Listener over a Link (http.Serve); grpclink |
| method | `Listener.Addr` | - | keep | net.Listener over a Link (http.Serve); grpclink |
| method | `Listener.Close` | - | keep | net.Listener over a Link (http.Serve); grpclink |
| method | `Listener.Handle` | g | keep | net.Listener over a Link (http.Serve); grpclink |
| type | `Node` | C R S k | keep |  |
| func | `New` | C R S g k q t | keep |  |
| method | `Node.DeadAfter` | C S k t | keep |  |
| method | `Node.Drain` | C R S g k | keep |  |
| method | `Node.Open` | S g | keep |  |
| method | `Node.Serve` | C R S g k | keep |  |
| type | `Route` | - | keep | the type of an argument or field |
| type | `Session` | C R S g k m q t | keep |  |
| imethod | `Session.CloseWrite` | C R S g k q t | keep |  |
| imethod | `Session.Reset` | C R S g q t | keep |  |

### peerlink/peerlinktest

| kind | identifier | users | verdict | note |
|---|---|---|---|---|
| const | `Within` | p | unexport | #15 no caller outside the kit |
| func | `Run` | C S g p q | keep |  |
| type | `ChainPath` | - | unexport | #15 no caller outside the kit |
| func | `NewChainPath` | - | unexport | #15 no caller outside the kit |
| field | `ChainPath.In` | - | unexport | #15 no caller outside the kit |
| field | `ChainPath.Out` | - | unexport | #15 no caller outside the kit |
| type | `Pair` | C S g p q | keep |  |
| field | `Pair.A` | C S g p q | keep |  |
| field | `Pair.B` | C S g p q | keep |  |
| field | `Pair.Reverse` | C p q | keep |  |
| field | `Pair.AName` | C S g p q | keep |  |
| field | `Pair.BName` | C S g p q | keep |  |
| field | `Pair.DeadAfter` | C S g p q | keep |  |
| field | `Pair.BlackHole` | C S g p q | keep |  |
| field | `Pair.Cut` | C S g p q | keep |  |
| field | `Pair.Throttle` | C S g p q | keep |  |
| type | `Proxy` | k p | keep |  |
| func | `NewProxy` | C S g k p q | keep |  |
| method | `Proxy.Addr` | C S g k p q | keep |  |
| method | `Proxy.BlackHole` | C S g k p q | keep |  |
| method | `Proxy.Cut` | C S g k p | keep |  |
| method | `Proxy.Throttle` | C S g k p | keep |  |
| type | `UDPProxy` | - | keep | returned by NewUDPProxy |
| func | `NewUDPProxy` | C R k q | keep |  |
| method | `UDPProxy.Addr` | C R k q | keep | returned by NewUDPProxy |
| method | `UDPProxy.BlackHole` | C R k q | keep | returned by NewUDPProxy |
| method | `UDPProxy.Cut` | C k q | keep | returned by NewUDPProxy |
| method | `UDPProxy.Rebind` | R k q | keep | returned by NewUDPProxy |
| method | `UDPProxy.Throttle` | C k q | keep | returned by NewUDPProxy |

### peerlink/quiclink

| kind | identifier | users | verdict | note |
|---|---|---|---|---|
| const | `CodeRefused` | - | v0 (outside v1) | #14 |
| const | `CodeReset` | - | v0 (outside v1) | #14 |
| const | `CodeClosed` | - | v0 (outside v1) | #14 |
| const | `CodeDone` | C R k | v0 (outside v1) | #14 |
| const | `CodeDrainDeadline` | - | v0 (outside v1) | #14 |
| const | `CodeUnnamed` | - | v0 (outside v1) | #14 |
| const | `CodeDeadPeer` | k | v0 (outside v1) | #14 |
| const | `ALPN` | R k m | v0 (outside v1) | #14 |
| const | `ALPNChain` | R k | v0 (outside v1) | #14 |
| const | `MaxCandidates` | R | v0 (outside v1) | #14 |
| const | `MaxHeader` | - | v0 (outside v1) | #14 |
| var | `ErrNoDatagrams` | - | v0 (outside v1) | #14 |
| func | `Probe` | - | v0 (outside v1) | #14 |
| func | `Punch` | R | v0 (outside v1) | #14 |
| type | `Config` | C R k m | v0 (outside v1) | #14 |
| field | `Config.Dial` | C R k m | v0 (outside v1) | #14 |
| field | `Config.Connect` | C R k | v0 (outside v1) | #14 |
| field | `Config.PacketConn` | C R k m | v0 (outside v1) | #14 |
| field | `Config.TLS` | C R k m | v0 (outside v1) | #14 |
| field | `Config.Transport` | R | v0 (outside v1) | #14 |
| field | `Config.ServedPeer` | C R k m | v0 (outside v1) | #14 |
| field | `Config.Liveness` | C R k m | v0 (outside v1) | #14 |
| field | `Config.Window` | C R m | v0 (outside v1) | #14 |
| field | `Config.MaxSessions` | R | v0 (outside v1) | #14 |
| field | `Config.Datagrams` | R | v0 (outside v1) | #14 |
| type | `Node` | C R k m | v0 (outside v1) | #14 |
| func | `New` | C R k m | v0 (outside v1) | #14 |
| method | `Node.DatagramsDropped` | - | v0 (outside v1) | #14 |
| method | `Node.DeadAfter` | C k | v0 (outside v1) | #14 |
| method | `Node.Drain` | C R m | v0 (outside v1) | #14 |
| method | `Node.Open` | R m | v0 (outside v1) | #14 |
| method | `Node.QUICConfig` | C R | v0 (outside v1) | #14 |
| method | `Node.Serve` | C R m | v0 (outside v1) | #14 |
| method | `Node.Stats` | C R | v0 (outside v1) | #14 |
| type | `Session` | R | v0 (outside v1) | #14 |
| method | `Session.Abort` | - | v0 (outside v1) | #14 |
| method | `Session.AbortWith` | - | v0 (outside v1) | #14 |
| method | `Session.Accept` | - | v0 (outside v1) | #14 |
| method | `Session.AcceptPath` | - | v0 (outside v1) | #14 |
| method | `Session.Answered` | - | v0 (outside v1) | #14 |
| method | `Session.Answers` | - | v0 (outside v1) | #14 |
| method | `Session.Chain` | - | v0 (outside v1) | #14 |
| method | `Session.Close` | - | v0 (outside v1) | #14 |
| method | `Session.CloseWrite` | - | v0 (outside v1) | #14 |
| method | `Session.LocalAddr` | - | v0 (outside v1) | #14 |
| method | `Session.OnDatagram` | R | v0 (outside v1) | #14 |
| method | `Session.OnGrant` | - | v0 (outside v1) | #14 |
| method | `Session.PathLen` | - | v0 (outside v1) | #14 |
| method | `Session.Read` | - | v0 (outside v1) | #14 |
| method | `Session.Refuse` | - | v0 (outside v1) | #14 |
| method | `Session.RemoteAddr` | - | v0 (outside v1) | #14 |
| method | `Session.Reset` | R | v0 (outside v1) | #14 |
| method | `Session.SendDatagram` | R | v0 (outside v1) | #14 |
| method | `Session.TagLen` | - | v0 (outside v1) | #14 |
| method | `Session.Write` | - | v0 (outside v1) | #14 |

### peerlink/grpclink

| kind | identifier | users | verdict | note |
|---|---|---|---|---|
| type | `Config` | - | delete or v0 | #13 no consumer |
| field | `Config.Dial` | - | delete or v0 | #13 no consumer |
| field | `Config.Listener` | - | delete or v0 | #13 no consumer |
| field | `Config.Liveness` | - | delete or v0 | #13 no consumer |
| field | `Config.DialOptions` | - | delete or v0 | #13 no consumer |
| field | `Config.ServerOptions` | - | delete or v0 | #13 no consumer |
| field | `Config.Identify` | - | delete or v0 | #13 no consumer |
| type | `Node` | - | delete or v0 | #13 no consumer |
| func | `New` | - | delete or v0 | #13 no consumer |
| method | `Node.DeadAfter` | - | delete or v0 | #13 no consumer |
| method | `Node.Drain` | - | delete or v0 | #13 no consumer |
| method | `Node.Open` | - | delete or v0 | #13 no consumer |
| method | `Node.Serve` | - | delete or v0 | #13 no consumer |
