# A formal model of link's sessions

`Link.tla` is a TLA+ model of the session protocol of WIRE.md (sections 5,
7, 9 and 12's answers) **as the Go code implements it**, model checked with
TLC (`make spec`). `Trace.tla` checks recorded runs of the real code
against the same model (`make spec-traces`): the Go tests run with
`-tags linktrace` write one JSON line per model action, and TLC must find a
behaviour of `Link` that takes exactly those steps.

This file is the design; it came before the spec and the recorder.

## What is modelled

Two sides, `d` (the dialer) and `l` (the listener), joined by one
connection: an ordered queue of frames each way (`out[p]`, what `p` sent
and the peer has not read yet). The queue stands for `Conn.wbuf` and the
network together: frames are appended under `wmu` in the order they go out,
and TCP keeps that order, so one FIFO per direction is exact.

Per side `p` and session `s`:

| variable | Go | meaning |
|---|---|---|
| `cr[p][s]` | `Credit.sessions[s]` (`sessionCredit`) | `on` (known to the Credit), `n` (send credit), `held`, `cons` |
| `ss[p][s]` | `Mux.sess[s]` (`Session`) | `st` (`none`, `open`, `gone`), `wfin` (`halfClosed`), `rfin` (`peerHalf`), `pclosed` (`peerClosed`), `q` (bytes queued unread), `acc` (answered, FeatureChain) |
| `pend[p][s]` | a `Session.Write` between `acquire` and `sendData` | credit taken, frame not yet buffered |
| `wt[p][s]` | a frame in `frameWaitLocked`, wmu let go | `data` or `open`: checked by its sender, waiting for room or its turn |
| `gpend[p][s]` | `Credit.Consumed` between its unlock and `grant` | a window update decided, not yet buffered |
| `ga`, `sentGA`, `peerGA`, `drained`, `leaving` | `Conn.goingAway`, `goAwayAt`, `peerGoAway`, `drained`, `Mux.leaving` | GOAWAY state |
| `pong[p]` | `Conn.pong` | the latest ping to answer (0 none, 1 before GOAWAY, 2 after) |
| `closed[p]`, `down[p]` | `Conn.Close` ran; `Mux.close` ran (Serve returned) | the connection's end, in two steps |

## What is abstracted

- Bytes are counts, never contents: the order of bytes inside one session
  is TCP's and the queue's, not the protocol's. A frame carries a count.
- Small sizes for model checking (`MCLink.tla`): each side's window `W`
  = 4 (WIRE.md's `window`), the most taken at once `Chunk` = 2
  (`MaxChunk`; `MinWindow` is 2 x `MaxChunk`), the grant threshold
  `W / 4`, a bound on the bytes each side writes on each session. Two
  configurations keep each check to minutes: `Link.cfg`, one
  session, the dialer writes 6 bytes through the window of 4, every close
  code, a side may close its connection abruptly; `LinkDrain.cfg`, one
  session the dialer opens, no data, GOAWAY from the listener (crossing
  the OPEN or not), the drain deadline's abort and a liveness ping.
  Trace validation runs the same actions with the real sizes (the trace's).
- Session ids: one per session, never reused (a `Mux` picks fresh
  uvarints); `Owner[s]` is the side that opens `s` (odd ids the dialer's).
  An open of an id the receiver keeps credit for is a protocol error
  (WIRE.md section 5; `reopen-known` puts the sender's reuse back).
  A `Conn` caller's own `Credit.Open` before its `SendOpen` is no session
  (`ss` stays `none` until the OPEN): the code's check is `Credit.inUse`,
  credit the link opened, not any credit (lane LINKBUMP).
- `max_sessions` (`MaxSess`, the same both ways): a side counts its own
  sessions by their credit; `RecvOpen` flags an open past it (`ErrFull`).
  The write scheduler's tickets are not modelled: a frame that waits may be
  overtaken by any other. Their one order the design relies on, a CLOSE
  or ABORT that took its place in line before an OPEN goes out before it,
  is modelled as `OpenCheck` waiting until no close of this side is
  pending (a trace in which an OPEN checks while a close waits would be
  rejected: no test does).
- Not modelled: the handshake, message frames,
  write deadlines (`refund`), `Join`/`Pump`, the chain's hop counts and
  refusals other than GOAWAY's, liveness timers (a ping may be sent at
  any time; a dead peer is the connection closing).
- A connection ends in two logged steps: `Close(p, k)` (`Conn.Close`: the
  peer still reads the first `k` frames of `out[p]`, all of them after a
  graceful `closeFlushed`; frames `p` buffers after it are lost) and
  `Down(p)` (`Mux.close`: Serve returned, every session at `p` is cut).
  Between them `p` may still apply frames (its read-ahead buffer).

## Actions and the Go they model

| action | Go (where the trace event is written) |
|---|---|
| `OpenCheck(p, s)` | `Conn.sendOpen`'s checks (GOAWAY, id, count), the credit started, then the OPEN waits (trace `OpenWait`) |
| `OpenBuffer(p, s)`, `OpenRefused(p, s)` | the GOAWAY checked again after the wait: the OPEN buffered (trace `Open`), or `ErrGoAway` and the credit undone (trace `OpenRefused`) |
| `Open(p, s)` | the check and the OPEN at once, one that did not wait (Trace.tla only) |
| `Accept(p, s)` | `Conn.sendAccept`, `Conn.acceptLocked` |
| `Acquire(p, s, n)` | `Credit.acquire` (credit taken) |
| `SendCheck(p, s)` | `Conn.sendData`'s check of the write side and the credit, then the frame waits (trace `SendWait`) |
| `SendBuffer(p, s)` | `Conn.frameWaitLocked`: the check again after the wait, the frame buffered (trace `SendData`, logged by that check: `Credit.sendable`) |
| `SendData(p, s)` | the two at once, a frame that did not wait: checked again just before it is buffered (C12; Trace.tla only) |
| `Refund(p, s)` | `Credit.refund` (a `Write`'s credit given back, nothing sent: its write side closed, its credit ended or its deadline passed) |
| `CloseWrite(p, s)` | `Session.CloseWrite` -> `Conn.sendClose` code 1, in two steps: `CloseWriteMark` (`halfClosed` set) and `CloseWriteSend` (the CLOSE buffered, `finSent`; review r2 LinkSession 1). Trace.tla takes the two as one (`Close` code 1) |
| `RefuseMark(p, s)` | `Session.AbortWith`/`answer`'s refusal: `ans` taken, the credit forgotten (`Credit.forget` code 4, trace `Forget` 4); its ABORT then is `End` (trace `Close` 4) |
| `Closing(p, s, code)` | `Conn.sendClose` (code 0 from `Session.Close`, 2 from `Session.Reset`) and `Conn.sendAbort` (3, from `Mux.cut`): the credit forgotten (`Credit.forget`) |
| `End(p, s)` | the same, once the CLOSE/ABORT is buffered (it may wait for room between the two) |
| `Read(p, s, k)` | a pop of the session's queue -> `Credit.Consumed` |
| `SendWindow(p, s)` | `Credit.Consumed`'s `grant` -> `Conn.sendWindow` |
| `GoAway(p)` | `Conn.GoAway` (from `Mux.goAway`) |
| `Ping(p)`, `Pong(p)` | `Conn.control` (`ping`, `keepAlive`'s answer, a waiting pong replaced) |
| `Recv(p)` | `Conn.recv`/`Conn.next` for each frame type, then `Mux.serve`/`Mux.data` |
| `CloseConn(p, k)` | `Conn.Close` (first call) |
| `Leave(p)` | `Mux.leave` -> `closeFlushed` (the leaving side's frames all go out) |
| `Down(p)` | `Mux.close` |
| `DrainPing(p)`, `LeaveWhenDrained(p)`, `EOFClose(p)`, `Crashed(p)` | `Ping(p, 1)` after the GOAWAY (`Mux.goAway`'s ping), `Leave` once drained, `CloseConn` when the peer's end was read (`Conn.recv`'s EOF), `CloseAt` at any point (a crash: the environment) |
| `Terminated` | no code: the end states TLC's deadlock check allows |

The receive side applies a frame in one step (`Conn.recv` and the `Mux`
that follows on the same goroutine); the trace event is written by
`Conn.recv`/`Conn.next`. The app-side steps that the code does under two
locks (`Write`: credit, then the frame; `Consumed`: the count, then the
window frame) are two actions, so other steps can fall between them as
they can in Go.

## Invariants

1. **Credit.** A sender never takes more than the peer's window: for a
   session both sides know, `n + pend + data in flight + held at the peer
   + gpend at the peer + window updates in flight = W` (`Conservation`);
   `0 <= n <= W` (`CreditBounds`); the receiver never sees data past its
   window or a window update past its own window (`ErrCredit` never
   happens: `NoProtocolError`).
2. **No data after FIN.** A side never receives data for a session after
   that direction's half-close (it would be dropped by a closed queue).
3. **FIN never overtakes data.** When a close (half or whole) arrives, the
   receiver has queued every byte the peer sent on the session before it
   (a close frame carries `at`, the bytes its sender had sent on the
   session; the receiver has queued exactly that many).
4. **GOAWAY.** After a side read the peer's GOAWAY it opens nothing; an
   open that arrives after the receiver's drain barrier (the pong of the
   ping it sent after its GOAWAY) or after the peer's GOAWAY never happens
   (the code refuses it with a reset: a refused crossed open is an error).
5. **Buffers.** A session's queue holds at most the receiver's window,
   and at most the bytes still charged to it (`QueueWithinHeld`): why
   `bytequeue.Push` never waits in the receive loop.
6. **FeatureChain answers.** The opener never receives the accepter's
   data before its ACCEPT, nor a second ACCEPT.
7. **Deadlock.** TLC's deadlock check: a state with nothing enabled is
   only an end: both sides down, or an idle connection (no session open,
   no frame on the way, no write, window update, close or pong pending).

Each protocol error is recorded in `err` by the receive step that would
return `ErrCredit`/`ErrFrame` or break 2-4, so one invariant,
`NoProtocolError` (`err = {}`), names which broke.

## Liveness (weak fairness on every step that is not the application's)

- A session ends at both sides once both applications ended it (Close,
  Reset or CloseWrite each), or once the connection is down.
- A connection that one side closed goes down on both sides.
- A side that sent GOAWAY closes the connection once every session at
  both sides ended.

## Mutants

`make spec-mutants` checks each `Mutant` value on `Link.cfg` (and
`open-after-goaway` on `LinkDrain.cfg`) and passes only when TLC reports a
violation for each:

- `fin-skips-queue`: a close goes ahead of the frames buffered before it
  (the FIN overtakes data);
- `grant-early`: a window update counts bytes not yet read (credit past
  the window);
- `open-after-goaway`: a side opens after it read the peer's GOAWAY;
- `write-race`: `Conn.sendData` does not look at the write side or the
  credit, so a `Write` that took credit before a `CloseWrite` or `Close`
  of another goroutine sends after its FIN (finding 1, the code before
  its fix).
- `turns-no-recheck`, `open-no-recheck`, `abort-forget-early`,
  `reopen-known`, `release-early`: the review's F1, F2, F3, F5 and F6
  ("Review findings").

## The session interface (LinkSession.tla, 2026-11-02, lane LINKIFACE)

`LinkSession.tla` is what a layer above link may assume of its sessions
(the guarantees the README's "What a session guarantees" lists), small
enough to read in a minute; `Link.tla` refines it. A layer above (`Pool.tla`
here, relay-core's specs) models link only by these guarantees, never by
`Link.tla`'s frames. The interface is a transition constraint: its `Next`
allows any step in which every guarantee holds, so one step of `Link`
(a receive that ends a session and types its end) is one step of it.

| guarantee (`LinkSession.tla`) | what it says | broken in Link.tla by (`make spec-mutants`, refinement only) |
|---|---|---|
| `GSessions` | an id goes none, open, gone, once (never reused); the opener opens only before GOAWAY (a failed Open, ErrGoAway or the connection's end, is none to gone); the peer's end opens only an id its opener opened; a session ends with a reason (its own whole close, a FIN each way, the peer's Close or reset, the connection's end) | `open-after-goaway`, `open-no-recheck` |
| `GWrites` | writes only on a session opened here whose write side is not ended (no data after FIN, Close or reset); credit: the bytes written that the peer's reader has not taken are at most the peer's window | `write-race`, `turns-no-recheck` (data after FIN); `grant-early`, `release-early` (past the window) |
| `GReads` | a reader gets a prefix of what the peer wrote (in order), at most its window unread, nothing after its end | (a consequence of `GWrites` in Link) |
| `GEnds` | a writer's end is typed once (`fin`, then perhaps `close` or `reset`); a reader's end is `eof` only after every byte the peer wrote before its FIN or Close, `reset` only from the peer's Reset or Abort, `lost` only with the connection's end | `fin-skips-queue` (EOF before the data), `abort-as-fin` (a reset read as EOF), `lost-as-eof` (a cut session read as EOF) |
| `GConn` | GOAWAY and the connection's end are final; the end ends every session at that side; no protocol error | `abort-forget-early` (open past max_sessions), `reopen-known` (open of an id in use): both only as a protocol error |

The mapping (`MCLink.tla`, `LS`): `st` is `Mux.sess`'s state, `w` the
bytes buffered (`sent`), `d` the bytes queued for the reader (`dlv`), `c`
those it took (`dlv - q`); `wend`, `shut` and `rend` come from two history
fields of a session (`code`: the code of this side's last close, a reset
kept over a later Close; `got`: the first close received while it was
open) and `down`; `fault` is `err # {}`. History only: no action of Link
reads them, so they change no behaviour (the state count grows by the
states they tell apart).

`make spec` checks `Refines` (`PROPERTY Refines`) on `Link.cfg`,
`LinkDrain.cfg` and `LinkSess.cfg` (`Crash = 0`) and on `LinkCrash.cfg`;
`make spec-invariants` (CI) only on `LinkCrash.cfg`, where it costs
seconds (on the three it tripled CI's run, lane SPECTRIM); `make
spec-nightly` on `Link.cfg` with its crash. `make
spec-mutants` checks each mutant above with `Refines` alone (every
invariant of Link left out), the two mutants that exist only for the
interface (`abort-as-fin`: `End` sends a Reset or Abort as a Close;
`lost-as-eof`: `Down` hands a cut session's reader `io.EOF`), and the
witnesses (each must be violated, so the guarantees constrain steps Link
takes): `WitnessEof`, `WitnessReset`, `WitnessLost` and `WitnessCredit`
(a writer past one window: credit came back) on `Link.cfg`,
`WitnessRefused` (an open refused by GOAWAY) on `LinkDrain.cfg`.

Assumptions and code (the drift check's table for this interface):

| assumption | source |
|---|---|
| a connection is one ordered, reliable byte stream each way until it ends (TCP, TLS over it, QUIC's stream in quiclink) | RFC 9293 section 3.1; WIRE.md section 1 |
| a process that dies ends its connections (its peer reads an error or nothing: the dead-peer bound) | `Liveness.tla` |

| guarantee | Go |
|---|---|
| ordered, reads | `mux.go` (`Session.Read`, `Mux.data`), `internal/bytequeue` |
| credit | `credit.go` (`Credit`), `conn.go` (`sendData`, `frameWaitLocked`) |
| ends | `mux.go` (`CloseWrite`, `Close`, `Reset`, `end`, `Mux.close`), `chain.go` (`AbortWith`), `conn.go` (`sendClose`, `sendAbort`, `recv`) |
| GOAWAY, ids | `conn.go` (`sendOpen`, `GoAway`), `mux.go` (`Mux.Open`) |
| dead peer | `conn.go` (`keepAlive`) |

Timed and liveness guarantees are not in the transition constraint:
the dead-peer bound is `Liveness.tla`'s `DetectBound` (Interval + Timeout
+ 2 x Late, tight) and `NoFalseDead`, and the ends of sessions and
connections are `Link.tla`'s `SessionsEnd`, `ConnEnds` and `DrainEnds`
(`make spec`). The interface sees their effect: a connection that ends
ends its sessions `lost`.

What `Pool.tla` assumes is this interface: a connection's end ends its
sessions with an error (`GConn`, `rend = "lost"`), an open is refused once
either side sent GOAWAY (`GSessions`), and the side that sent GOAWAY closes
the connection once its sessions ended (`DrainEnds`).

Not in the interface, on purpose: that a session that ended cleanly at a
side (both halves closed) had its tail received by the peer. `CloseWrite`
after the peer's FIN ends the session at once (`Mux.drop`) while this
side's CLOSE and the bytes before it may still be in the write buffer;
a connection that dies then cuts the peer's end (`lost`) after this
side's application saw a clean end, as TCP's close does. relay-core's
frame streams give that guarantee (`NoSilentLoss`, their end
acknowledgement); a layer that needs it over link waits for an answer of
the peer's application, or syncs after its CloseWrite (`Session.Sync`: a
ping after the FIN, answered: the peer's Mux read it), as `peerlink.Join`
does ("Join's clean end" below). `CloseWrite`'s result is not that
guarantee either: an error says its CLOSE could not be written (the
connection's writes failed or it closed: the peer reads `lost`), nil only
that it was buffered (finding F9).

A Close after `Mux.cut`'s abort (the drain deadline) sends a CLOSE after
the ABORT and a CloseWrite a half CLOSE (`Session.Close`'s `once` and `halfClosed` are not spent by `cut`); the peer
ignores it (the session is gone there) and the writer's end stays typed
as the reset. Harmless; the model has it (`End` and `CloseWrite` keep `code`).

**Finding I1 (spec, lane LINKIFACE): `Link.cfg`'s crash was never
taken.** `Crashed` set `crashes' = crashes + 1` while `CloseConn`, which
it called, left `crashes` unchanged: the conjunction was never true, so
no connection ever closed in `Link.cfg` (it has no GOAWAY, the other way
to an end) and its invariants, `ConnEnds` and `SessionsEnd` were checked
only on connections that never end. Found by the interface's witness
`WitnessLost` (never violated: no session ever ended `lost`). Fixed:
`Crashed` counts on `CloseAt`, the close itself; `WitnessLost` guards it.
Severity: medium (a vacuous part of the model, not a code bug: the
connection's end was checked by `LinkDrain.cfg`'s drain and the traces).
`make spec` and `make spec-invariants` run `Link.cfg` with `Crash = 0`.
With the crash taken, invariants and `Refines` hold on `Link.cfg`
(checked once, 2026-11-02: 87,768,000 states, 20,031,413 distinct, depth
43, 51 min with 3 workers on a loaded Mac): `make spec-nightly` (the Linux
runner's nightly, `CI_NIGHTLY`). In CI the crash is `LinkCrash.cfg`'s (one
byte, invariants and `Refines`, 143,610 states, lane SPECTRIM) and the mutants' and
witnesses' (`WitnessLost`, `CleanEndReceived`, `lost-as-eof`).

**Finding I2 (model, lane LINKIFACE): an OPEN that waited its turn could
open its session after the connection ended.** With I1 fixed, TLC found
`OpenCheck`, a crash, `Down`, then `OpenBuffer`: a session open at a side
whose Mux had stopped (the interface: a connection's end ends every
session). The code does not: `Mux.add` refuses once the Mux is done, and
an OPEN buffered after `Conn.Close` fails at `OpenChain`'s Flush, which
drops the session. The model now refuses it (`OpenBuffer` needs the
connection open; `OpenRefused` also takes a closed one; `Trace.tla` maps
an `Open` logged after the close to the refusal). The interface's refused
open is any failed Open (ErrGoAway, the connection's end), before any
byte. Severity: low (model only).

## Join's clean end (Join.tla, 2026-11-03, lane SPECTRIM)

relay-core finding 23: `peerlink.Join` of a client's TCP conn and a link
session ended the client cleanly once both directions ended, while the
client's tail and FIN could still be in the link connection's write buffer;
a connection that died then cut the peer (`lost`) after the client saw a
clean end. The same holds at an agent, its target joined to the session.

Design: a direction that ends with the session's `CloseWrite` then syncs
(`Session.Sync`: `Conn.sync` registers a wait for every ping from here on,
buffers a ping; a pong of one of them answers it; pings carry strictly
growing times, `lastPing`, so a pong says which frames the peer read:
every frame before its ping, WIRE.md section 8, no wire change). The
joint ends that direction only after the sync; the other direction's clean
end (the peer's FIN to the client) is held meanwhile, so the client reads
no EOF while its own tail may still be lost; a connection that ends first
fails the sync and the joint aborts the client (an RST: `abort`). A FIN
that crosses the client's own on the way is no end the joint can type (a
TCP proxy's limit): the model's clean end is the joint's clean close, or
an EOF given after it read the client's FIN. Bound: one round trip plus
the write buffer; the connection's liveness bounds a dead peer.

`Join.tla` (`Join.cfg`, `make spec` and `make spec-invariants`, 306
states): the client (`CWrite`, `CFin`), the joint (`Copy`, `CloseWrite`,
`RRecv`, `Finish`, `Lost`), the peer's Mux (`ARecv`, `AFin`) and the
connection as two FIFOs that a `Crash` empties. Invariant `NoSilentLoss`:
a client given a clean end had every byte and its FIN read by the peer
once the connection is gone. Mutants (`make spec-mutants`): `no-sync`
(the code before: the end once the FIN is buffered) and `no-hold` (the
peer's FIN passed on while the sync waits), both caught by
`NoSilentLoss`. Witnesses: `WitnessClean`, `WitnessReset`, `WitnessHeld`.
Guard: `peerlink` `TestJoinCleanEndOnlyOnceThePeerReadTheFIN` (failed
before: Join returned nil with the tail in the dead connection's buffer).
Review r2 finding 1 (high, lane W5-LINK): the peer's FIN arriving while
the sync waits ended the session at the joint (both halves closed:
`Mux.drop`, `Done`), and `ended`'s branch for a session closed whole
closed the client at once, outside the hold: a clean end with the tail
unacknowledged, finding 23 back. The model's `RRecv` held every FIN, so
the spec was stronger than the code. Fixed: that branch goes through
`hold` too; mutant `done-skips-hold` (the code before: the same step as
`no-hold`, caught by `NoSilentLoss`); guard
`TestJoinPeerFinDuringSyncIsHeld` (failed before, 3 of 3).
Open: `link.Join` (two link sessions: a relay's forwarding hop) passes a
FIN on without a sync; end to end over several hops the last hop's sync
answers only for its own connection (Join 3, decided). The joint's trace
recorder and `TraceJoin.tla`: "Conformance" below. `LostEnds` (weak
fairness on the joint's and the connection's steps, none on the
client's): a client whose connection died ends, however idle (Join 2,
mutant `no-watch`).

| model | Go |
|---|---|
| `CloseWrite`, `RRecv` (pong), `Finish`, `Lost` | `peerlink/chain.go` (`joint.ended`, `sync`, `hold`, `finish`, `abort`) |
| the ping and its pong | `mux.go` (`Session.Sync`), `conn.go` (`sync`, `synced`, `pingTime`, `keepAlive`) |
| `ARecv` | `conn.go` (`recv`: a ping handed to `keepAlive` after every frame before it) |

## Code map (lane W5-LINK)

The files each model stands for (`scripts/check-spec-drift.sh`: a commit
range that changes one changes `spec/` too, or says why the spec still
holds in a `Spec-Reviewed:` trailer).

| model | what | files |
|---|---|---|
| `Link.tla`, `MCLink.tla`, `Trace.tla` | frames, credit, ends, GOAWAY, ids, the write scheduler's waits | `conn.go`, `credit.go`, `mux.go`, `chain.go`, `read.go`, `internal/bytequeue/`, `trace_on.go` |
| `LinkSession.tla` | the interface: what a session guarantees | `mux.go`, `chain.go`, `conn.go` |
| `Liveness.tla`, `TraceLive.tla` | `keepAlive`, the dead-peer bound, `rxAt` | `conn.go`, `read.go`, `liveness.go` |
| `Pool.tla`, `TracePool.tla` | `Pool`: Min, Max, `Open`'s pick, dial and growth, `Drain`, `Close` | `pool.go`, `trace_pool_on.go` |
| `Join.tla`, `TraceJoin.tla` | `peerlink.Join`'s clean end (`Session.Sync`, hold, the watch) | `peerlink/chain.go`, `join.go`, `peerlink/trace_on.go` |

## Action map (lane CONFORM-link)

Each spec action and the code that performs it, and each write of the
code to state a model stands for and the action it implements. An entry
without a counterpart on the other side is a finding (spec/README.md
"Conformance (lane CONFORM-link)").

**`Link.tla`**: action to code is "Actions and the Go they model" above;
every event the recorder writes (`trace_on.go`'s `traceEvent`, `traceLive`)
is a step of `Trace.tla` or `TraceLive.tla` and each of theirs has a hook
point in the code (`TestSpecTraceEventsMapped`). Code to action: the table
below lists every function that writes a field standing for a model
variable; `TestSpecWritesMapped` (`spec_map_test.go`, go/types over the
package) fails on a writer the table does not list and on one listed that
no longer writes.

<!-- writes: TestSpecWritesMapped -->
| field (model variable) | written by | action |
|---|---|---|
| `Credit.sessions` (`cr.on`) | `Credit.open`, `Credit.forget` | `OpenCheck`/`Open`, `RecvOpen` (open); `Closing`, `RefuseMark`, `Down` (forget) |
| `Credit.opened` (`Count`) | `Credit.open`, `Credit.forget`, `Credit.cutSessions` | the same, and `Down` |
| `Credit.cut` | `Credit.cutSessions` | `Down` (`CloseConn`'s credit end) |
| `sessionCredit.n` (`cr.n`) | `Credit.acquire`, `Credit.refund`, `Credit.Grant` | `Acquire`, `Refund`, `RecvWindow` |
| `sessionCredit.pend` (`pend`) | `Credit.acquire`, `Credit.sent`, `Credit.refund` | `Acquire`, `SendBuffer`/`SendData`, `Refund` |
| `sessionCredit.held` (`cr.held`) | `Credit.chargeLocked`, `Credit.Consumed` | `RecvData`, `Read` (a grant decided: `gpend`) |
| `sessionCredit.consumed` (`cr.cons`) | `Credit.Consumed` | `Read` |
| `sessionCredit.answered` (`ss.acc`, `Answered`) | `Credit.answered`, `Credit.answering` | `RecvAccept` (and the refusal's `Recv` close 4), `Accept`/`RefuseMark` |
| `Session.ans` | `Session.answer`, `Session.AbortWith` | `Accept`, `RefuseMark` |
| `Session.halfClosed` (`ss.wfin`) | `Session.CloseWrite` | `CloseWriteMark` |
| `Session.finSent` (`ss.fin`) | `Session.CloseWrite` | `CloseWriteSend` |
| `Session.peerHalf` (`ss.rfin`) | `Mux.serve` | `RecvClose` code 1 |
| `Session.peerClosed` (`ss.pclosed`) | `Mux.serve`, `Mux.close`, `Mux.OpenChain`, `Session.CloseWrite` | `RecvClose` 0, 2, 3; `Down`; `OpenRefused` (the peer never heard of it); `CloseWriteSend` after the peer's FIN (both halves: `Mux.drop`) |
| `Session.closed` (`ss.st = "gone"`) | `Session.ended` | every end: `End`, `RecvClose`, `Down`, `CloseWriteSend` with `rfin` |
| `Mux.sess` (`ss.st`) | `Mux.add`, `Mux.drop`, `Mux.close` | `OpenCheck`, `RecvOpen` (add); every end (drop); `Down` |
| `Mux.leaving` | `Mux.goAway` | `GoAway` |
| `Mux.left` | `Mux.leave`, `Mux.closeIdle` | `Leave` (`LeaveWhenDrained`); `closeIdle` is `Pool.Close`'s `Leave` without a GOAWAY (`Pool.tla` `CloseSnap`) |
| `Mux.cutting` | `Mux.cut` | `Closing(p, s, ABT)` after a GOAWAY |
| `Mux.done` (`down`) | `Mux.close` | `Down` |
| `Conn.goingAway` (`ga`) | `Conn.GoAway`, `Conn.recv`, `Pool.goAway` | `GoAway`, `Recv` of a GOAWAY; `Pool.goAway` marks leaving just before its `Conn.GoAway`s (C10), the same `GoAway` |
| `Conn.goAwayAt` (`sentGA`) | `Conn.GoAway` | `GoAway` |
| `Conn.lastPing` (the drain barrier's order) | `Conn.pingTime` | `Ping` |
| `Conn.closing` (`wshut`) | `Conn.closeFlushed` | `Leave` |
| `Conn.rxAt` (`Liveness.tla` `rx`) | `Conn.read` | `Liveness.tla` `Recv` |
| `Pool.dials` (`dials`) | `Pool.pick`, `Pool.maintain`, `Pool.dial`, `Pool.Drain` | `Pick` (room; growth: `gr`), `MaintainCheck`, `DialUp`/`DialFails`, `DrainStart` (the first replacement, under the snapshot's lock: C6) and `DrainDial` (`dial`'s `next`: the next one as this one ends) |
| `Pool.conns` (`cs`) | `Pool.dial` | `DialUp`; its Serve goroutine's removal is `Remove` |
| `Pool.changed` (`mtw`, `opw`) | `Pool.changedLocked` | `Changed` in `DialUp`, `DialFails`, `PeerGoAway`, `DrainGoAway`, `Remove` |
| `Pool.growing` (`gr`) | `Pool.pick`, `Pool.grow` | `Pick`'s growth branch, `GrowDial` (M5) |
| `Pool.refused` | `Pool.dial` | unmodelled (L4: maintain's refused backoff, a delay only) |

**`LinkSession.tla`** has no actions of its own: its `Next` is a
transition constraint every step of `Link` must meet, and its variables
are Link's by the refinement mapping `LS` (`MCLink.tla`), checked by
`Refines`. Its code is Link's.

**`Liveness.tla`** (`Conn.keepAlive`, `Conn.read`):

| action | code |
|---|---|
| `Tick` | time passing (no code) |
| `Arrive` | the peer's bytes reach the socket (no code) |
| `Recv` | `Conn.read`: any bytes read set `rxAt` |
| `Die` | the environment (the peer or path stops) |
| `Sleep`, `Wake` | the environment: the process stops running (starved, a stopped VM) and runs again |
| `FireAt(n, r)`, `Fire` | one pass of `keepAlive`'s loop after its timer fired: `liveFire` (now, `rxAt`, `pingAt`, `looked`) decides dead, look, re-arm or ping (`keepAlive` reads the clock, pings, `t.Reset`s) |
| `Stall`, `FireMutant` | mutants only (`reader-writes`, `deadline-from-rx`) |

Code writes: `pingAt`, `looked` and the timer are `keepAlive`'s locals, written only
by `liveFire`'s result (`FireAt`); `rxAt` by `Conn.read` (`Recv`); a pong
answered (`Conn.control`) changes no variable of the model.

**`Pool.tla`** (`pool.go`):

| action | code |
|---|---|
| `MaintainCheck`, `MaintainWake`, `MaintainDial` | `Pool.maintain`: the need check under `p.mu` (its second check reserves: `dials++`), the wait on `p.changed`, `Pool.dial` |
| `DialUp(c)`, `DialFails` | `Pool.dial`'s end under `p.mu`: `dials--`, `changedLocked`, the connection added (closed when `p.ctx` ended) or the error |
| `OpenStart`, `Pick`, `Place`, `OpenDial`, `OpenWake` | `Pool.Open`'s loop: `pick` (best, room, `p.changed` read under `p.mu`), `Mux.OpenChain` (`ErrGoAway`, `ErrFull`, `net.ErrClosed`: pick again), `dial` (Backoff, or its error), `waitChange` |
| `SessionClose` | the application's `Session.Close` (its end on the Mux) |
| `OpenStart`'s growth, `GrowDial` | `pick`'s `growBG` (`PoolConfig.Grow`, room, one at a time: `growing`), `Pool.grow`'s `dial`; `GrowWait`: the Open takes the new connection |
| `DrainStart`, `DrainDial`, `DrainBack`, `DrainGoAway` | `Pool.Drain`: the snapshot, count and first reservation under one `p.mu`, `dial(ctx, next)` per replacement, a failure's `goAway(reps)`, `goAway(old)` |
| `CloseStart`, `CloseEnd`, `CloseSnap` | `Pool.Close`: `p.cancel`, the snapshot, `Mux.closeIdle` on each |
| `PeerGoAway(c)` | the peer's GOAWAY: `Conn.recv`, then `dial`'s watcher goroutine closes `p.changed` |
| `PathDies(c)`, `CloseConn(c)` | the environment; the connection's end (`Liveness.tla`'s verdict, `Mux.leave` once drained, or `Drain`'s ctx deadline closing it: a path death to the model, L2) |
| `Remove(c)` | `dial`'s Serve goroutine once `Mux.Serve` returned: removed from `p.conns`, `changedLocked` |
| `MaintainReserve`, `OpenTakeLate`, `DrainReserve` | mutants only |

Unmapped code, each decided ("Review r2" L4, L7): `maintain`'s sleep
after refused connections (`refused`: a delay, no state; untimed here,
`TestPoolBacksOffRefusingPeer` guards it) and sessions the peer opens on a
Pool's connection (`Accept`: they count in `takes`; `TestPoolAccept` is
traced, a pick they changed would be rejected). The recorder
(`trace_pool_on.go`, M6) logs each step above but the wakes, with the
Pool's view of its sessions' and connections' ends (`End`, `PeerGoAway`,
`Down`).

**`Join.tla`** (`peerlink/chain.go`'s joint of a plain conn and a link
session):

| action | code |
|---|---|
| `CWrite`, `CFin` | the client (environment) |
| `Copy` | `joint.dir(0)`'s copy: `connWriter.Write` onto the session |
| `CloseWrite` | `joint.ended(0)` -> `joint.sync`: `syncing` set, `Session.CloseWrite`, `Session.Sync`'s ping |
| `ARecv`, `AFin` | the peer's `Conn.recv` (and `keepAlive`'s pong); the peer's application's `CloseWrite` |
| `RRecv` (FIN) | `joint.dir(1)`'s Pump ends at the peer's FIN: `joint.ended(1)` -> `hold` while syncing, else `closeWrite` on the client (`Eof`) |
| `RRecv` (pong) | `Session.Sync` returns: `sync`'s goroutine runs the held end (`fin`) and `finish` |
| `Finish` | `joint.finish` of both directions: the client closed |
| `Crash` | the environment (the link connection ends) |
| `Lost` | `joint.abort` of the client: from `ended` (Pump's error, a failed write), `sync` (Sync's error), or `watch` (the session's end while the client is idle: Join 2) |

Unmapped code, outside `Join.tla` on purpose: `Forward`'s answer wait
(`pending`, `held`, `answered`: an onward hop, not a plain Join), a write
deadline's abort of both sides, and the peer's whole close (`cleanClose`:
Join 6, decided). The joint's fields are in package `peerlink`, not
covered by `TestSpecWritesMapped`.

## Conformance (lane CONFORM-link, 2026-11-04)

The pass that checks the code follows each model exactly (owner
2026-11-04): an action map both ways, the real tests' traces checked,
model-based tests replaying TLC's behaviours on the code, the contract the
layer above may assume, and each divergence decided.

**1. Action map**: "Action map" above, for every model;
`TestSpecTraceEventsMapped` and `TestSpecWritesMapped` (`spec_map_test.go`)
fail when a recorder hook point goes or a modelled field gains a writer
the table does not list. Unmapped, found by the map: `Pool`'s growth (now
modelled, M5), Drain's deadline, refused backoff and peer-opened sessions
(decided: L2, L4, L7); `Session.Abort`'s drop (LinkSession 4, decided:
the interface allows a drop or a cut, `Link.tla` models the cut).

**2. Traces** (`make spec-traces`): the session tests named in
`scripts/spec.sh` (now with the refusals: `TestChainRefuse`,
`TestRefusedOpenReadsReset`, `TestWriteAfterRefuseSendsNoData`,
`TestCloseWriteRacingThePeersFINDropsAfterItsCLOSE`,
`TestMuxOpenSkipsAnIDThePeerTook`: LinkSession 8; lane LINKF7:
`TestReceiveLoopReadsWhileARefusalWaits`, F7, and
`TestCloseWriteReportsAFailedFIN`, F9), the dead-peer tests,
and peerlink's `TestJoin*`: their joints against `TraceJoin.tla` and their
link connections against `Trace.tla`. The run prints how often the traces
took each event of `Trace.tla` and of `TraceJoin.tla`; every one is taken
(refusals: `Forget`/`Close` code 4). The joint's recorder
(`peerlink/trace_on.go`, Join 4) logs its own steps (`Copy`, `CloseWrite`,
`Fin` held or passed, `Synced`, `Finish`, `Lost`); the client's, the
peer's and the network's (`CWrite`, `CFin`, `ARecv`, `AFin`, `Crash`) are
TLC's to find between them. Fixtures (`make spec-mutants`): `join-ok`
accepted; `join-fin-during-sync` (the peer's FIN passed on while the sync
waits: `no-hold`), `join-finish-before-synced` (`no-sync`),
`join-lost-twice` and `join-unknown-step` rejected. The Pool's recorder
(`trace_pool_on.go`, M6) logs every Pool test's steps (`TracePool.tla`;
the run prints each event's count and each pick's outcome: place, dial,
full, wait, grow, closed all taken); fixtures `pool-ok` accepted,
`pool-past-max` (a dial past Max), `pool-on-leaving` (a pick of a
connection going away), `pool-closed-pick` (a session placed after Close)
and `pool-unknown-step` rejected.

**3. Model-based tests.** `scripts/spec.sh behaviours` has TLC walk the
models at random (`-simulate`, seeded) and write each walk, one state per
line with the action that led to it (`spec/MBTLink.tla`,
`spec/MBTLive.tla`, an action constraint over `TLCExt!Trace`), into
`testdata/mbt/` (committed; `make check` replays them; nightly a fresh,
larger set with `MBT_DIR`). `TestCreditFollowsTheModel` replays the
credit steps of `Link.tla`'s walks (Link, LinkCrash, LinkSess, LinkRefuse,
LinkDrain and Link without closes, `LinkData`) on two `Credit`s: each
change of `cr`, `pend`, `gpend` is the call the code makes for it, which
must succeed as the model's step did, and after every step each Credit's
session, credit, held, consumed and pend must be the model's.
`TestLivenessFollowsTheModel` replays every `Fire` of `Liveness.tla`'s
walks (`Liveness.cfg` and `LivenessStarve.cfg`, `live-*` and
`live-starve-*`) on `liveFire`, keepAlive's decision (extracted for it,
the loop unchanged): the verdict, the timer's deadline, `pingAt`,
`looked`, and whether it pings. Code mutants each caught (tried once, by hand): a grant at half a
window, a grant that keeps one byte held, a window update bound one short
(Credit); the ping re-armed at Timeout, an idle check off by one
(liveFire). Ceiling: a charge bound one too loose is not caught (the
model's peer never sends past the window: a faulty peer is the fuzzers');
the frames, the Mux and the write scheduler are checked by the traces, not
replayed (a replay needs a deterministic scheduler the code does not
have).

**4. Layer contract** (what relay-core and peerlink may assume of link,
each checked here):

| guarantee | checked by |
|---|---|
| sessions: an id opens once, only before GOAWAY, ends with a typed reason | `LinkSession.tla` `GSessions`, `GEnds` (`Refines` on every Link configuration) |
| writes and reads: in order, no data after this side's FIN, Close or reset, never past the peer's window | `GWrites`, `GReads`; `Link.tla` `CreditBounds`, `BufferBound`, `Conservation`, `FinBeforeShut`, `NoProtocolError` |
| the connection's end ends every session (`lost`); GOAWAY and the end are final | `GConn` |
| sessions end once both applications ended them; a closed connection goes down on both sides; a drained one closes | `SessionsEnd`, `ConnEnds`, `DrainEnds` (`Link.tla` liveness) |
| a dead peer is declared within Interval + Timeout + 2 x Late of its last bytes, never a live one | `Liveness.tla` `DetectBound` (tight), `NoFalseDead`, `NeverEarly` |
| `peerlink.Join`: a clean end to a plain conn only once the peer read its bytes and FIN; a dead link connection ends the conn however idle | `Join.tla` `NoSilentLoss`, `LostEnds` |
| a Pool: no session on a connection going away, within Max but Drain's make-before-break, Min held, every Open progresses, nothing left after Close | `Pool.tla` `NoSessionOnLeaving`, `MaxHolds`, `MakeBeforeBreak`, `MinHeld`, `OpensProgress`, `NoLeak`, `CloseEnds` |

Not a guarantee (stated above, "The session interface"): a session that
ended cleanly at one side had its tail read by the peer; a layer that
needs that syncs (`Session.Sync`) or waits for the peer's answer.
`Session.Sync`'s own guarantee (its pong answers every frame before its
ping) is assumed by `Join.tla` and guarded by `TestJoin*`, not yet a
property of `Link.tla` (LinkSession 3, Join 7: decided, argued below).

**5. Divergences found by this pass**

| # | found by | which side was wrong | outcome |
|---|---|---|---|
| C1 | `Trace.tla` on peerlink's link traces: a `GoAway` logged after `Down` (a Drain of a connection that had closed) | the model, stricter than the code (`GoAway` needed `~down`); the code's GOAWAY on a closed connection goes nowhere | **fixed in the spec**: `GoAway` any time; Link's invariants and `Refines` hold |
| C2 | the action map: `Join.tla`'s `Lost` resets an idle client once the connection dies; the code noticed only at the client's next write (review r2 Join 2) | the code | **fixed**: `joint.watch` aborts the plain conn once its session ends with an error (new `Session.Err`); `LostEnds`, mutant `no-watch` (the code before), `TestJoinLostWhileTheClientIsIdle` (failed before: Join never returned) |
| C3 | `TestCreditFollowsTheModel`: the model's accepter sends its ACCEPT after its own Abort forgot the credit, where `answering` reports false | neither: `sendAccept` ignores `answering` (the ACCEPT goes, as in the model; the peer then reads the ABORT) | **refuted** (the replay's expectation corrected) |
| C4 | the recorder: a `Close` racing a `Write`'s buffer is logged after `ConnClose` (LINKREVIEW) | the recorder, not the code: such a test closes mid-write | **decided**: the traced tests end their sessions before the connection; a step after `ConnClose` stays a rejection (it found C1), so a test that closes mid-write is not traced |
| C5 | LinkSession 5: data after the peer's FIN flagged by the model, dropped by the code | neither (decided in r2) | **decided**, unchanged: the model's flag catches `write-race`/`turns-no-recheck`, which this side never sends |
| C6 | `TracePool.tla`, splitting `Drain`'s reservation as the code made it: `Drain` took its snapshot under `p.mu` and reserved its first replacement under a second; a connection that ended in between was replaced by maintain too, and the Pool stayed past Max after Drain | the code | **fixed**: the first reservation under the snapshot's lock, the next ones as the previous dial ends (`dial`'s `next`); mutant `drain-reserve-late` (`MaxHolds`), `TestPoolDrainReservesWithItsSnapshot` (failed before: 2 connections, Max 1) |
| C7 | `TracePool.tla`: a failed replacement's `goAway(reps)` is a second step after the dial's end, with its own wake; the model did both at once | the model, coarser than the code | **fixed in the spec**: `DrainBack`; `MaxHolds` exempts it (the Pool is past Max until the GOAWAYs: as during the dials) |
| C8 | `TracePool.tla`: `Pool.Close` cancelled `p.ctx` before taking `p.mu`; a pick in between returned `net.ErrClosed` before Close's snapshot | neither: a pick then fails as after Close | **decided**, the code tightened for the recorder: `p.cancel` under `p.mu`, so a pick or dial sees the Pool open or closed with Close's snapshot (`CloseStart`) |
| C9 | `Trace.tla`, peerlink traces under load: `Leave` (`closeFlushed`) logged after `ConnClose`, and a `CloseWrite`'s CLOSE buffered after `Down` cut its session (it checked the session open just before) | neither: both act on a closed connection, the frame goes nowhere, nothing changes | **decided**: the trace maps both to a stutter when the side is closed (and the session cut), as C1 does `GoAway`; no code change |
| C10 | `TracePool.tla`, `TestPool` (3 of 15 runs rejected): a pick saw a connection leaving (the peer's GOAWAY, or Drain's own between its frames and its `GoAway` line) or ended before the watcher, `Serve` or Drain logged it | neither: the trace's order was not the order the Pool acted on | **decided**, the code tightened (as C8): Drain marks its connections leaving under `p.mu` with its `GoAway` line, before the frames; the recorder logs, before each decision under `p.mu`, the ends and GOAWAYs it sees first (their later lines are stutters). 0 of 15 runs rejected after |
| C11 | `Trace.tla`, `TestReceiveLoopReadsWhileARefusalWaits` (lane LINKF7, 1 of 3 runs rejected): a refusal's `Forget` (code 4) logged after `Down` cut the session: `Mux.refuse`'s goroutine ran after the connection's end | neither: its ABORT goes nowhere, nothing changes | **decided**: as C9, the trace maps a refusal's `Forget` and `Close` (code 4) of a session the peer opened to a stutter when the side is closed and the session cut before its refusal began; no code change. Fixture `link-ok-refuse-after-down` (accepted; rejected before) |
| C12 | `Trace.tla`, `TestChainRefuse` under load (lane LINKF7; lane LINKC12 measured 12 of 30 runs with a rejected trace, two shapes): (a) the opener's `SendData` logged after its `Recv` close 4 (the peer's refusal forgot the credit); (b) the refusing side's `Recv` data with credit -1 between its `Forget` 4 and `Close` 4 | (a) code: `Conn.sendData` checked `sendable` under `wmu` once, but the receive loop's `Credit.Forget` does not take `wmu`, so a frame checked before the refusal was read was buffered and logged after it (the peer dropped it: no credit there, WIRE.md section 5); `sendData` promises no data once the credit ended, as the model says. (b) neither: `Conn.sendAbort` forgets the credit, then buffers the ABORT (`wmu` held, the receive loop not stopped: F7), where `End` does both at once; the data goes to no reader either way | **fixed** (lane LINKC12): (a) `frameWaitLocked` checks every data frame again just before it is buffered and `Credit.sendable` logs its `SendData` under the credit's lock, so a `Forget` is either before the check (nothing buffered, `Refund`) or after the logged step; the receive loop takes no new lock. (b) `Trace.tla`'s `RecvStep` takes data dropped (credit -1) on a session that is `Refusing` (marked, ABORT not yet buffered): the model queues it for a session nothing reads, which `End` drops. Fixtures `link-data-after-refusal` (a's trace, rejected), `link-ok-data-during-refusal` (b's, accepted; rejected before) and `link-data-dropped-while-open` (b's with the `Forget` after the data: dropped on an open session, rejected). `TestChainRefuse` traced 30 of 30 runs accepted under `taskpolicy -b` |

## Environment assumptions (lane W5-LINK)

What the models take of the world below link, and where that is stated.

| assumption | models | source |
|---|---|---|
| a connection is one ordered, reliable byte stream each way until it ends; a frame either arrives whole and in order or the stream ends | `Link`, `Join` (FIFOs), `Pool` | RFC 9293 section 3.1 (TCP); RFC 8446 section 5 (TLS records over it); RFC 9000 section 2.2 (a QUIC stream, quiclink) |
| a connection that ends loses what is on the way: any suffix of the frames sent (`CloseAt`'s `k`, `Crash`) | `Link`, `Join` | RFC 9293 section 3.6 (an abort: RST discards queued data); WIRE.md section 1 |
| a process that dies or a path that breaks ends nothing by itself: the peer learns only by silence (the liveness bound) | `Liveness`, `Pool` (`dead`) | RFC 9293 section 3.8.4 (keep-alives are optional, off by default); WIRE.md section 8 |
| timers fire at or after their deadline, late by a bounded amount on a loaded machine (`Late`) | `Liveness` | Go `time.Timer` docs ("at least duration d"); `TraceLive.tla` checks the real fires |
| a process may not run at all for any time (starved, a stopped VM: `Sleep`); its timers and its receive loop then wait and the peer's bytes wait in the socket; once a goroutine blocks on a timer, the runtime polls the network and runs the goroutines it readies before the process stops again (A1) | `Liveness` (`LivenessStarve.cfg`) | Go runtime: `findRunnable` polls the network before it parks a P idle; the OS scheduler (any process may be descheduled); lane RCFOLLOW's measured stalls of 5.1-7.3 s |
| the monotonic clock never goes back (`now`, `rxAt`, ping times) | `Liveness`, `Link` (drain barrier) | Go `time` package docs, "Monotonic Clocks" |
| a goroutine may pause between any two steps (the scheduler): steps under one lock are atomic, nothing else is | every model | Go memory model (sync.Mutex, sync/atomic: sequentially consistent) |
| a dial may fail or hang; the peer may refuse (end a connection early) or send GOAWAY at any time | `Pool` | `pool.go` (`Dial`), WIRE.md section 9 |

## Review findings (2026-11-02, lane LINKREVIEW)

The independent review of this model against the code
(`~/nfltr-evidence/specreview/Link.md`) found the model weaker than the
code in four places: check and send were one step (the write
scheduler's waits not modelled), one session at a time, no
`max_sessions`, ids that could not be reused. The model now has them
(`wt`, `LinkSess.cfg`: two sessions of one side, `MaxSess` 1, opens
racing Close and `Session.Abort`), and TLC finds each finding as a
counterexample of the code before its fix (`make spec-mutants`):

| finding | severity | mutant (the code before) | caught | fix |
|---|---|---|---|---|
| F1 a Write parked for its turn sends after the FIN | high | `turns-no-recheck` | data after FIN, 10 states | `frameWaitLocked` checks again before the frame is buffered (`TestWriteParkedForItsTurnSendsNothingAfterFIN`) |
| F2 an OPEN parked for its turn goes after the drain's pong | medium | `open-no-recheck` (`LinkDrain.cfg`) | open after GOAWAY, 10 | the GOAWAY checked again: `ErrGoAway`, credit undone (`TestOpenParkedPastPeerGoAwayIsNotSent`) |
| F3 an Abort racing an Open at max_sessions ends the connection | medium | `abort-forget-early` (`LinkSess.cfg`) | open past max_sessions, 8 | `sendAbort` forgets under wmu (`TestAbortAtMaxSessionsRacingOpenKeepsTheConnection`) |
| F4 the Mux capped both directions at 1024 | medium | (no state machine: a constant) | | the cap removed, the Conn's per-direction counts rule (`TestMuxHonoursConfigMaxSessions`) |
| F5 an open of an id in use restarted the session | medium | `reopen-known` (`LinkSess.cfg`) | open of an id in use, 7 | a protocol error (`ErrFrame`), `SendOpen` refuses it; WIRE.md section 5 says so (`TestOpenOfAnOpenIDIsRefused`) |
| F6 no invariant said why the receive loop never waits | low | `release-early` | `QueueWithinHeld`, 9 | the invariant |
| F7 the receive loop wrote refusals inline | low | `reader-writes` (`Liveness.tla`, lane LINKPOOL) | `NoFalseDead`, 9 | `Mux.refuse`: a goroutine sends them, the queue bounded by max_sessions (`TestReceiveLoopReadsWhileARefusalWaits`); `Liveness.tla`'s assumption now holds |
| F8 a grant was bounded loosely | low | (a faulty peer: not modelled) | | `Grant` counts a Write's credit not yet sent (`TestGrantCountsCreditInAWrite`) |
| F9 `CloseWrite` returned nil when its CLOSE could not be written (lane LINKF7, the yamux comparison) | low | (a return value: no state changes, so no mutant; `CloseWriteSend` is the same step either way) | | **decided: returned**, as `Write` returns its bytes' failure and `grpclink`/`quiclink` return theirs: `io.ErrClosedPipe` wrapping the connection's end, so a caller knows the peer will never read its `io.EOF`; nil still means buffered, not delivered (`Sync`) (`TestCloseWriteReportsAFailedFIN`) |

Each test failed on the code before its fix. F7 re-checked (lane LINKF7,
after the yamux comparison read "the code today" in the mutant's note as
the code now): no step of `Mux.Serve`'s loop (`Conn.recv`, `Mux.data`,
`Mux.serve`) waits on the network: a pong goes through `keepAlive`, a
refusal through `Mux.refuse`, `Mux.leave`'s close through `closeFlushed`'s
goroutine, a window update from the reader's `Consumed`; what runs on the
loop are `OnGrant` and `Observer` callbacks, documented as not to block.
`TestReceiveLoopReadsWhileARefusalWaits` fails with the refusal sent
inline again, and its trace is checked (`make spec-traces`). Left open then: F7's related
note, the slow frame (fixed, lane W5-LINK: "The Pool and liveness"), and
a `Mux` refusing a peer's open of an id of its own parity (decided, lane
W5-LINK, below).

**Witnesses (lane W5-LINK).** Every invariant has a witness that
`make spec-mutants` requires TLC to violate, so the case the invariant
guards is reached: `NoProtocolError` `WitnessOpenAtMax` (an OPEN on its
way to a side at max_sessions, `LinkSess.cfg`), `CreditBounds`
`WitnessNoCredit` (a writer out of credit), `BufferBound`
`WitnessWindowFull`, `QueueWithinHeld` `WitnessQueueHeld`, `Conservation`
`WitnessInFlight` (data and a window update of one session on the wire
at once), all on `Link.cfg` but the first; `Liveness.tla` `WitnessDead`,
`WitnessLateAlive` (a live peer's timer fired late), `WitnessTight`
(`DetectBound` reached exactly); `Pool.tla` `WitnessBesideLeaving`
(`NoSessionOnLeaving`: a session placed while a connection going away is
up), `WitnessAtMax` (`MaxHolds`); `Join.tla` `WitnessCleanDown`
(`NoSilentLoss`: a clean end, then the connection gone).

## Review r2 (2026-11-03, lane W5-LINK)

The round-2 reviews of `LinkSession.tla` and `Join.tla`
(`~/nfltr-evidence/specreview/r2-link-LinkSession.md`, `r2-link-Join.md`)
and the leftovers of LINKREVIEW, LINKPOOL and LINKIFACE. Each fixed one has
a test that failed on the code before and a mutant that `make spec-mutants`
requires TLC to catch.

| item | severity | outcome |
|---|---|---|
| Join 1: the peer's FIN during the sync ended the session whole and reached the client outside the hold | high | **fixed** (`done-skips-hold`, `TestJoinPeerFinDuringSyncIsHeld`; "Join's clean end") |
| LinkSession 1: the peer's FIN applied between `CloseWrite`'s `halfClosed` and its CLOSE dropped the session; a leaving connection then shut its write side before the CLOSE, and the peer read a graceful end as lost | medium | **fixed**: `CloseWriteMark`/`CloseWriteSend`, the drop on `finSent` (Dekker with `peerHalf`); invariant `FinBeforeShut` (`LinkDrain.cfg`), mutant `drop-on-mark`, witness `WitnessFinRace`; `TestCloseWriteRacingThePeersFINDropsAfterItsCLOSE` |
| LinkSession 2: a `Refuse` racing a `Write` sent data before any answer; the opener ended the whole connection (`ErrFrame`) | medium | **fixed**: `sendData` sends only on credit `sendable` (an ACCEPT went for a session the peer opened); `RefuseMark` and `Refusing` in `Link.tla`, `LinkRefuse.cfg` (invariants, liveness and `Refines` in CI, seconds), mutant `refuse-then-write`, witness `WitnessRefusing`; `TestWriteAfterRefuseSendsNoData` |
| LinkSession 3: the interface states no answer, no Sync guarantee, no liveness | medium | **decided** (lane CONFORM-link): liveness is `SessionsEnd`/`ConnEnds`/`DrainEnds` and the answer is `Link.tla`'s (`RefuseMark`, `Accept`, traced); Sync's guarantee (its pong answers every frame sent before its ping) follows from what `Link.tla` checks, frames in order on one connection (`GWrites`, `Conservation`) and a pong sent only after the ping is read, and holds with a pong replaced by a later one, since pings carry strictly growing times and `Sync` takes any pong at or past its own. It stays `Join.tla`'s assumption; `TraceJoin.tla` checks the joint takes `Synced` only from a pong. A `Sync` action in `Link.tla` would add ping identities to its largest state space for this argument |
| LinkSession 4: an application's `Abort` modelled as `Mux.cut`'s (kept open) | low | **decided** (lane CONFORM-link): a refusal drops (`End`); `Session.Abort`'s drop of an accepted session is the interface's either-or (drop or cut, `GEnds`), and `Link.tla` checks the cut, the one with more steps after it |
| LinkSession 5: data after the peer's FIN is a protocol error in the model, dropped (and its charge kept) in the code | low | **decided**: keep both. The model's flag is how `write-race` and `turns-no-recheck` are caught (this side never sends it); the code tolerates a faulty or older peer (link before the F1 fix could send it in a race) rather than end every session on the connection; the charge it keeps is on a session whose reads have ended |
| LinkSession 6: `GReads` allowed bytes at a side whose session is `none` | low | **fixed**: `GReads` requires the session not `none` |
| LinkSession 7: configurations miss a listener-opened session, crossed opens, data both ways | low | **done** (lane CONFORM-link): `LinkBoth.cfg` (a session of each side opened at once, a byte each way on each, a FIN each way; invariants and `Refines`) in `make spec-nightly` |
| LinkSession 8: traces cover no answers (refusal code 4, `Drop`) | low | **done** (lane CONFORM-link): `Trace.tla` maps code 4 (W5-LINK); the refusal tests are traced, 29 refusals accepted ("Conformance") |
| Join 2: a dead link connection does not end the joint while the client is idle | medium | **fixed** (lane CONFORM-link, C2): `joint.watch`, `LostEnds`, mutant `no-watch`, `TestJoinLostWhileTheClientIsIdle` |
| Join 3: `link.Join` (two sessions, a forwarding hop) passes a FIN on without a sync | medium | **decided**: a clean end is per link: `Join.tla` models one link, `link.Join`'s doc says so; end to end over several hops needs the far end's answer (an application acknowledgement), not a sync per hop |
| Join 4: no trace recorder for the joint | medium | **done** (lane CONFORM-link): `peerlink/trace_on.go`, `TraceJoin.tla`, in `make spec-traces`; fixtures in `make spec-mutants` |
| Join 5: steps the code takes that `Join.tla` does not model | low | **done**: listed in "Action map" (Forward's answer wait, a write deadline, the peer's whole close: outside the model on purpose); the traced `TestJoin*` take none |
| Join 6: a peer's whole close during the client's tail ends the client cleanly | low | **decided**: as WIRE.md section 5 says, a whole close (code 0) is "neither sends nor reads more": bytes after it are dropped by the peer's choice, not lost; `NoSilentLoss` is stated for a peer that reads to the FIN |
| Join 7: Sync's guarantee assumed, not tied to `Link.tla` | low | **decided** with LinkSession 3 |
| LINKREVIEW: one frame slower than Interval + Timeout read as a dead peer | low | **fixed** (`Conn.read`, `TestSlowFrameIsNoDeadPeer`; "The Pool and liveness") |
| LINKREVIEW: a `Mux` takes a peer's open of an id of its own parity | low | **decided and fixed**: a `Conn` peer picks its own ids (WIRE.md section 5), so the `Mux` keeps taking them and `Open` skips an id the peer took instead of failing (`TestMuxOpenSkipsAnIDThePeerTook`, failed before: `session id in use`) |
| LINKREVIEW: the model's OPEN waits for no pending close, stricter than the code's ticket order | low | **decided**: a sound reduction. The code's OPEN may check while a close waits its turn, but that close's ticket is earlier, its credit is already forgotten (the count the check reads is the same) and it goes out first: the same frames in the same order. A trace with `OpenWait` between a `Forget` and its `Close` would be rejected (a false alarm, never a missed bug); no test logs one |
| LINKREVIEW: the recorder logs steps the model lacks when a test closes mid-write | low | **decided** (lane CONFORM-link, C4): such tests are not traced; a step after `ConnClose` stays a rejection |
| LINKPOOL L1: two Drains at once replace the same connections twice | low | **decided**: one at a time; `Pool.Drain`'s doc says so (a second Drain while one runs holds both sets of replacements beside the old ones) |
| LINKPOOL L5: `Pool.cfg` is the smallest shape only | low | **done**: `PoolWide.cfg` (two Opens racing for the last of Max 2, Min 1) in `make spec-nightly`. It found that `OpensOpen` does not hold with two Opens: one that closes and opens again can take the room the other picked each time, and the other re-picks forever. **Decided**: the Pool promises progress, not fairness among Opens (`OpensProgress`); a queue of Opens is the fix if a starved Open is ever seen |
| LINKPOOL M5: background growth not modelled | medium | **done** (lane CONFORM-link): `gr`, `Pick`'s growth, `GrowDial`; `PoolGrow.cfg` in `make spec`, mutant `grow-past-max` |
| LINKPOOL M6: no trace recorder for the Pool | medium | **done** (lane CONFORM-link): `trace_pool_on.go`, `TracePool.tla`, the Pool tests in `make spec-traces`, fixtures `pool-*` in `make spec-mutants`; it found C6 |
| LINKPOOL L2: Drain's wait and deadline cut | low | **decided**: the cut is a connection's end with its sessions, which the model has (a path death); Drain's return is bounded by its ctx, no property needed |
| LINKPOOL L3: Open re-picks at once while `takes` and the Conn disagree | low | **decided**: a short spin, not a hang (the Conn agrees once the dropped session's CLOSE is buffered); `Place`'s retry is fair and `OpensProgress` holds |
| LINKPOOL L4: refused backoff not modelled | low | **decided**: a delay without state; `TestPoolBacksOffRefusingPeer` |
| LINKPOOL L6: slot shortage turns a dial into a wait the code never does | low | **decided**: a model artifact of a finite `Ids`; the trace check sizes slots from the trace, one spare, so no traced dial waits on it |
| LINKPOOL L7: peer-opened sessions not modelled | low | **decided**: outside the model; `TestPoolAccept` traced |
| LINKIFACE: a small crash config in CI | | **done** before this lane (`LinkCrash.cfg`, lane SPECTRIM) |
| LOADTESTS: nfltr's `TestRelayDrain` saw `connection lost` instead of the drain deadline's ABORT | | **decided**: link's guarantee is the interface's: a session cut at the drain deadline ends `reset` (the ABORT read) or `lost` (the connection's end) (`GEnds`). `Mux.cut` writes the ABORTs, half-closes and closes within 2 s: a deadline must end, and a peer too starved to read for 2 s reads the end of the connection instead. The test (nfltr) should accept `lost` past the drain's deadline; no link change |

## Trace validation

With `-tags linktrace` and `LINKTRACE=<dir>`, each `Conn` appends one JSON
line per action to `<dir>/<pair>.ndjson` under one process-wide lock, at
the point where the state changed and under the lock that changed it, so
the file's order is an order the two sides could have taken. `pair` is the
dialer's local address. Each line has the side, the action, the session id
(hex), sizes and the credit after the step, which `Trace.tla` compares with
the model's. `LINKTRACE_FILE=<file>` puts every connection in one file
instead, for one test whose two sides have different addresses (the
dead-peer tests' proxy). A window update is logged when its grant is
spent, also when a closed connection drops the frame. Without the tag the
recorder is a constant `false` and every call is compiled away.

`make spec-traces` runs the tests named in `scripts/spec.sh` (slow-reader
flow control, half close, reset, GOAWAY drain, crossed OPEN, sessions cut
with their connection, many concurrent sessions, and the dead-peer tests'
fast preset) and checks every connection's trace with `Trace.tla`, one TLC
per trace. `TraceLog.tla` holds the parsed file: TLC evaluates an
extended module's constants first, so the file is read once (defined in
`Trace.tla` itself, it was read again for each reference, quadratic).

## Design of the fixes (2026-11-02)

Findings 1, 3 and 5 below are fixed by this design; it landed before the
code.

**A Write ends at this side's FIN (finding 1).**

- Invariant: no data frame of a session is buffered after this side's
  CLOSE of it (code 1, 0 or 2) or its ABORT, and a `Write` reports only
  the bytes it buffered before that frame: for the rest it returns
  `io.ErrClosedPipe` (wrapping why the session was cut, `writeErr`), never
  a nil error.
- Where: `Conn.sendData`, under `wmu`, before the frame is buffered. It
  sends nothing when the session's write side is closed (`halfClosed`,
  which `CloseWrite` sets before it takes `wmu` to buffer its CLOSE) or
  its credit is gone (`Credit.forget`, which `sendClose` runs under `wmu`
  before it buffers the CLOSE, `sendAbort` before it takes `wmu`, and
  `Mux.drop` and the receive of the peer's CLOSE/ABORT run too). Every
  CLOSE and ABORT is buffered under `wmu` after one of the two, so a
  frame that passes the check is ahead of it on the wire. Lock order:
  `wmu`, then `Credit.mu` (as `sendClose` already takes them); no new
  lock.
- The `Write` then gives its credit back (`Credit.refund`, as a write
  deadline does) and returns `io.ErrClosedPipe` with the bytes buffered
  before. A `Write` waiting for credit when `CloseWrite` runs keeps
  waiting until credit comes (the peer reads to the FIN, so it returns
  its window), its deadline passes or the session ends; then it returns
  the same. `Close`, `Reset` and the drain's abort forget the credit,
  which wakes it at once.
- Model: `SendData` needs `~wfin /\ cr.on`; a new `Refund(p, s)` gives a
  taken credit back without sending (any time: a deadline's refund is the
  same step); `CloseWrite` and `Closing` may run while a `Write` holds
  credit. The old behaviour is the mutant `write-race` (`make
  spec-mutants`: TLC finds data after the FIN).
- Test: `TestWriteRacingCloseWriteSendsNothingAfterFIN`: a `Write` of a
  window + 1000 bytes waits for credit, `CloseWrite` runs, the peer reads
  to EOF. The `Write` must return the window and `io.ErrClosedPipe`, the
  peer exactly the window. It fails on the old code (263144, nil).

**Data for a session whose credit is gone is dropped (finding 3).**
WIRE.md section 5 is right and the code changes: a receiver that keeps
no credit for a session neither charges nor acknowledges its data, so
queueing it would buffer bytes outside every window bound; and the bytes
are past a close or an abort this side sent, so no reader should see
them. `Conn.next` marks a data frame of a session the Credit does not
know, and `Mux.data` drops it as it drops one of a session it does not
know. Model: `RecvData` drops (no queue, no `dlv`), and the FIN check of
`RecvClose` applies while the receiver's credit lives.

**Only a ping buffered after the GOAWAY is the drain barrier's (finding
5).** The ping's time is read under `wmu` (`Conn.control`), and once
`goAwayAt` is set (also under `wmu`, when the GOAWAY is buffered) it is at
least `goAwayAt + 1`; a pong counts when its time is past `goAwayAt`
(`>`, not `>=`). A ping buffered before the GOAWAY was timed at or before
`goAwayAt`, one after it past it: the order of the buffer, not the
clock's resolution, decides. Test: `TestPingBeforeGoAwayIsNotTheBarrier`
puts a ping and the GOAWAY on the wire with the same clock reading
(`goAwayAt` set to the ping's time) and checks that its pong does not
close `Drained`, and that the next ping's does.

## Findings

What writing the model and checking it against the code found (the pilot
of 2026-11-01). 1, 3, 4 and 5 are fixed (2026-11-02, "Design of the
fixes"); 2 stays, modelled.

1. **A Write racing a CloseWrite sends data after the FIN** (code; TLC,
   `WriteRace`). `Session.Write` checks `halfClosed` in `acquire`, then
   takes credit and buffers the frame without looking again; a
   `CloseWrite` from another goroutine in between buffers its CLOSE
   (code 1) first. The peer drops the bytes (its queue is closed) and
   still charges them, while the `Write` returns their count and no error.
   `net.Conn` lets Write and CloseWrite run at once; TCP's answer to that
   race is EPIPE, not a silent loss. Reproduced deterministically: a
   `Write` of a window + 1000 bytes waits for credit, `CloseWrite` runs,
   the peer reads: the `Write` returns 263144, nil; the peer reads 262144
   bytes, then a clean `io.EOF`. (The waiting `Credit.acquire` returns ok
   when credit comes, and `Session.acquire` does not look at `halfClosed`
   again.) `Close` has the same window (the
   bytes go to a session the peer forgot), where losing them is what
   closing means. A fix would check `halfClosed` under `wmu` in
   `sendData`, or let `CloseWrite` wait for a running Write. **Fixed**:
   the first ("Design of the fixes"); the model's `write-race` mutant.
2. **The credit ends before the CLOSE is buffered** (model; a rejected
   trace of `TestMuxSessions`). `Conn.sendClose` and `Conn.sendAbort`
   forget the credit, then buffer the frame, which may wait for room in
   the write buffer; window updates and data that arrive meanwhile meet an
   unknown session (ignored, and queued uncharged by the `Mux`). Harmless:
   the session is ending and the peer's sends stay within its credit. The
   model has the two steps (`Closing`, `End`).
3. **Data for a session whose credit is gone is still queued** (model,
   first counterexample). `Mux.data` queues by `Mux.sess`, not by the
   Credit: after an abort (`Mux.cut`) or during 2, received bytes are
   readable though never charged or acknowledged. Bounded by the peer's
   credit; WIRE.md section 5 says such data is dropped, which holds only
   once the Mux dropped the session. **Fixed** in the code: dropped, as
   WIRE.md says ("Design of the fixes").
4. **`Credit.Consumed`'s comment says half a window** ("a window update
   goes out once half a window has"); the code and WIRE.md section 7 use a
   quarter. **Fixed**: the comment says a quarter.
5. **A liveness ping can count as the drain barrier's** (review while
   mapping `Ping`). `Conn.recv` takes a pong as the barrier's when the
   ping's time is `>= goAwayAt`. A keepAlive ping timed just before
   `GoAway` buffers its frame can carry the same clock reading (the
   monotonic clock's resolution: about 42 ns on Apple silicon, 100 ns on
   Windows) and be buffered before the GOAWAY; its pong then marks the
   peer drained before the peer read the GOAWAY, and an OPEN the peer
   sends after it would be refused. Deciding under `wmu` whether a ping
   follows the GOAWAY (a counter of pings buffered, say), not by clock
   readings, closes it. Low odds; not seen in a trace. **Fixed**: the
   ping's time is read under `wmu` and past `goAwayAt` only after the
   GOAWAY ("Design of the fixes").

## Measurements (2026-11-01)

On a 10-core arm64 laptop already at load 10 to 16 from other work, every
run under `taskpolicy -b` (background priority), 4 TLC workers:

| target | result | time |
|---|---|---|
| `make spec`, `Link.cfg` | 2,362,156 states, 678,327 distinct, depth 35; invariants and liveness hold | 6 min 42 s (invariants alone: 31 s) |
| `make spec`, `LinkDrain.cfg` | 1,755,547 states, 485,277 distinct, depth 31; invariants and liveness hold | 8 min 49 s (invariants alone: 16 s) |
| `make spec-mutants` | 4 of 4 caught (`WriteRace` then a finding, not a mutant), 7 to 9 states deep | 6 s |
| `make spec-traces` | 13 traces (8 to 1,209 lines), 13 accepted | 55 s |

After the fixes (2026-11-02, same laptop, load 7 to 11, 4 TLC workers,
background priority; `make spec-traces` ran beside the second check):

| target | result | time |
|---|---|---|
| `make spec-invariants` | `Link.cfg` 4,230,188 states, 1,130,912 distinct (the closes during a Write and `Refund` add them), depth 35; `LinkDrain.cfg` as before | about 1 min (GitHub's runner: 24 s + 9 s, the job 44 s) |
| `make spec`, `Link.cfg` | 4,230,188 states, 1,130,912 distinct, depth 35; invariants and liveness hold | 12 min 57 s |
| `make spec`, `LinkDrain.cfg` | 1,755,547 states, 485,277 distinct, depth 31; invariants and liveness hold | 21 min 41 s |
| `make spec-mutants` | 5 of 5 caught, 7 to 9 states deep (`write-race`: data after FIN, 8) | 6 s |
| `make spec-traces` | 14 traces (8 to 15,695 lines), 14 accepted | 16 min 20 s |

A dead-peer trace's length depends on how much the sessions moved
before the cut: 115 to 15,068 lines seen; TLC checks about 40 lines a
second at background priority (400 at normal priority), one state per
line.

## The Pool and liveness (2026-11-02, lane LINKPOOL)

Two more models, each small enough for CI (`make spec-invariants` checks
both in full, liveness included; `make spec-mutants` their mutants).

**`Pool.tla`** (`pool.go`): the connections a Pool keeps to one peer, the
sessions `Open` places on them, `maintain`'s Min, `Drain`, and what the
peer and the network do (GOAWAY, a dead path, a failed dial). Sessions
are modelled only by what `Link.tla` guarantees: a connection that ends
ends its sessions with an error, `OpenChain` refuses once either side
sent GOAWAY (`ErrGoAway`) or the connection closed, and the side that
sent GOAWAY closes the connection once its sessions ended. A dead path is
detected eventually (the bound is `Liveness.tla`'s). `p.changed` is a wake
flag per waiter (`maintain`, each `Open`'s `waitChange`), not a counter,
so the state space is finite. `Pool.cfg`: Min 1, Max 1, one `Open`
racing `maintain`, three connection slots, two environment events; about
7 s.

- Invariants: no session goes on a connection going away or closed
  (`NoSessionOnLeaving`); concurrent `Open`s and `maintain` never dial
  past Max, only `Drain`'s replacements do until its GOAWAYs (`MaxHolds`).
- Liveness (weak fairness on the Pool's goroutines and the connections'
  ends, none on the application or the environment, so a session may
  stay open forever): once the environment stops, the Pool has Min
  connections that take sessions (`MinHeld`); every `Open` ends with a
  session (`OpensOpen`).
- Mutants: `no-wake-on-goaway` (finding P1, the code before),
  `drain-past-max` (finding P2, the code before), `open-on-goaway`
  (`OpenChain` without its GOAWAY check), `maintain-no-recheck`
  (`maintain` reserves its dial on what it decided before letting go of
  `p.mu`: an `Open` dialled meanwhile, past Max), `wait-takes-late`
  (finding H1: `Pick`'s wait split from the step that takes `p.changed`),
  `no-wake-on-dial-fail` (finding H2: a failed dial leaves `p.changed`
  open). An `Open` whose dial failed picks again (Backoff) or returns the
  dial's error (Backoff nil, its ctx ended); `OpensOpen` counts both.
- Since lane POOLFIX (review r2 M1-M4) the model has a session cap per
  connection (`Cap`, the peer's max_sessions: a full connection counts
  against Max but takes no session; `Open` fails `ErrFull` when such ones
  hold the room), `Close` (cancel `p.ctx`, then close a snapshot of the
  connections; a dial ending after the cancel closes its own) and these
  checks: `MakeBeforeBreak` (a Drain whose old connections counted against
  Max has a replacement before its GOAWAYs), `NoLeak` (no connection open
  once `Close` returned), `CloseEnds` (after `Close` every connection ends
  and is dropped); `MinHeld` holds until `Close`. Mutants
  `drain-counts-takes` (M1), `drain-fail-keeps` (M2, on `PoolDrain.cfg`:
  Min = Max = 2, no `Open`, invariants only, full run in `make spec`),
  `dial-no-closed-check`, `snapshot-before-cancel` (M3). Witnesses, each
  violated by TLC (`make spec-mutants`): `WitnessOpen`, `WitnessWait`,
  `WitnessReplaced`, `WitnessPastMax`, `WitnessClosed`. `-coverage 1` on
  `Pool.cfg`: every action taken but the mutants' own (`MaintainReserve`,
  `OpenTakeLate`, `DrainReserve`) and `GrowDial` (Max 1: no room to
  grow). `PoolGrow.cfg` (Min 1, Max 2, in `make spec`): background growth
  (M5), mutant `grow-past-max` caught by `MaxHolds`. `drain-reserve-late`
  (C6, the code before) caught by `MaxHolds` on `Pool.cfg`. Decided, not
  modelled: concurrent Drains (L1), the refused backoff (L4), sessions the
  peer opens (L7).

**`Liveness.tla`** (`Conn.keepAlive`) in discrete time: the last frame
received (`rxAt`), the ping waiting for an answer (`pingAt`), the one
timer, which fires at its deadline or up to `Late` ticks after (a starved
process: the Go timer and the goroutine that reads it), a peer that
answers a ping within `Rtt < Timeout` while alive and goes silent at any
time. `Liveness.cfg`: Interval 2, Timeout 4, Late 1, Rtt 3, Gap 1; under a
second. `LivenessStarve.cfg`: the same with a process that stops running
up to twice for as long as it may (Rtt 1); seconds ("Starvation" below).

- `NoFalseDead`: a peer that answers within Timeout is never taken for
  dead, however late the timer fires. The verdict is anchored at the
  ping (`now - pingAt >= Timeout`), not at the last frame: a ping that
  went out late still gets its full Timeout.
- `NeverEarly`: never sooner than Interval + Timeout after the last frame.
- `DetectBound`: a silent peer is declared dead by Interval + Timeout +
  2 x Late after its last frame, and that bound is tight (TLC finds a
  behaviour that needs all of it). Only two fires add their lateness:
  the ping's (each earlier fire re-arms at `rxAt + Interval`, anchored at
  the frame, so their lateness does not add up) and the verdict's
  (anchored at the ping).
- Mutants: `rearm-timeout` (after the ping the timer looks again Timeout
  on, not Interval: the next ping goes late, `DetectBound` breaks),
  `deadline-from-rx` (the verdict at `rxAt + Interval + Timeout` whatever
  the ping's time: `NoFalseDead` breaks once a fire is late),
  `reader-writes` (finding F7 of the spec review, the code before lane
  LINKREVIEW: the receive loop waits on a write, `NoFalseDead` breaks).
- Assumption: the receive loop never waits on the network, so a live
  peer's frames are read as they arrive. True since lane LINKREVIEW
  (review F7, "Review findings"): `Mux.serve` hands an OPEN's refusal to
  `Mux.refuse`, whose goroutine buffers and flushes it; the queue is
  bounded by `max_sessions` (a refused session keeps its credit, and
  counts, until its refusal is buffered; an OPEN past the max ends the
  connection, `ErrFull`). The slow frame (fixed, lane W5-LINK): `rxAt`
  was set per frame header, so one frame slower to arrive than Interval +
  Timeout (64 KiB from a `Mux`, up to 1 MiB on the wire, on a slow path)
  read as a dead peer. Now every read of the connection that returns bytes
  sets it (`Conn.read`): the model's "a frame arrives" is any bytes
  arriving, so `Liveness.tla` is unchanged. Test:
  `TestSlowFrameIsNoDeadPeer` (failed before: both a read-ahead and a
  straight read declared the peer dead mid-frame).

**`TraceLive.tla`** checks keepAlive's recorded steps against
`Liveness.tla`: with `-tags linktrace` each connection also writes
`<trace>.ndjson.live`, one line at keepAlive's start (`Live`: Interval,
Timeout), one per timer fire (`Fire`: now, `rxAt`, `pingAt`, `looked`, as read) and
one when it ends the connection (`Dead`), times in microseconds since the
Conn's `t0`, floored (a deadline fires at or after it, so every
comparison keeps its outcome). Each side's steps must be `FireAt` steps
of the model: no fire before the model's deadline, the model's `pingAt`
at each, `Dead` exactly when the model ends the connection, and
`NeverEarly`. `make spec-traces` checks every `.live` file the traced
tests write (the dead-peer tests' take each `FireAt` branch);
`make spec-mutants` checks the fixtures in `spec/traces/`: `live-ok`
accepted, `live-dead-early` (dead before Timeout after the ping) and
`live-fire-early` (a fire before its deadline) rejected; and the session
trace `link-ok` (TestSessionCleanEnd's), `link-ok-refuse-after-down`
(C11) and `link-ok-data-during-refusal` (C12) accepted by Trace.tla, each
`link-*` beside it rejected: a bug put in per invariant of Link
(`link-credit-not-taken` CreditBounds, `link-charge-past-window`
BufferBound, `link-read-past-queue` QueueWithinHeld,
`link-grant-from-nothing` Conservation, `link-reused-id`
NoProtocolError, `link-fin-after-shut` FinBeforeShut,
`link-data-after-fin` GWrites, `link-data-after-refusal` DataOK (C12: data
buffered after the peer's refusal was read), `link-data-dropped-while-open`
RecvStep's credit (C12: data dropped on a session not refusing), `link-unknown-step` an unmodelled
event). The Pool has no recorder yet (left: `maintain`, `Open`'s pick and dial, `Drain`).

### What this means for the tests (finding P3)

The product promises detection within Interval + Timeout of the peer's
last frame, on a process that runs when its timers are due. The
dead-peer tests (`deadpeer_test.go`) measure from the last frame the
connection received (`rxAt`), check `NeverEarly` exactly, and allow
`DetectBound`'s lateness as measured during the run: a probe goroutine
sleeps in short steps beside the connection and records how late its own
timers fire; the bound is Interval + Timeout + 3 x that (the ping's fire,
the verdict's, and the wake of the goroutine that observes the end) +
a fixed allowance for the frames and wakeups of the path. On a starved
machine the probe is late too, so the test fails only when the mechanism
is wrong: a ping that never went out, a verdict anchored wrong, a timer
never re-armed. `TestDeadPeerBusy` failed in promote with
`dialer 15.517s, bound 15s` on a loaded Mac under `-race`: the
mechanism's own lateness, not a fault.

### Findings

- **P1. A Pool stays below Min while a connection the peer sent GOAWAY on
  carries sessions** (TLC, `MinHeld`; medium). `maintain` sleeps on
  `p.changed`, which closes when a connection is added or removed, not
  when one stops taking sessions; a connection going away no longer
  counts toward Min (nor against Max), so with a session that runs on
  (a tunnel open for days) the Pool keeps fewer than Min connections,
  and each `Open` dials in its own time. **Fixed**: the Pool closes
  `p.changed` when the peer's GOAWAY arrives on one of its connections
  and after `Drain` sent its GOAWAYs. Test:
  `TestPoolMinReplacesAConnectionGoingAway` (fails before: no second
  connection in 5 s).
- **P2. `Drain` dials past Max** (TLC, `MaxHolds`; low). It dialled
  max(Min, 1) replacements whatever the old connections were: when they
  were already going away (they count against nothing) or fewer than
  Min, the replacements and the dials in flight ended above Max for as
  long as they lived. **Fixed**: `Drain` replaces the connections that
  take sessions, at most one replacement each (`maintain` brings the
  Pool back to Min); it still sends GOAWAY on, and waits for, every old
  connection. Test: `TestPoolDrainStaysWithinMax` (fails before: 3 dials,
  2 connections at Max 1).
- **P3. The dead-peer tests failed on a starved machine** (test; see
  above). **Fixed** in the tests; `keepAlive` is unchanged (`Liveness.tla`
  holds for it).
- **H1. `Open` could wait forever after a dial it waited for had ended**
  (review r2 of `Pool.tla`, TLC `OpensOpen` with `wait-takes-late`; high).
  `pick` decided to wait under `p.mu`, let it go, and `waitChange` took
  `p.changed` later: a dial ending in between closed the channel before,
  so `Open` waited on the next one (with a background ctx, for ever). The
  model took the channel in `Pick`'s step, so it was stronger than the
  code. **Fixed**: `pick` returns the `p.changed` it read under `p.mu`
  and `Open` waits on that one. Test: `TestPoolOpenWaitsOnTheChangeItsPickSaw`.
- **H2. A failed dial woke no one** (review r2, TLC `MinHeld` and
  `OpensOpen` with `no-wake-on-dial-fail`; high). `dial` closed
  `p.changed` only when a connection came up. `maintain`, waiting because
  an `Open`'s (or a growth) dial counted toward Min, stayed below Min
  once that dial failed and its `Open` returned; an `Open` waiting for
  `maintain`'s dial never failed with the dial's error (Backoff nil) and
  waited for its ctx. The model hid it: a failed `Open` dial always picked
  again. **Fixed**: every dial's end closes `p.changed`. Tests:
  `TestPoolOpenFailsWithTheDialErrorWhileMaintainDials`,
  `TestPoolMinRedialsAfterAnOpensDialFails`.
- **M1. `Drain` broke before it made when the old connections were full**
  (review r2, `MakeBeforeBreak` with `drain-counts-takes`; medium). It
  counted replacements by `takes`, which excludes a connection at its most
  sessions, yet such one counts against Max and got its GOAWAY: a busy
  Pool got no replacement. **Fixed**: replacements are counted by what
  counts against Max. Test: `TestPoolDrainReplacesAFullConnectionFirst`.
- **M2. A `Drain` failing halfway left the Pool above Max** (review r2,
  `MaxHolds` on `PoolDrain.cfg` with `drain-fail-keeps`; medium). The
  replacements already dialled stayed beside the old connections.
  **Fixed**: they get GOAWAY. Test: `TestPoolDrainFailingHalfwayStaysWithinMax`.
- **M3. `Close` was not modelled** (review r2; medium): now `NoLeak` and
  `CloseEnds`; the code held (no change).
- **M4. `Open`'s error exits were not modelled** (review r2; medium): an
  `Open` now ends with its dial's error, `ErrFull` or `net.ErrClosed`.

## Starvation (2026-11-04, lane LIVESTARVE)

**Finding S1** (medium, fixed). A process that does not run for longer
than its Timeout (5.1-7.3 s measured under two GOMAXPROCS=1 low-priority
test processes with build loops, lane RCFOLLOW; link's `falsedead` cell at
0.1 CPU with `fast`) took a live peer for dead. The pong it waited for was
in the socket; when the process ran again, `keepAlive`'s timer (long
overdue) ran before the receive loop and saw `rxAt` older than the ping.
quiclink's `watch` and quic-go's idle timeout judge silence the same way
(wall clock, before the queued packets are read).

**Invariant.** A peer is declared dead only after silence that this
process observed while it was able to run, and a running process still
declares a silent peer dead within the preset's bound: Interval + Timeout
after the later of its last read and the moment it ran again, plus the
lateness of the two fires that decide it (`DetectBound`). Time the process
did not run is not observed silence.

**Failure model.** The process may stop running between any two steps for
any time (`Sleep`), up to twice in the model; its timers fire late by that
much and the peer's bytes wait unread. A1 (Environment assumptions): once
`keepAlive` blocks on its timer, the runtime runs the receive loop on what
arrived before the process can stop again. A live peer's pong comes
within an Interval of the ping (`Rtt < Iv`, `LivenessStarve.cfg`); on a
process that keeps running any `Rtt < Timeout` holds as before
(`Liveness.cfg`).

**Design.** The verdict needs a look. A fire that finds nothing read since
the ping is a look (`looked`); the peer is dead only at a later fire that
again finds nothing read and is at least Timeout after the ping. A look
re-arms at `max(pingAt + Timeout, now + Interval/4)`: after a starvation
the first fire is always a look, the receive loop then reads what waited,
and the verdict comes a quarter interval later at the earliest. On a
running process the look is the fire at `pingAt + Interval` that the loop
already took, so detection is unchanged whenever `Interval/4 <=
Timeout - Interval - lateness` (every preset: Timeout >= 2 x Interval);
with Timeout = Interval the verdict comes Interval/4 later.

quiclink's `watch` (no model: it samples, it has no verdict state beyond
`seen`) applies the same rule as a discount: a tick that comes more than
1.25 periods after the one before (the process did not run) moves `seen`
on by that gap, so the silence it measures is time it was able to run, and
the tick after it judges with what quic-go read meanwhile. quiclink sets
quic-go's own idle timeout to a backstop (4 x (Interval + Timeout)) so the
watch is the one judge; against a peer that advertises a smaller idle
timeout quic-go uses that (QUIC takes the smaller of the two).

Checked: `Liveness.cfg` and `LivenessStarve.cfg` (`-coverage 1`: every
action taken; `Sleep`/`Wake` only in the second); mutants
`verdict-unlooked` (the code before: `NoFalseDead`, 12 states, the
measured shape: a pong waiting, the process back, the timer first) and
`look-no-gap` (the look re-armed at `pingAt + Timeout`, already past)
caught on `LivenessStarve.cfg`, both hold on `Liveness.cfg` (no false
dead without starvation); `never-dead` caught by `DetectBound`; witness
`WitnessStarved` (a live peer's fire due with its ping unread for Timeout:
reachable only with `Sleep`). Trace fixtures: `live-dead-starved` (dead at
a late first fire) rejected, `live-ok-starved` (the look, then the read)
accepted. Test: `TestStarvedProcessKeepsALivePeer` (failed before).

**The 15 s detection with busy sessions** (yamux comparison, `part`,
`netchange`): link's bound is Interval + Timeout from the last frame read
(15 s at `default`, 4.5 s at `fast`), and a busy connection's last frame
is the moment the path broke, so it is reached in full. yamux pings every
Interval whatever the traffic and fails the ping's write after its 10 s
timeout: dead 10-15 s after the break, uniform over where in the ping
period it fell (12.5 s on average, 12.7-13 s measured); idle link
connections measured 13 s for the same reason (their last frame is a ping
or pong before the break). Both worst cases are 15 s; link's is its
documented bound and this lane does not change it.

**Open.** The two RCFOLLOW failures with at most 1.7 s of stall (A's 10 s
watch fired, B silent for 10 s while its process ran) are not explained by
this: no fire in them was late past a period. Not reproduced here; needs
qlog of both ends (B's connection may have died first and its
CONNECTION_CLOSE been lost).
