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
| `CloseWrite(p, s)` | `Session.CloseWrite` (and `CloseWriteSync`): `closeWrite` -> `Conn.sendClose` code 1, in two steps: `CloseWriteMark` (`halfClosed` set) and `CloseWriteSend` (the CLOSE buffered, `finSent`; review r2 LinkSession 1). Trace.tla takes the two as one (`Close` code 1) |
| `Drop(p, s)` | `Mux.drop` after `closeWrite`'s CLOSE ended both halves (trace `Drop`): the session leaves `Mux.sess`; its credit went with the CLOSE (`Credit.halfClosed`, lane LINKDROPSPLIT) |
| `LateAbortForget(p, s)`, `LateAbortSend(p, s)` | `Session.Abort`/`AbortWith` of a session already gone at `p`: `Conn.sendAbort` (trace `Forget` 3, `Close` 3) forgets nothing more and buffers an ABORT the peer drops (Trace.tla only) |
| `RefuseMark(p, s)` | `Session.AbortWith`/`answer`'s refusal: `ans` taken, the credit forgotten (`Credit.forget` code 4, trace `Forget` 4); its ABORT then is `End` (trace `Close` 4) |
| `Closing(p, s, code)` | `Conn.sendClose` (code 0 from `Session.Close`, 2 from `Session.Reset`) and `Conn.sendAbort` (3, from `Session.Abort`/`AbortWith`): the credit forgotten (`Credit.forget`) |
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
| `CutBegin(p, q)`, `CutWait(p)` | `Mux.cut`'s first call (`cutting` set, the snapshot `q` of `Mux.sess` under `m.mu`; trace `Cut`, n: its size) and the second (it waits for `cutEnded`; trace `CutWait`) |
| `CutForget(p, s)`, `CutSend(p)` | the cut's goroutine: `Conn.sendAbort` of each session of the snapshot (`cutAbort`): the credit forgotten (trace `Forget` 5), then the ABORT buffered (`DDL`; trace `Close` 5) |
| `CutFlush(p)`, `CutClose(p)` | `closeFlushed` once every ABORT is buffered (trace `Leave`); the cut's closes: at once with no ABORT to send, at closeFlushed's 1 s or the cut's 2 s (trace `ConnClose`) |
| `CutEnd(p)`, `CutReturn(p)` | the connection closed: `cutEnded` closed (trace `CutEnd`); a call of cut returns (trace `CutRet`) |
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
   An OPEN read once the drain deadline's cut began is refused
   (CodeDraining); once the write side shut, every session the peer opened
   that this side read before has its end on the way (`OpensAnswered`,
   "The crossed open's refusal").
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
- `drop-forgets-late` (`LinkSessHalf.cfg`): a CLOSE that ends both
  halves leaves the credit to the drop after it (the code before lane
  LINKDROPSPLIT): the peer opens again on reading it, past max_sessions;
- `open-after-goaway-unanswered` (`LinkCut.cfg`): an OPEN read after
  the cut's snapshot runs (the code before lane LINKOPENREFUSE) and gets
  no ABORT (`OpensAnswered`); `shut-before-refusal` (`LinkCut.cfg`): the
  write side shuts with a refusal still to buffer;
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
`LinkDrain.cfg` and `LinkSess.cfg` (`Crash = 0`) and on `LinkCrash.cfg`
and `LinkSessHalf.cfg`;
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

## A joint's side closed whole (JoinClose.tla, 2026-11-12, lane REFUSALFLAKE)

nfltr's connect conformance `TestE2EE/exit_10` flaked on Linux: a client
refused by an organisation service read EOF without the agent's TLS
alert ("cut in transit", exit 6 for 10). The agent writes the alert,
then closes its session whole; the relay's joint (`link.Join`, and
`peerlink.Join` toward a plain conn) copies the alert toward the client
while the client's own bytes (its last handshake flight's tail) go the
other way. Once the agent's CLOSE is read, the write onto its session
fails (`io.ErrClosedPipe`), and `ended` closed the client's side at
once: the other direction, still writing the alert, failed and dropped
it, and the client read a clean close without it. Silent loss of the
peer's last bytes, whatever they are.

Design: the failed write ends its direction only. The client's side is
closed whole by the later of the two directions: by the other direction
at its end (link's `ended`: `Close` rather than the half close it gives
after the peer's FIN, `joint.whole`; peerlink's: its end, then `finish`
closes both), or by the failing direction when the other ended first
(`joint.over`). `JoinClose.tla` (`JoinClose.cfg`, `make spec` and
`make spec-invariants`, 199 states, every action taken): B's peer writes
K bytes, maybe its FIN, then its whole close; the joint's two directions;
A's peer writes a byte and reads. Invariant `TailDelivered`: A's peer
reads all K bytes before A's end. Liveness `AClosedInTheEnd`: A is
closed whole (a client still writing is never left on a half-open
session). Mutants: `close-at-write-fail` (the code before, caught by
`TailDelivered`), `no-whole` (the failed write leaves A to a half close,
caught by `AClosedInTheEnd`). Witnesses: `WitnessFailFirst`,
`WitnessHalfThenWhole`. Guards: `link` `TestJoinWholeCloseDeliversTheTail`
and `peerlink` `TestPeerWholeCloseDeliversTheTail` (both failed before,
5 of 5: the client read the first window, or nothing, then EOF). Open:
the joint's trace recorder logs no step of this path (a failed write,
`over`, `whole`), so no trace is checked against `JoinClose.tla`, and
`TraceJoin.tla` (whose `Join.tla` leaves the whole close out) would
reject one: the peerlink guard is outside spec-traces' `^TestJoin`.

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

`Join.tla` (`Join.cfg`, `make spec` and `make spec-invariants`, 723
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
A drain (lane LINKDRAIN, 2026-11-08: peerlink `ChainDrainMiddle` on a
Reverse pair failed about 1 in 75 runs under load, the joined session
reset after every byte arrived): the joint's side of the session's
connection drains (`Drain`); once the session ended at its Mux (the
joint's FIN buffered, the peer's read) the leaving connection writes
every buffered frame and shuts its write side (`Leave`, `Mux.leave`), the
peer reads to the end and closes (`PeerClose`), its pongs not yet written
lost (`PFlush` writes one), and the connection ends cleanly (`ConnEnd`).
The code before buffered Sync's ping in a step after the CloseWrite, so
a leave between the two never sent it and the clean end reset the
client. Invariant `DrainNeverResets`: a reset only after a `Crash`.
Fixed: `Session.CloseWriteSync` buffers the CLOSE and Sync's ping under
one write lock (`Conn.sendCloseSync`), the Sync pending before the peer
can read the CLOSE, and `Mux.leave` closes only once every Sync's pong
was read (`Leave` not while `d0` is `sync`). A first fix registered the
Sync just after the CLOSE was flushed: the far end's close answering the
CLOSE ended the session and the leave closed first (1 in about 3,600
runs; the same step as `ping-late`). Mutants `ping-late` (the code
before) and `leave-ignores-sync` (the ping with the CLOSE, the leave not
waiting: the peer's close loses the pong), both caught by
`DrainNeverResets`; witness `WitnessDrained`. Guards: link
`TestCloseWriteSyncOnALeavingConnection` (failed before: CloseWrite then
Sync), peerlink `ChainDrainMiddle` under `GOMAXPROCS=2 -race`, 4 at once
x 150 (failed 3 of 4 before; after, 0 in 30 such rounds).

| model | Go |
|---|---|
| `CloseWrite`, `RRecv` (pong), `Finish`, `Lost` | `peerlink/chain.go` (`joint.ended`, `sync`, `hold`, `finish`, `abort`) |
| the ping and its pong | `mux.go` (`Session.Sync`), `conn.go` (`sync`, `synced`, `pingTime`, `keepAlive`) |
| `ARecv` | `conn.go` (`recv`: a ping handed to `keepAlive` after every frame before it) |

## Credit's waiters (CreditWaiters.tla, 2026-11-05, lane LINKWAKE)

`Credit`'s methods are safe from any goroutine, so several senders may
`Acquire` on one session at once (relay's `serverStream.Send` did: lane
FLAKECC, 427 of 500 frames, every sender waiting). `Link.tla` gives a
session one writer and takes `Acquire` as one step; `CreditWaiters.tla`
is the wait inside it: `Waiters` acquiring `Need[w]` bytes `K` times each
on one session of window `W`, the receiver reading the bytes one at a
time and granting each quarter window back (`Consumed`), the grants in
order.

| action | the Go it models |
|---|---|
| `Check(w)` | `Credit.acquire`'s check under the lock: take the bytes (and send them), or wait on the session's wake channel, taken under the lock |
| `Read` | `Credit.Consumed`: a byte read, a window update once a quarter window was |
| `Grant` | `Credit.Grant` (and `refund`): the credit grows, `wakeAll` |

The code before woke waiters through one token of capacity one
(`sessionCredit.wake`, filled by `Grant`, emptied by a waiter): a grant
woke one waiter, which found too little for itself or took its bytes
and left the rest, and the others slept with credit there; the receiver
grants only for bytes it read, so nothing woke them. The design: a grant
closes the session's wake channel (`wakeAll`), so every waiter checks
again; the first waiter after it makes the next channel (none is made
while no sender waits).

- **`NoStrandedWaiter`** (invariant): no waiter sleeps while the session
  has the credit it waits for, unless it is woken.
- **`Conservation`**: credit, bytes on the wire, bytes read and not yet
  granted, and grants on the way make up the window.
- **`AllDone`** (liveness, weak fairness on every step): every `Acquire`
  completes. It needs `Need[w] <= W - T + 1` (`T` a quarter window): the
  receiver keeps up to `T - 1` read bytes ungranted, in any design.

`CreditWaiters.cfg` (in `make spec` and `spec-invariants`): three waiters
needing 5, 2 and 1 bytes, twice each, `W` 8 (grants of 2): 1,638
distinct states, depth 31, under a second; every action taken
(`-coverage 1`; `Grant`'s one-slot branch only by the mutants).
Mutants (`make spec-mutants`): `one-slot` (the code before) breaks
`NoStrandedWaiter` and, alone (`CreditWaitersLive.cfg`), `AllDone`;
`pass-on` (one-slot, and a waiter that takes its bytes and leaves credit
fills the token again: right only while every waiter needs the same)
breaks `NoStrandedWaiter` (a waiter needing less sleeps until the next
grant) and keeps `AllDone`. Witnesses (violated): `WitnessTwoWait`,
`WitnessTooLittle` (a waiter with credit there, too little for it),
`WitnessWakeAll` (one grant waking two). The guard:
`TestConcurrentAcquiresShareAGrant` (credit_wake_test.go: waiters of 5, 2
and 1 bytes, a grant of 3; red 20 of 20 before the fix).

## The drain's cut (Link.tla "Mux.cut", 2026-11-09, lane LINKATOMIC)

CONNDRAINFLAKE found a race in `Mux.cut` (fixed in 7dabd5e, guard
`TestDrainReturnsAfterTheCutItRaced`): the drain's timer and Drain's ctx
both call it, and the second returned while the first was still sending
its ABORTs. The model had the cut as per-session `Closing(ABT)` steps with
no Mux-level action and no Drain return, so TLC had no state where it
could happen; `-race` cannot see it (every step is locked) and a load
test rarely hits the window. This lane audited every spec action whose
code runs in more than one locked step or is entered from more than one
goroutine or timer.

| action | code | steps in the code | entered from | atomic in the code | in the model |
|---|---|---|---|---|---|
| the drain's cut | `mux.go` `Mux.cut` | `m.mu`: `cutting`, snapshot, `fail`; per session `Conn.sendAbort` (`wmu`: forget; frame, may wait); `closeFlushed` (`wmu`: flush; unlock; `CloseWrite`; wait for the peer's close, 1 s); 2 s timer; `Mux.Close`; `close(cutEnded)` | goAway's `AfterFunc` (timer goroutine), Drain's ctx | no: 6 steps, 2 callers | **split** (was: per-session `Closing(ABT)`): `CutBegin`, `CutWait`, `CutForget`, `CutSend`, `CutFlush`, `CutClose`, `CutEnd`, `CutReturn` |
| Drain's return | `mux.go` `Mux.Drain`, `lateErr` | `goAway`; select on `Done` or ctx; ctx: `cut` | the caller | no: returns after `cut` returns or the connection closed | **added**: `CutReturn`; `DrainReturnsAfterCut` (a return on `Done` is after `closed` by construction) |
| goAway / drain | `mux.go` `Mux.goAway`, `conn.go` `Conn.GoAway` | `goingAway` (no lock); `wmu`: GOAWAY, `goAwayAt`, flush; `ping` (`wmu`); `m.mu`: `leaving`; timer; goroutine waits for `drained` -> `leave` | Drain, `Pool.goAway` | no | already split: `GoAway`, `DrainPing`, `LeaveWhenDrained`; `goingAway` before `wmu` is the open's recheck (`OpenCheck`/`OpenBuffer`, review F2) |
| close after GOAWAY | `mux.go` `Mux.leave`, `conn.go` `Conn.closeFlushed` | `m.mu`: decide `left`; goroutine: `wmu` flush loop, unlock, `CloseWrite`, wait | `drop`, goAway's goroutine, `closeIdle`, the cut | no: a frame buffered between unlock and `CloseWrite` goes or not | one step (`Leave`): sound, a frame's step falls before or after it as in the code; a second call (leave and the cut) does nothing more (`CutFlush` on a shut side) |
| the connection's end | `conn.go` `Conn.Close`, `mux.go` `Mux.close` | `credit.Close`, `nc.Close`, `downOne`; `m.mu`: `done`, sessions taken; each session's end | many (`Mux.Close`, the cut, closeFlushed's timer, keepAlive, `recv`'s EOF); `Mux.close` from Serve only | `Conn.Close` idempotent (`downOne`); `Mux.close` one caller | `CloseConn` (first call), `Down` |
| credit grant, window update | `credit.go` `Credit.Consumed`, `conn.go` `Conn.sendWindow` | `credit.mu`: count, grant decided; unlock; `wmu`: WINDOW | the session's reader (`OnPop`) | no | already split: `Read` (gpend), `SendWindow`; a grant after the session's ABORT goes out and the peer ignores it (`RecvWindow` of an unknown session) |
| Write | `mux.go` `Session.send`, `credit.go` `Credit.acquire`, `conn.go` `Conn.sendData` | credit taken; `wmu`: check, frame (may wait), check again | the writer | no | already split: `Acquire`, `SendCheck`, `SendBuffer`, `Refund` |
| open | `mux.go` `Mux.OpenChain`, `conn.go` `Conn.sendOpen` | `goingAway`; `m.mu`: `add`; `wmu`: check, frame (may wait), check again; Flush; on error `drop` | the opener | no | already split: `OpenCheck`, `OpenBuffer`, `OpenRefused`; a session in `Mux.sess` before its OPEN is in the cut's snapshot (`CutCand`); against the connection's close: `OpenRefused` (closed during the wait), `CloseAt` (a buffered OPEN dropped), `Down` (the opener reads lost) |
| accept of a peer's open | `conn.go` `Conn.recv` (credit opened, trace `Recv`), `mux.go` `Mux.serve` (`add` under `m.mu`) | two: the credit, then the Mux | the receive loop | no: the cut's snapshot can fall between | `RecvOpen` one step; the trace's `CutBegin` takes a snapshot that may lack it; since lane LINKOPENREFUSE one read after the snapshot is refused (`Mux.add` under `m.mu`: `OpensAnswered`, "The crossed open's refusal") |
| answer | `chain.go` `Session.answer`, `conn.go` `Conn.sendAccept` | `ansOnce`; `wmu`: ACCEPT | `autoAccept` (first frame), `AcceptPath` | one `wmu` step; not checked against a forgotten credit: an ACCEPT after the cut's ABORT goes out, the peer ignores it | `Accept` (no credit guard, as the code) |
| Close, Reset, Abort | `mux.go` `Session.Close`/`Reset`, `chain.go` `Session.AbortWith`, `conn.go` `sendClose`/`sendAbort` | `once`; `wmu`: forget, frame (may wait); `m.mu`: `drop` | the application, a joint (`joint.abort`, `ended`) | no: the drop follows the frame | already split: `Closing`, `End`; the gap before the drop puts an ended session in the cut's snapshot (`CutCand`: its ABORT goes out after the CLOSE, the peer drops it) |
| CloseWrite | `mux.go` `Session.closeWrite` | `halfClosed`; `wmu`: CLOSE; `finSent` | the application, a joint | no | already split (review r2 LinkSession 1): `CloseWriteMark`, `CloseWriteSend` |
| refusal | `mux.go` `Mux.refuse`, `sendRefusals` | queued under `m.mu`; a goroutine's `sendAbort` | the receive loop | no | `RefuseMark`, `End`; after the end: C11 |

**Bug found (low; fixed in this lane)**: the cut with no session in its
snapshot closed the connection at once (`Mux.Close`), so frames buffered
behind a write in flight were lost: the CLOSE of a session that ended
just before the deadline (dropped from `Mux.sess`, so not in the
snapshot) never reached the peer, which read the connection's end
instead of io.EOF. The model's snapshot (`CutBase` plus any of
`CutEnded`: a session's drop follows its last frame) reaches it;
`CutLosesNoEnd` (the cut's close drops no session's end but at its 2 s
bound) fails with mutant `cut-closes-at-once` (the code before), 9
states deep. Fix: the cut runs closeFlushed whenever the connection has
FeatureChain, with no ABORT to send too (`mux.go` `Mux.cut`). Guard
`TestDrainDeadlineKeepsAnEndedSessionsClose` (failed before: the peer
read `link: connection closed`). `Conservation` now holds while both
sides still write (a frame after the cut's shut goes nowhere: the
crossed OPEN of `CutAbortsAll`), and `FinBeforeShut` is the graceful
drain's (`Leave`), not the cut's: a half-closed session the cut did not
abort loses its FIN to the cut's shut, as `CutAbortsAll` says.

**Invariants.** `DrainReturnsAfterCut`: a call of cut (so Drain) returns
only once the cut ended: its connection closed, so every ABORT it buffered
before was written or the connection's end came first, and the caller may
close what the sessions were joined to. `NoProtocolError` now also flags
**data after the session's end**: a data frame read for a session whose
CLOSE or ABORT this side already read (it was dropped unseen before: the
receiver keeps no credit for it).

**Mutants** (`make spec-mutants`, `LinkCut.cfg`):
`second-cut-early` (the code before 7dabd5e: the second call returns at
once) is caught by `DrainReturnsAfterCut`, 4 states deep;
`cut-keeps-credit` (the cut's ABORT leaves the credit: a Write after it)
by `NoProtocolError` (data after the session's end), 16 states deep
(`Conservation` no longer sees it: it holds only while both sides write);
`cut-closes-at-once` by `CutLosesNoEnd` (the bug above).

**Witnesses** (each must be violated): `WitnessCutWaits` (two calls, one
waiting), `WitnessAbortBeforeCut`, `WitnessCutAbortsEnded`, and the
non-guarantee `CutAbortsAll` (below).

**CONNDRAINFLAKE's open question**: a joint's `AbortWith` on a client
session racing the cut's own `sendAbort` of it. `LinkCut.cfg` has
`Session.Abort` (ABT) of the session the cut aborts (`DDL`), both orders.
Settled: both ABORTs go out (neither looks at the other: `sendAbort` does
not check the credit, `AbortWith`'s `once` is not the cut's); the peer
reads the first one buffered (`WitnessAbortBeforeCut`: the joint's reason,
though the cut began) and drops the second (its credit is gone: no
protocol error, `NoProtocolError` holds, and `Refines` holds on
`LinkCut.cfg`: the reader's end is typed once). Which reason the client
reads is the order of the two `wmu` steps; link guarantees neither. In a
relay, a joint aborts the client session only when the agent's side ended
first; after 7dabd5e a single relay lets its agents go only once its
clients' cuts ended, so the agent's end no longer comes first there.

**Finding (low; decided again and fixed in lane LINKOPENREFUSE, "The
crossed open's refusal" below)**: `CutAbortsAll` (once the cut
closed the connection, every session open here had its ABORT sent or
ended at the peer) does not hold: an OPEN that crossed the GOAWAY and was
read after the cut's snapshot (a drain deadline shorter than the peer's
delay, or a snapshot that found no session: the cut then closes at once)
gets no ABORT, and its opener reads `lost`. Within link's guarantee (a
session cut at the deadline ends `reset` or `lost`, `GEnds`, LOADTESTS
above); an ABORT or refusal for it could only race the close the same
way. The cut's doc says "each session still open"; it means the ones in
its snapshot.

**Traced since lane LINKDROPSPLIT**: `TestChainDrainMiddleHop`'s
connections were rejected here, at the drop of a session whose CLOSE
ended both halves (README "The drop's split" below).

**Sizes** (TLC 1.8.0, `-workers 2`, loaded machine): `LinkCut.cfg`
(`Sids {1}`, the accepter writes 1 byte, `Ends {3, 5}`, `MaxOut 4`):
162,643 distinct states, depth 34, 7 s for the invariants (CI's
`spec-invariants`), 2 min with `Refines` and the liveness properties
(`make spec`). `-coverage 1`: `CutBegin` 377, `CutWait` 1,270,
`CutForget` 1,640, `CutSend` 787, `CutFlush` 2,303, `CutClose` 12,658,
`CutEnd` 40,696, `CutReturn` 61,866 distinct states (every cut action
taken). `LinkCutHalf.cfg` (with CloseWrite, `Ends {1, 3, 5}`): 2,931,298
states, 2.5 min (`make spec-nightly-wide`). `LinkDrain.cfg` no longer has the
cut (with it: past 6.4 million states): `Ends {0, 1}`, 299,581 states.

**Traces**: the recorder writes `Cut` (n: the snapshot's size),
`CutWait`, `Forget`/`Close` 5 (`cutAbort`), `CutEnd` and `CutRet`;
`Trace.tla` maps them (`CutTrace`), `Leave` to `CutFlush` once the
cut's ABORTs are buffered. In `make spec-traces`:
`TestDrainReturnsAfterTheCutItRaced`, `TestDrainDeadlineKeepsAnEndedSessionsClose`
(an empty snapshot's flush) and `TestDrainDeadlineAbortsAHeldSession`
(`Close`/`Forget` 5, so `CutForget`, `CutSend`); the summary counts
`Close 5`/`Forget 5`. `TestDrainDeadlineAbortsReachABusyPeer` was checked
once (21 traces, 80 cut ABORT events, all accepted; about 13k lines a
connection, 40 min): too long for each night. `TestChainDrainMiddleHop`
is traced (lane LINKDROPSPLIT).

## The crossed open's refusal (Link.tla `RecvOpen`, 2026-11-09, lane LINKOPENREFUSE)

LINKATOMIC's finding F1 (`CutAbortsAll` violated): an OPEN that crossed
the GOAWAY and was read after the drain deadline's snapshot became a
session the cut never aborted: its opener read the connection's end
(`lost`), though nothing had run on it and it could have opened
elsewhere. Decision (coordinator, 2026-11-09): such an OPEN is answered
with link's open refusal, never silence, as HTTP/2's REFUSED_STREAM.

**Only once the cut began.** An OPEN that crosses the GOAWAY before the
drain's deadline still runs to its end like any other session (since
v0.4.2; peerlink's drain contract, `ExampleNode_Drain`,
`TestDrainDuringHandshake`, `TestCrossedOpenRunsOnTheLeavingConnection`):
the graceful drain loses nothing, so it is unchanged (`LinkDrain.cfg`
has the same 342,742 states as before). Refusing every crossed OPEN (the
first reading of the decision) refused requests a drain should have
served (`ExampleNode_Drain` failed on the Linux runner) and would refuse
more clients' opens while a relay drains an agent link.

**Guarantee** `OpensAnswered` (`LinkDrain.cfg`, `LinkCut.cfg`,
`LinkCutHalf.cfg`): once a side's write side shut (Mux.leave's or the
cut's closeFlushed), every session the peer opened whose OPEN it read
before has its end (refusal, ABORT, CLOSE) on the way or ended at the
peer: it ends refused, aborted or ended, never lost while the connection
was up to answer it. Not with a crash, nor at the cut's 2 s bound (its
close comes before any shut). An OPEN read after the shut can get no
answer (`late`); `CutAbortsAll` stays a witness for that case only.

**Model**: `RecvOpen` of an OPEN read once the cut began (`cut.st`; the
code's check is in Mux.add under m.mu, where the cut takes its snapshot,
so an OPEN is in the snapshot or refused) on FeatureChain opens its
credit (it counts until the refusal is buffered, as every refusal's) and
marks the session `refd` with an ABORT due (`closing`); `End` buffers it
(Mux.sendRefusals). A `refd` session is in no snapshot (`CutBase`,
`CutEnded`: not in Mux.sess) and has no application (`AppStep`).
`CutFlush` waits for every refusal due (`RefusalDue`), and
`LeaveWhenDrained` for no session open, a refusal's too: Mux.shutWrite,
before closeFlushed. The refinement maps a refusal due to no read ended
yet (`shut`), and a refusal `End` buffers after the connection's end
(`Down`) types no end (the session ended first: `Refines` on
`LinkCut.cfg` failed without it, 9 states deep, `make spec` only: CI's
`spec-invariants` checks `Refines` on `LinkCrash.cfg`); the interface is
unchanged (the opener reads `reset`: ErrOpenRefused).

**Mutants** (`LinkCut.cfg`): `open-after-goaway-unanswered` (the code
before: the OPEN read after the snapshot runs, out of it) is caught by
`OpensAnswered` 7 states deep; `shut-before-refusal` (the cut's
closeFlushed with a refusal still queued: the race Mux.shutWrite closes)
too, 7 states. Witnesses (violated, as they must be): `WitnessShutAnswers`
(the write side shut under a session the peer opened, its ABORT on the
way), `WitnessDrainRefused` (an opener read the cut's refusal).

**Sizes and coverage** (TLC 1.8.0, invariants, `-coverage 1`):
`LinkCut.cfg` 126,838 distinct states (162,643 before: an OPEN after the
cut is refused, not a session), depth 34, every action taken, the
refusing branch of `RecvOpen` in 317 states; with `Refines` and the
liveness properties 6.5 min; `LinkDrain.cfg` 342,742, as before (13.5 min
with `Refines` and liveness).

**Code** (`mux.go`): `Mux.add` refuses a peer's open once `m.cutting`
(`errCut`), and `Mux.serve` answers it `CodeDraining`; `Mux.shutWrite`
waits for the refusal goroutine and then queues no more (an OPEN read
after it is one read after the shut), called by `Mux.closeFlushed`
before `Conn.closeFlushed` in `leave`, `closeIdle` and the cut. Guard
`TestOpenReadAfterTheCutsSnapshotIsRefused` (a test hook holds the cut's
first ABORT while the OPEN is read; failed before: `link: connection
closed`), in `make spec-traces`. No wire change: the refusal is the
existing abort refuse (code 2, draining; WIRE.md 9).

**Traces**: the cut's refusal logs `Forget` 4 and `Close` 4 (sendAbort);
`Trace.tla` maps the forget to no step (`RefusalDue`: End forgets) and
the close to `End`; one whose `Recv` was logged before the cut's `Cut`
event is an accepted session in the model, refused (`RefuseMark`). A
refusal Mux.shutWrite drops (read after the shut was decided, before
closeFlushed logs `Leave`) leaves the model's refusal due: the trace
check does not hold `Leave` to it for that reason.

## The drop's split (Link.tla `Drop`, 2026-11-09, lane LINKDROPSPLIT)

LINKATOMIC found `TestChainDrainMiddleHop`'s traces rejected: the model
dropped a session whose CLOSE ended both halves (`CloseWrite` after the
peer's FIN) and forgot its credit in the step that buffered the CLOSE,
where the code forgot it in `Mux.drop`, after the frame: a WINDOW
received between was charged to a credit the code still knew.

**The code was wrong, not only the model (medium; fixed in this lane).**
The peer drops the session when it reads that CLOSE (`RecvClose`: its
own FIN was buffered) and may open another at once: its count of open
sessions fell. This side counts its sessions by their credit
(`Credit.count`, max_sessions), so a receive loop reading that OPEN
before `Mux.drop` ran refused it with `ErrFull`, the connection's end,
with every session on it. Guard: `TestCloseWriteEndingBothHalvesFreesItsSessionAtOnce`
(`MaxSessions` 1; a test hook holds `closeWrite` after its CLOSE; failed
before: `link: connection closed`, 3 of 3). Mutant `drop-forgets-late`
(the code before: the credit forgotten in `Drop`) on `LinkSessHalf.cfg`
(two sessions of the dialer, max_sessions 1, a CloseWrite each way, no
data) is caught by `NoProtocolError` (open past max_sessions), 15 states
deep.

Fix (`Credit.halfClosed`): the credit records each direction's half CLOSE
(`fins`); the second ends it under the credit's lock: this side's, once
its CLOSE is buffered and wmu still held (no write takes the buffer
without wmu, so the peer cannot have read it), with the trace's `Close` 1
logged under the same lock; the peer's, in `Conn.recv` before the next
frame is read, with its `Recv` close. `Mux.drop` then forgets nothing
more.

**The split**: `CloseWriteSend` (and `CloseWrite`, its trace form) forgets
the credit when the peer's FIN was read (`HalfForget`) and leaves the
session open; `Drop` (new, weakly fair, a `Next` disjunct of its own for
`-coverage`) takes it out of `Mux.sess` (`st` gone, `pclosed`). Between
the two, a WINDOW for it is ignored (no credit) and `Mux.leave` waits
(`LeaveWhenDrained`: no session open). The peer's FIN read after this
side's CLOSE still drops the session in the receive step (`RecvClose`:
the receive loop's `Mux.serve` drops it before the next frame). No
invariant was narrowed.

**Traces**: the recorder logs `Drop` in `Mux.drop` (every drop; Trace.tla
maps it to `Drop` while the session waits for it, else to no step: the
other drops follow a step that already ended the session in the model).
With the drop split, `TestChainDrainMiddleHop` showed a second gap: a
joint's `AbortWith` of a session already gone (the peer's ABORT read, or
the connection's end), whose `Forget` 3 and `Close` 3 the model had no
step for (`Closing` needs the session open); `LateAbortForget` and
`LateAbortSend` (Trace.tla only) take them, the ABORT dropped by the peer.
`TestChainDrainMiddleHop` and the guard are in `make spec-traces`.

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
| `QuicWatch.tla`, `MBTWatch.tla` | quiclink's liveness: `conn.watch`, `watchState.tick`, `keepAliveAfter`, QUIC's idle timeout as its backstop | `peerlink/quiclink/quiclink.go` |
| `Join.tla`, `TraceJoin.tla` | `peerlink.Join`'s clean end (`Session.Sync`, hold, the watch) | `peerlink/chain.go`, `join.go`, `peerlink/trace_on.go` |
| `JoinClose.tla` | a joint's side closed whole by its peer while the other side writes: who closes the other side, and when | `join.go`, `peerlink/chain.go` |
| `CreditWaiters.tla` | senders waiting in `Credit.Acquire` on one session: the wait and `wakeAll` | `credit.go` |
| `DirectPath.tla`, `TraceDirectPath.tla` | the path layer: OPEN, RESUME, ACK, END, ABORT, the window, `Move` and `Attach` | `pathswitch/conn.go`, `pathswitch/frame.go`, `pathswitch/trace_on.go` |

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
| `Credit.sessions` (`cr.on`) | `Credit.open`, `Credit.dropLocked` | `OpenCheck`/`Open`, `RecvOpen` (open); `Closing`, `RefuseMark`, `Down` (`Credit.forget`); `CloseWriteSend`, `RecvClose` (the second half CLOSE, `Credit.halfClosed`) |
| `Credit.opened` (`Count`) | `Credit.open`, `Credit.dropLocked`, `Credit.cutSessions` | the same, and `Down` |
| `Credit.cut` | `Credit.cutSessions` | `Down` (`CloseConn`'s credit end) |
| `sessionCredit.n` (`cr.n`) | `Credit.acquire`, `Credit.refund`, `Credit.Grant` | `Acquire`, `Refund`, `RecvWindow` |
| `sessionCredit.pend` (`pend`) | `Credit.acquire`, `Credit.sent`, `Credit.refund` | `Acquire`, `SendBuffer`/`SendData`, `Refund` |
| `sessionCredit.held` (`cr.held`) | `Credit.chargeLocked`, `Credit.Consumed` | `RecvData`, `Read` (a grant decided: `gpend`) |
| `sessionCredit.consumed` (`cr.cons`) | `Credit.Consumed` | `Read` |
| `sessionCredit.answered` (`ss.acc`, `Answered`) | `Credit.answered`, `Credit.answering` | `RecvAccept` (and the refusal's `Recv` close 4), `Accept`/`RefuseMark` |
| `Session.ans` | `Session.answer`, `Session.AbortWith` | `Accept`, `RefuseMark` |
| `Session.halfClosed` (`ss.wfin`) | `Session.closeWrite` | `CloseWriteMark` |
| `Session.finSent` (`ss.fin`) | `Session.closeWrite` | `CloseWriteSend` |
| `Session.peerHalf` (`ss.rfin`) | `Mux.serve` | `RecvClose` code 1 |
| `Session.peerClosed` (`ss.pclosed`) | `Mux.serve`, `Mux.close`, `Mux.OpenChain`, `Session.closeWrite` | `RecvClose` 0, 2, 3; `Down`; `OpenRefused` (the peer never heard of it); `CloseWriteSend` after the peer's FIN (both halves: `Mux.drop`) |
| `Session.closed` (`ss.st = "gone"`) | `Session.ended` | every end: `End`, `RecvClose`, `Down`, `CloseWriteSend` with `rfin` |
| `Mux.sess` (`ss.st`) | `Mux.add`, `Mux.drop`, `Mux.close` | `OpenCheck`, `RecvOpen` (add); every end (drop; `Drop` after a CloseWrite that ended both halves); `Down` |
| `Mux.leaving` | `Mux.goAway` | `GoAway` |
| `Mux.left` | `Mux.leave`, `Mux.closeIdle` | `Leave` (`LeaveWhenDrained`); `closeIdle` is `Pool.Close`'s `Leave` without a GOAWAY (`Pool.tla` `CloseSnap`) |
| `Mux.cutting` | `Mux.cut` | `CutBegin`, `CutWait` (README "The drain's cut") |
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
| `CloseWrite` | `joint.ended(0)` -> `joint.sync`: `syncing` set, `Session.CloseWriteSync` (`Conn.sendCloseSync`: the CLOSE, the Sync registered and its ping, under one `wmu`) |
| `SyncPing` | mutant `ping-late` only: the code before, `Session.Sync`'s ping after `CloseWrite` |
| `ARecv`, `AFin` | the peer's `Conn.recv` (and `keepAlive`'s pong); the peer's application's `CloseWrite` |
| `RRecv` (FIN) | `joint.dir(1)`'s Pump ends at the peer's FIN: `joint.ended(1)` -> `hold` while syncing, else `closeWrite` on the client (`Eof`) |
| `RRecv` (pong) | `Session.Sync` returns: `sync`'s goroutine runs the held end (`fin`) and `finish` |
| `Finish` | `joint.finish` of both directions: the client closed |
| `Crash` | the environment (the link connection ends) |
| `Drain` | `Mux.Drain` / `goAway` on the session's connection (peerlink `Node.Drain` on an accepted one) |
| `Leave` | `Mux.leave` -> `Conn.closeFlushed`, once `Conn.syncPending` is nil |
| `PFlush`, `PeerClose` | the peer's write of a pong it buffered; its close after reading the shut (environment) |
| `ConnEnd` | the receive loop's clean EOF: `Conn.Done` |
| `Lost` | `joint.abort` of the client: from `ended` (Pump's error, a failed write), `sync` (Sync's error), or `watch` (the session's end while the client is idle: Join 2) |

Unmapped code, outside `Join.tla` on purpose: `Forward`'s answer wait
(`pending`, `held`, `answered`: an onward hop, not a plain Join), a write
deadline's abort of both sides, and the peer's whole close (`cleanClose`:
Join 6, decided; modelled since in `JoinClose.tla`, above). The joint's fields are in package `peerlink`, not
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
| `Join` and `peerlink.Join`: a side closed whole by its peer has every byte it sent delivered to the other side before that side's close, and the other side is closed whole in the end | `JoinClose.tla` `TailDelivered`, `AClosedInTheEnd` |
| a Pool: no session on a connection going away, within Max but Drain's make-before-break, Min held, every Open progresses and every waiting Open wakes, ErrFull only with the room full and no dial in flight, nothing left after Close | `Pool.tla` `NoSessionOnLeaving`, `MaxHolds`, `MakeBeforeBreak`, `MinHeld`, `OpensProgress`, `WaitWakes`, `FullOnlyAtMax`, `NoLeak`, `CloseEnds` |

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
| LinkSession 7: configurations miss a listener-opened session, crossed opens, data both ways | low | **done** (lane CONFORM-link): `LinkBoth.cfg` (a session of each side opened at once, a byte each way on each, a FIN each way; invariants and `Refines`) in `make spec-nightly-wide` |
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
| LINKPOOL L5: `Pool.cfg` is the smallest shape only | low | **done**: `PoolWide.cfg` (two Opens racing for the last of Max 2, Min 1) in `make spec-nightly-wide`. It found that `OpensOpen` does not hold with two Opens: one that closes and opens again can take the room the other picked each time, and the other re-picks forever. **Decided**: the Pool promises progress, not fairness among Opens (`OpensProgress`); a queue of Opens is the fix if a starved Open is ever seen |
| LINKPOOL M5: background growth not modelled | medium | **done** (lane CONFORM-link): `gr`, `Pick`'s growth, `GrowDial`; `PoolGrow.cfg` in `make spec`, mutant `grow-past-max` |
| LINKPOOL M6: no trace recorder for the Pool | medium | **done** (lane CONFORM-link): `trace_pool_on.go`, `TracePool.tla`, the Pool tests in `make spec-traces`, fixtures `pool-*` in `make spec-mutants`; it found C6 |
| LINKPOOL L2: Drain's wait and deadline cut | low | **decided**, then **superseded** (lane LINKATOMIC, 2026-11-09): the cut as one connection's end hid CONNDRAINFLAKE's race (Drain returned while the other call's ABORTs were unsent, fixed in 7dabd5e); the cut is now modelled in its steps with Drain's return (`DrainReturnsAfterCut`, "The drain's cut") |
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
- A client past its peer's max_sessions (lane CONNPERF: tcp-connect's
  data connections, the relay's 1,024 sessions each): `FullOnlyAtMax`, an
  action property on every Pool config: an `Open` fails `ErrFull` only
  with the room full (every connection that counts against Max at its Cap,
  no slot to dial one more). `PoolClient.cfg` (Min 0, Max 2, Cap 1, two
  `Open`s, under a second; in `make spec`, and with a third slot and an
  environment event, 487 thousand states, in `make spec-nightly-wide`): mutant
  `full-below-max` (a full connection fails the `Open`: one connection
  only, the code before) caught by `FullOnlyAtMax`; witness
  `WitnessActFull` (an `ErrFull` is reached; `WitnessAct*` are action
  properties, checked as a `PROPERTY`) on `PoolClientWake.cfg`, since
  lane CONNCAP (below) two `Open`s no longer fill `PoolClient.cfg`'s room. Min 0 leaves `maintain`'s and
  `Drain`'s reservations untaken there: `Pool.cfg` covers them.
- A burst of opens past one connection's sessions (lane CONNCAP:
  tcp-connect at 2,000 new connections at once failed hundreds with
  `ErrFull` while its second connection was being dialled):
  `FullOnlyAtMax` also requires no dial in flight, so an `Open` beside a
  full connection waits on `p.changed` for the dials that hold the rest of
  the room. Mutant `full-with-dials` (the code before: `ErrFull` with a
  full connection beside dials in flight) caught by `FullOnlyAtMax` on
  `PoolClient.cfg` (an `Open` holds the full connection, a growth dial
  the rest of the room); witness `WitnessWaitBesideFull`. `WaitWakes`
  (every config with `Open`s): an `Open` that waits wakes, each dial
  ending up or failed (`DialFails`: a failed or cancelled dial) closing
  `p.changed`; per `Open`, where `OpensProgress` is met by another's
  session. `PoolClientWake.cfg` (`PoolClient.cfg` at Max 1 with one
  environment event, `WaitWakes` alone; `make spec-mutants`): mutant
  `no-wake-on-dial-fail` caught by `WaitWakes` (an `Open` waiting on
  another's dial that fails).
  `TestPoolOpenWaitsForADialBesideAFullConnection` (the dial up, and
  failed: the waiter dials one itself).

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
(C11), `link-ok-data-during-refusal` (C12) and `link-ok-pong-burst`
(`TestStarvedProcessKeepsALivePeer`'s: pings read in a burst, keepAlive
answers both, the second pong in place of the first) accepted by Trace.tla, each
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

**Superseded** by "quiclink's watch" below (QW3): the hypothesis that
follows is refuted. The two RCFOLLOW failures with at most 1.7 s of stall (A's 10 s
watch fired, B silent for 10 s while its process ran) are not explained by
the starvation: no fire in them was late past a period. A cause in the
same code, not proven: QUIC's idle timeout was the smaller preset's
Timeout (3 s, negotiated down from A's 10 s), it judges by the clock
alone, and an idle timeout closes silently (RFC 9000 section 10.1: no
CONNECTION_CLOSE), so B's connection could end on a short stall plus a
lost or late packet and A would hear nothing until its own watch. With the
idle timeout now a backstop (4 x (Interval + Timeout), the watch closing
first with CONNECTION_CLOSE) that path is gone if that was the cause; a
qlog of both ends would settle it.

## quiclink's watch (QuicWatch.tla, 2026-11-04, lane QLWATCH)

`conn.watch` and quic-go beneath it, in quarter ticks (G = Timeout/4 = 4,
Interval 8, Timeout 16: the shape of every preset, Timeout = 2 x
Interval). quic-go processes packets (`Recv`: `lastPacketReceivedTime`,
`PacketsReceived`), sends its keep-alive PING K after the last one
(`KeepAlive`; K = max(Interval, 1.5 PTO), quic-go's `nextKeepAliveTime`),
and closes silently once nothing came for B (`IdleClose`, by the clock,
possibly before it processes what waits in the socket: its run loop's
timer case). The watch ticks (`WatchAt`, `watchState.tick`): a new packet
moves `seen`; a tick more than 1.25 G after the one before moves `seen` on
by the gap; otherwise no packet for Quiet = K + Timeout/2, and none for
Timeout/2 since quic-go's idle timer started (`IdleStart`: the keep-alive's
send, else the last packet; `ConnectionStats.IdleTimeoutStart` of nfltr's
quic-go, finding QW2), is the verdict; its second look, D = Timeout/16
later (`Look`, `watchState.look`, after quic-go's run loop had its turn),
closes (CONNECTION_CLOSE, `CodeDeadPeer`) unless a packet came since
(finding QW4).
The peer answers the keep-alive within Rtt (`Rtt + Late < Timeout/2`); the
process may stop (`Sleep`, `Wake`), and between two stops runs a moment,
long enough for its run loop's due work (`Tick`'s guard: the packet
waiting, the keep-alive due).

| property | statement |
|---|---|
| `NoFalseDead` | a live peer is never closed, by the watch or the backstop, for any keep-alive period K and any stop of MinSleep..MaxSleep |
| `DetectBound` | a running process closes a silent connection within Quiet + Timeout/2 + D + 2 Late of the later of its last packet and its last start (Interval + 17/16 Timeout + 2 Late when K = Interval; `WitnessTight`: reached) |
| `WatchFirst` | on a running process the watch closes before the idle timeout: the peer is told (CONNECTION_CLOSE), it does not wait out its own bound |
| ASSUMEs | B > K + Timeout + D + 3 Late (the backstop past the watch's bound); MaxSleep + K + Rtt + Late < B: a stop longer than B - (K + Rtt + Late) lets the backstop close a live peer silently, the ceiling (with B = 4 x (Interval + Timeout): about 18 s at `fast`, 60 s at `default`) |

Configurations (`make spec`, seconds): `QuicWatch.cfg` (K = Interval, a
running process), `QuicWatchPTO.cfg` (K = 12 > Interval: a path whose
probe timeout stretches the keep-alive), `QuicWatchStarve.cfg` (one stop
of 6..12, B = 30: smaller than the code's but within the ASSUMEs, so that
the backstop's close is reached; every action taken there, `-coverage
1`; the first two take no `Sleep`, `Wake`, discounted gap or `IdleClose`),
`QuicWatchStops.cfg` (two stops of 0..5, each unseen in the tick gaps, Rtt
6, the code's backstop; `make spec` only, about 12 s: its mutants are CI's;
no `IdleClose`).

Mutants (`make spec-mutants`): `quiet-ignores-pto` (Quiet = Interval +
Timeout/2 without the idle start, the code before QW1: `NoFalseDead` on
`QuicWatchPTO.cfg`, 22 states; the idle start alone implies K while quic-go
sends its keep-alive, Quiet's K bounds a side that sends none), `count-gap` (no discount, the code before S1: `NoFalseDead` on
`QuicWatchStarve.cfg`), `backstop-low` (the idle timeout at Timeout, the
code before LIVESTARVE: `WatchFirst`), `never-close` (`DetectBound`),
`ignores-idle-start` (the code before QW2: `NoFalseDead` on
`QuicWatchStops.cfg`), `one-look` (the code before QW4: the verdict closes
at once) and `look-at-once` (D = 0: a second look with no time for quic-go's
run loop), both `NoFalseDead` there.
Witnesses: `WitnessWatch`,
`WitnessTight` (both configurations), `WitnessStarvedLive` (a live peer's
tick due with nothing processed for Quiet: only with `Sleep`),
`WitnessIdle` (the backstop's close), `WitnessLateKeepAlive` (a live
peer's tick with Quiet passed, no gap seen, the keep-alive sent late: the
verdict the idle start withholds, `QuicWatchStops.cfg`),
`WitnessSecondLook` (a verdict the second look took back, there).

**Action map.**

| action | code |
|---|---|
| `Tick`, `Arrive`, `Die`, `Sleep`, `Wake` | the environment (time, the peer, the path, the scheduler) |
| `Recv` | quic-go's `handlePackets` (`lastPacketReceivedTime`, `connStats.PacketsReceived`) |
| `KeepAlive` | quic-go's run loop, `nextKeepAliveTime` (`keepAlivePingSent`) |
| `IdleClose` | quic-go's run loop, `nextIdleTimeoutTime` (`ErrIdleTimeout`, no CONNECTION_CLOSE) |
| `WatchAt` | `conn.watch`'s tick: `watchState.tick` (its verdict: the first look) |
| `Look` | `conn.watch`, g/4 after a verdict: `watchState.look` (dead: `CloseWithError(CodeDeadPeer)`) |

| field (model variable) | written by | action |
|---|---|---|
| `watchState.last` (`fresh`) | `watchState.tick` | `WatchAt` |
| `watchState.seen` (`seen`) | `conn.watch` (start), `watchState.tick`, `watchState.look` | `Init`, `WatchAt`, `Look` |
| the verdict's first look (`look`) | `conn.watch` (the g/4 wait after a verdict) | `WatchAt`, `Look` |
| `watchState.prev` (`prev`) | `conn.watch` (start), `watchState.tick` | `Init`, `WatchAt` |
| the ticker (`due`) | `time.NewTicker(Timeout/4)` | `WatchAt` (the first multiple of G after the tick: a Go Ticker drops what it could not deliver) |
| `Quiet` | `keepAliveAfter` + Timeout/2, per tick from `ConnectionStats` | (a constant per configuration) |
| `IdleStart` (`ka`, `rx`) | quic-go's `setIdleStart` (`idleTimeoutStartTime`), read per tick as `ConnectionStats.IdleTimeoutStart` | `Recv`, `KeepAlive` |
| `MaxIdleTimeout` (`B`) | `New` (`idleBackstop`) | `IdleClose` |

No trace recorder: the watch's state is three fields, and the
model-based test below replays its every step. `TestWatchFollowsTheModel`
(peerlink/quiclink/watch_mbt_test.go) replays every `WatchAt` of the TLC
walks in `testdata/mbt/qwatch-<cfg>-*.ndjson` (`scripts/spec.sh
behaviours`, 16 walks per configuration) on `watchState.tick` and compares
`seen`, `prev` and the verdict, with the model's idle start (`idle`), and
every `Look` on `watchState.look`; it counts the verdicts the idle start
withholds and the second looks, and fails when there are none. Random walks
never reach a verdict the second look takes back (none in 200000
`QuicWatchStops.cfg` walks: the live peer's own packets keep coming);
`TestWatchSecondLookTakesBack` replays TLC's walk to `WitnessSecondLook`.

**Finding QW1** (medium, fixed). The watch allowed Interval + Timeout/2 of
silence, taking quic-go's keep-alive to come every Interval; quic-go sends
it no sooner than 1.5 PTO after the last packet (PTO = srtt + 4 x rttvar
+ max_ack_delay). On a path with a round trip of 0.8 s (under `fast`'s
Timeout/2) the first sample's rttvar (half the RTT) makes PTO about 2 s,
the keep-alive 3 s, and the watch closed a live idle peer 3 s in
(`TestSlowPathKeepsALiveIdlePeer`, failed before; mutant
`quiet-ignores-pto`). Fix: Quiet = `keepAliveAfter` + Timeout/2, the
keep-alive period computed per tick from `ConnectionStats` as quic-go
does; the dead-peer bound on such a path is 1.5 PTO + Timeout, not
Interval + Timeout (`DeadAfter`'s doc). The same holds under load: RTT
samples taken while a process is starved inflate rttvar.

**Finding QW2** (low, fixed, lane QW2). A stop shorter than 1.25 ticks
does not show in the tick gaps, yet it delays quic-go's keep-alive and
counted as silence: two such stops (TLC: 5 and 2 quarter ticks, the
keep-alive due in the first) ended a live peer on a running process. The
margin was Timeout/2 - Rtt - Late; with `fast` on loopback 1.5 s of short
stops in one Quiet window. Fix: nfltr's quic-go exposes its idle timer
start (`ConnectionStats.IdleTimeoutStart`, the later of the last packet
received and the first ack-eliciting packet sent after it,
`TestConnectionStatsIdleTimeoutStart` there), and the watch's verdict also
needs Timeout/2 since it: a keep-alive a stop delayed moves the verdict
with it. The detection bound stays (`DetectBound`: the keep-alive goes out
K after the last packet on a running process). Mutant
`ignores-idle-start` on `QuicWatchStops.cfg`; `TestWatchFollowsTheModel`
failed before (a tick 7 after a keep-alive sent one late: the code closed,
the model did not).

**Finding QW4** (low, fixed, lane FLAKEQW4). The other half of a short stop: one that
starts after the peer's answer reached the socket and ends at the verdict
tick, which runs before quic-go reads the answer (the run loop and the
watch's ticker wake together). The idle start cannot see it (the
keep-alive went out on time). TLC: unreachable while Rtt + Late + 1.25 G
<= Timeout/2 (`QuicWatchStops.cfg`, Rtt 2), reached at Rtt 3 (mutant
`Rtt = 3`, one stop of 5). With `fast` (Timeout 3 s): a round trip over
about 0.4 s plus such a stop. Fix: the verdict is looked at again
Timeout/16 later (`Look`; the process ran since, so quic-go processed what
waited: `Tick`'s guard, and a process that woke runs a moment before it
stops again), and a packet since takes it back. `QuicWatchStops.cfg` now
holds at Rtt 6 (the ASSUME's largest); mutants `one-look` and
`look-at-once`; the dead-peer bound grows by Timeout/16 + Late. A later
look (another stop between the two) adds nothing in the model: a
"late-look-decides" variant that closes then is not caught, so the code
has no such branch.

**Finding QW3** (the open item of "Starvation", refuted). That B's 3 s
idle timeout closed B silently while A (`default`, 10 s) heard nothing
until its own watch cannot be: QUIC's idle timeout is the smaller of the
two endpoints' (RFC 9000 section 10.1), so A's was 3 s too and A ends by
its own idle timer (`TestIdleTimeoutIsBothSides`: 5.2-5.8 s, "no recent
network activity"), never by a 10 s watch. Consistent with the two
failures instead: QW1 under load. A's watch at 10 s fires only when A's
idle timer (max(3 s, 3 PTO)) has not, so A's PTO was over 3.3 s there; B's
keep-alive was then 1.5 PTO, over 5 s, past B's `fast` watch (3 s), which
closed a live A; its one CONNECTION_CLOSE, sent as both sides came back
from the stall, lost (A's socket full of the bulk transfer). Not
reproduced (a qlog of both ends would settle it); the QW1 fix removes
that path.


## Direct paths (DirectPath.tla, 2026-11-11, lane DIRECTPATH)

A design, checked before any code (AGENTS.md "Engineering Quality Bar"):
nfltr `docs/decisions/direct-paths-2026-11-11.md`. An end-to-end encrypted
session (organisation-mode tcp first) starts relayed and may move to a
direct QUIC path between the two machines (hole punched under quiclink),
and back, with the relayed path as the fallback that always works. The
layer that moves it sits between the session's byte stream (the end-to-end
TLS records, opaque to it) and the paths: package `pathswitch` (lane
DIRECTPATH2), whose trace recorder `make spec-traces` checks against this
model (`TraceDirectPath.tla`, below).

One direction of one session: S hands bytes `0..Total-1` to the path
layer, R delivers them. The other direction is the same model with the
decider's side swapped, so `decider` is chosen at Init: the client (the
side that dials) is the only side that opens path streams, S on the
client-to-agent direction, R on the other.

| variable | meaning |
|---|---|
| `st[e]` | epoch `e`'s stream: its path (`relay`, `direct`), its holder (`peer`, or `rogue`: whoever the relay pointed at), `dead` |
| `d[e]`, `k[e]` | frames on stream `e`, S to R (`open`, `data`, `end`, `abort`) and R to S (`resume(n)`, `ack(n)`, `abort`) |
| `sEp`, `sWait`, `hi`, `nxt`, `acked` | S's stream; S opened it and waits for R's RESUME; bytes the application handed it; the next it sends; every byte below `acked` freed |
| `rEp`, `rcvd`, `rEnds`, `rAck`, `out` | R's stream (the only one it reads); bytes delivered; ENDs taken; the count last told S; the bytes delivered |
| `ab`, `sGone`, `rGone` | who reset the session (`none`, `S`, `R`); each end knows the reset |
| `relayUp`, `dir` | the relayed path is up; the direct path (`none`, `probing`, `up`, `down`) and its holder |

The protocol: each attach opens a fresh stream (an epoch) on one path. R
opens one with `RESUME(rcvd)` (decider R), or answers S's `OPEN` with it
(decider S); from then on R reads only that stream. S sends from `n` of
the RESUME it took and frees a byte only when R's `ACK` or `RESUME` covers
it, so it keeps at most `W` bytes R may lack. A stream's data carries no
offsets: whatever arrives next is byte `rcvd`. A path dies by losing every
frame in flight on it; the decider then falls back to a path that works
(`Fallback`); a move on a live stream (`Move`, the policy) is bounded. The
relay may withhold candidates (no `Offer`) or offer one whose holder is
not the peer (a forged QAD address or candidate); `Verify` (the peer's
organisation certificate and the session's exporter token, relay
`spec/proverif/e2ee_tcp.pv` queries 21-24) takes only the peer's.

| invariant | what |
|---|---|
| `InOrder` | R delivers bytes 0, 1, 2, ... once each, in order: none lost, duplicated or reordered, at a switch or ever |
| `NoLoss` | `acked <= rcvd`: S never frees a byte R lacks |
| `BufferBound` | `hi - acked <= W`: at most W bytes kept per direction of a session |
| `OnlyPeer` | no stream to whoever the relay pointed at |
| `EndAfterData` | R ends the direction only after its last byte: an END never overtakes data |
| `EndOnce` | R takes the END once, however often it was sent |
| `AllDelivered` (liveness) | every byte and the END arrive and S frees them all, or the reset reached both ends (paths die at most `MaxDie` times; TLC with `-deadlock`, so a stuck state violates it) |
| `ResetReaches` (liveness) | a reset, across any switch, reaches both ends |

A direction's end and a session's reset (gap G1, lane DIRECTPATH2): after
its `Total` bytes S sends `END`, which takes position `Total`: retained,
resent from a RESUME and acknowledged like a byte (R's counts include it
once taken). Either end may reset the session (`AbortBy`, `MaxAbort` 1)
at any time before the direction ended cleanly: the aborter stops its
data and keeps answering every stream the session opens with `ABORT`
(the decider aborting opens one when its stream ended, `Lingers`) until
the peer took it, so an ABORT sent on a stream the peer left, or lost
with a path, is sent again on the next.

The session's close (lane PSCLOSE, after relay's leakcheck found an
agent's `Close` waiting out the 30 s detach bound after a relayed
fallback: the client had left once its last ACK, of the agent's END, was
written, and a stream dropped on the client's `Done` lost it, while only
the client can open another stream). An end whose view of the direction
is complete (S: the END acknowledged, `SDone`; R: the END taken and its
last ACK sent, `RDone`) ends its stream with FIN after its last frame
(`SFin`, `RFin`) and leaves (`sLeft`, `rLeft`: `Close` returned and
`Done`). The end that is not the decider (the agent) leaves at its FIN:
whatever it still lacked would have to come on a stream only the decider
opens. The decider waits for that FIN (`STakeFin`, `RTakeFin`), which says
the peer has everything, and only then sends its own and leaves: it ends
last. Until it took the FIN a stream that ends makes it open another
(`Fallback` stays enabled), and an end that left answers that stream's
OPEN with its count and FIN (`RLeftAnswer`, `SLeftAnswer`: a refusal would
reset a session both ends had ended cleanly). Each end's other direction
is taken as already complete, the worst case: an end may leave as soon as
this direction lets it. Invariant `LeaveLast` (the decider leaves only
once the direction is clean); liveness `CloseEnds` (both ends leave, with
no reset, across any switch and death: no end waits out a bound while
both live); `AllDelivered` and `ResetReaches` count an end that left as
done. A reset needs both ends present (`AbortBy` only before either left).

`DirectPath.cfg` (3 bytes, window 2, each path dies once, one candidate,
two moves, either decider, no reset): 430,763 distinct states, depth 48
(333,496 and 47 before the close, lane PSCLOSE), 3 min with the liveness
properties, invariants only in `make spec-invariants`. `DirectPathReset.cfg`
(2 bytes, window 2, each path dies once, one candidate, one move, a
reset): 221,178 distinct states, depth 38, 1.5 min with the liveness
properties (`make spec`). `DirectPathWide.cfg` (`DirectPath.cfg` with two
candidates, the rogue's then the peer's): 2,896,776 distinct states,
depth 50, 2 min, invariants, in `make spec` only. `-coverage 1` takes every
action on `DirectPathReset.cfg` and `DirectPath.cfg` (but `AbortBy`,
`STakeAbort`, `SAnswerAbort` there: no reset); the three that only a
mutant enables (`SendStale`, `RecvStale`, `SendEndEarly`) take none.

Mutants (`make spec-mutants`), each caught:

| mutant | the bug | caught by |
|---|---|---|
| `free-on-send` | S frees a byte once sent (the path's transport ack), not when R has it: lost at a switch | `NoLoss` |
| `send-before-resume` | S sends on a stream it opened before R's RESUME, from its own place | `InOrder` |
| `write-stale` | S still sends on a stream it left (a stale path kept) | `InOrder` |
| `resume-from-acked` | S resends from what it freed, not from R's RESUME: duplicates | `InOrder` |
| `read-stale` | R still reads a stream it left | `InOrder` |
| `no-bind` | the direct handshake takes whoever answers at the address: the relay redirects | `OnlyPeer` |
| `keep-dead-path` | a dead direct path is kept, no fallback | `AllDelivered`, `CloseEnds` |
| `end-early` | S sends END once the application closed, before its last bytes went | `EndAfterData` |
| `resume-drops-fin` | a RESUME leaves out the END R took: S sends it again and R takes it twice | `EndOnce` |
| `leave-at-complete` | the decider (the client) leaves at its own FIN, not after the peer's: its last ACK lost with a path, the agent waits for a stream nobody opens (the code before lane PSCLOSE) | `LeaveLast`; `CloseEnds` without it |
| `abort-not-resent` (`DirectPathReset.cfg`) | the ABORT goes once, on the aborter's stream: lost with a path or on a stream the peer left | `ResetReaches` |

Witnesses (each violated): `WitnessDirect` (all delivered, the last on
the direct path), `WitnessFallback` (direct, then back on the relay, all
delivered), `WitnessResent` (a byte sent on two streams, delivered once),
`WitnessRogue` (another holder offered and refused), `WitnessFull` (the
window full), `WitnessWithheld` (no candidate, a relay death, all
delivered relayed), `WitnessEndResent` (the END sent on two streams,
taken once), `WitnessLeftAnswer` (both ends left, the decider after an
end that had left answered its new stream); on `DirectPathReset.cfg`: `WitnessResetAcross` (the ABORT
sent again on a newer stream, known to both), `WitnessResetDirect` (the
reset reached both ends on the direct path).

Trace validation (`TraceDirectPath.tla`, lane DIRECTPATH2): with
`-tags linktrace` a session's two `pathswitch.Conn`s write one line per
model step to one file (`pathswitch/trace_on.go`; a step on k bytes is k
lines, the model's frames being bytes), the test's in-memory paths the
network's (`Offer`, `Verify`, `Die`, `Revive`: a kill holds both ends'
locks, so a frame read before it but taken after is dropped as the
model's `Kill` drops it). `make spec-traces` checks each file twice, once
per direction (`Dir` `c2a`: the client is S; `a2c`: R), with `Total`, `W`
and the bounds counted from the file (TLC would evaluate them per state).
The physical OPEN carries both directions' steps: S's OPEN of `c2a` and
R's RESUME of `a2c`; the server's `Attach` is R's `TakeOpen` of `c2a` and
S's `Resume` (or, lingering, `SAnswerAbort`) of `a2c`; a server that
left answers with `Attach` x `left`, `RLeftAnswer` of `c2a` and
`SLeftAnswer` of `a2c`. An end's `Fin` is `SFin` of the direction it
sends and `RFin` of the other; the client's `TakeFin` is `STakeFin` of
`c2a` and `RTakeFin` of `a2c` (lane PSCLOSE: the close tests
`TestCloseAfterReattachEndsBeforeTheBound`,
`TestCloseAfterReattachPathDiesBeforeTheFIN`,
`TestCloseLeftAnswersAReattach` are traced too). Not in the model:
the ABORT a peer sends back on taking one (it ends the aborter's linger
early), `Refuse` (no session: the trace has no server end). Fixtures
`spec/traces/path-*` (`make spec-mutants`): two recorded runs accepted, an
END taken twice and data sent before the RESUME rejected.

Not modelled: the peer vanishing (link's and quiclink's liveness end the
session: `Liveness.tla`, `QuicWatch.tla`); the hole punch itself (an
`Offer` that becomes `up` or `down`); revocation (nfltr
`spec/enrol/Enrol.tla` `RevocationBound` cuts the session on every path:
the direct connection carries the same organisation certificates).
