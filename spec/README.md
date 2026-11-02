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
- Not modelled: the handshake, message frames, `max_sessions` (`ErrFull`),
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
| `Open(p, s)` | `Conn.sendOpen` (after the OPEN/CHAIN-OPEN is buffered) |
| `Accept(p, s)` | `Conn.sendAccept`, `Conn.acceptLocked` |
| `Acquire(p, s, n)` | `Credit.acquire` (credit taken) |
| `SendData(p, s)` | `Conn.dataLocked` (a data frame buffered; `Conn.sendData` checks the write side and the credit first) |
| `Refund(p, s)` | `Credit.refund` (a `Write`'s credit given back, nothing sent: its write side closed, its credit ended or its deadline passed) |
| `CloseWrite(p, s)` | `Session.CloseWrite` -> `Conn.sendClose` code 1 |
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
5. **Buffers.** A session's queue holds at most the receiver's window.
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
  `p.mu`: an `Open` dialled meanwhile, past Max).

**`Liveness.tla`** (`Conn.keepAlive`) in discrete time: the last frame
received (`rxAt`), the ping waiting for an answer (`pingAt`), the one
timer, which fires at its deadline or up to `Late` ticks after (a starved
process: the Go timer and the goroutine that reads it), a peer that
answers a ping within `Rtt < Timeout` while alive and goes silent at any
time. `Liveness.cfg`: Interval 2, Timeout 3, Late 1, Rtt 2; under a
second.

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
  the ping's time: `NoFalseDead` breaks once a fire is late).

**`TraceLive.tla`** checks keepAlive's recorded steps against
`Liveness.tla`: with `-tags linktrace` each connection also writes
`<trace>.ndjson.live`, one line at keepAlive's start (`Live`: Interval,
Timeout), one per timer fire (`Fire`: now, `rxAt`, `pingAt`, as read) and
one when it ends the connection (`Dead`), times in microseconds since the
Conn's `t0`, floored (a deadline fires at or after it, so every
comparison keeps its outcome). Each side's steps must be `FireAt` steps
of the model: no fire before the model's deadline, the model's `pingAt`
at each, `Dead` exactly when the model ends the connection, and
`NeverEarly`. `make spec-traces` checks every `.live` file the traced
tests write (the dead-peer tests' take each `FireAt` branch);
`make spec-mutants` checks the fixtures in `spec/traces/`: `live-ok`
accepted, `live-dead-early` (dead before Timeout after the ping) and
`live-fire-early` (a fire before its deadline) rejected. The Pool has no
recorder yet (left: `maintain`, `Open`'s pick and dial, `Drain`).

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
