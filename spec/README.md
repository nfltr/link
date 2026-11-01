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
- Small sizes for model checking: each side's window `W` (WIRE.md's
  `window`), the most taken at once `Chunk` (`MaxChunk`; `MinWindow` is
  2 x `MaxChunk`, so `W = 2 x Chunk`), the grant threshold `W / 4`, two
  sessions, a bound on bytes written per session and direction. Trace
  validation runs the same actions with the real sizes (the trace's).
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
| `SendData(p, s)` | `Conn.dataLocked` (a data frame buffered) |
| `CloseWrite(p, s)` | `Session.CloseWrite` -> `Conn.sendClose` code 1 |
| `Close(p, s)` | `Session.Close` -> `Conn.sendClose` code 0 |
| `Reset(p, s)` | `Session.Reset` -> `Conn.sendClose` code 2 |
| `Abort(p, s)` | `Mux.cut` -> `Conn.sendAbort` (code 3 here) |
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
   (`finAt`, the bytes sent when the close was buffered, equals what
   arrived).
4. **GOAWAY.** After a side read the peer's GOAWAY it opens nothing; an
   open that arrives after the receiver's drain barrier (the pong of the
   ping it sent after its GOAWAY) or after the peer's GOAWAY never happens
   (the code refuses it with a reset: a refused crossed open is an error).
5. **Buffers.** A session's queue holds at most the receiver's window.
6. **FeatureChain answers.** The opener never receives the accepter's
   data before its ACCEPT, nor a second ACCEPT.
7. **Deadlock.** TLC's deadlock check: a state with nothing enabled is
   only the end (both sides down).

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

`make spec-mutants` checks each `Mutant` value and passes only when TLC
reports a violation for each:

- `fin-skips-queue`: a close goes ahead of the frames buffered before it
  (the FIN overtakes data);
- `grant-early`: a window update counts bytes not yet read (credit past
  the window);
- `open-after-goaway`: a side opens after it read the peer's GOAWAY.

## Trace validation

With `-tags linktrace` and `LINKTRACE=<dir>`, each `Conn` appends one JSON
line per action to `<dir>/<pair>.ndjson` under one process-wide lock, at
the point where the state changed and under the lock that changed it, so
the file's order is an order the two sides could have taken. `pair` is the
dialer's local address. Each line has the side, the action, the session id
(hex), sizes and the credit after the step, which `Trace.tla` compares with
the model's. Without the tag the recorder is a constant `false` and every
call is compiled away.
