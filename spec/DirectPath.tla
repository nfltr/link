----------------------------- MODULE DirectPath -----------------------------
(* A session's byte stream moved between paths: relayed and direct (QUIC   *)
(* hole punching), and back, with no byte lost, duplicated or reordered.   *)
(* The design is nfltr docs/decisions/direct-paths-2026-11-11.md "Design"; *)
(* this spec came before any code (lane DIRECTPATH).                       *)
(*                                                                         *)
(* One direction of one session: the sender S hands bytes 0..Total-1 (the  *)
(* end-to-end TLS stream's, opaque here) to the path layer, the receiver R *)
(* delivers them. The other direction is the same model with the decider's *)
(* side swapped, so the decider is chosen at Init: the client (the side    *)
(* that dials) is the one side that opens path streams; it is S on the     *)
(* client-to-agent direction and R on the other.                           *)
(*                                                                         *)
(* Each attach opens a fresh stream (an epoch) on one path: a new link     *)
(* session through the relay, or a new QUIC stream on the direct           *)
(* connection. A stream is FIFO; its data carries no offsets (the first    *)
(* byte S sends on it is byte n of the RESUME(n) R sent on it). R reads    *)
(* only its newest stream; S frees a byte only when R acknowledged it      *)
(* (ACK(n), RESUME(n)), so S keeps at most W bytes R may lack. A path      *)
(* dies by losing every frame in flight on it. The relay may withhold a    *)
(* candidate (no Offer), or offer one whose holder is not the peer (a      *)
(* forged QAD address or candidate): the binding check (the peer's         *)
(* organisation certificate and the session's exporter token, ProVerif     *)
(* relay spec/proverif/e2ee_tcp.pv queries 21-24) refuses it.              *)
(*                                                                         *)
(* A direction's end (gap G1, lane DIRECTPATH2): after its Total bytes S   *)
(* sends END, which takes position Total of the stream: retained, resent   *)
(* from a RESUME and acknowledged like a byte (R's counts include it once  *)
(* it took it). A session's reset (ABORT), by either end, at any time: the *)
(* aborter stops its data and keeps answering every stream the session     *)
(* opens with ABORT (the decider aborting opens one when its stream ended) *)
(* until the peer took it; an ABORT sent on a stream the peer left, or     *)
(* lost with a path, is sent again on the next.                            *)
(*                                                                         *)
(* The session's close (lane PSCLOSE): the end that is not the decider     *)
(* (the agent), once its view of the direction is complete (S: the END     *)
(* acknowledged; R: the END taken and its last ACK sent), ends its stream  *)
(* with FIN after its last frame and leaves. The decider, complete, waits  *)
(* for that FIN, which says the peer has everything; then it sends its own *)
(* FIN and leaves. It ends last because it alone can open a stream: until  *)
(* it took the FIN, a stream that ends makes it open another, which an end *)
(* that left answers with its count and FIN. Each end's other direction is *)
(* taken as complete already (the worst case: an end may leave as soon as  *)
(* this direction allows).                                                 *)
EXTENDS Integers, Sequences

CONSTANTS
    Total,    \* bytes S writes, then it ends its direction (END)
    W,        \* the resume window: bytes S keeps until R acknowledges them
    MaxDie,   \* how often each path may die
    MaxPunch, \* candidates the relay may offer (hole punches)
    MaxMoves, \* voluntary moves (relayed to direct, direct to relayed)
    MaxAbort, \* 0 or 1: whether an end may reset the session
    Mutant    \* "none" or a bug put back (spec-mutants)

Paths == {"relay", "direct"}
Max(a, b) == IF a > b THEN a ELSE b

VARIABLES
    decider, \* "S" or "R": the side that opens streams (the client)
    st,      \* st[e]: epoch e's stream: path, holder ("peer", "rogue"), dead
    d,       \* d[e]: frames S -> R on stream e: open, data(b), end, abort
    k,       \* k[e]: frames R -> S on stream e: resume(n), ack(n), abort
    sEp,     \* S's stream
    sWait,   \* S opened sEp and waits for R's RESUME on it (decider S)
    hi,      \* bytes the application handed S so far
    nxt,     \* the next position S sends on sEp (Total: the END)
    acked,   \* S freed every position below acked (Total + 1: the END too)
    rEp,     \* R's stream: the only one it reads
    rcvd,    \* bytes R delivered (out's length)
    rEnds,   \* how often R took an END (its direction's end)
    rAck,    \* the count R last told S on rEp
    out,     \* the bytes R delivered, in order
    ab,      \* who reset the session: "none", "S" or "R"
    sGone,   \* S knows the session is reset (it reset it, or took the ABORT)
    rGone,   \* R knows
    relayUp, \* the relayed path (the agent links, both ends) is up
    dir,     \* the direct path: [s |-> "none" | "probing" | "up" | "down", h |-> holder]
    dies,    \* dies[p]: how often p died
    punches, moves, sends,
    endSends, \* ENDs S sent (on any stream)
    abSends,  \* ABORTs sent (on any stream)
    sFin, rFin, \* the stream the end last sent its FIN on (0: none)
    sGot, rGot, \* the stream the end (the decider) took the peer's FIN on (0: none)
    sLeft, rLeft, \* the end left the session after its clean end (Close and Done)
    answers   \* streams an end that left answered (LeftAnswer)

vars == <<decider, st, d, k, sEp, sWait, hi, nxt, acked, rEp, rcvd, rEnds, rAck, out,
          ab, sGone, rGone, relayUp, dir, dies, punches, moves, sends, endSends, abSends,
          sFin, rFin, sGot, rGot, sLeft, rLeft, answers>>
sVars == <<sEp, sWait, hi, nxt, acked>>
rVars == <<rEp, rcvd, rEnds, rAck, out>>
endVars == <<ab, sGone, rGone, endSends, abSends>>
pathVars == <<relayUp, dir, dies, punches>>
closeVars == <<sFin, rFin, sGot, rGot, sLeft, rLeft, answers>>

Init ==
    /\ decider \in {"S", "R"}
    \* a session starts relayed: epoch 1, on the relay, attached at both ends
    /\ st = <<[path |-> "relay", h |-> "peer", dead |-> FALSE]>>
    /\ d = <<<<>>>> /\ k = <<<<>>>>
    /\ sEp = 1 /\ sWait = FALSE /\ hi = 0 /\ nxt = 0 /\ acked = 0
    /\ rEp = 1 /\ rcvd = 0 /\ rEnds = 0 /\ rAck = 0 /\ out = <<>>
    /\ ab = "none" /\ sGone = FALSE /\ rGone = FALSE
    /\ relayUp = TRUE /\ dir = [s |-> "none", h |-> "peer"]
    /\ dies = [p \in Paths |-> 0] /\ punches = 0 /\ moves = 0 /\ sends = 0
    /\ endSends = 0 /\ abSends = 0
    /\ sFin = 0 /\ rFin = 0 /\ sGot = 0 /\ rGot = 0 /\ sLeft = FALSE /\ rLeft = FALSE /\ answers = 0

Usable(p) == IF p = "relay" THEN relayUp ELSE dir.s = "up"
Cur == IF decider = "S" THEN sEp ELSE rEp     \* the decider's stream
\* the positions S may send: its bytes, and the END once it wrote them all
Sendable == IF hi = Total THEN Total + 1 ELSE hi
\* R's count: the bytes it delivered, and the END once it took it. Mutant
\* "resume-drops-fin": a RESUME leaves the END out (S sends it again).
RPos == rcvd + (IF rEnds > 0 THEN 1 ELSE 0)
RResume == IF Mutant = "resume-drops-fin" THEN rcvd ELSE RPos
\* the direction ended cleanly: every byte and the END delivered, all freed
Clean == rcvd = Total /\ rEnds > 0 /\ acked = Total + 1
\* the aborter keeps answering with ABORT; an end that took the peer's is done.
\* Mutant "abort-not-resent": the ABORT goes once, on the aborter's stream.
Lingers(x) == ab = x /\ Mutant # "abort-not-resent"
SLive == ~sGone /\ ~sLeft
RLive == ~rGone /\ ~rLeft
DeciderActs == IF decider = "S" THEN SLive \/ Lingers("S") ELSE RLive \/ Lingers("R")
Abort == [t |-> "abort"]

\* --- the paths -----------------------------------------------------------
\* The relay offers a candidate (an address from QAD or the peer, carried in
\* the session's end-to-end control messages: the relay can only withhold,
\* or point at another holder through the address).
Offer(h) ==
    /\ dir.s \in {"none", "down"} /\ punches < MaxPunch
    /\ dir' = [s |-> "probing", h |-> h] /\ punches' = punches + 1
    /\ UNCHANGED <<decider, st, d, k, moves, sends, relayUp, dies>> /\ UNCHANGED <<sVars, rVars, endVars>>
    /\ UNCHANGED closeVars
\* The direct QUIC handshake: the peer's organisation certificate (chain,
\* name, CRL) and the session's exporter token, both ways. Mutant "no-bind":
\* whoever answers at the address is taken.
Verify ==
    /\ dir.s = "probing" /\ (dir.h = "peer" \/ Mutant = "no-bind")
    /\ dir' = [dir EXCEPT !.s = "up"]
    /\ UNCHANGED <<decider, st, d, k, moves, sends, relayUp, dies, punches>> /\ UNCHANGED <<sVars, rVars, endVars>>
    /\ UNCHANGED closeVars
\* A punch that fails: no answer, or a holder that is not the peer.
Refuse ==
    /\ dir.s = "probing"
    /\ dir' = [dir EXCEPT !.s = "down"]
    /\ UNCHANGED <<decider, st, d, k, moves, sends, relayUp, dies, punches>> /\ UNCHANGED <<sVars, rVars, endVars>>
    /\ UNCHANGED closeVars
\* A path dies: every frame in flight on it is lost, its streams end.
Kill(p) ==
    /\ st' = [e \in DOMAIN st |-> IF st[e].path = p THEN [st[e] EXCEPT !.dead = TRUE] ELSE st[e]]
    /\ d' = [e \in DOMAIN d |-> IF st[e].path = p THEN <<>> ELSE d[e]]
    /\ k' = [e \in DOMAIN k |-> IF st[e].path = p THEN <<>> ELSE k[e]]
Die(p) ==
    /\ Usable(p) /\ dies[p] < MaxDie
    /\ Kill(p) /\ dies' = [dies EXCEPT ![p] = @ + 1]
    /\ IF p = "relay" THEN relayUp' = FALSE /\ UNCHANGED dir
       ELSE dir' = [dir EXCEPT !.s = "down"] /\ UNCHANGED relayUp
    /\ UNCHANGED <<decider, punches, moves, sends>> /\ UNCHANGED <<sVars, rVars, endVars>>
    /\ UNCHANGED closeVars
\* The agents redial the relay (a new agent link): new relayed streams work,
\* the old ones stay dead.
Revive ==
    /\ ~relayUp /\ relayUp' = TRUE
    /\ UNCHANGED <<decider, st, d, k, dir, dies, punches, moves, sends>> /\ UNCHANGED <<sVars, rVars, endVars>>
    /\ UNCHANGED closeVars

\* --- the decider opens a stream ----------------------------------------
\* On p: R opens it with RESUME(its count) and reads only it from now on;
\* S opens it with OPEN and sends nothing on it until R's RESUME. An
\* aborter opens it with its ABORT instead (S: after the OPEN).
Open(p) ==
    LET e == Len(st) + 1 IN
    /\ st' = Append(st, [path |-> p, h |-> IF p = "direct" THEN dir.h ELSE "peer", dead |-> FALSE])
    /\ IF decider = "R"
       THEN /\ d' = Append(d, <<>>)
            /\ k' = Append(k, <<IF rGone THEN Abort ELSE [t |-> "resume", n |-> RResume]>>)
            /\ abSends' = abSends + (IF rGone THEN 1 ELSE 0)
            /\ rEp' = e /\ rAck' = RResume
            /\ UNCHANGED <<sEp, sWait>>
       ELSE /\ d' = Append(d, IF sGone THEN <<[t |-> "open"], Abort>> ELSE <<[t |-> "open"]>>)
            /\ k' = Append(k, <<>>)
            /\ abSends' = abSends + (IF sGone THEN 1 ELSE 0)
            /\ sEp' = e /\ sWait' = TRUE
            /\ UNCHANGED <<rEp, rAck>>
\* Fallback: the decider's stream ended (its path died); it opens one on a
\* path that works. Mutant "keep-dead-path": a dead direct path is kept.
Fallback(p) ==
    /\ st[Cur].dead /\ Usable(p) /\ DeciderActs
    /\ (Mutant = "keep-dead-path" => st[Cur].path # "direct")
    /\ Open(p)
    /\ UNCHANGED <<decider, hi, nxt, acked, rcvd, rEnds, out, ab, sGone, rGone, endSends>>
    /\ UNCHANGED <<pathVars, moves, sends>>
    /\ UNCHANGED closeVars
\* A move on a live stream: to the direct path once it is verified, or back
\* (the decider's policy; bounded here; none once the session is reset).
Move(p) ==
    /\ ~st[Cur].dead /\ st[Cur].path # p /\ Usable(p) /\ moves < MaxMoves
    /\ SLive /\ RLive
    /\ Open(p) /\ moves' = moves + 1
    /\ UNCHANGED <<decider, hi, nxt, acked, rcvd, rEnds, out, ab, sGone, rGone, endSends>>
    /\ UNCHANGED <<pathVars, sends>>
    /\ UNCHANGED closeVars

\* --- either end resets the session --------------------------------------
\* The ABORT goes on the aborter's stream if it is alive (the peer may have
\* left it, or its path may die: Lingers sends it again).
AbortBy(x) ==
    /\ MaxAbort > 0 /\ ab = "none" /\ ~Clean /\ ~sLeft /\ ~rLeft
    /\ ab' = x /\ abSends' = abSends + 1
    /\ IF x = "S"
       THEN /\ sGone' = TRUE /\ UNCHANGED <<rGone, k>>
            /\ d' = IF st[sEp].dead THEN d ELSE [d EXCEPT ![sEp] = Append(@, Abort)]
       ELSE /\ rGone' = TRUE /\ UNCHANGED <<sGone, d>>
            /\ k' = IF st[rEp].dead THEN k ELSE [k EXCEPT ![rEp] = Append(@, Abort)]
    /\ UNCHANGED <<decider, st, endSends, moves, sends>> /\ UNCHANGED <<sVars, rVars, pathVars>>
    /\ UNCHANGED closeVars

\* --- S --------------------------------------------------------------------
\* The application hands S a byte; S blocks while it keeps W bytes.
Write ==
    /\ SLive /\ hi < Total /\ hi - acked < W /\ hi' = hi + 1
    /\ UNCHANGED <<decider, st, d, k, sEp, sWait, nxt, acked, moves, sends>> /\ UNCHANGED <<rVars, endVars, pathVars>>
    /\ UNCHANGED closeVars
\* S sends position nxt on its stream: byte nxt, or the END after the last.
\* Mutant "free-on-send": S frees it at once (it trusts the path's transport
\* ack, not R's). Mutant "send-before-resume": S sends on a stream it opened
\* before R's RESUME.
Send ==
    /\ SLive /\ nxt < Sendable /\ ~st[sEp].dead /\ (~sWait \/ Mutant = "send-before-resume")
    /\ d' = [d EXCEPT ![sEp] = Append(@, IF nxt = Total THEN [t |-> "end"] ELSE [t |-> "data", b |-> nxt])]
    /\ nxt' = nxt + 1 /\ sends' = sends + 1
    /\ endSends' = endSends + (IF nxt = Total THEN 1 ELSE 0)
    /\ acked' = IF Mutant = "free-on-send" THEN nxt + 1 ELSE acked
    /\ UNCHANGED <<decider, st, k, sEp, sWait, hi, ab, sGone, rGone, abSends, moves>> /\ UNCHANGED <<rVars, pathVars>>
    /\ UNCHANGED closeVars
\* Mutant "end-early": S ends its direction once the application closed,
\* before every byte was sent (the END overtakes data).
SendEndEarly ==
    /\ Mutant = "end-early" /\ SLive /\ hi = Total /\ nxt < Total /\ ~st[sEp].dead /\ ~sWait
    /\ d' = [d EXCEPT ![sEp] = Append(@, [t |-> "end"])]
    /\ nxt' = Total + 1 /\ sends' = sends + 1 /\ endSends' = endSends + 1
    /\ UNCHANGED <<decider, st, k, sEp, sWait, hi, acked, ab, sGone, rGone, abSends, moves>> /\ UNCHANGED <<rVars, pathVars>>
    /\ UNCHANGED closeVars
\* Mutant "write-stale": S still sends on a stream it left.
SendStale ==
    /\ Mutant = "write-stale"
    /\ \E e \in 1..(sEp - 1) :
        /\ nxt < hi /\ ~st[e].dead
        /\ d' = [d EXCEPT ![e] = Append(@, [t |-> "data", b |-> nxt])]
        /\ nxt' = nxt + 1 /\ sends' = sends + 1
    /\ UNCHANGED <<decider, st, k, sEp, sWait, hi, acked, moves>> /\ UNCHANGED <<rVars, endVars, pathVars>>
    /\ UNCHANGED closeVars
\* S takes R's RESUME(n): on the stream it waits on (decider S), or on a
\* newer one R opened (decider R). It sends from n and frees what is below.
\* Mutant "resume-from-acked": S resends from what it freed, not from n.
Resume ==
    \E e \in DOMAIN k :
        /\ SLive
        /\ (IF decider = "S" THEN e = sEp /\ sWait ELSE e > sEp)
        /\ k[e] # <<>> /\ Head(k[e]).t = "resume"
        /\ LET n == Head(k[e]).n IN
           /\ nxt' = IF Mutant = "resume-from-acked" THEN acked ELSE n
           /\ acked' = Max(acked, n)
        /\ k' = [k EXCEPT ![e] = Tail(@)] /\ sEp' = e /\ sWait' = FALSE
        /\ UNCHANGED <<decider, st, d, hi, moves, sends>> /\ UNCHANGED <<rVars, endVars, pathVars>>
        /\ UNCHANGED closeVars
\* S takes an ACK on its stream: R holds every position below n.
TakeAck ==
    /\ SLive /\ k[sEp] # <<>> /\ Head(k[sEp]).t = "ack" /\ ~sWait
    /\ acked' = Max(acked, Head(k[sEp]).n) /\ k' = [k EXCEPT ![sEp] = Tail(@)]
    /\ UNCHANGED <<decider, st, d, sEp, sWait, hi, nxt, moves, sends>> /\ UNCHANGED <<rVars, endVars, pathVars>>
    /\ UNCHANGED closeVars
\* S takes R's ABORT: on its stream, or (decider R) on a newer one R opened.
STakeAbort ==
    \E e \in DOMAIN k :
        /\ SLive
        /\ (IF decider = "S" THEN e = sEp ELSE e >= sEp)
        /\ k[e] # <<>> /\ Head(k[e]).t = "abort"
        /\ sGone' = TRUE /\ sEp' = e /\ k' = [k EXCEPT ![e] = Tail(@)]
        /\ UNCHANGED <<decider, st, d, sWait, hi, nxt, acked, ab, rGone, endSends, abSends, moves, sends>>
        /\ UNCHANGED <<rVars, pathVars>>
        /\ UNCHANGED closeVars
\* S reset the session and R (the decider) opened a newer stream: S answers
\* its RESUME with the ABORT.
SAnswerAbort ==
    \E e \in DOMAIN k :
        /\ Lingers("S") /\ decider = "R" /\ e > sEp
        /\ k[e] # <<>> /\ Head(k[e]).t = "resume"
        /\ k' = [k EXCEPT ![e] = Tail(@)] /\ sEp' = e
        /\ d' = IF st[e].dead THEN d ELSE [d EXCEPT ![e] = Append(@, Abort)]
        /\ abSends' = abSends + 1
        /\ UNCHANGED <<decider, st, sWait, hi, nxt, acked, ab, sGone, rGone, endSends, moves, sends>>
        /\ UNCHANGED <<rVars, pathVars>>
        /\ UNCHANGED closeVars

\* --- R --------------------------------------------------------------------
\* R takes the OPEN of a newer stream (decider S): it reads only that one
\* from now on and tells S where to start, or (it reset the session)
\* answers with the ABORT.
TakeOpen ==
    \E e \in DOMAIN d :
        /\ RLive \/ Lingers("R")
        /\ e > rEp /\ d[e] # <<>> /\ Head(d[e]).t = "open"
        /\ d' = [d EXCEPT ![e] = Tail(@)]
        /\ k' = [k EXCEPT ![e] = Append(@, IF rGone THEN Abort ELSE [t |-> "resume", n |-> RResume])]
        /\ abSends' = abSends + (IF rGone THEN 1 ELSE 0)
        /\ rEp' = e /\ rAck' = RResume
        /\ UNCHANGED <<decider, st, rcvd, rEnds, out, ab, sGone, rGone, endSends, moves, sends>>
        /\ UNCHANGED <<sVars, pathVars>>
        /\ UNCHANGED closeVars
\* R takes the next frame of its stream: the stream has no offsets, so a
\* byte is byte rcvd, an END the end of the direction, an ABORT the reset.
\* Mutant "read-stale": R reads a stream it left too.
RecvOn(e) ==
    /\ RLive /\ d[e] # <<>> /\ Head(d[e]).t \in {"data", "end", "abort"}
    /\ LET f == Head(d[e]) IN
       /\ out' = IF f.t = "data" THEN Append(out, f.b) ELSE out
       /\ rcvd' = rcvd + (IF f.t = "data" THEN 1 ELSE 0)
       /\ rEnds' = rEnds + (IF f.t = "end" THEN 1 ELSE 0)
       /\ rGone' = (f.t = "abort")
    /\ d' = [d EXCEPT ![e] = Tail(@)]
    /\ UNCHANGED <<decider, st, k, rEp, rAck, ab, sGone, endSends, abSends, moves, sends>>
    /\ UNCHANGED <<sVars, pathVars>>
    /\ UNCHANGED closeVars
Recv == RecvOn(rEp)
RecvStale == Mutant = "read-stale" /\ \E e \in 1..(rEp - 1) : RecvOn(e)
\* R acknowledges what it took, on its stream.
Ack ==
    /\ RLive /\ RPos > rAck /\ ~st[rEp].dead
    /\ k' = [k EXCEPT ![rEp] = Append(@, [t |-> "ack", n |-> RPos])] /\ rAck' = RPos
    /\ UNCHANGED <<decider, st, d, rEp, rcvd, rEnds, out, moves, sends>> /\ UNCHANGED <<sVars, endVars, pathVars>>
    /\ UNCHANGED closeVars

\* --- the close (lane PSCLOSE) ------------------------------------------
Fin == [t |-> "fin"]
\* the end's view of the direction is complete
SDone == acked = Total + 1
RDone == rEnds > 0 /\ rAck = RPos
\* An end complete in its view ends its stream with FIN after its last
\* frame and leaves; the decider only once it took the peer's FIN on that
\* stream. Mutant "leave-at-complete": the decider does not wait for it
\* (its last ACK may then be lost with a path, and nobody opens another).
Waits(x) == decider = x /\ Mutant # "leave-at-complete"
SFin ==
    /\ SLive /\ SDone /\ sFin # sEp /\ ~st[sEp].dead /\ (Waits("S") => sGot = sEp)
    /\ d' = [d EXCEPT ![sEp] = Append(@, Fin)] /\ sFin' = sEp /\ sLeft' = TRUE
    /\ UNCHANGED <<decider, st, k, rFin, sGot, rGot, rLeft, moves, sends, answers>> /\ UNCHANGED <<sVars, rVars, endVars, pathVars>>
RFin ==
    /\ RLive /\ RDone /\ rFin # rEp /\ ~st[rEp].dead /\ (Waits("R") => rGot = rEp)
    /\ k' = [k EXCEPT ![rEp] = Append(@, Fin)] /\ rFin' = rEp /\ rLeft' = TRUE
    /\ UNCHANGED <<decider, st, d, sFin, sGot, rGot, sLeft, moves, sends, answers>> /\ UNCHANGED <<sVars, rVars, endVars, pathVars>>
\* The decider, complete, takes the peer's FIN on its stream.
STakeFin ==
    /\ SLive /\ decider = "S" /\ SDone /\ k[sEp] # <<>> /\ Head(k[sEp]).t = "fin"
    /\ k' = [k EXCEPT ![sEp] = Tail(@)] /\ sGot' = sEp
    /\ UNCHANGED <<decider, st, d, sFin, rFin, rGot, sLeft, rLeft, moves, sends, answers>> /\ UNCHANGED <<sVars, rVars, endVars, pathVars>>
RTakeFin ==
    /\ RLive /\ decider = "R" /\ RDone /\ d[rEp] # <<>> /\ Head(d[rEp]).t = "fin"
    /\ d' = [d EXCEPT ![rEp] = Tail(@)] /\ rGot' = rEp
    /\ UNCHANGED <<decider, st, k, sFin, rFin, sGot, sLeft, rLeft, moves, sends, answers>> /\ UNCHANGED <<sVars, rVars, endVars, pathVars>>
\* An end that left answers a newer stream the decider opened with its
\* count (R: the RESUME) and FIN.
RLeftAnswer ==
    \E e \in DOMAIN d :
        /\ rLeft /\ ~rGone /\ e > rEp /\ d[e] # <<>> /\ Head(d[e]).t = "open"
        /\ d' = [d EXCEPT ![e] = Tail(@)]
        /\ k' = [k EXCEPT ![e] = @ \o <<[t |-> "resume", n |-> RResume], Fin>>]
        /\ rEp' = e /\ rFin' = e /\ answers' = answers + 1
        /\ UNCHANGED <<decider, st, rcvd, rEnds, rAck, out, sFin, sGot, rGot, sLeft, rLeft, moves, sends>>
        /\ UNCHANGED <<sVars, endVars, pathVars>>
SLeftAnswer ==
    \E e \in DOMAIN k :
        /\ sLeft /\ ~sGone /\ e > sEp /\ k[e] # <<>> /\ Head(k[e]).t = "resume"
        /\ k' = [k EXCEPT ![e] = Tail(@)] /\ d' = [d EXCEPT ![e] = Append(@, Fin)]
        /\ sEp' = e /\ sFin' = e /\ answers' = answers + 1
        /\ UNCHANGED <<decider, st, sWait, hi, nxt, acked, rFin, sGot, rGot, sLeft, rLeft, moves, sends>>
        /\ UNCHANGED <<rVars, endVars, pathVars>>

Next ==
    \/ \E h \in {"peer", "rogue"} : Offer(h)
    \/ Verify \/ Refuse \/ Revive
    \/ \E p \in Paths : Die(p) \/ Fallback(p) \/ Move(p)
    \/ \E x \in {"S", "R"} : AbortBy(x)
    \/ Write \/ Send \/ SendEndEarly \/ SendStale \/ Resume \/ TakeAck \/ STakeAbort \/ SAnswerAbort
    \/ TakeOpen \/ Recv \/ RecvStale \/ Ack
    \/ SFin \/ RFin \/ STakeFin \/ RTakeFin \/ RLeftAnswer \/ SLeftAnswer

\* Every step of the ends is taken while enabled; the relay is redialled;
\* a decider whose stream ended falls back. Moves, offers, deaths and
\* resets are free choices (the relay's, the network's, the policy's, the
\* application's).
Fairness ==
    /\ WF_vars(Write) /\ WF_vars(Send) /\ WF_vars(Resume) /\ WF_vars(TakeAck)
    /\ WF_vars(STakeAbort) /\ WF_vars(SAnswerAbort)
    /\ WF_vars(TakeOpen) /\ WF_vars(Recv) /\ WF_vars(Ack) /\ WF_vars(Revive)
    /\ WF_vars(\E p \in Paths : Fallback(p))
    /\ WF_vars(SFin) /\ WF_vars(RFin) /\ WF_vars(STakeFin) /\ WF_vars(RTakeFin)
    /\ WF_vars(RLeftAnswer) /\ WF_vars(SLeftAnswer)

Spec == Init /\ [][Next]_vars /\ Fairness

\* --- what must hold ------------------------------------------------------
TypeOK ==
    /\ decider \in {"S", "R"} /\ Len(st) = Len(d) /\ Len(st) = Len(k)
    /\ sEp \in DOMAIN st /\ rEp \in DOMAIN st /\ sWait \in BOOLEAN
    /\ 0 <= acked /\ acked <= Sendable /\ hi <= Total /\ nxt <= Sendable
    /\ rcvd = Len(out) /\ rEnds \in Nat
    /\ ab \in {"none", "S", "R"} /\ sGone \in BOOLEAN /\ rGone \in BOOLEAN
    /\ {sFin, rFin, sGot, rGot} \subseteq 0..Len(st) /\ sLeft \in BOOLEAN /\ rLeft \in BOOLEAN /\ answers \in Nat
\* R delivers bytes 0, 1, 2, ... once each, in order: none lost, none
\* twice, none out of place (at a switch or ever).
InOrder == \A i \in 1..Len(out) : out[i] = i - 1
\* S never frees a position R lacks: a switch can always resend what R misses.
NoLoss == acked <= RPos
\* S keeps at most W bytes per direction of a session.
BufferBound == hi - acked <= W
\* A stream goes only to the peer: never to whoever the relay pointed at.
OnlyPeer == \A e \in DOMAIN st : st[e].h = "peer"
\* R ends the direction once, and only after its last byte: an END never
\* overtakes data, and one sent again is never taken twice.
EndAfterData == rEnds > 0 => rcvd = Total
EndOnce == rEnds <= 1
\* Every byte and the END arrive and S frees them all, if paths stop dying
\* (they do here: MaxDie), or the session's reset reached both ends. TLC
\* runs with -deadlock: a stuck state violates this.
AllDelivered == <>(Clean \/ ((sGone \/ sLeft) /\ (rGone \/ rLeft)))
\* A reset, across any switch, reaches both ends (or an end that had left).
ResetReaches == (ab # "none") ~> ((sGone \/ sLeft) /\ (rGone \/ rLeft))
\* The decider (the one end that can open a stream) leaves only once the
\* direction is clean: the peer has every byte, the END and its count.
LeaveLast == (IF decider = "S" THEN sLeft ELSE rLeft) => Clean
\* Both ends leave (Close, then Done) after a clean end, across any path
\* switch and death, unless the session was reset: no end waits out a
\* bound while both live (lane PSCLOSE).
CloseEnds == <>((sLeft /\ rLeft) \/ ab # "none")

\* Witnesses (spec-mutants: TLC must violate each).
\* every byte delivered, the last ones on the direct path
WitnessDirect == ~(rcvd = Total /\ st[rEp].path = "direct")
\* direct, then back on the relay, every byte delivered
WitnessFallback == ~(rcvd = Total /\ st[rEp].path = "relay" /\ \E e \in DOMAIN st : st[e].path = "direct" /\ e < rEp)
\* a byte sent twice (on two streams), delivered once
WitnessResent == ~(rcvd = Total /\ sends > Total + endSends)
\* the relay pointed at another holder (refused)
WitnessRogue == ~(dir.h = "rogue" /\ dir.s = "down")
\* the window full: S keeps W bytes
WitnessFull == hi - acked < W
\* the relay withheld every candidate: all delivered, relayed throughout
WitnessWithheld == ~(rcvd = Total /\ punches = 0 /\ dies["relay"] > 0)
\* the END sent on two streams, taken once, the direction clean
WitnessEndResent == ~(Clean /\ endSends > 1)
\* the reset sent again on a newer stream (across a switch), known to both
WitnessResetAcross == ~(sGone /\ rGone /\ abSends > 1)
\* the reset reached both ends on the direct path
WitnessResetDirect == ~(sGone /\ rGone /\ st[rEp].path = "direct" /\ st[sEp].path = "direct")
\* both ends left, the decider after it opened a stream to the end that
\* had left (that end's FIN lost with a path) and took its answer
WitnessLeftAnswer == ~(sLeft /\ rLeft /\ answers > 0)
=============================================================================
