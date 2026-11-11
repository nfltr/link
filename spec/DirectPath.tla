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
EXTENDS Integers, Sequences

CONSTANTS
    Total,    \* bytes S writes
    W,        \* the resume window: bytes S keeps until R acknowledges them
    MaxDie,   \* how often each path may die
    MaxPunch, \* candidates the relay may offer (hole punches)
    MaxMoves, \* voluntary moves (relayed to direct, direct to relayed)
    Mutant    \* "none" or a bug put back (spec-mutants)

Paths == {"relay", "direct"}
Max(a, b) == IF a > b THEN a ELSE b

VARIABLES
    decider, \* "S" or "R": the side that opens streams (the client)
    st,      \* st[e]: epoch e's stream: path, holder ("peer", "rogue"), dead
    d,       \* d[e]: frames S -> R on stream e: [t |-> "open"] or [t |-> "data", b |-> byte]
    k,       \* k[e]: frames R -> S on stream e: [t |-> "resume" or "ack", n |-> count]
    sEp,     \* S's stream
    sWait,   \* S opened sEp and waits for R's RESUME on it (decider S)
    hi,      \* bytes the application handed S so far
    nxt,     \* the next byte S sends on sEp
    acked,   \* S freed every byte below acked
    rEp,     \* R's stream: the only one it reads
    rcvd,    \* bytes R delivered (out's length)
    rAck,    \* the count R last told S on rEp
    out,     \* the bytes R delivered, in order
    relayUp, \* the relayed path (the agent links, both ends) is up
    dir,     \* the direct path: [s |-> "none" | "probing" | "up" | "down", h |-> holder]
    dies,    \* dies[p]: how often p died
    punches, moves, sends

vars == <<decider, st, d, k, sEp, sWait, hi, nxt, acked, rEp, rcvd, rAck, out,
          relayUp, dir, dies, punches, moves, sends>>

Init ==
    /\ decider \in {"S", "R"}
    \* a session starts relayed: epoch 1, on the relay, attached at both ends
    /\ st = <<[path |-> "relay", h |-> "peer", dead |-> FALSE]>>
    /\ d = <<<<>>>> /\ k = <<<<>>>>
    /\ sEp = 1 /\ sWait = FALSE /\ hi = 0 /\ nxt = 0 /\ acked = 0
    /\ rEp = 1 /\ rcvd = 0 /\ rAck = 0 /\ out = <<>>
    /\ relayUp = TRUE /\ dir = [s |-> "none", h |-> "peer"]
    /\ dies = [p \in Paths |-> 0] /\ punches = 0 /\ moves = 0 /\ sends = 0

Usable(p) == IF p = "relay" THEN relayUp ELSE dir.s = "up"
Cur == IF decider = "S" THEN sEp ELSE rEp     \* the decider's stream
Data(e) == {i \in 1..Len(d[e]) : d[e][i].t = "data"}

\* --- the paths -----------------------------------------------------------
\* The relay offers a candidate (an address from QAD or the peer, carried in
\* the session's end-to-end control messages: the relay can only withhold,
\* or point at another holder through the address).
Offer(h) ==
    /\ dir.s \in {"none", "down"} /\ punches < MaxPunch
    /\ dir' = [s |-> "probing", h |-> h] /\ punches' = punches + 1
    /\ UNCHANGED <<decider, st, d, k, sEp, sWait, hi, nxt, acked, rEp, rcvd, rAck, out, relayUp, dies, moves, sends>>
\* The direct QUIC handshake: the peer's organisation certificate (chain,
\* name, CRL) and the session's exporter token, both ways. Mutant "no-bind":
\* whoever answers at the address is taken.
Verify ==
    /\ dir.s = "probing" /\ (dir.h = "peer" \/ Mutant = "no-bind")
    /\ dir' = [dir EXCEPT !.s = "up"]
    /\ UNCHANGED <<decider, st, d, k, sEp, sWait, hi, nxt, acked, rEp, rcvd, rAck, out, relayUp, dies, punches, moves, sends>>
\* A punch that fails: no answer, or a holder that is not the peer.
Refuse ==
    /\ dir.s = "probing"
    /\ dir' = [dir EXCEPT !.s = "down"]
    /\ UNCHANGED <<decider, st, d, k, sEp, sWait, hi, nxt, acked, rEp, rcvd, rAck, out, relayUp, dies, punches, moves, sends>>
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
    /\ UNCHANGED <<decider, sEp, sWait, hi, nxt, acked, rEp, rcvd, rAck, out, punches, moves, sends>>
\* The agents redial the relay (a new agent link): new relayed streams work,
\* the old ones stay dead.
Revive ==
    /\ ~relayUp /\ relayUp' = TRUE
    /\ UNCHANGED <<decider, st, d, k, sEp, sWait, hi, nxt, acked, rEp, rcvd, rAck, out, dir, dies, punches, moves, sends>>

\* --- the decider opens a stream ----------------------------------------
\* On p: R opens it with RESUME(rcvd) and reads only it from now on; S
\* opens it with OPEN and sends nothing on it until R's RESUME.
Open(p) ==
    LET e == Len(st) + 1 IN
    /\ st' = Append(st, [path |-> p, h |-> IF p = "direct" THEN dir.h ELSE "peer", dead |-> FALSE])
    /\ IF decider = "R"
       THEN /\ d' = Append(d, <<>>) /\ k' = Append(k, <<[t |-> "resume", n |-> rcvd]>>)
            /\ rEp' = e /\ rAck' = rcvd
            /\ UNCHANGED <<sEp, sWait>>
       ELSE /\ d' = Append(d, <<[t |-> "open"]>>) /\ k' = Append(k, <<>>)
            /\ sEp' = e /\ sWait' = TRUE
            /\ UNCHANGED <<rEp, rAck>>
\* Fallback: the decider's stream ended (its path died); it opens one on a
\* path that works. Mutant "keep-dead-path": a dead direct path is kept.
Fallback(p) ==
    /\ st[Cur].dead /\ Usable(p)
    /\ (Mutant = "keep-dead-path" => st[Cur].path # "direct")
    /\ Open(p)
    /\ UNCHANGED <<decider, hi, nxt, acked, rcvd, out, relayUp, dir, dies, punches, moves, sends>>
\* A move on a live stream: to the direct path once it is verified, or back
\* (the decider's policy; bounded here).
Move(p) ==
    /\ ~st[Cur].dead /\ st[Cur].path # p /\ Usable(p) /\ moves < MaxMoves
    /\ Open(p) /\ moves' = moves + 1
    /\ UNCHANGED <<decider, hi, nxt, acked, rcvd, out, relayUp, dir, dies, punches, sends>>

\* --- S --------------------------------------------------------------------
\* The application hands S a byte; S blocks while it keeps W bytes.
Write ==
    /\ hi < Total /\ hi - acked < W /\ hi' = hi + 1
    /\ UNCHANGED <<decider, st, d, k, sEp, sWait, nxt, acked, rEp, rcvd, rAck, out, relayUp, dir, dies, punches, moves, sends>>
\* S sends byte nxt on its stream. Mutant "free-on-send": S frees it at
\* once (it trusts the path's transport ack, not R's). Mutant
\* "send-before-resume": S sends on a stream it opened before R's RESUME.
Send ==
    /\ nxt < hi /\ ~st[sEp].dead /\ (~sWait \/ Mutant = "send-before-resume")
    /\ d' = [d EXCEPT ![sEp] = Append(@, [t |-> "data", b |-> nxt])]
    /\ nxt' = nxt + 1 /\ sends' = sends + 1
    /\ acked' = IF Mutant = "free-on-send" THEN nxt + 1 ELSE acked
    /\ UNCHANGED <<decider, st, k, sEp, sWait, hi, rEp, rcvd, rAck, out, relayUp, dir, dies, punches, moves>>
\* Mutant "write-stale": S still sends on a stream it left.
SendStale ==
    /\ Mutant = "write-stale"
    /\ \E e \in 1..(sEp - 1) :
        /\ nxt < hi /\ ~st[e].dead
        /\ d' = [d EXCEPT ![e] = Append(@, [t |-> "data", b |-> nxt])]
        /\ nxt' = nxt + 1 /\ sends' = sends + 1
    /\ UNCHANGED <<decider, st, k, sEp, sWait, hi, acked, rEp, rcvd, rAck, out, relayUp, dir, dies, punches, moves>>
\* S takes R's RESUME(n): on the stream it waits on (decider S), or on a
\* newer one R opened (decider R). It sends from n and frees what is below.
\* Mutant "resume-from-acked": S resends from what it freed, not from n.
Resume ==
    \E e \in DOMAIN k :
        /\ (IF decider = "S" THEN e = sEp /\ sWait ELSE e > sEp)
        /\ k[e] # <<>> /\ Head(k[e]).t = "resume"
        /\ LET n == Head(k[e]).n IN
           /\ nxt' = IF Mutant = "resume-from-acked" THEN acked ELSE n
           /\ acked' = Max(acked, n)
        /\ k' = [k EXCEPT ![e] = Tail(@)] /\ sEp' = e /\ sWait' = FALSE
        /\ UNCHANGED <<decider, st, d, hi, rEp, rcvd, rAck, out, relayUp, dir, dies, punches, moves, sends>>
\* S takes an ACK on its stream: R holds every byte below n.
TakeAck ==
    /\ k[sEp] # <<>> /\ Head(k[sEp]).t = "ack" /\ ~sWait
    /\ acked' = Max(acked, Head(k[sEp]).n) /\ k' = [k EXCEPT ![sEp] = Tail(@)]
    /\ UNCHANGED <<decider, st, d, sEp, sWait, hi, nxt, rEp, rcvd, rAck, out, relayUp, dir, dies, punches, moves, sends>>

\* --- R --------------------------------------------------------------------
\* R takes the OPEN of a newer stream (decider S): it reads only that one
\* from now on and tells S where to start.
TakeOpen ==
    \E e \in DOMAIN d :
        /\ e > rEp /\ d[e] # <<>> /\ Head(d[e]).t = "open"
        /\ d' = [d EXCEPT ![e] = Tail(@)] /\ k' = [k EXCEPT ![e] = Append(@, [t |-> "resume", n |-> rcvd])]
        /\ rEp' = e /\ rAck' = rcvd
        /\ UNCHANGED <<decider, st, sEp, sWait, hi, nxt, acked, rcvd, out, relayUp, dir, dies, punches, moves, sends>>
\* R delivers the next byte of its stream: the stream has no offsets, so
\* whatever comes next is byte rcvd. Mutant "read-stale": R reads a stream
\* it left too.
RecvOn(e) ==
    /\ d[e] # <<>> /\ Head(d[e]).t = "data"
    /\ out' = Append(out, Head(d[e]).b) /\ rcvd' = rcvd + 1
    /\ d' = [d EXCEPT ![e] = Tail(@)]
    /\ UNCHANGED <<decider, st, k, sEp, sWait, hi, nxt, acked, rEp, rAck, relayUp, dir, dies, punches, moves, sends>>
Recv == RecvOn(rEp)
RecvStale == Mutant = "read-stale" /\ \E e \in 1..(rEp - 1) : RecvOn(e)
\* R acknowledges what it delivered, on its stream.
Ack ==
    /\ rcvd > rAck /\ ~st[rEp].dead
    /\ k' = [k EXCEPT ![rEp] = Append(@, [t |-> "ack", n |-> rcvd])] /\ rAck' = rcvd
    /\ UNCHANGED <<decider, st, d, sEp, sWait, hi, nxt, acked, rEp, rcvd, out, relayUp, dir, dies, punches, moves, sends>>

Next ==
    \/ \E h \in {"peer", "rogue"} : Offer(h)
    \/ Verify \/ Refuse \/ Revive
    \/ \E p \in Paths : Die(p) \/ Fallback(p) \/ Move(p)
    \/ Write \/ Send \/ SendStale \/ Resume \/ TakeAck
    \/ TakeOpen \/ Recv \/ RecvStale \/ Ack

\* Every step of the ends is taken while enabled; the relay is redialled;
\* a decider whose stream ended falls back. Moves, offers and deaths are
\* free choices (the relay's, the network's, the policy's).
Fairness ==
    /\ WF_vars(Write) /\ WF_vars(Send) /\ WF_vars(Resume) /\ WF_vars(TakeAck)
    /\ WF_vars(TakeOpen) /\ WF_vars(Recv) /\ WF_vars(Ack) /\ WF_vars(Revive)
    /\ WF_vars(\E p \in Paths : Fallback(p))

Spec == Init /\ [][Next]_vars /\ Fairness

\* --- what must hold ------------------------------------------------------
TypeOK ==
    /\ decider \in {"S", "R"} /\ Len(st) = Len(d) /\ Len(st) = Len(k)
    /\ sEp \in DOMAIN st /\ rEp \in DOMAIN st /\ sWait \in BOOLEAN
    /\ 0 <= acked /\ acked <= hi /\ hi <= Total /\ nxt <= hi
    /\ rcvd = Len(out)
\* R delivers bytes 0, 1, 2, ... once each, in order: none lost, none
\* twice, none out of place (at a switch or ever).
InOrder == \A i \in 1..Len(out) : out[i] = i - 1
\* S never frees a byte R lacks: a switch can always resend what R misses.
NoLoss == acked <= rcvd
\* S keeps at most W bytes per direction of a session.
BufferBound == hi - acked <= W
\* A stream goes only to the peer: never to whoever the relay pointed at.
OnlyPeer == \A e \in DOMAIN st : st[e].h = "peer"
\* Every byte arrives, if paths stop dying (they do here: MaxDie). TLC runs
\* with -deadlock: a stuck state (nothing but stuttering before every byte
\* arrived) violates this.
AllDelivered == <>(rcvd = Total)

\* Witnesses (spec-mutants: TLC must violate each).
\* every byte delivered, the last ones on the direct path
WitnessDirect == ~(rcvd = Total /\ st[rEp].path = "direct")
\* direct, then back on the relay, every byte delivered
WitnessFallback == ~(rcvd = Total /\ st[rEp].path = "relay" /\ \E e \in DOMAIN st : st[e].path = "direct" /\ e < rEp)
\* a byte sent twice (on two streams), delivered once
WitnessResent == ~(rcvd = Total /\ sends > Total)
\* the relay pointed at another holder (refused)
WitnessRogue == ~(dir.h = "rogue" /\ dir.s = "down")
\* the window full: S keeps W bytes
WitnessFull == hi - acked < W
\* the relay withheld every candidate: all delivered, relayed throughout
WitnessWithheld == ~(rcvd = Total /\ punches = 0 /\ dies["relay"] > 0)
=============================================================================
