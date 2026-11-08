-------------------------------- MODULE Join --------------------------------
(* peerlink.Join of a plain conn (a TCP client) and a link session, and the *)
(* end of the session it gives the client (spec/README.md "Join's clean   *)
(* end", relay-core finding 23). The client writes N bytes and its FIN;    *)
(* the joint copies them onto the session; the peer (an agent's Mux) reads *)
(* them in order from the connection; the peer's FIN comes back the other  *)
(* way; the connection may die at any step.                                *)
(*                                                                         *)
(* The design: a direction that ends with the session's CloseWrite then    *)
(* syncs (Session.Sync: a ping buffered after the FIN, its pong read back:  *)
(* the peer's Mux read everything before it, WIRE.md section 8). While it *)
(* waits the peer's FIN is held from the client (no EOF yet); the joint   *)
(* ends the client cleanly only after the sync; a connection that dies    *)
(* before it resets the client.                                            *)
(*                                                                         *)
(* A drain (lane LINKDRAIN): the joint's side of the connection may drain *)
(* (GOAWAY): once the session ended at its Mux (both FINs crossed) the    *)
(* connection closes cleanly (Mux.leave: every buffered frame written,     *)
(* then the write side shut; the peer reads to the end and closes, its   *)
(* pongs not yet written lost). The sync's ping is buffered with the FIN  *)
(* (Session.CloseWriteSync), and a leaving connection closes only once    *)
(* every Sync's pong was read (Mux.leave): a drained connection never     *)
(* resets the client (DrainNeverResets).                                  *)
EXTENDS Naturals, Sequences

CONSTANTS
    N,       \* the client's bytes
    Mutant   \* "none"; "no-sync": end once the FIN is buffered (the code before);
             \* "no-watch": the code before review r2 Join 2: nothing watches the
             \* session while the client is idle, so a connection that dies after
             \* the peer's FIN reached the client is noticed only at the client's
             \* next write or close;
             \* "no-hold": pass the peer's FIN on while the sync waits;
             \* "done-skips-hold": the code before review r2 finding 1: the
             \* peer's FIN after the joint's CloseWrite drops the session
             \* (Done: both halves closed) and that branch closed the client
             \* whole without the hold (the same step as no-hold's);
             \* "ping-late": the code before lane LINKDRAIN: Sync buffers its
             \* ping in a step of its own after the CloseWrite, so a drain that
             \* shut the connection's write side in between (the session ended
             \* at the Mux) never sends it, and the connection's clean end
             \* resets the client (peerlink ChainDrainMiddle, Reverse);
             \* "leave-ignores-sync": the ping with the FIN, but the leaving
             \* connection closes with its pong not yet read: the peer's close
             \* may lose the pong it had not written

VARIABLES
    cw,      \* bytes the client wrote
    cfin,    \* the client wrote its FIN (CloseWrite)
    rcd,     \* client bytes the joint wrote onto the session
    d0,      \* client -> session: "run", "fin" (FIN buffered, no ping yet:
             \* ping-late only), "sync" (FIN buffered, ping out), "done"
    d1,      \* session -> client: "run", "held" (the peer's FIN waits), "done"
    toA,     \* frames on the connection toward the peer: "d", "fin", "ping"
    toR,     \* frames toward the joint: "fin", "pong"
    afs,     \* the peer wrote its FIN
    arcv,    \* bytes the peer's Mux read
    afin,    \* the peer's Mux read the client's FIN
    up,      \* the connection is up
    ceof,    \* the client read EOF
    cend,    \* the client's connection: "" open, "close" (clean), "reset"
    clean,   \* history: the joint gave the client a clean end: EOF after it read the
             \* client's FIN, or the clean close (an EOF that crosses the client's FIN
             \* on the way is no end the joint can type: as a TCP proxy's)
    dr,      \* the joint's side drains the connection (GOAWAY sent)
    wshut,   \* the drained connection's write side shut (Mux.leave: flushed first)
    pclosed, \* the peer closed its side (it read to the joint's shut)
    ppong,   \* pongs the peer's Mux buffered, not yet written
    crashed  \* history: the connection died (Crash), not a drain's clean close

vars == <<cw, cfin, rcd, d0, d1, toA, toR, afs, arcv, afin, up, ceof, cend, clean,
          dr, wshut, pclosed, crashed, ppong>>
dv == <<dr, wshut, pclosed, crashed, ppong>>

Init ==
    /\ cw = 0 /\ cfin = FALSE /\ rcd = 0
    /\ d0 = "run" /\ d1 = "run"
    /\ toA = <<>> /\ toR = <<>>
    /\ afs = FALSE /\ arcv = 0 /\ afin = FALSE
    /\ up = TRUE /\ ceof = FALSE /\ cend = "" /\ clean = FALSE
    /\ dr = FALSE /\ wshut = FALSE /\ pclosed = FALSE /\ crashed = FALSE /\ ppong = 0

\* The client.
CWrite == cw < N /\ ~cfin /\ cend = "" /\ cw' = cw + 1
          /\ UNCHANGED <<cfin, rcd, d0, d1, toA, toR, afs, arcv, afin, up, ceof, cend, clean>> /\ UNCHANGED dv
CFin == ~cfin /\ cend = "" /\ cfin' = TRUE
        /\ UNCHANGED <<cw, rcd, d0, d1, toA, toR, afs, arcv, afin, up, ceof, cend, clean>> /\ UNCHANGED dv

\* The joint: a client byte onto the session (Pump's write).
Copy == rcd < cw /\ d0 = "run" /\ up /\ rcd' = rcd + 1 /\ toA' = Append(toA, "d")
        /\ UNCHANGED <<cw, cfin, d0, d1, toR, afs, arcv, afin, up, ceof, cend, clean>> /\ UNCHANGED dv
\* The client's FIN read: CloseWrite on the session, then Sync's ping.
CloseWrite ==
    /\ rcd = cw /\ cfin /\ d0 = "run" /\ up
    /\ CASE Mutant = "no-sync" -> toA' = Append(toA, "fin") /\ d0' = "done"
         [] Mutant = "ping-late" -> toA' = Append(toA, "fin") /\ d0' = "fin"
         [] OTHER -> toA' = toA \o <<"fin", "ping">> /\ d0' = "sync"
    /\ UNCHANGED <<cw, cfin, rcd, d1, toR, afs, arcv, afin, up, ceof, cend, clean>> /\ UNCHANGED dv

\* ping-late: Sync's ping, a step after the CloseWrite; never sent once the
\* drain shut the write side.
SyncPing ==
    /\ d0 = "fin" /\ d0' = "sync"
    /\ toA' = IF up /\ ~wshut THEN Append(toA, "ping") ELSE toA
    /\ UNCHANGED <<cw, cfin, rcd, d1, toR, afs, arcv, afin, up, ceof, cend, clean>> /\ UNCHANGED dv

\* The peer's Mux reads a frame; keepAlive answers a ping.
ARecv ==
    /\ up /\ toA # <<>>
    /\ toA' = Tail(toA)
    /\ CASE Head(toA) = "d" -> arcv' = arcv + 1 /\ UNCHANGED <<afin, toR>>
         [] Head(toA) = "fin" -> afin' = TRUE /\ UNCHANGED <<arcv, toR>>
         [] Head(toA) = "ping" -> ppong' = ppong + 1 /\ UNCHANGED <<arcv, afin, toR>>
    /\ Head(toA) # "ping" => ppong' = ppong
    /\ UNCHANGED <<cw, cfin, rcd, d0, d1, afs, up, ceof, cend, clean>>
    /\ UNCHANGED <<dr, wshut, pclosed, crashed>>
\* The peer's Mux writes a pong it buffered.
PFlush == up /\ ~pclosed /\ ppong > 0 /\ ppong' = ppong - 1 /\ toR' = Append(toR, "pong")
          /\ UNCHANGED <<cw, cfin, rcd, d0, d1, toA, afs, arcv, afin, up, ceof, cend, clean>>
          /\ UNCHANGED <<dr, wshut, pclosed, crashed>>
\* The peer's FIN (its target half-closed).
AFin == ~afs /\ up /\ ~pclosed /\ afs' = TRUE /\ toR' = Append(toR, "fin")
        /\ UNCHANGED <<cw, cfin, rcd, d0, d1, toA, arcv, afin, up, ceof, cend, clean>> /\ UNCHANGED dv

\* EOF to the client (the joint's CloseWrite on it): an end once the joint
\* read the client's FIN.
Eof == ceof' = TRUE /\ clean' = (clean \/ d0 # "run")

\* The joint reads a frame of the session's connection. A FIN while the
\* sync waits ends the session at the joint (both halves closed: Mux.drop,
\* Done); peerlink's ended then closes the client whole, held as the
\* half close is (review r2 finding 1).
RRecv ==
    /\ up /\ toR # <<>>
    /\ toR' = Tail(toR)
    /\ IF Head(toR) = "fin"
       THEN IF d0 \in {"fin", "sync"} /\ Mutant \notin {"no-hold", "done-skips-hold"}
            THEN d1' = "held" /\ UNCHANGED <<d0, ceof, clean>>
            ELSE d1' = "done" /\ Eof /\ UNCHANGED d0
       ELSE \* the pong: the peer read the FIN
            /\ d0' = IF d0 = "sync" THEN "done" ELSE d0
            /\ IF d1 = "held" /\ d0 = "sync"
               THEN d1' = "done" /\ Eof
               ELSE UNCHANGED <<d1, ceof, clean>>
    /\ UNCHANGED <<cw, cfin, rcd, toA, afs, arcv, afin, up, cend>> /\ UNCHANGED dv

\* Both directions ended: the client's connection closed (a FIN, cleanly).
Finish ==
    /\ d0 = "done" /\ d1 = "done" /\ cend = ""
    /\ cend' = "close" /\ clean' = TRUE
    /\ UNCHANGED <<cw, cfin, rcd, d0, d1, toA, toR, afs, arcv, afin, up, ceof>> /\ UNCHANGED dv

\* The connection dies: what is on the way is lost.
Crash == up /\ up' = FALSE /\ toA' = <<>> /\ toR' = <<>> /\ crashed' = TRUE
         /\ UNCHANGED <<cw, cfin, rcd, d0, d1, afs, arcv, afin, ceof, cend, clean>>
         /\ UNCHANGED <<dr, wshut, pclosed, ppong>>

\* The joint's side drains the connection (Node.Drain: GOAWAY on it).
Drain == up /\ ~dr /\ dr' = TRUE
         /\ UNCHANGED <<cw, cfin, rcd, d0, d1, toA, toR, afs, arcv, afin, up, ceof, cend, clean>>
         /\ UNCHANGED <<wshut, pclosed, crashed, ppong>>
\* The session ended at the joint's Mux (its FIN buffered, the peer's read:
\* Mux.drop) on a draining connection: Mux.leave writes every buffered
\* frame, then shuts the write side (closeFlushed); not while a Sync's pong
\* is still to come (d0 "sync").
Leave == dr /\ up /\ ~wshut /\ d0 # "run" /\ d1 # "run" /\ wshut' = TRUE
         /\ (d0 # "sync" \/ Mutant \in {"ping-late", "leave-ignores-sync"})
         /\ UNCHANGED <<cw, cfin, rcd, d0, d1, toA, toR, afs, arcv, afin, up, ceof, cend, clean>>
         /\ UNCHANGED <<dr, pclosed, crashed, ppong>>
\* The peer read every frame, then the shut: it closes its side; a pong it
\* had not written is lost.
PeerClose == up /\ wshut /\ toA = <<>> /\ ~pclosed /\ pclosed' = TRUE /\ ppong' = 0
             /\ UNCHANGED <<cw, cfin, rcd, d0, d1, toA, toR, afs, arcv, afin, up, ceof, cend, clean>>
             /\ UNCHANGED <<dr, wshut, crashed>>
\* The joint's Mux read every frame the peer sent, then its close: the
\* connection ended cleanly (Done).
ConnEnd == up /\ pclosed /\ toR = <<>> /\ up' = FALSE
           /\ UNCHANGED <<cw, cfin, rcd, d0, d1, toA, toR, afs, arcv, afin, ceof, cend, clean>>
           /\ UNCHANGED dv
\* The session ends lost: a direction still running or syncing aborts the
\* client (an RST: Pump's error, a failed write, Sync's error).
Lost ==
    /\ ~up /\ cend = ""
    /\ (d0 # "done" \/ d1 # "done")
    \* no-watch: only a step of the joint's own notices (a Pump running, a
    \* sync waiting, a client byte or FIN to copy); joint.watch is the rest.
    /\ Mutant = "no-watch" => (d1 = "run" \/ d0 = "sync" \/ cw > rcd \/ (cfin /\ d0 = "run"))
    /\ cend' = "reset" /\ d0' = "done" /\ d1' = "done"
    /\ UNCHANGED <<cw, cfin, rcd, toA, toR, afs, arcv, afin, up, ceof, clean>> /\ UNCHANGED dv

Next == CWrite \/ CFin \/ Copy \/ CloseWrite \/ SyncPing \/ ARecv \/ AFin \/ RRecv
        \/ Finish \/ Crash \/ Lost \/ Drain \/ Leave \/ PeerClose \/ ConnEnd \/ PFlush

\* The joint's and the connection's steps are taken once they can be; the
\* client's and the peer's application's need not be (an idle client).
Fairness == WF_vars(Copy) /\ WF_vars(CloseWrite) /\ WF_vars(ARecv) /\ WF_vars(RRecv)
            /\ WF_vars(Finish) /\ WF_vars(Lost) /\ WF_vars(SyncPing)
            /\ WF_vars(Leave) /\ WF_vars(PeerClose) /\ WF_vars(ConnEnd)
            /\ WF_vars(PFlush)

Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK ==
    /\ d0 \in {"run", "fin", "sync", "done"} /\ d1 \in {"run", "held", "done"}
    /\ cend \in {"", "close", "reset"} /\ rcd <= cw /\ arcv <= rcd

\* A client that saw a clean end had every byte and its FIN read by the
\* peer once the connection is gone (relay-core finding 23).
NoSilentLoss == clean /\ ~up => arcv = cw /\ afin /\ cfin

\* A drained connection's clean end never resets the client: only a
\* connection that died does (lane LINKDRAIN, peerlink ChainDrainMiddle).
DrainNeverResets == cend = "reset" => crashed

\* A client whose link connection died ends (review r2 Join 2): reset, or
\* cleanly closed, however idle it is.
LostEnds == ~up ~> cend # ""

\* Witnesses (make spec-mutants: TLC must violate each): a clean end, a
\* reset, a FIN held while the sync waits.
WitnessClean == ~clean
WitnessReset == cend # "reset"
WitnessHeld == d1 # "held"
\* NoSilentLoss's case: a clean end, then the connection gone.
WitnessCleanDown == ~(clean /\ ~up)
\* DrainNeverResets' case: the drained connection's clean end, the client
\* ended cleanly.
WitnessDrained == ~(~up /\ ~crashed /\ cend = "close")
=============================================================================
