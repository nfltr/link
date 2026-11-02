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
EXTENDS Naturals, Sequences

CONSTANTS
    N,       \* the client's bytes
    Mutant   \* "none"; "no-sync": end once the FIN is buffered (the code before);
             \* "no-hold": pass the peer's FIN on while the sync waits

VARIABLES
    cw,      \* bytes the client wrote
    cfin,    \* the client wrote its FIN (CloseWrite)
    rcd,     \* client bytes the joint wrote onto the session
    d0,      \* client -> session: "run", "sync" (FIN buffered, ping out), "done"
    d1,      \* session -> client: "run", "held" (the peer's FIN waits), "done"
    toA,     \* frames on the connection toward the peer: "d", "fin", "ping"
    toR,     \* frames toward the joint: "fin", "pong"
    afs,     \* the peer wrote its FIN
    arcv,    \* bytes the peer's Mux read
    afin,    \* the peer's Mux read the client's FIN
    up,      \* the connection is up
    ceof,    \* the client read EOF
    cend,    \* the client's connection: "" open, "close" (clean), "reset"
    clean    \* history: the joint gave the client a clean end: EOF after it read the
             \* client's FIN, or the clean close (an EOF that crosses the client's FIN
             \* on the way is no end the joint can type: as a TCP proxy's)

vars == <<cw, cfin, rcd, d0, d1, toA, toR, afs, arcv, afin, up, ceof, cend, clean>>

Init ==
    /\ cw = 0 /\ cfin = FALSE /\ rcd = 0
    /\ d0 = "run" /\ d1 = "run"
    /\ toA = <<>> /\ toR = <<>>
    /\ afs = FALSE /\ arcv = 0 /\ afin = FALSE
    /\ up = TRUE /\ ceof = FALSE /\ cend = "" /\ clean = FALSE

\* The client.
CWrite == cw < N /\ ~cfin /\ cend = "" /\ cw' = cw + 1
          /\ UNCHANGED <<cfin, rcd, d0, d1, toA, toR, afs, arcv, afin, up, ceof, cend, clean>>
CFin == ~cfin /\ cend = "" /\ cfin' = TRUE
        /\ UNCHANGED <<cw, rcd, d0, d1, toA, toR, afs, arcv, afin, up, ceof, cend, clean>>

\* The joint: a client byte onto the session (Pump's write).
Copy == rcd < cw /\ d0 = "run" /\ up /\ rcd' = rcd + 1 /\ toA' = Append(toA, "d")
        /\ UNCHANGED <<cw, cfin, d0, d1, toR, afs, arcv, afin, up, ceof, cend, clean>>
\* The client's FIN read: CloseWrite on the session, then Sync's ping.
CloseWrite ==
    /\ rcd = cw /\ cfin /\ d0 = "run" /\ up
    /\ IF Mutant = "no-sync"
       THEN toA' = Append(toA, "fin") /\ d0' = "done"
       ELSE toA' = toA \o <<"fin", "ping">> /\ d0' = "sync"
    /\ UNCHANGED <<cw, cfin, rcd, d1, toR, afs, arcv, afin, up, ceof, cend, clean>>

\* The peer's Mux reads a frame; keepAlive answers a ping.
ARecv ==
    /\ up /\ toA # <<>>
    /\ toA' = Tail(toA)
    /\ CASE Head(toA) = "d" -> arcv' = arcv + 1 /\ UNCHANGED <<afin, toR>>
         [] Head(toA) = "fin" -> afin' = TRUE /\ UNCHANGED <<arcv, toR>>
         [] Head(toA) = "ping" -> toR' = Append(toR, "pong") /\ UNCHANGED <<arcv, afin>>
    /\ UNCHANGED <<cw, cfin, rcd, d0, d1, afs, up, ceof, cend, clean>>
\* The peer's FIN (its target half-closed).
AFin == ~afs /\ up /\ afs' = TRUE /\ toR' = Append(toR, "fin")
        /\ UNCHANGED <<cw, cfin, rcd, d0, d1, toA, arcv, afin, up, ceof, cend, clean>>

\* EOF to the client (the joint's CloseWrite on it): an end once the joint
\* read the client's FIN.
Eof == ceof' = TRUE /\ clean' = (clean \/ d0 # "run")

\* The joint reads a frame of the session's connection.
RRecv ==
    /\ up /\ toR # <<>>
    /\ toR' = Tail(toR)
    /\ IF Head(toR) = "fin"
       THEN IF d0 = "sync" /\ Mutant # "no-hold"
            THEN d1' = "held" /\ UNCHANGED <<d0, ceof, clean>>
            ELSE d1' = "done" /\ Eof /\ UNCHANGED d0
       ELSE \* the pong: the peer read the FIN
            /\ d0' = IF d0 = "sync" THEN "done" ELSE d0
            /\ IF d1 = "held" /\ d0 = "sync"
               THEN d1' = "done" /\ Eof
               ELSE UNCHANGED <<d1, ceof, clean>>
    /\ UNCHANGED <<cw, cfin, rcd, toA, afs, arcv, afin, up, cend>>

\* Both directions ended: the client's connection closed (a FIN, cleanly).
Finish ==
    /\ d0 = "done" /\ d1 = "done" /\ cend = ""
    /\ cend' = "close" /\ clean' = TRUE
    /\ UNCHANGED <<cw, cfin, rcd, d0, d1, toA, toR, afs, arcv, afin, up, ceof>>

\* The connection dies: what is on the way is lost.
Crash == up /\ up' = FALSE /\ toA' = <<>> /\ toR' = <<>>
         /\ UNCHANGED <<cw, cfin, rcd, d0, d1, afs, arcv, afin, ceof, cend, clean>>
\* The session ends lost: a direction still running or syncing aborts the
\* client (an RST: Pump's error, a failed write, Sync's error).
Lost ==
    /\ ~up /\ cend = ""
    /\ (d0 # "done" \/ d1 # "done")
    /\ cend' = "reset" /\ d0' = "done" /\ d1' = "done"
    /\ UNCHANGED <<cw, cfin, rcd, toA, toR, afs, arcv, afin, up, ceof, clean>>

Next == CWrite \/ CFin \/ Copy \/ CloseWrite \/ ARecv \/ AFin \/ RRecv
        \/ Finish \/ Crash \/ Lost

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ d0 \in {"run", "sync", "done"} /\ d1 \in {"run", "held", "done"}
    /\ cend \in {"", "close", "reset"} /\ rcd <= cw /\ arcv <= rcd

\* A client that saw a clean end had every byte and its FIN read by the
\* peer once the connection is gone (relay-core finding 23).
NoSilentLoss == clean /\ ~up => arcv = cw /\ afin /\ cfin

\* Witnesses (make spec-mutants: TLC must violate each): a clean end, a
\* reset, a FIN held while the sync waits.
WitnessClean == ~clean
WitnessReset == cend # "reset"
WitnessHeld == d1 # "held"
=============================================================================
