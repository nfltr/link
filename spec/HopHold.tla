------------------------------- MODULE HopHold -------------------------------
(* Two joints on one relayed path, both ends plain TCP apps (spec/README.md *)
(* "Two joints' holds", lane ENDACK2D): an agent's joint G (the relay SDK's  *)
(* joinTarget, peerlink.Join of its target T and the agent-link session)    *)
(* and the relay's joint R (peerlink.Join of a TCP visitor V and the agent  *)
(* session). Both apps half-close at about the same time: "up" is T's FIN  *)
(* (T -> G -> R -> V), "down" V's (V -> R -> G -> T). The link between G   *)
(* and R is one FIFO each way.                                              *)
(*                                                                          *)
(* A joint passes its app's FIN onto the session (CloseWrite) and syncs:    *)
(* while it syncs it holds the other direction's clean end from its own    *)
(* app (Join.tla's hold: the app reads no EOF while its own tail may still *)
(* be lost), and that held end includes the ENDACK of it (peerlink         *)
(* joint.hold: closeWrite(dst) then ackEnd(src)). The receiving Mux        *)
(* answers a FIN at once with the receipt (the Sync's pong, WIRE.md 8); the *)
(* ENDACK (WIRE.md 13) comes once the exit joint passed the FIN to its app  *)
(* (a TCP app: its kernel took it).                                         *)
(*                                                                          *)
(* The design: the hold ends at the receipt (the next hop's Mux has the     *)
(* tail and the FIN: Join.tla's sync), never at the ENDACK. The ENDACK     *)
(* waits for the onward end, the hold for nothing but a Mux: no wait-for   *)
(* cycle. Since lane RELAYCLOSE the hold ends at CloseWriteSync's return,  *)
(* the far end's ENDACK (mutant hold-until-ack): with both FINs crossing,  *)
(* G waits for R's ENDACK of down, which R holds until its sync of down    *)
(* returns with G's ENDACK of up, which G holds until its own sync of up   *)
(* returns: a cycle only the idle bound (ErrEndAckIdle, a reset) breaks.   *)
(* Failures (resets, cuts) are RelayedClose.tla's and Join.tla's; here     *)
(* nothing fails, so every end must come cleanly.                           *)
EXTENDS Naturals, Sequences

CONSTANTS
    Mutant   \* "none"; "hold-until-ack": the hold ends at the ENDACK
             \* (peerlink joint.sync since RELAYCLOSE: syncing cleared only
             \* when CloseWriteSync returned); "no-hold": the other
             \* direction's end passes while the sync waits (Join.tla
             \* no-hold); "ack-on-receipt": the exit joint acknowledges an
             \* end once its Mux read it (relay-core's gRPC stream today,
             \* RelayedClose.tla relay-read-ack); "held-end-no-ack": a held end
             \* whose session ended meanwhile (both halves closed) closes the
             \* TCP app without the ENDACK (peerlink joint.ended's Done branch
             \* onto a conn that is no syncer: no ackEnd), so the peer's
             \* CloseWriteSync waits for the idle bound

Dirs == {"up", "down"}
Joints == {"G", "R"}
Entry(d) == IF d = "up" THEN "G" ELSE "R"   \* the joint that reads d's FIN from its app
Exit(d) == IF d = "up" THEN "R" ELSE "G"    \* the joint that passes d's FIN to its app
Other(d) == IF d = "up" THEN "down" ELSE "up"  \* the direction the app reading d writes

VARIABLES
    st,       \* [Dirs -> {"open", "sent", "at", "held", "passed"}]: sent: the
              \* entry joint wrote the FIN onto the session (it syncs); at: the
              \* exit joint's Mux has it; held: the exit joint holds it; passed:
              \* the exit app's kernel has it (its reads end with io.EOF)
    rcvd,     \* [Dirs -> BOOLEAN]: the entry joint has the receipt (pong)
    acked,    \* [Dirs -> BOOLEAN]: the entry joint has the ENDACK
    syncing,  \* [Joints -> BOOLEAN]: the joint's hold is on (joint.syncing)
    g2r,      \* the session's frames G -> R: <<kind, d>>
    r2g,      \* R -> G
    early,    \* history: an app's reads ended while its own FIN was on the
              \* wire, not yet at the next hop's Mux
    heldEver  \* history: an end was held

vars == <<st, rcvd, acked, syncing, g2r, r2g, early, heldEver>>

Init ==
    /\ st = [d \in Dirs |-> "open"] /\ rcvd = [d \in Dirs |-> FALSE]
    /\ acked = [d \in Dirs |-> FALSE] /\ syncing = [j \in Joints |-> FALSE]
    /\ g2r = <<>> /\ r2g = <<>> /\ early = FALSE /\ heldEver = FALSE

\* Joint j's frame m onto the session (its own FIFO).
SendFrom(j, m) == IF j = "G" THEN g2r' = Append(g2r, m) /\ UNCHANGED r2g
                             ELSE r2g' = Append(r2g, m) /\ UNCHANGED g2r

\* An app half-closes: its joint passes the FIN on and syncs.
AppFin(d) ==
    /\ st[d] = "open"
    /\ st' = [st EXCEPT ![d] = "sent"]
    /\ syncing' = [syncing EXCEPT ![Entry(d)] = TRUE]
    /\ SendFrom(Entry(d), <<"fin", d>>)
    /\ UNCHANGED <<rcvd, acked, early, heldEver>>

\* The end passes to the exit app (its kernel takes the FIN) and the exit
\* joint sends the ENDACK (unless the mutant sent it at receipt).
Pass(d) ==
    /\ st' = [st EXCEPT ![d] = "passed"]
    /\ early' = (early \/ st[Other(d)] = "sent")

\* Joint j's Mux reads the head of its incoming FIFO.
Recv(j) ==
    LET in == IF j = "G" THEN r2g ELSE g2r
        m == Head(in) d == m[2]
    IN /\ in # <<>>
       /\ IF j = "G" THEN r2g' = Tail(r2g) ELSE g2r' = Tail(g2r)
       /\ CASE m[1] = "fin" ->
               \* a FIN: the Mux answers with the receipt at once
               /\ st' = [st EXCEPT ![d] = "at"]
               /\ LET out == IF Mutant = "ack-on-receipt" THEN <<<<"rcv", d>>, <<"eack", d>>>> ELSE <<<<"rcv", d>>>>
                  IN IF j = "G" THEN g2r' = g2r \o out ELSE r2g' = r2g \o out
               /\ UNCHANGED <<rcvd, acked, syncing, early, heldEver>>
            [] m[1] = "rcv" ->
               /\ rcvd' = [rcvd EXCEPT ![d] = TRUE]
               /\ syncing' = IF Mutant = "hold-until-ack" THEN syncing ELSE [syncing EXCEPT ![j] = FALSE]
               /\ (IF j = "G" THEN UNCHANGED g2r ELSE UNCHANGED r2g)
               /\ UNCHANGED <<st, acked, early, heldEver>>
            [] m[1] = "eack" ->
               /\ acked' = [acked EXCEPT ![d] = TRUE]
               /\ syncing' = [syncing EXCEPT ![j] = FALSE]
               /\ (IF j = "G" THEN UNCHANGED g2r ELSE UNCHANGED r2g)
               /\ UNCHANGED <<st, rcvd, early, heldEver>>

\* The exit joint's direction ends: held while its own sync runs, else
\* passed on (and acknowledged).
ExitEnd(d) ==
    /\ st[d] = "at"
    /\ IF syncing[Exit(d)] /\ Mutant # "no-hold"
       THEN /\ st' = [st EXCEPT ![d] = "held"] /\ heldEver' = TRUE
            /\ UNCHANGED <<early, g2r, r2g>>
       ELSE /\ Pass(d) /\ UNCHANGED heldEver
            /\ IF Mutant = "ack-on-receipt" THEN UNCHANGED <<g2r, r2g>> ELSE SendFrom(Exit(d), <<"eack", d>>)
    /\ UNCHANGED <<rcvd, acked, syncing>>

\* The held end runs once the joint's sync no longer holds.
Release(d) ==
    /\ st[d] = "held" /\ ~syncing[Exit(d)]
    /\ Pass(d)
    /\ IF Mutant \in {"ack-on-receipt", "held-end-no-ack"} THEN UNCHANGED <<g2r, r2g>> ELSE SendFrom(Exit(d), <<"eack", d>>)
    /\ UNCHANGED <<rcvd, acked, syncing, heldEver>>

Next ==
    \/ \E d \in Dirs : AppFin(d) \/ ExitEnd(d) \/ Release(d)
    \/ \E j \in Joints : Recv(j)

\* The joints and the Muxes take their steps; the apps' FINs are theirs.
Fairness ==
    /\ \A d \in Dirs : WF_vars(ExitEnd(d)) /\ WF_vars(Release(d))
    /\ \A j \in Joints : WF_vars(Recv(j))

Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK ==
    /\ st \in [Dirs -> {"open", "sent", "at", "held", "passed"}]
    /\ rcvd \in [Dirs -> BOOLEAN] /\ acked \in [Dirs -> BOOLEAN]
    /\ syncing \in [Joints -> BOOLEAN]

\* An app's reads end (the peer's FIN) only once its own FIN, sent
\* before, is at the next hop's Mux (Join.tla's hold, relay-core finding 2).
HoldTruth == ~early

\* An end is acknowledged only once the exit app's kernel has it.
AckTruth == \A d \in Dirs : acked[d] => st[d] = "passed"

\* Every end sent is acknowledged: no wait-for cycle between the joints
\* (with nothing failing, no idle bound may be needed).
BothEnd == \A d \in Dirs : (st[d] # "open") ~> acked[d]

\* Witnesses (make spec-mutants: TLC must violate each): an end held,
\* then both acknowledged; both FINs on the wire at once.
WitnessHeldThenDone == ~(heldEver /\ acked["up"] /\ acked["down"])
WitnessCrossing == ~(st["up"] = "sent" /\ st["down"] = "sent")
=============================================================================
