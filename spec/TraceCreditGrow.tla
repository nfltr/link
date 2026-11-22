------------------------- MODULE TraceCreditGrow -------------------------
(* Trace validation of a session's growing window (spec/README.md          *)
(* "Growing windows", lane RELAYWIN2): the steps one receiving side's       *)
(* sessions recorded (-tags linktrace, LINKTRACE_GROW; trace_grow_on.go),   *)
(* its own and their senders', must be steps of CreditGrow.tla at the       *)
(* code's grain: a data frame and a read of n bytes, an update per quarter  *)
(* window (Q = 4). The growth a BLOCKED earns is CreditGrow's (KeepsUp,     *)
(* WholeWindow, Room); the values the code saw after a step (the growth,    *)
(* the window, the budget used, held, the update, the sender's credit) must *)
(* be the model's. The frames between the two sides are counted per        *)
(* session (inD data bytes, inB BLOCKEDs, inG update bytes) rather than     *)
(* queued: no receiver step depends on another session's frame order. A    *)
(* session's two ends start and end apart (live: the receiver's, slive: the *)
(* sender's); a frame for an end that is gone is dropped. Every step is     *)
(* logged; the trace is accepted when TLC takes its every line              *)
(* (TraceAccepted) with every invariant held at each.                       *)
EXTENDS TraceLog, CreditGrow, TLC

Lines(ev) == {i \in DOMAIN Log : Log[i].ev = ev}
Up == Log[CHOOSE i \in Lines("Up") : TRUE]
TraceR == Up.side
TraceSids == {Log[i].sid : i \in DOMAIN Log} \ {""}
TraceW == Up.n
TraceMax == IF Up.grow = 1 /\ Up.max > Up.n THEN Up.max ELSE Up.n
TraceG == Up.g
ASSUME Cardinality(Lines("Up")) = 1

VARIABLES slive, inD, inB, inG, l
traceVars == <<vars, slive, inD, inB, inG, l>>

E == Log[l]
S == E.sid
Rcv == E.side = TraceR

\* Either end's credit starts: a session's receiver and sender, once each.
TOpen ==
    IF Rcv
    THEN /\ ~live[S] /\ rcvd[S] = 0
         /\ live' = [live EXCEPT ![S] = TRUE]
         /\ UNCHANGED <<cred, bsent, slive>>
    ELSE /\ ~slive[S] /\ sent[S] = 0 /\ E.cr = W
         /\ slive' = [slive EXCEPT ![S] = TRUE]
         /\ cred' = [cred EXCEPT ![S] = W]
         /\ UNCHANGED <<bsent, live>>

\* Send(s), n bytes in one frame.
TSend ==
    /\ ~Rcv /\ slive[S] /\ cred[S] >= E.n
    /\ cred' = [cred EXCEPT ![S] = @ - E.n]
    /\ sent' = [sent EXCEPT ![S] = @ + E.n]
    /\ inD' = [inD EXCEPT ![S] = @ + E.n]

\* Block(s): once until the next update.
TBlock ==
    /\ ~Rcv /\ slive[S] /\ ~bsent[S] /\ E.cr = cred[S]
    /\ bsent' = [bsent EXCEPT ![S] = TRUE]
    /\ inB' = [inB EXCEPT ![S] = @ + 1]

\* RecvAB's data frame of n bytes (dropped for a receiver gone).
TData ==
    /\ Rcv /\ inD[S] >= E.n
    /\ inD' = [inD EXCEPT ![S] = @ - E.n]
    /\ (E.known = 1) = live[S]
    /\ IF live[S]
       THEN IF held[S] + E.n > win[S]
            THEN err' = TRUE /\ UNCHANGED <<held, rcvd>>
            ELSE /\ held' = [held EXCEPT ![S] = @ + E.n]
                 /\ rcvd' = [rcvd EXCEPT ![S] = @ + E.n]
                 /\ E.held = held'[S]
                 /\ UNCHANGED err
       ELSE UNCHANGED <<held, rcvd, err>>

\* RecvAB's BLOCKED: the growth CreditGrow's rule gives, or none.
TBlocked ==
    /\ Rcv /\ inB[S] >= 1
    /\ inB' = [inB EXCEPT ![S] = @ - 1]
    /\ (E.known = 1) = live[S]
    /\ IF live[S]
       THEN LET g == IF KeepsUp(S) /\ WholeWindow(S) /\ Room(S) > 0 THEN Room(S) ELSE 0 IN
            /\ E.g = g
            /\ win' = [win EXCEPT ![S] = @ + g]
            /\ owed' = [owed EXCEPT ![S] = @ + g]
            /\ epoch' = IF g > 0 THEN [epoch EXCEPT ![S] = rcvd[S]] ELSE epoch
            /\ grown' = grown + g
            /\ E.win = win'[S] /\ E.grown = grown'
       ELSE UNCHANGED <<win, owed, epoch, grown>>

\* Read(s) of n bytes: the update, with the growth owed, once a quarter
\* window is due.
TRead ==
    /\ Rcv /\ live[S] /\ ~stopped[S] /\ Unread(S) >= E.n
    /\ rd' = [rd EXCEPT ![S] = @ + E.n]
    /\ IF cons[S] + E.n + owed[S] >= Thr(S)
       THEN /\ E.grant = cons[S] + E.n + owed[S]
            /\ inG' = [inG EXCEPT ![S] = @ + E.grant]
            /\ held' = [held EXCEPT ![S] = @ - (cons[S] + E.n)]
            /\ cons' = [cons EXCEPT ![S] = 0]
            /\ owed' = [owed EXCEPT ![S] = 0]
       ELSE /\ E.grant = 0
            /\ cons' = [cons EXCEPT ![S] = @ + E.n]
            /\ UNCHANGED <<inG, held, owed>>
    /\ E.held = held'[S]

\* RecvWin: the sender's credit grows by the update (dropped for a sender gone).
TWin ==
    /\ ~Rcv /\ inG[S] >= E.n
    /\ inG' = [inG EXCEPT ![S] = @ - E.n]
    /\ (E.known = 1) = slive[S]
    /\ IF slive[S]
       THEN IF cred[S] + E.n > SendCap
            THEN err' = TRUE /\ UNCHANGED <<cred, bsent>>
            ELSE /\ cred' = [cred EXCEPT ![S] = @ + E.n]
                 /\ bsent' = [bsent EXCEPT ![S] = FALSE]
                 /\ E.cr = cred'[S]
                 /\ UNCHANGED err
       ELSE UNCHANGED <<cred, bsent, err>>

\* Drop(s) at either end: the receiver's gives its growth back.
TDrop ==
    IF Rcv
    THEN /\ live[S]
         /\ live' = [live EXCEPT ![S] = FALSE]
         /\ grown' = IF Mutant = "drop-keeps-growth" THEN grown ELSE grown - (win[S] - W)
         /\ UNCHANGED slive
    ELSE /\ slive[S]
         /\ slive' = [slive EXCEPT ![S] = FALSE]
         /\ UNCHANGED <<live, grown>>

Step ==
    CASE E.ev = "Up" -> UNCHANGED <<vars, slive, inD, inB, inG>>
      [] E.ev = "Open" -> TOpen /\ UNCHANGED <<sent, ab, ba, win, held, cons, owed, rcvd, rd, epoch, grown, stopped, stopWin, err, inD, inB, inG>>
      [] E.ev = "Send" -> TSend /\ UNCHANGED <<bsent, ab, ba, win, held, cons, owed, rcvd, rd, epoch, grown, stopped, stopWin, live, err, slive, inB, inG>>
      [] E.ev = "Block" -> TBlock /\ UNCHANGED <<cred, sent, ab, ba, win, held, cons, owed, rcvd, rd, epoch, grown, stopped, stopWin, live, err, slive, inD, inG>>
      [] E.ev = "Data" -> TData /\ UNCHANGED <<cred, sent, bsent, ab, ba, win, cons, owed, rd, epoch, grown, stopped, stopWin, live, slive, inB, inG>>
      [] E.ev = "Blocked" -> TBlocked /\ UNCHANGED <<cred, sent, bsent, ab, ba, held, cons, rcvd, rd, stopped, stopWin, live, err, slive, inD, inG>>
      [] E.ev = "Read" -> TRead /\ UNCHANGED <<cred, sent, bsent, ab, ba, win, rcvd, epoch, grown, stopped, stopWin, live, err, slive, inD, inB>>
      [] E.ev = "Win" -> TWin /\ UNCHANGED <<sent, ab, ba, win, held, cons, owed, rcvd, rd, epoch, grown, stopped, stopWin, live, slive, inD, inB>>
      [] E.ev = "Drop" -> TDrop /\ UNCHANGED <<cred, sent, bsent, ab, ba, win, held, cons, owed, rcvd, rd, epoch, stopped, stopWin, err, inD, inB, inG>>
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit ==
    /\ cred = [s \in Sids |-> 0]
    /\ sent = [s \in Sids |-> 0]
    /\ bsent = [s \in Sids |-> FALSE]
    /\ ab = <<>> /\ ba = <<>>
    /\ win = [s \in Sids |-> W]
    /\ held = [s \in Sids |-> 0]
    /\ cons = [s \in Sids |-> 0]
    /\ owed = [s \in Sids |-> 0]
    /\ rcvd = [s \in Sids |-> 0]
    /\ rd = [s \in Sids |-> 0]
    /\ epoch = [s \in Sids |-> 0]
    /\ grown = 0
    /\ stopped = [s \in Sids |-> FALSE]
    /\ stopWin = [s \in Sids |-> 0]
    /\ live = [s \in Sids |-> FALSE]
    /\ err = FALSE
    /\ slive = [s \in Sids |-> FALSE]
    /\ inD = [s \in Sids |-> 0] /\ inB = [s \in Sids |-> 0] /\ inG = [s \in Sids |-> 0]
    /\ l = 1
TraceNext == l <= Len(Log) /\ Step /\ l' = l + 1

\* CreditGrow's Conservation at the trace's grain: every byte of the
\* window is the sender's credit, data on the way, held, an update on the
\* way, or growth not yet sent.
TraceConservation == \A s \in Sids : live[s] /\ slive[s] =>
    cred[s] + inD[s] + held[s] + inG[s] + owed[s] = win[s]

\* GrownAccount and ConnBound, their sums taken one session at a time
\* (CreditGrow's Sum walks SUBSET Sids: a trace has dozens of sessions).
RECURSIVE SumOf(_, _)
SumOf(f, X) == IF X = {} THEN 0 ELSE LET x == CHOOSE y \in X : TRUE IN f[x] + SumOf(f, X \ {x})
TraceGrownAccount == grown = SumOf([s \in Sids |-> IF live[s] THEN win[s] - W ELSE 0], Sids)
TraceConnBound == SumOf([s \in Sids |-> IF live[s] THEN Unread(s) ELSE 0], Sids) <= Cardinality(Sids) * W + G

\* The longest behaviour TLC found took every line.
TraceAccepted ==
    LET d == TLCGet("stats").diameter IN
    IF d - 1 = Len(Log) THEN TRUE
    ELSE Print(<<"trace rejected at line", d, Log[d]>>, FALSE)
=============================================================================
