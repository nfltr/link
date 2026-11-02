------------------------------ MODULE TraceLive ------------------------------
(* Trace validation of keepAlive (spec/README.md "The Pool and liveness"):  *)
(* the timer fires a recorded run of Conn.keepAlive took (-tags linktrace,  *)
(* the files <connection>.ndjson.live) must be steps of Liveness.tla for    *)
(* one side, Side: each fire at or after the model's deadline, with the    *)
(* model's pingAt, and the connection ended exactly when the model says.   *)
(* Times are microseconds since the Conn's t0, floored: every comparison  *)
(* keepAlive makes keeps its outcome (a deadline fires at or after it).    *)
EXTENDS TraceLog, Integers, Sequences, TLC

CONSTANT Side

SLog == SelectSeq(Log, LAMBDA e : e.side = Side)
LiveLine == SLog[CHOOSE i \in DOMAIN SLog : SLog[i].ev = "Live"]

VARIABLES now, rx, pingAt, timer, alive, dead, l
traceVars == <<now, rx, pingAt, timer, alive, dead, l>>

LV == INSTANCE Liveness WITH Iv <- LiveLine.n, To <- LiveLine.credit,
    Late <- 0, Rtt <- 0, Horizon <- 0, Mutant <- "none"

E == SLog[l]

Step ==
    CASE E.ev = "Live" -> UNCHANGED <<now, rx, pingAt, timer, alive, dead>>
      [] E.ev = "Fire" -> pingAt = E.held /\ E.n >= now /\ E.credit >= rx /\ LV!FireAt(E.n, E.credit)
      [] E.ev = "Dead" -> dead /\ UNCHANGED <<now, rx, pingAt, timer, alive, dead>>
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

\* A Fire the model ends the connection on must be followed by Dead (or end
\* the trace), and the model never ends it otherwise: a Fire after it is
\* refused by FireAt's ~dead.
TraceInit == LV!Init /\ l = 1
TraceNext == l <= Len(SLog) /\ Step /\ l' = l + 1

TraceNeverEarly == LV!NeverEarly

TraceAccepted ==
    LET d == TLCGet("stats").diameter IN
    IF d - 1 = Len(SLog) THEN TRUE
    ELSE Print(<<"trace rejected at line", d, SLog[d]>>, FALSE)
=============================================================================
