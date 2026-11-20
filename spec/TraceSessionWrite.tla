------------------------- MODULE TraceSessionWrite -------------------------
(* Trace validation of a Session's writes waiting for credit with a write *)
(* deadline (spec/README.md "Session writes", lane LINKTRACE2): the steps  *)
(* one session recorded (-tags linktrace, LINKTRACE_WRITE;                *)
(* trace_write_on.go) must be steps of SessionWrite.tla, in order. Each   *)
(* call of Session.acquire is a writer of its own, needing its own n (its *)
(* Take names the channel it holds: the model's numbers, 0 nil); the      *)
(* credit starts at the Open line's and each Check and Grant line carries *)
(* it after. A Check of a writer already waiting is Credit.acquire's look *)
(* again after a grant: Wake, or with too little a step that changes      *)
(* nothing. Every step is logged; the trace is accepted when TLC takes    *)
(* its every line (TraceAccepted) with every invariant held at each.      *)
EXTENDS TraceLog, SessionWrite, TLC, Sequences

Lines(ev) == {i \in DOMAIN Log : Log[i].ev = ev}
TraceWriters == {Log[i].w : i \in Lines("Take")}
TraceSets == Cardinality(Lines("Set"))
TraceWindow == Log[1].cr
\* a writer's n: its first Check (a later one with another n rejects the
\* trace below); only writers that reach Credit.acquire have one
TraceNeed(w) == Log[CHOOSE i \in Lines("Check") : Log[i].w = w].n
ASSUME Log[1].ev = "Open"

VARIABLE l
traceVars == <<vars, l>>

E == Log[l]

Step ==
    CASE E.ev = "Set" -> Set(E.kind)
      [] E.ev = "Fire" -> Fire
      [] E.ev = "Take" -> Take(E.w) /\ snap'[E.w] = E.ch
      [] E.ev = "Past" -> CheckPast(E.w) /\ (res'[E.w] = "timeout") = (E.res = "timeout")
      [] E.ev = "Check" -> /\ Need(E.w) = E.n /\ cr' = E.cr
                           /\ CASE pc[E.w] = "credit" -> CheckCredit(E.w) /\ (E.took = 1) = (pc'[E.w] = "done")
                                [] E.took = 1 -> Wake(E.w) /\ pc'[E.w] = "done"
                                [] OTHER -> pc[E.w] = "wait" /\ cr < Need(E.w) /\ UNCHANGED vars
      [] E.ev = "Wake" -> Wake(E.w) /\ pc'[E.w] = "take"
      [] E.ev = "Grant" -> GrantOf(E.n) /\ cr' = E.cr
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ l = 2
TraceNext == l <= Len(Log) /\ Step /\ l' = l + 1

\* The longest behaviour TLC found took every line.
TraceAccepted ==
    LET d == TLCGet("stats").diameter + 1 IN
    IF d - 1 = Len(Log) THEN TRUE
    ELSE Print(<<"trace rejected at line", d, Log[d]>>, FALSE)
=============================================================================
