-------------------------- MODULE TraceCreditWaiters --------------------------
(* Trace validation of Credit's waiters (spec/README.md "Credit's          *)
(* waiters", lane LINKTRACE): the steps one session's send credit recorded *)
(* (-tags linktrace, LINKTRACE_WAIT; trace_wait_on.go) must be steps of    *)
(* CreditWaiters.tla, in order. Each acquire is a waiter of its own (K =   *)
(* 1), needing its own n. The receiver is not the model's: a grant comes   *)
(* from the peer's window update, a test or a refund, so a trace's Grant   *)
(* gives back n bytes the waiters took (Grant with the read in it: the     *)
(* window's bytes still add up, Conservation). Every step is logged; the   *)
(* trace is accepted when TLC takes its every line (TraceAccepted) with    *)
(* every invariant held at each.                                           *)
EXTENDS TraceLog, CreditWaiters, TLC

Lines(ev) == {i \in DOMAIN Log : Log[i].ev = ev}
TraceWaiters == {Log[i].w : i \in Lines("Check")}
\* each waiter's n: the first Check of it (a later one with another n
\* rejects the trace below)
TraceNeed == [w \in TraceWaiters |-> Log[CHOOSE i \in Lines("Check") : Log[i].w = w].n]
TraceW == Log[1].n
ASSUME Log[1].ev = "Open"

VARIABLE l
traceVars == <<vars, l>>

E == Log[l]

\* The peer read n bytes the waiters sent and gave them back (or a Write
\* gave back what it did not send); every waiter waiting wakes.
TraceGrant(n) ==
    /\ wire >= n
    /\ cr' = cr + n /\ wire' = wire - n
    /\ woken' = [w \in Waiters |-> woken[w] \/ pc[w] = "wait"]
    /\ UNCHANGED <<left, pc, token, cons, grants>>

Step ==
    CASE E.ev = "Check" -> /\ Need[E.w] = E.n /\ Check(E.w)
                           /\ (E.took = 1) = (left'[E.w] < left[E.w])
      [] E.ev = "Grant" -> TraceGrant(E.n)
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ l = 2
TraceNext == l <= Len(Log) /\ Step /\ cr' = E.cr /\ l' = l + 1

\* The longest behaviour TLC found took every line.
TraceAccepted ==
    LET d == TLCGet("stats").diameter + 1 IN
    IF d - 1 = Len(Log) THEN TRUE
    ELSE Print(<<"trace rejected at line", d, Log[d]>>, FALSE)
=============================================================================
