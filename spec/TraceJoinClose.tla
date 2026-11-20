---------------------------- MODULE TraceJoinClose ----------------------------
(* Trace validation of a joint whose side closed whole (spec/README.md     *)
(* "A joint's side closed whole", lane LINKTRACE2): the steps a joint      *)
(* (link.Join, Forward) recorded (-tags linktrace, LINKTRACE_JOIN;         *)
(* trace_join_on.go) must be steps of JoinClose.tla, in order. The peers'  *)
(* and the network's steps (BWrite, BHalf, BClose, JRecv, AWrite, ARead)   *)
(* are not logged: TLC takes any of them between two logged steps. K is    *)
(* the number of D1Copy lines (one model byte each); direction 0's copies  *)
(* and clean end, which the model leaves out, are not logged. The trace is *)
(* accepted when TLC reaches its end: TraceUnfinished violated.            *)
EXTENDS TraceLog, JoinClose, TLC, FiniteSets

TraceK == Cardinality({i \in DOMAIN Log : Log[i].ev = "D1Copy"})

VARIABLE l
traceVars == <<vars, l>>

E == Log[l]

Env == BWrite \/ BHalf \/ BClose \/ JRecv \/ AWrite \/ ARead

\* D1Copy: ok unless A was closed; D1End: A's half close after B's, unless
\* direction 0 failed first (whole); D0Fail: A closed now once direction 1
\* ended, else left to it.
Step ==
    CASE E.ev = "D1Copy" -> D1Copy /\ ((E.ok = 1) = (d1' = "run"))
      [] E.ev = "D1End" -> D1End /\ ((E.kind = "fin") = (bhalf /\ ~whole))
      [] E.ev = "D0Fail" -> D0Fail /\ ((E.now = 1) = (aclosed' /\ ~aclosed))
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ l = 1
TraceNext == (l <= Len(Log) /\ Step /\ l' = l + 1) \/ (Env /\ UNCHANGED l)

\* Violated once every line was taken: the trace is a behaviour of JoinClose.
TraceUnfinished == l <= Len(Log)
=============================================================================
