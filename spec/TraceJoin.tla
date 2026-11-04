------------------------------ MODULE TraceJoin ------------------------------
(* Trace validation of peerlink's joint (spec/README.md "Join's clean     *)
(* end"): the steps a joint of a plain conn and a link session recorded    *)
(* (-tags linktrace, peerlink/trace_on.go) must be steps of Join.tla, in   *)
(* order. The client's, the peer's and the network's steps (CWrite, CFin,  *)
(* ARecv, AFin, Crash) are not logged: TLC takes any of them between two   *)
(* logged steps. N is the number of Copy lines (one model byte each). The  *)
(* trace is accepted when TLC reaches its end: TraceUnfinished violated.   *)
EXTENDS TraceLog, Join, TLC, FiniteSets

TraceN == Cardinality({i \in DOMAIN Log : Log[i].ev = "Copy"})

VARIABLE l
traceVars == <<vars, l>>

E == Log[l]

Env == CWrite \/ CFin \/ ARecv \/ AFin \/ Crash

Step ==
    CASE E.ev = "Copy" -> Copy
      [] E.ev = "CloseWrite" -> CloseWrite
      \* the peer's FIN read: held while the sync waits (n = 1), else passed on
      [] E.ev = "Fin" -> toR # <<>> /\ Head(toR) = "fin" /\ RRecv /\ ((E.n = 1) = (d1' = "held"))
      [] E.ev = "Synced" -> toR # <<>> /\ Head(toR) = "pong" /\ RRecv
      [] E.ev = "Finish" -> Finish
      [] E.ev = "Lost" -> Lost
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ l = 1
TraceNext == (l <= Len(Log) /\ Step /\ l' = l + 1) \/ (Env /\ UNCHANGED l)

\* Violated once every line was taken: the trace is a behaviour of Join.
TraceUnfinished == l <= Len(Log)
=============================================================================
