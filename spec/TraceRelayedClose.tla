------------------------- MODULE TraceRelayedClose -------------------------
(* Trace validation of a relayed session's Close (spec/README.md            *)
(* "RelayedClose.tla", lane ENDACKCODE): the steps the sessions of one     *)
(* relayed path recorded (-tags linktrace, LINKTRACE_RELAYED;              *)
(* trace_relayed_on.go) must be steps of RelayedClose.tla, in order. The   *)
(* bytes are not logged (A's writes, J's and C's receipt of data, J's      *)
(* copies, C's reads and writes): TLC takes any of them between two logged *)
(* steps, with K = 1. ARet is an observation, no step: what A's wait for   *)
(* its ENDACK returned must be the model's ret (CloseTruth on the code's   *)
(* own result). The trace is accepted when TLC reaches its end:            *)
(* TraceUnfinished violated.                                               *)
EXTENDS TraceLog, RelayedClose, TLC

VARIABLE l
traceVars == <<vars, l>>

E == Log[l]

Env == AWrite \/ (JRecv /\ Head(toJ) = "d") \/ D1Copy \/ (CRecv /\ Head(toC) = "d")
       \/ (CDrop /\ Head(toC) = "d") \/ CRead \/ CEnd \/ CWrite

\* A frame J received: the code's abort (a RESET, an ABORT) is the model's rst.
JFrame(t) == IF t = "abort" THEN "rst" ELSE t

Step ==
    CASE E.ev = "AClose" -> AClose /\ ret' = E.ret
      [] E.ev = "ARet" -> UNCHANGED vars /\ ret = E.r
      [] E.ev = "ARecv" -> ARecv /\ Head(fromJ) = E.t
      [] E.ev = "ADown" -> ADown
      [] E.ev = "AIdle" -> AIdle
      [] E.ev = "AReset" -> AReset \/ AResetWaiting \/ AResetFirst
      [] E.ev = "JRecv" -> JRecv /\ Head(toJ) = JFrame(E.t)
      [] E.ev = "D1End" -> D1End /\ ((E.eof = "1") = ReadsEof(jfin, jfail))
      [] E.ev = "JAck" -> JAck
      [] E.ev = "JIdle" -> JIdle
      [] E.ev = "JSyncFail" -> JSyncFail
      [] E.ev = "D0CFail" -> D0CFail
      [] E.ev = "CRecv" -> (CRecv \/ CDrop) /\ Head(toC) = E.t
      [] E.ev = "CClose" -> CClose
      [] E.ev = "Cut" -> Cut
      [] E.ev = "CCut" -> CCut
      [] E.ev = "CMute" -> CMute
      [] E.ev = "Exit" -> Exit
      [] E.ev = "JDown" -> JDown
      [] E.ev = "D0Fail" -> D0Fail /\ ((E.held = "1") = (d0' = "held"))
      [] E.ev = "RelayGone" -> RelayGone
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ l = 1
TraceNext == (l <= Len(Log) /\ Step /\ l' = l + 1) \/ (Env /\ UNCHANGED l)

\* Violated once every line was taken: the trace is a behaviour of RelayedClose.
TraceUnfinished == l <= Len(Log)
=============================================================================
