--------------------------- MODULE TraceEndAckLeg ---------------------------
(* Trace validation of quiclink's end acknowledgement (spec/README.md "One  *)
(* leg's end acknowledgement", lane ENDACKQUIC): the steps both ends of one *)
(* session on an ALPNEndAck connection recorded (-tags linktrace,          *)
(* LINKTRACE_ENDACK; peerlink/quiclink/trace_on.go) must be steps of       *)
(* EndAckLeg.tla, Kind quic, in order. S is the end of the first SClose; a  *)
(* line about the other direction (S's own 'A' about its peer's end, the   *)
(* peer's 'E' or 'A' read by the peer) is no step. The bytes are not logged *)
(* (S's writes, R's receipt and its app's reads before the end; REnd is   *)
(* Read's io.EOF, REof, lane ENDACK) and neither is S's stream            *)
(* reset (quic-go's): TLC takes any of them between two logged steps, with *)
(* K = 1; SClose's w: "0" when S wrote no byte, else "1" (lane QUICEND).   *)
(* SRet is an observation, no step: what S's wait returned must be         *)
(* the model's ret. The trace is accepted when TLC reaches its end:        *)
(* TraceUnfinished violated.                                               *)
EXTENDS TraceLog, EndAckLeg, TLC

VARIABLE l
traceVars == <<vars, l>>

E == Log[l]

Closes == {i \in 1..Len(Log) : Log[i].ev = "SClose"}
SBy == IF Closes = {} THEN "" ELSE Log[CHOOSE i \in Closes : \A j \in Closes : i <= j].by

SEvents == {"SClose", "SRecv", "SRet", "SIdle", "Cut"}
REvents == {"REnd", "RFail", "RClose", "RMute"}
Mine == (E.ev \in SEvents /\ E.by = SBy) \/ (E.ev \in REvents /\ E.by # SBy)
Other == (E.ev \in SEvents /\ E.by # SBy) \/ (E.ev \in REvents /\ E.by = SBy)

Env == SWrite \/ RRecv \/ RRead \/ SReset

Step ==
    \/ Other /\ UNCHANGED vars
    \/ Mine /\
       CASE E.ev = "SClose" -> SClose /\ ("w" \in DOMAIN E => ((E.w = "0") = (sw = 0)))
         [] E.ev = "SRet" -> UNCHANGED vars /\ ret = E.r
         [] E.ev = "SRecv" -> SRecv /\ Head(back) = E.t
         [] E.ev = "SIdle" -> SIdle
         [] E.ev = "Cut" -> Cut
         [] E.ev = "REnd" -> REof /\ ((E.ack = "1") = (eackSent' /\ ~eackSent))
         [] E.ev = "RFail" -> RFail
         [] E.ev = "RClose" -> RClose
         [] E.ev = "RMute" -> RMute
    \/ ~Mine /\ ~Other /\ Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ l = 1
TraceNext == (l <= Len(Log) /\ Step /\ l' = l + 1) \/ (Env /\ UNCHANGED l)

\* Violated once every line was taken: the trace is a behaviour of EndAckLeg.
TraceUnfinished == l <= Len(Log)
=============================================================================
