------------------------------ MODULE TraceHopHold ------------------------------
(* Trace validation of two joints' holds (spec/README.md "Two joints'      *)
(* holds", lane LINKTRACE2): the steps the two marked joints of one relayed *)
(* path recorded (-tags linktrace, LINKTRACE_HOPHOLD; peerlink/trace_on.go) *)
(* must be steps of HopHold.tla, in order. Logged: a joint's CloseWrite     *)
(* (AppFin of the direction it enters), Fin (ExitEnd of the one it exits:  *)
(* n = 1 held), Synced (its Mux read the receipt), Acked (the ENDACK) and  *)
(* Closed (JointDone: the joint closed its app conn whole; a close before  *)
(* the app's own end was acknowledged is no step, lane ENDTRACE).          *)
(* Not logged: a Mux reading a FIN (it answers the receipt) and a held     *)
(* end's Release: TLC takes them between two logged steps. Lost (a joint   *)
(* aborted its app) is no step: nothing fails in HopHold. The trace is     *)
(* accepted when TLC reaches its end with every end sent acknowledged      *)
(* and both joints closed (BothEnd, Closed on the finished run):           *)
(* TraceUnfinished violated.                                               *)
EXTENDS TraceLog, HopHold, TLC

VARIABLE l
traceVars == <<vars, l>>

E == Log[l]

EntryDir(j) == IF j = "G" THEN "up" ELSE "down"   \* the direction joint j's app ends
In(j) == IF j = "G" THEN r2g ELSE g2r

\* Joint j's Mux reads a frame of kind k.
RecvKind(j, k) == In(j) # <<>> /\ Head(In(j))[1] = k /\ Recv(j)

Env == \E d \in Dirs : Release(d) \/ \E j \in Joints : RecvKind(j, "fin")

Step ==
    CASE E.ev = "CloseWrite" -> AppFin(EntryDir(E.j))
      [] E.ev = "Fin" -> ExitEnd(Other(EntryDir(E.j))) /\ ((E.n = 1) = (st'[Other(EntryDir(E.j))] = "held"))
      [] E.ev = "Synced" -> RecvKind(E.j, "rcv")
      [] E.ev = "Acked" -> RecvKind(E.j, "eack")
      [] E.ev = "Closed" -> JointDone(E.j)
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ l = 1
TraceNext == (l <= Len(Log) /\ Step /\ l' = l + 1) \/ (Env /\ UNCHANGED l)

\* Violated once every line was taken, every end sent was acknowledged and
\* both joints closed their apps' conns.
TraceUnfinished ==
    \/ l <= Len(Log)
    \/ \E d \in Dirs : st[d] # "open" /\ ~acked[d]
    \/ \E j \in Joints : ~appClosed[j]
=============================================================================
