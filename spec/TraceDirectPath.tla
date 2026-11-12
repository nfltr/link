--------------------------- MODULE TraceDirectPath ---------------------------
(* Trace validation of the path layer (link pathswitch, -tags linktrace,   *)
(* trace_on.go): one direction of a recorded session, Dir, must be a       *)
(* behaviour of DirectPath.tla, line by line. The client is S on "c2a"     *)
(* and R on "a2c"; the decider is the client. A line of the other          *)
(* direction's end leaves the state as it is. Accepted when TLC reaches    *)
(* the trace's end (TraceUnfinished violated) with every invariant held; a *)
(* line that maps to no action fails.                                      *)
EXTENDS TraceLog, DirectPath, FiniteSets, TLC

CONSTANT Dir

SEnd == IF Dir = "c2a" THEN "client" ELSE "server"
REnd == IF Dir = "c2a" THEN "server" ELSE "client"
Lines(ev, by) == {i \in DOMAIN Log : Log[i].ev = ev /\ Log[i].by = by}
\* the bytes S wrote; END sendable only if the trace sends it
TraceTotal == Cardinality(Lines("Write", SEnd)) + (IF Lines("SendEnd", SEnd) = {} THEN 1 ELSE 0)
TraceW == Log[1].n
TraceBig == Len(Log) + 1

VARIABLE l

E == Log[l]
Skip == UNCHANGED vars
Head1(q) == IF q = <<>> THEN [t |-> "none"] ELSE Head(q)

Step ==
    CASE E.ev = "Init" -> Skip
      [] E.ev = "Offer" -> Offer(E.x)
      [] E.ev = "Verify" -> Verify
      [] E.ev = "Refuse" -> Refuse
      [] E.ev = "Die" -> Die(E.x)
      [] E.ev = "Revive" -> Revive
      [] E.ev = "Rebind" -> Rebind
      [] E.ev = "RePunch" -> RePunch
      [] E.ev = "Open" -> (Fallback(E.x) \/ Move(E.x)) /\ Len(st') = E.e
      [] E.ev = "Abort" -> AbortBy(IF E.by = SEnd THEN "S" ELSE "R")
      [] E.ev = "Write" -> IF E.by = SEnd THEN Write ELSE Skip
      [] E.ev = "Send" -> IF E.by = SEnd THEN Send /\ sEp = E.e /\ nxt < Total ELSE Skip
      [] E.ev = "SendEnd" -> IF E.by = SEnd THEN Send /\ sEp = E.e /\ nxt = Total ELSE Skip
      [] E.ev = "TakeAck" -> IF E.by = SEnd THEN TakeAck /\ sEp = E.e /\ Head(k[sEp]).n = E.n ELSE Skip
      [] E.ev = "Resume" -> IF Dir = "c2a" THEN Resume /\ sEp' = E.e /\ nxt' = E.n ELSE Skip
      [] E.ev = "Recv" -> IF E.by = REnd THEN Recv /\ rEp = E.e /\ Head1(d[rEp]).t = "data" ELSE Skip
      [] E.ev = "RecvEnd" -> IF E.by = REnd THEN Recv /\ rEp = E.e /\ Head1(d[rEp]).t = "end" ELSE Skip
      [] E.ev = "Ack" -> IF E.by = REnd THEN Ack /\ rEp = E.e /\ rAck' = E.n ELSE Skip
      \* the server took the client's OPEN on e: R's TakeOpen on c2a; on
      \* a2c S's RESUME (the client's count in the OPEN), or its ABORT's
      \* answer; an OPEN carrying the reset is S's TakeAbort (next line)
      \* an end's FIN after its clean end; the client's of the server's
      [] E.ev = "Fin" ->
            IF (E.by = SEnd) THEN SFin /\ sEp = E.e ELSE RFin /\ rEp = E.e
      [] E.ev = "TakeFin" ->
            IF Dir = "c2a" THEN STakeFin /\ sEp = E.e ELSE RTakeFin /\ rEp = E.e
      \* a server that left answered the client's OPEN on e (count and FIN)
      [] E.ev = "Attach" /\ E.x = "left" ->
            IF Dir = "c2a" THEN RLeftAnswer /\ rEp' = E.e ELSE SLeftAnswer /\ sEp' = E.e
      [] E.ev = "Attach" /\ E.x # "left" ->
            IF Dir = "c2a" THEN TakeOpen /\ rEp' = E.e
            ELSE IF E.x = "abort" THEN Skip
            ELSE ((Resume /\ nxt' = E.n) \/ SAnswerAbort) /\ sEp' = E.e
      [] E.ev = "TakeAbort" ->
            IF E.by = SEnd THEN STakeAbort /\ sEp' = E.e
            ELSE Recv /\ rEp = E.e /\ Head1(d[rEp]).t = "abort"
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ decider = (IF Dir = "c2a" THEN "S" ELSE "R") /\ l = 1
TraceNext == l <= Len(Log) /\ Step /\ l' = l + 1

\* Violated once every line was taken: the trace is a behaviour of DirectPath.
TraceUnfinished == l <= Len(Log)
=============================================================================
