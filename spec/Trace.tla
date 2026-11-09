-------------------------------- MODULE Trace --------------------------------
(* Trace validation (spec/README.md "Trace validation"): the steps a run of  *)
(* the Go code recorded (-tags linktrace) must be a behaviour of Link. Each  *)
(* line is one action of Link with its arguments; the values the code saw   *)
(* after the step (credit, held, grants) must be the model's. TLC accepts    *)
(* the trace when it reaches the last line (TraceAccepted).                  *)
EXTENDS TraceLog, Link, TLC

\* Link's constants, from the trace.
TraceSids == {Log[i].sid : i \in DOMAIN Log} \ {""}
UpOf(p) == Log[CHOOSE i \in DOMAIN Log : Log[i].ev = "Up" /\ Log[i].side = p]
TraceWin == [p \in Sides |-> UpOf(p).n]
TraceChain == UpOf("d").held = 1
TraceOwner == [s \in TraceSids |->
    IF \E i \in DOMAIN Log : Log[i].ev \in {"Open", "OpenWait"} /\ Log[i].sid = s
    THEN Log[CHOOSE i \in DOMAIN Log : Log[i].ev \in {"Open", "OpenWait"} /\ Log[i].sid = s].side
    ELSE "?"]

VARIABLE l
traceVars == <<vars, l>>

E == Log[l]
P == E.side
S == E.sid

\* A received frame: the peer's next one is of the logged type, sid and n.
RecvStep ==
    LET f == Head(out[Peer(P)]) IN
    /\ out[Peer(P)] # <<>>
    /\ f.t = E.t
    /\ E.t \in {"open", "data", "win", "accept", "close"} => f.s = S
    /\ E.t \in {"data", "win"} => f.n = E.n
    \* 4: a refusal's ABORT; 3: an ABORT, Mux.cut's (DDL) or Session.Abort's
    /\ E.t = "close" => f.n \in (CASE E.n = 4 -> {ABT} [] E.n = 3 -> {ABT, DDL} [] OTHER -> {E.n})
    /\ Recv(P)
    /\ E.t \in {"data", "win"} /\ E.credit >= 0 =>
        cr'[P][S].on /\ cr'[P][S].n = E.credit /\ (E.t = "data" => cr'[P][S].held = E.held)
    \* A refusal's Credit.forget (its Forget 4) comes before its ABORT is
    \* buffered, End here: data read between is dropped, not charged; the
    \* model queues it for a session nothing reads and End drops (C12).
    /\ E.credit < 0 => ~cr[P][S].on \/ (E.t = "data" /\ Refusing(P, S))

LinkStep ==
    CASE E.ev = "Up" -> UNCHANGED vars
      [] E.ev = "OpenWait" -> OpenCheck(P, S)
      \* An OPEN buffered after Conn.Close is logged, then undone (its Flush fails).
      [] E.ev = "Open" -> IF wt[P][S] = "open" THEN OpenBuffer(P, S) \/ (closed[P] /\ OpenRefused(P, S)) ELSE Open(P, S)
      [] E.ev = "OpenRefused" -> OpenRefused(P, S)
      [] E.ev = "Accept" -> Accept(P, S)
      [] E.ev = "Acquire" -> Acquire(P, S, E.n) /\ cr'[P][S].n = E.credit
      [] E.ev = "SendWait" -> SendCheck(P, S)
      [] E.ev = "SendData" -> pend[P][S] = E.n /\ IF wt[P][S] = "data" THEN SendBuffer(P, S) ELSE SendData(P, S)
      [] E.ev = "Refund" -> pend[P][S] = E.n /\ Refund(P, S) /\ (E.credit >= 0 => cr'[P][S].n = E.credit)
      \* A refusal (code 4): the answer taken (RefuseMark, not logged), its ABORT forgets.
      \* Mux.refuse's goroutine may run after the connection's end cut the
      \* session: its ABORT goes nowhere (finding C11, as C9).
      \* A drain's refusal (RecvOpen marked it): its forget is End's here.
      [] E.ev = "Forget" /\ E.n = 4 -> IF (closed[P] /\ Owner[S] # P /\ ss[P][S].st # "open" /\ ss[P][S].closing = 0)
                                          \/ RefusalDue(P, S)
                                       THEN UNCHANGED vars ELSE RefuseMark(P, S)
      [] E.ev = "Close" /\ E.n = 4 -> IF closed[P] /\ Owner[S] # P /\ ss[P][S].st # "open" /\ ss[P][S].closing = 0
                                      THEN UNCHANGED vars ELSE ss[P][S].closing = ABT + 1 /\ End(P, S)
      \* A joint's Abort of a session already gone here (LateAbort).
      [] E.ev = "Forget" /\ E.n \notin {4, DDL} -> IF E.n = ABT /\ LateAbort(P, S) THEN LateAbortForget(P, S)
                                                ELSE Closing(P, S, E.n)
      \* FinBeforeShut at a trace's grain (the mark is not logged): no FIN goes
      \* after this side's write side shut while the peer still reads, but
      \* after an abort (Mux.cut).
      \* A CLOSE buffered after the connection's end cut the session (CloseWrite
      \* checked it open just before): the frame goes nowhere (finding C9).
      [] E.ev = "Close" /\ E.n = Half -> IF closed[P] /\ ss[P][S].st # "open" THEN UNCHANGED vars
                                         ELSE CloseWrite(P, S)
                                         /\ (wshut[P] => ss[P][S].code = ABT \/ ss[Peer(P)][S].st # "open")
      [] E.ev = "Close" /\ E.n \notin {Half, 4, DDL} -> IF E.n = ABT /\ LateAbort(P, S) THEN LateAbortSend(P, S)
                                                      ELSE ss[P][S].closing = E.n + 1 /\ End(P, S)
      \* Mux.drop: a session whose CLOSE ended both halves leaves the Mux
      \* (Drop); the other drops follow a step that already ended it here.
      [] E.ev = "Drop" -> IF Dropping(P, S) THEN Drop(P, S) ELSE UNCHANGED vars
      [] E.ev = "Read" -> Read(P, S, E.n) /\ cr'[P][S].held = E.held /\ gpend'[P][S] = E.grant
      [] E.ev = "Window" -> gpend[P][S] = E.n /\ SendWindow(P, S)
      [] E.ev = "GoAway" -> GoAway(P)
      [] E.ev = "Ping" -> Ping(P, E.n)
      \* keepAlive took a ping off its channel before the receive loop handed
      \* it the next (pings read in a burst): it answers both, the second in
      \* place of the first, still last in the write buffer; the model's one
      \* pong already answered the latest (lane LIVESTARVE, link-ok-pong-burst).
      [] E.ev = "Pong" -> IF E.n = 1 /\ pong[P] = 0 /\ out[P] # <<>> /\ out[P][Len(out[P])].t = "pong"
                          THEN UNCHANGED vars ELSE Pong(P, E.n = 1)
      [] E.ev = "Recv" -> RecvStep
      \* closeFlushed after Conn.Close (a Node or Pool closing a connection
      \* that ended meanwhile): nothing left to shut (finding C9).
      [] E.ev = "Leave" -> IF closed[P] THEN UNCHANGED vars ELSE Leave(P)
      [] E.ev = "ConnClose" -> \E k \in 0..Len(out[P]) : CloseConn(P, k)
      [] E.ev = "Down" -> Down(P)
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

\* Mux.cut's steps (Link.tla "Mux.cut"): its snapshot (Cut, n: the
\* sessions in it), the second call (CutWait), each ABORT (Forget and
\* Close 5), closeFlushed (Leave), cutEnded closed (CutEnd), each call's
\* return (CutRet).
CutReady == cut[P].st = "run" /\ cut[P].q = {} /\ cut[P].w = NoSid /\ ~closed[P]
CutEv == E.ev \in {"Cut", "CutWait", "CutEnd", "CutRet"} \/ (E.ev \in {"Forget", "Close"} /\ E.n = DDL)
CutTrace ==
    CASE E.ev = "Cut" -> \E q \in SUBSET CutCand(P) : Cardinality(q) = E.n /\ CutBegin(P, q)
      [] E.ev = "CutWait" -> CutWait(P)
      [] E.ev = "Forget" -> CutForget(P, S)
      [] E.ev = "Close" -> cut[P].w = S /\ CutSend(P)
      [] E.ev = "CutEnd" -> CutEnd(P)
      [] E.ev = "CutRet" -> CutReturn(P)
\* closeFlushed: the cut's once its ABORTs are buffered, or Mux.leave's.
Step ==
    IF CutEv THEN CutTrace
    ELSE IF E.ev = "Leave" /\ CutReady THEN CutFlush(P) \/ (LinkStep /\ UNCHANGED cut)
    ELSE LinkStep /\ UNCHANGED cut

TraceInit == Init /\ l = 1
TraceNext == l <= Len(Log) /\ Step /\ l' = l + 1
TraceSpec == TraceInit /\ [][TraceNext]_traceVars

\* The longest behaviour TLC found took every line.
TraceAccepted ==
    LET d == TLCGet("stats").diameter IN
    IF d - 1 = Len(Log) THEN TRUE
    ELSE Print(<<"trace rejected at line", d, Log[d]>>, FALSE)
=============================================================================
