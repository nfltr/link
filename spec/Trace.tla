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
    IF \E i \in DOMAIN Log : Log[i].ev = "Open" /\ Log[i].sid = s
    THEN Log[CHOOSE i \in DOMAIN Log : Log[i].ev = "Open" /\ Log[i].sid = s].side
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
    /\ E.t \in {"data", "win", "close"} => f.n = E.n
    /\ Recv(P)
    /\ E.t \in {"data", "win"} /\ E.credit >= 0 =>
        cr'[P][S].on /\ cr'[P][S].n = E.credit /\ (E.t = "data" => cr'[P][S].held = E.held)
    /\ E.credit < 0 => ~cr[P][S].on

Step ==
    CASE E.ev = "Up" -> UNCHANGED vars
      [] E.ev = "Open" -> Open(P, S)
      [] E.ev = "Accept" -> Accept(P, S)
      [] E.ev = "Acquire" -> Acquire(P, S, E.n) /\ cr'[P][S].n = E.credit
      [] E.ev = "SendData" -> pend[P][S] = E.n /\ SendData(P, S)
      [] E.ev = "Refund" -> pend[P][S] = E.n /\ Refund(P, S) /\ (E.credit >= 0 => cr'[P][S].n = E.credit)
      [] E.ev = "Forget" -> Closing(P, S, E.n)
      [] E.ev = "Close" /\ E.n = Half -> CloseWrite(P, S)
      [] E.ev = "Close" /\ E.n # Half -> ss[P][S].closing = E.n + 1 /\ End(P, S)
      [] E.ev = "Read" -> Read(P, S, E.n) /\ cr'[P][S].held = E.held /\ gpend'[P][S] = E.grant
      [] E.ev = "Window" -> gpend[P][S] = E.n /\ SendWindow(P, S)
      [] E.ev = "GoAway" -> GoAway(P)
      [] E.ev = "Ping" -> Ping(P, E.n)
      [] E.ev = "Pong" -> Pong(P, E.n = 1)
      [] E.ev = "Recv" -> RecvStep
      [] E.ev = "Leave" -> Leave(P)
      [] E.ev = "ConnClose" -> \E k \in 0..Len(out[P]) : CloseConn(P, k)
      [] E.ev = "Down" -> Down(P)
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ l = 1
TraceNext == l <= Len(Log) /\ Step /\ l' = l + 1
TraceSpec == TraceInit /\ [][TraceNext]_traceVars

\* The longest behaviour TLC found took every line.
TraceAccepted ==
    LET d == TLCGet("stats").diameter IN
    IF d - 1 = Len(Log) THEN TRUE
    ELSE Print(<<"trace rejected at line", d, Log[d]>>, FALSE)
=============================================================================
