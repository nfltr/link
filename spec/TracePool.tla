------------------------------ MODULE TracePool ------------------------------
(* Trace validation of the Pool (spec/README.md "The Pool"): the steps a   *)
(* Pool recorded (-tags linktrace, trace_pool_on.go) must be steps of      *)
(* Pool.tla, in order. The Pool's view of its sessions and connections is  *)
(* logged too (a session's end, the peer's GOAWAY, a connection's close:   *)
(* the path died, or it was going away and idle); only the wakes on       *)
(* p.changed are TLC's to take between two logged steps. Opens and         *)
(* connections are numbered as the trace                                   *)
(* numbers them, one spare slot more (a dial reserved while none is free   *)
(* in the model: review r2 L6). Accepted when TLC reaches the trace's end  *)
(* (TraceUnfinished violated); a step that maps to no action fails.       *)
EXTENDS TraceLog, Pool, Sequences, TLC

Lines(ev) == {i \in DOMAIN Log : Log[i].ev = ev}
TraceConns == {Log[i].c : i \in {j \in Lines("Dial") : Log[j].c > 0}}
TraceIds == TraceConns \cup {Cardinality(TraceConns) + 1}
TraceOpens == {Log[i].o : i \in Lines("Start")}
TraceMin == Log[1].n
TraceMax == Log[1].c
TraceCap == LET caps == {Log[i].n : i \in {j \in Lines("Dial") : Log[j].c > 0}}
            IN IF caps = {} THEN 1 ELSE CHOOSE x \in caps : \A y \in caps : y <= x
TraceEnv == Len(Log) + 2 * Cardinality(TraceIds)

VARIABLE l

E == Log[l]

Env == MaintainWake \/ \E o \in TraceOpens : OpenWake(o)

\* c closed: CloseConn, after the path died (PathDies) unless it was going
\* away with no session; already closed by Close (CloseSnap): nothing.
Down(c) ==
    IF ~Up(c) THEN UNCHANGED vars
    ELSE IF ga[c] /\ sess[c] = 0 THEN CloseConn(c)
    ELSE /\ env < MaxEnv /\ env' = env + 1
         /\ cs' = [cs EXCEPT ![c] = "closed"]
         /\ sess' = [sess EXCEPT ![c] = 0]
         /\ op' = [o \in Opens |-> IF op[o].st = "open" /\ op[o].c = c THEN [st |-> "idle", c |-> 0] ELSE op[o]]
         /\ UNCHANGED <<ga, dead, dials, mtw, opw, mt, dr, pc, bad, gr>>

Picked(o, t) ==
    CASE t = "closed" -> Closed /\ op'[o].st = "idle"
      [] t = "full"   -> ~Closed /\ op'[o].st = "idle"
      [] t = "place"  -> op'[o] = [st |-> "place", c |-> E.c] /\ gr' = gr
      [] t = "grow"   -> op'[o] = [st |-> "place", c |-> E.c] /\ gr' = o
      [] t = "dial"   -> op'[o].st = "dial"
      [] t = "wait"   -> op'[o].st = "wait"
      [] OTHER        -> Print(<<"unknown pick", E>>, FALSE)

\* A dial ended: maintain's, an Open's, Drain's or growth's (the trace does not say
\* whose: TLC finds one), with the connection the trace names or failed.
Dialled ==
    /\ \/ MaintainDial \/ DrainDial \/ GrowDial \/ \E o \in TraceOpens : OpenDial(o)
    /\ IF E.c > 0 THEN cs[E.c] = "free" /\ cs'[E.c] # "free" ELSE cs' = cs

Placed(o) == Place(o) /\ ((op'[o].st = "open") = (E.n = 1))

\* OpenChain failed on a connection the model still has up: its writes
\* failed, or it closed before Serve returned (Down): the path died.
PlaceLost(o) ==
    LET c == op[o].c IN
    /\ op[o].st = "place" /\ Up(c) /\ env < MaxEnv /\ env' = env + 1
    /\ cs' = [cs EXCEPT ![c] = "closed"]
    /\ sess' = [sess EXCEPT ![c] = 0]
    /\ op' = [x \in Opens |-> IF x = o THEN [op[o] EXCEPT !.st = "pick"]
                              ELSE IF op[x].st = "open" /\ op[x].c = c THEN [st |-> "idle", c |-> 0] ELSE op[x]]
    /\ UNCHANGED <<ga, dead, dials, mtw, opw, mt, dr, pc, bad, gr>>

Step ==
    CASE E.ev = "Pool" -> l = 1 /\ UNCHANGED vars
      [] E.ev = "Start" -> OpenStart(E.o)
      [] E.ev = "Pick" -> Pick(E.o) /\ Picked(E.o, E.t)
      [] E.ev = "Place" -> op[E.o].c = E.c /\ (Placed(E.o) \/ (E.n = 0 /\ PlaceLost(E.o)))
      [] E.ev = "OpenErr" -> op[E.o].st = "idle" /\ UNCHANGED vars
      [] E.ev = "End" -> IF op[E.o].st = "open" /\ op[E.o].c = E.c THEN SessionClose(E.o) ELSE UNCHANGED vars \* ended with its connection
      [] E.ev = "PeerGoAway" -> IF Up(E.c) /\ ~ga[E.c] THEN PeerGoAway(E.c) ELSE UNCHANGED vars
      [] E.ev = "Down" -> Down(E.c)
      [] E.ev = "MaintainWait" -> MaintainCheck /\ mt' = "wait"
      [] E.ev = "MaintainDial" -> MaintainCheck /\ mt' = "dial"
      [] E.ev = "Dial" -> Dialled
      [] E.ev = "Remove" -> Remove(E.c)
      [] E.ev = "DrainStart" -> DrainStart /\ DrainN = E.n
      [] E.ev = "GoAway" -> DrainBack \/ DrainGoAway
      [] E.ev = "CloseStart" -> IF pc = "open" THEN CloseStart ELSE UNCHANGED vars
      [] E.ev = "CloseEnd" -> IF pc = "cancelled" THEN CloseEnd ELSE UNCHANGED vars
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ l = 1
TraceNext == (l <= Len(Log) /\ Step /\ l' = l + 1) \/ (Env /\ UNCHANGED l)

\* Violated once every line was taken: the trace is a behaviour of Pool.
TraceUnfinished == l <= Len(Log)
=============================================================================
