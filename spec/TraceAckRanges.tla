--------------------------- MODULE TraceAckRanges ---------------------------
(* Trace validation of the quic-go fork's 1-RTT receiver against           *)
(* AckRanges.tla (spec/README.md "ACK ranges"): the lines one              *)
(* appDataReceivedPacketTracker wrote (-tags quicacktrace, QUICACKTRACE;   *)
(* the fork's internal/ackhandler/trace_on.go: Recv pn, Ack rs), in order, *)
(* must be AckRanges!Receive and AckRanges!Ack steps: every Ack's ranges   *)
(* (rs, [smallest, largest], descending) cover each packet received since  *)
(* the previous Ack (ReportedOnce), only received ones (AckedReceived),    *)
(* at most K of them, valid (descending, a gap between two: FrameBound and *)
(* the wire's encoding). Policy, Imm and Ignore change neither. The        *)
(* history's ranges are not logged: HistoryBound is the fork's unit tests'.*)
(* Accepted when TLC reaches the trace's end: TraceUnfinished violated.    *)
EXTENDS TraceLog, Integers, Sequences, FiniteSets, TLC

CONSTANT K

VARIABLES l, rcvd, fresh

traceVars == <<l, rcvd, fresh>>

E == Log[l]

Covered(rs, p) == \E i \in DOMAIN rs : rs[i][1] <= p /\ p <= rs[i][2]

Valid(rs) ==
    /\ Len(rs) \in 1..K
    /\ \A i \in DOMAIN rs : rs[i][1] <= rs[i][2]
    /\ \A i \in 2..Len(rs) : rs[i][2] + 1 < rs[i - 1][1]

Recv ==
    /\ E.ev = "Recv"
    /\ rcvd' = rcvd \cup {E.pn}
    /\ fresh' = fresh \cup {E.pn}

Ack ==
    /\ E.ev = "Ack"
    /\ Valid(E.rs)
    /\ \A p \in fresh : Covered(E.rs, p)
    /\ \A i \in DOMAIN E.rs : (E.rs[i][1]..E.rs[i][2]) \subseteq rcvd
    /\ fresh' = {}
    /\ UNCHANGED rcvd

Other ==
    /\ E.ev \in {"Policy", "Imm", "Ignore"}
    /\ UNCHANGED <<rcvd, fresh>>

TraceInit == l = 1 /\ rcvd = {} /\ fresh = {}

TraceNext == l <= Len(Log) /\ (Recv \/ Ack \/ Other) /\ l' = l + 1

TraceUnfinished == l <= Len(Log)
=============================================================================
