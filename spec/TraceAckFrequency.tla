------------------------- MODULE TraceAckFrequency -------------------------
(* Trace validation of the quic-go fork's 1-RTT receiver against            *)
(* AckFrequency.tla's receiver (spec/README.md "ACK frequency"): the lines  *)
(* one appDataReceivedPacketTracker wrote (-tags quicacktrace,             *)
(* QUICACKTRACE; the fork's internal/ackhandler/trace_on.go), each logged  *)
(* after its step, must be steps of the receiver in order. A Recv of an    *)
(* ack-eliciting packet with no ACK queued must queue one exactly when     *)
(* AckFrequency!Receive's now_ holds: more than T waiting (cnt), the       *)
(* packet R or more below the largest acknowledged (late), or a hole the   *)
(* last ACK did not report R deep now R below the largest received (deep,  *)
(* AckFrequency!DeepHoles; none before the first ACK), or ECN-CE (RFC 9000 *)
(* 13.2.1; not in the model). Otherwise the ACK alarm is MAD after the     *)
(* oldest packet waiting (AckWithinDelay: the alarm the code sets; when    *)
(* its run loop wakes for it is the scheduler's). An ACK the code sends    *)
(* only if queued is queued or due (Timer); one riding another frame       *)
(* (oq 0) may go any time and reports all (a superset: the model's ACKs    *)
(* read only ranges). State is the code's packet history in the model's    *)
(* terms: base the lowest packet held (below it every number looks         *)
(* missing, as in the code), holes the missing ones above it, ign          *)
(* IgnoreBelow's bound (nothing below it is missing). The sender's packet  *)
(* threshold (S = R) is not traced (the fork's unit test                   *)
(* TestSentPacketHandlerSetPacketThreshold). Accepted when TLC reaches     *)
(* the trace's end: TraceUnfinished violated.                              *)
EXTENDS TraceLog, Integers, Sequences, FiniteSets, TLC

VARIABLES l, T, R, MAD, base, maxr, holes, ign, la, cnt, queued, alarm, oldest, now, why

traceVars == <<l, T, R, MAD, base, maxr, holes, ign, la, cnt, queued, alarm, oldest, now, why>>

E == Log[l]

Max(s) == CHOOSE x \in s : \A y \in s : y <= x

\* The highest number at most x the history reports missing (-1: none),
\* the code's HighestMissingUpTo with no deletedBelow.
HM(b, hs, x) ==
    LET c == {h \in hs : h <= x}
    IN IF x < b THEN x ELSE IF c # {} THEN Max(c) ELSE b - 1

\* The line's observations of the code's state after its step.
Agrees == /\ (E.q = 1) = queued'
          /\ E.alarm = alarm'
          /\ E.lo = base'

\* A packet the history did not hold: new above, a hole, or below base.
IsNew(p) == p >= ign /\ (base < 0 \/ p > maxr \/ p \in holes \/ p < base)

Recv ==
    LET p   == E.pn
        mx  == IF p > maxr THEN p ELSE maxr
        top == IF base >= 0 /\ p > maxr + 1 THEN (maxr + 1)..(p - 1) ELSE {}
        bot == IF base >= 0 /\ p < base THEN (p + 1)..(base - 1) ELSE {}
        hs  == {h \in (holes \cup top \cup bot) \ {p} : h > E.lo}
        c   == cnt + 1
        x   == mx - R
        hm  == HM(E.lo, hs, x)
        cntQ  == c > T
        lateQ == R > 0 /\ la >= 0 /\ p + R <= la
        deepQ == R > 0 /\ la >= 0 /\ mx >= R /\ hm >= 0 /\ hm >= ign /\ hm + R > la
        ceQ   == E.ce = 1
    IN /\ IsNew(p)
       /\ (base >= 0 /\ p >= base) => E.lo >= base \* only a trim raises it
       /\ p < base => E.lo = p
       /\ base' = E.lo /\ maxr' = mx /\ holes' = hs
       /\ IF E.el = 0
            THEN UNCHANGED <<cnt, queued, alarm, oldest, why>>
            ELSE /\ cnt' = c
                 /\ IF queued
                      THEN UNCHANGED <<queued, alarm, oldest, why>>
                      ELSE IF cntQ \/ lateQ \/ deepQ \/ ceQ
                        THEN /\ queued' = TRUE /\ alarm' = -1 /\ oldest' = -1
                             /\ why' = IF cntQ THEN "count" ELSE IF ceQ THEN "ce"
                                       ELSE IF lateQ THEN "late" ELSE "deep"
                        ELSE /\ queued' = FALSE
                             /\ alarm' = IF alarm < 0 THEN E.t + MAD ELSE alarm
                             /\ oldest' = IF alarm < 0 THEN E.t ELSE oldest
                             /\ UNCHANGED why
       /\ UNCHANGED <<T, R, MAD, ign, la, now>>
       /\ Agrees

Policy ==
    /\ T' = E.T /\ R' = E.R /\ MAD' = E.MAD
    /\ queued' = (queued \/ cnt > E.T)
    /\ alarm' = IF alarm >= 0 /\ E.MAD < MAD THEN alarm + E.MAD - MAD ELSE alarm
    /\ UNCHANGED <<base, maxr, holes, ign, la, cnt, oldest, now, why>>
    /\ Agrees

Imm ==
    /\ queued' = TRUE /\ alarm' = -1 /\ why' = "imm"
    /\ UNCHANGED <<T, R, MAD, base, maxr, holes, ign, la, cnt, oldest, now>>
    /\ Agrees

Ignore ==
    /\ ign' = IF E.pn > ign THEN E.pn ELSE ign
    /\ E.lo < 0 \/ E.lo >= ign'
    /\ base' = E.lo
    /\ holes' = {h \in holes : h > E.lo /\ h >= ign'}
    /\ UNCHANGED <<T, R, MAD, maxr, la, cnt, queued, alarm, oldest, now, why>>
    /\ Agrees

\* An ACK: queued, due (the alarm passed), or riding another frame (oq 0);
\* only after a new ack-eliciting packet; it reports the largest received.
Ack ==
    /\ cnt > 0
    /\ E.t >= now
    /\ E.oq = 1 => (queued \/ (alarm >= 0 /\ E.t >= alarm))
    /\ E.la = maxr
    /\ la' = E.la /\ now' = E.t
    /\ cnt' = 0 /\ queued' = FALSE /\ alarm' = -1 /\ oldest' = -1
    /\ why' = IF E.oq = 0 THEN "piggy" ELSE IF queued THEN why ELSE "timer"
    /\ UNCHANGED <<T, R, MAD, base, maxr, holes, ign>>
    /\ Agrees

Step ==
    CASE E.ev = "Recv" -> Recv
      [] E.ev = "Policy" -> Policy
      [] E.ev = "Imm" -> Imm
      [] E.ev = "Ignore" -> Ignore
      [] E.ev = "Ack" -> Ack
      [] OTHER -> Print(<<"unknown event", l, E>>, FALSE)

\* The first line: the default policy the tracker starts with.
TraceInit ==
    /\ Log[1].ev = "Policy"
    /\ l = 2
    /\ T = Log[1].T /\ R = Log[1].R /\ MAD = Log[1].MAD
    /\ base = -1 /\ maxr = -1 /\ holes = {} /\ ign = 0 /\ la = -1
    /\ cnt = 0 /\ queued = FALSE /\ alarm = -1 /\ oldest = -1 /\ now = 0
    /\ why = "none"

TraceNext == l <= Len(Log) /\ Step /\ l' = l + 1

-----------------------------------------------------------------------------
\* AckFrequency.tla's receiver invariants on the code's state.
\* CountBound: no more than T wait with no ACK queued.
CountBound == ~queued => cnt <= T
\* AckWithinDelay: the alarm at most MAD after the oldest packet waiting
\* (earlier after a policy with a longer MAD: the code keeps the alarm).
AlarmOnTime == (~queued /\ cnt > 0) => (alarm >= 0 /\ alarm <= oldest + MAD)
\* ImmediateAcked: an IMMEDIATE_ACK leaves an ACK queued until one is sent.
ImmediateAcked == why = "imm" => queued

\* Violated once every line was taken: the trace is a behaviour.
TraceUnfinished == l <= Len(Log)
=============================================================================
