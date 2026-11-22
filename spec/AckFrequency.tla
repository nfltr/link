---------------------------- MODULE AckFrequency ----------------------------
(* A QUIC receiver's ACK policy under ACK_FREQUENCY against the sender's   *)
(* loss detection (spec/README.md "ACK frequency", lane SPEED-ACK;          *)
(* draft-ietf-quic-ack-frequency-14 sections 6 and 9, RFC 9002 6.1.1).     *)
(* quiclink's sender asks its peer, once (ACK_FREQUENCY, sequence 0), for  *)
(* an ACK after more than T ack-eliciting packets or MAD after the oldest  *)
(* one not yet acknowledged, and for an immediate ACK only once a hole is  *)
(* R packets deep (draft 6.2: the smallest unreported missing packet R     *)
(* below the largest received) or a packet arrives R or more below the     *)
(* largest acknowledged; a packet carrying IMMEDIATE_ACK is acknowledged   *)
(* at once. The sender declares a packet lost once a packet S or more      *)
(* above it is acknowledged (S = R: draft section 9). The network delivers *)
(* each packet at most Reorder places late, or drops one of Lossy.         *)
(* The receiver's steps are atomic (quic-go's run loop handles one packet, *)
(* then asks for an ACK); an ACK reaches the sender at once (a delayed ACK *)
(* carries the same ranges: packet-threshold loss detection reads only its *)
(* ranges; time-threshold detection is not modelled). Time is ticks.       *)
(* Adaptation (lane ACKFIX): at its first spurious loss the sender turns   *)
(* its packet threshold off (upstream quic-go #5905: time threshold only)  *)
(* and asks again (ACK_FREQUENCY sequence 1, draft section 4) with         *)
(* Reordering Threshold 0, so the receiver stops acknowledging reordering  *)
(* at once (draft 6.2: 0, out-of-order packets elicit no immediate ACK):   *)
(* with the packet threshold off those ACKs speed no loss detection. On a  *)
(* path reordering deeper than R (Reorder >= R, AckFrequencyDeep.cfg) the  *)
(* receiver otherwise acked nearly every deep-late packet at once (Helsinki *)
(* -> Singapore, 0.3 ACKs per data packet against 0.1). The update is in   *)
(* flight until the receiver applies it (a control frame, resent if lost). *)
EXTENDS Naturals, FiniteSets

CONSTANTS
    N,        \* packets the sender sends, 1..N, every one ack-eliciting
    T,        \* Ack-Eliciting Threshold
    R,        \* Reordering Threshold (> 0)
    MAD,      \* Requested Max Ack Delay, in ticks
    S,        \* the sender's packet threshold
    Reorder,  \* how many places late the network may deliver a packet
    Lossy,    \* packets the network may drop
    Imm,      \* packets carrying IMMEDIATE_ACK
    Horizon,  \* last tick
    Mutant    \* "none";
              \* "threshold-ignored": an immediate ACK for any packet below
              \*   the largest acknowledged (R treated as 1: the fork's
              \*   wasMissing rule before this lane);
              \* "no-reorder-ack": no immediate ACK for a hole at all;
              \* "immediate-dropped": IMMEDIATE_ACK ignored;
              \* "alarm-latest": the ACK alarm moved to each new packet's
              \*   receipt (upstream quic-go's timer when not decimating),
              \*   so the oldest one waits past MAD;
              \* "count-late": an ACK only after more than T + 1;
              \* "sender-threshold": the sender keeps packet threshold
              \*   Reorder (upstream's 3 under a deeper reordering);
              \* "no-update": the sender never asks again after turning
              \*   its packet threshold off (quiclink v0.8.0's one frame at
              \*   the handshake: the bug lane ACKFIX found).

ASSUME R > 0 /\ T >= 0 /\ T < R /\ MAD > 0 /\ Lossy \subseteq 1..N /\ Imm \subseteq 1..N

VARIABLES
    sent,     \* packets sent so far (1..sent)
    net,      \* packets in flight
    rcvd,     \* packets the receiver holds
    dropped,  \* packets the network dropped
    unacked,  \* received ack-eliciting packets no ACK reported yet
    oldest,   \* receipt tick of the oldest of unacked
    alarm,    \* the receiver's ACK alarm (0: none)
    la,       \* Largest Acked: the largest packet of the last ACK sent
    acked,    \* what the sender knows acknowledged
    lost,     \* what the sender declared lost
    now,      \* the clock
    acks,     \* ACKs sent (a count, for the witnesses)
    why,      \* why the last ACK was sent
    reorderAck, \* history: an immediate ACK forced by reordering alone
    ptOff,    \* the sender's packet threshold is off (a spurious loss seen)
    upd,      \* the second ACK_FREQUENCY: "none", "flight", "applied"
    rR,       \* the Reordering Threshold the receiver applies (R, then 0)
    wasted    \* history: a reordering ACK after ptOff with no update asked

vars == <<sent, net, rcvd, dropped, unacked, oldest, alarm, la, acked, lost,
          now, acks, why, reorderAck, ptOff, upd, rR, wasted>>

Max(s) == IF s = {} THEN 0 ELSE CHOOSE x \in s : \A y \in s : y <= x
Min(s) == CHOOSE x \in s : \A y \in s : x <= y

S0 == IF Mutant = "sender-threshold" THEN Reorder ELSE S

TypeOK ==
    /\ sent \in 0..N /\ net \subseteq 1..N /\ rcvd \subseteq 1..N
    /\ dropped \subseteq Lossy /\ unacked \subseteq rcvd
    /\ alarm \in 0..(Horizon + MAD + 1) /\ la \in 0..N
    /\ acked \subseteq rcvd /\ lost \subseteq 1..N /\ now \in 0..Horizon
    /\ ptOff \in BOOLEAN /\ upd \in {"none", "flight", "applied"} /\ rR \in {0, R}

Init ==
    /\ sent = 0 /\ net = {} /\ rcvd = {} /\ dropped = {} /\ unacked = {}
    /\ oldest = 0 /\ alarm = 0 /\ la = 0 /\ acked = {} /\ lost = {}
    /\ now = 0 /\ acks = 0 /\ why = "none" /\ reorderAck = FALSE
    /\ ptOff = FALSE /\ upd = "none" /\ rR = R /\ wasted = FALSE

\* The sender reads an ACK of rcvd: a packet it declared lost acknowledged
\* is a spurious loss, which turns its packet threshold off before this
\* ACK's loss detection (quic-go's detectSpuriousLosses, then
\* detectLostPackets) and asks for Reordering Threshold 0; while on,
\* packet-threshold loss detection.
SenderReads(a) ==
    LET off == ptOff \/ lost \cap a # {}
    IN /\ acked' = a
       /\ ptOff' = off
       /\ upd' = IF off /\ upd = "none" /\ Mutant # "no-update" THEN "flight" ELSE upd
       /\ lost' = IF off THEN lost
                  ELSE lost \cup {p \in 1..sent : p \notin a /\ \E q \in a : q >= p + S0}

Send ==
    /\ sent < N
    /\ sent' = sent + 1
    /\ net' = net \cup {sent + 1}
    /\ UNCHANGED <<rcvd, dropped, unacked, oldest, alarm, la, acked, lost, now,
                   acks, why, reorderAck, ptOff, upd, rR, wasted>>

\* At most Reorder places late: nothing in flight more than Reorder below p.
Deliverable(p) == \A q \in net : q >= p - Reorder

\* Holes the receiver has not reported R deep yet (draft 6.2 "Unreported
\* Missing") that are now R below the largest received. Before its first
\* ACK the receiver reports none (quic-go: the 1-RTT packet numbers follow
\* the handshake's in one space, so the numbers below the first 1-RTT
\* packet look missing); the count bound (T < R) sends that ACK in time.
DeepHoles(r, lu) == {h \in 1..lu : la > 0 /\ h \notin r /\ lu - h >= rR /\ h + rR > la}

Receive(p) ==
    /\ p \in net /\ Deliverable(p)
    /\ LET r   == rcvd \cup {p}
           u   == unacked \cup {p}
           lu  == Max(r)
           imm == p \in Imm /\ Mutant # "immediate-dropped"
           cnt == Cardinality(u) > (IF Mutant = "count-late" THEN T + 1 ELSE T)
           late == IF Mutant = "threshold-ignored" THEN la > 0 /\ p < la
                   ELSE rR > 0 /\ la > 0 /\ p + rR <= la
           deep == IF Mutant \in {"no-reorder-ack", "threshold-ignored"} \/ rR = 0 THEN {}
                   ELSE DeepHoles(r, lu)
           reo == Mutant # "no-reorder-ack" /\ (late \/ deep # {})
           now_ == imm \/ cnt \/ reo
       IN /\ net' = net \ {p}
          /\ rcvd' = r
          /\ IF now_
               THEN /\ unacked' = {} /\ alarm' = 0 /\ la' = lu
                    /\ acks' = acks + 1
                    /\ why' = IF imm THEN "imm" ELSE IF cnt THEN "count" ELSE "reorder"
                    /\ reorderAck' = (reorderAck \/
                          (~imm /\ ~cnt /\ (late \/ \E h \in deep : h \notin dropped)))
                    /\ wasted' = (wasted \/ (~imm /\ ~cnt /\ ptOff /\ upd = "none"))
                    /\ SenderReads(r)
                    /\ oldest' = 0
                    /\ UNCHANGED rR
               ELSE /\ unacked' = u
                    /\ oldest' = IF unacked = {} THEN now ELSE oldest
                    /\ alarm' = IF alarm = 0 \/ Mutant = "alarm-latest"
                                THEN now + MAD ELSE alarm
                    /\ UNCHANGED <<la, acks, why, reorderAck, acked, lost, ptOff, upd, rR, wasted>>
    /\ UNCHANGED <<sent, dropped, now>>

Drop(p) ==
    /\ p \in net /\ p \in Lossy
    /\ net' = net \ {p} /\ dropped' = dropped \cup {p}
    /\ UNCHANGED <<sent, rcvd, unacked, oldest, alarm, la, acked, lost, now,
                   acks, why, reorderAck, ptOff, upd, rR, wasted>>

\* The alarm fires: an ACK of everything received.
Timer ==
    /\ alarm # 0 /\ now >= alarm
    /\ unacked' = {} /\ alarm' = 0 /\ oldest' = 0 /\ la' = Max(rcvd)
    /\ acks' = acks + 1 /\ why' = "timer"
    /\ SenderReads(rcvd)
    /\ UNCHANGED <<sent, net, rcvd, dropped, now, reorderAck, rR, wasted>>

\* Time passes only up to a due alarm (quic-go's run loop wakes at it).
Tick ==
    /\ now < Horizon
    /\ alarm = 0 \/ now < alarm
    /\ now' = now + 1
    /\ UNCHANGED <<sent, net, rcvd, dropped, unacked, oldest, alarm, la, acked,
                   lost, acks, why, reorderAck, ptOff, upd, rR, wasted>>

\* The receiver applies the second ACK_FREQUENCY (SetAckFrequency): the
\* frame arrives some time after the ACK that turned the threshold off.
Update ==
    /\ upd = "flight"
    /\ upd' = "applied" /\ rR' = 0
    /\ UNCHANGED <<sent, net, rcvd, dropped, unacked, oldest, alarm, la, acked,
                   lost, now, acks, why, reorderAck, ptOff, wasted>>

Next == Send \/ Timer \/ Tick \/ Update \/ \E p \in 1..N : Receive(p) \/ Drop(p)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Every ack-eliciting packet is acknowledged within MAD of its receipt.
AckWithinDelay == unacked # {} => now <= oldest + MAD

\* No more than T ack-eliciting packets wait for an ACK.
CountBound == Cardinality(unacked) <= T

\* A packet carrying IMMEDIATE_ACK is acknowledged when it is received.
ImmediateAcked == \A p \in Imm \cap rcvd : p \notin unacked

\* A hole R deep is reported at once: the sender holds an ACK of a packet R
\* or more above it (and so, S = R, has declared it lost).
\* (While the sender's packet threshold is on: after it turned it off,
\* time-threshold detection, not modelled, declares losses.)
PromptLoss ==
    ~ptOff => \A p \in 1..N : (p \notin rcvd /\ \E q \in rcvd : q >= p + R)
        => \E q \in acked : q >= p + R

LossDeclared ==
    ~ptOff => \A p \in 1..N : (p \notin rcvd /\ \E q \in rcvd : q >= p + R) => p \in lost

\* No packet the network delivers is declared lost (Reorder < S).
NoSpuriousLoss == lost \subseteq dropped

\* Reordering within R forces no immediate ACK (the CPU this lane saves).
NoReorderAck == ~reorderAck

\* Once the sender's packet threshold is off, it has asked for Reordering
\* Threshold 0 (AckFrequencyDeep.cfg; lane ACKFIX) ...
UpdateAsked == ptOff => upd # "none"
\* ... so no ACK is forced by reordering that the sender did not ask for:
\* one comes only while its packet threshold is on or the update flies.
NoWastedReorderAck == ~wasted
\* The receiver applies Reordering Threshold 0 only after the sender
\* turned its packet threshold off (it never loses packet-threshold
\* detection's prompt ACKs while it relies on them).
UpdateAfterOff == rR = 0 => ptOff

\* Non-vacuity: each must be violated (scripts/spec.sh witness).
WitnessTimer == why # "timer"
WitnessCount == why # "count"
WitnessImm == why # "imm"
WitnessReorderReport == why # "reorder"
WitnessLost == lost = {}
WitnessLateQuiet == ~(acks = 0 /\ \E p \in 1..N : p \notin rcvd /\ \E q \in rcvd : q > p)
WitnessWait == ~(Cardinality(unacked) = T /\ T > 0)
WitnessSpurious == ~ptOff
WitnessApplied == upd # "applied"
=============================================================================
