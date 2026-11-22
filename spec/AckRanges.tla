----------------------------- MODULE AckRanges -----------------------------
(* A QUIC receiver's bounded ACK range history against what its ACKs      *)
(* report (spec/README.md "ACK ranges", lane SPURIOUS; RFC 9000 13.2.3,   *)
(* RFC 2018 section 4). quic-go's receiver keeps at most K ranges of the  *)
(* packets it received (MaxNumAckRanges, a DoS bound) and drops the       *)
(* lowest past it. A packet arriving below the lowest range kept made a   *)
(* new lowest range, dropped at once: processed, never acknowledged. The  *)
(* sender then declared it lost by its time threshold and resent it: on   *)
(* Helsinki -> Singapore (half the packets late by tens to hundreds of    *)
(* places, 100-300 holes open at once) 2.6 % of the packets of a 4-        *)
(* connection run on a path that lost 0.03 %. The receiver now reports   *)
(* first, in every ACK, the ranges that hold a packet received since its  *)
(* last ACK (RFC 2018: the block holding the newest segment first), those *)
(* the history dropped among them, then the highest others, K in all.     *)
(* An ACK reaches the sender at once and is never lost (the assumption    *)
(* the invariant needs); the receiver sends one after more than T new     *)
(* packets (ACK_FREQUENCY's threshold), or at any time before.             *)
EXTENDS Naturals, FiniteSets

CONSTANTS
    N,       \* packets the sender sends, 1..N
    K,       \* ACK ranges the history keeps and an ACK carries
    T,       \* Ack-Eliciting Threshold: an ACK after more than T new
    Lossy,   \* packets the network may drop
    Mutant   \* "none";
             \* "top-k": the ACK reports the history's ranges only (quic-go
             \*   before this lane: a packet dropped at once goes
             \*   unreported);
             \* "top-first": the history's ranges and the dropped new
             \*   packets, but the highest K of them (the new ones not
             \*   first: one low in the history gives way)

ASSUME K >= 1 /\ T + 1 <= K /\ Lossy \subseteq 1..N

VARIABLES
    sent,    \* packets sent so far (1..sent)
    net,     \* packets in flight (delivered in any order)
    rcvd,    \* packets the receiver processed
    hist,    \* the packets its history holds (at most K ranges)
    fresh,   \* packets received since its last ACK
    acked,   \* what the sender knows acknowledged
    nr,      \* ranges in the last ACK
    evicted  \* history: a received packet the history no longer holds

vars == <<sent, net, rcvd, hist, fresh, acked, nr, evicted>>

\* The maximal runs of s, as their sets.
Runs(s) == {<<a, b>> \in s \X s : a <= b /\ (a..b) \subseteq s /\ a - 1 \notin s /\ b + 1 \notin s}
Ranges(s) == {a..b : <<a, b>> \in Runs(s)}
Hi(r) == CHOOSE hi \in r : \A y \in r : y <= hi
Union(rs) == UNION rs

\* The n highest ranges of rs (all of them if fewer).
RECURSIVE Top(_, _)
Top(rs, n) == IF n = 0 \/ rs = {} THEN {}
              ELSE LET h == CHOOSE r \in rs : \A x \in rs : Hi(x) <= Hi(r)
                   IN {h} \cup Top(rs \ {h}, n - 1)

\* The history past K ranges drops its lowest (receivedPacketHistory).
Trim(s) == Union(Top(Ranges(s), K))

\* The ranges an ACK reports, from the history and the fresh packets.
Frame ==
    LET u   == hist \cup fresh
        all == Ranges(u)
        req == {r \in all : r \cap fresh # {}}
    IN CASE Mutant = "top-k"     -> Ranges(hist)
         [] Mutant = "top-first" -> Top(all, K)
         [] OTHER                -> req \cup Top(all \ req, K - Cardinality(req))

TypeOK ==
    /\ sent \in 0..N /\ net \subseteq 1..N /\ rcvd \subseteq 1..N
    /\ hist \subseteq rcvd /\ fresh \subseteq rcvd /\ acked \subseteq 1..N
    /\ nr \in 0..K /\ evicted \in BOOLEAN

Init ==
    /\ sent = 0 /\ net = {} /\ rcvd = {} /\ hist = {} /\ fresh = {}
    /\ acked = {} /\ nr = 0 /\ evicted = FALSE

Send ==
    /\ sent < N
    /\ sent' = sent + 1 /\ net' = net \cup {sent + 1}
    /\ UNCHANGED <<rcvd, hist, fresh, acked, nr, evicted>>

Drop(p) ==
    /\ p \in net /\ p \in Lossy
    /\ net' = net \ {p}
    /\ UNCHANGED <<sent, rcvd, hist, fresh, acked, nr, evicted>>

\* A packet arrives (not one already held: quic-go drops those); after more
\* than T fresh ones the receiver must ACK first.
Receive(p) ==
    /\ p \in net /\ Cardinality(fresh) <= T
    /\ net' = net \ {p}
    /\ rcvd' = rcvd \cup {p}
    /\ hist' = Trim(hist \cup {p})
    /\ fresh' = fresh \cup {p}
    /\ evicted' = (evicted \/ Trim(hist \cup {p}) # rcvd \cup {p})
    /\ UNCHANGED <<sent, acked, nr>>

Ack ==
    /\ fresh # {}
    /\ acked' = acked \cup Union(Frame)
    /\ nr' = Cardinality(Frame)
    /\ fresh' = {}
    /\ UNCHANGED <<sent, net, rcvd, hist, evicted>>

Next == Send \/ Ack \/ \E p \in 1..N : Receive(p) \/ Drop(p)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Every packet received before the last ACK is acknowledged: the sender
\* declares none of them lost (its time threshold waits 9/8 RTT, an ACK
\* comes within the ACK delay).
ReportedOnce == \A p \in rcvd \ fresh : p \in acked

\* Nothing not received is acknowledged.
AckedReceived == acked \subseteq rcvd

\* The history and an ACK hold at most K ranges (the DoS bound and
\* MaxNumAckRanges in an ACK frame).
HistoryBound == Cardinality(Ranges(hist)) <= K
FrameBound == nr <= K

\* Non-vacuity: each must be violated (scripts/spec.sh witness).
WitnessEvicted == ~evicted
WitnessFull == nr < K
WitnessOrphanAcked == ~(\E p \in acked : p \notin hist)
=============================================================================
