------------------------------ MODULE QuicWatch -----------------------------
(* quiclink's liveness (peerlink/quiclink/quiclink.go, conn.watch) in      *)
(* discrete time: a ticker every G = Timeout/4 samples quic-go's count of  *)
(* packets received; a tick that finds no new packet for Quiet closes the  *)
(* connection (CONNECTION_CLOSE, CodeDeadPeer); a tick more than 1.25 G    *)
(* after the one before came to a process that did not run, and moves     *)
(* seen on by that gap (not silence seen). quic-go's own idle timer is the *)
(* backstop: it closes silently once nothing was received for B, judged by *)
(* the clock alone, and may run before the packets waiting in the socket   *)
(* are read. The peer's packets come from QUIC's keep-alive: quic-go sends *)
(* a PING K = max(Interval, 1.5 PTO) after the last packet received, so a  *)
(* live peer's next packet arrives within K + Rtt. spec/README.md,         *)
(* "quiclink's watch (QuicWatch.tla)".                                     *)
EXTENDS Integers

CONSTANTS
    Iv, To,   \* Liveness.Interval, Timeout (To = 4 * G)
    KA,       \* quic-go's keep-alive floor: 1.5 x the path's probe timeout
    Rtt,      \* a live peer's answer arrives within Rtt (Rtt + Late < To/2)
    Late,     \* how late a running process's tick or timer may come
    B,        \* QUIC's idle timeout: idleBackstop x (Interval + Timeout)
    Sleeps,   \* how many times the process may stop running
    MinSleep, \* a stop lasts at least this (shorter ones: Late, or QW2)
    MaxSleep, \* how long one stop may last
    Horizon,  \* the ticks TLC explores
    Mutant    \* "none" or a bug put back (spec-mutants)

G == To \div 4
K == IF KA > Iv THEN KA ELSE Iv                \* quic-go's keep-alive period
Quiet == IF Mutant = "quiet-ignores-pto" THEN Iv + To \div 2
         ELSE K + To \div 2                    \* watch's silence allowed
\* the design's conditions: a live peer answers within To/2, ticks on a
\* running process are late by under G/4, the backstop is past the watch's
\* bound (Mutant "backstop-low" breaks the last one).
ASSUME Rtt + Late < To \div 2 /\ Late <= G \div 4 /\ To % 4 = 0
ASSUME B > K + To + 2 * Late
\* the backstop judges by the clock: a stop longer than B - (K + Rtt + Late)
\* lets it close a live peer silently (the ceiling: 18 s at LivenessFast,
\* 60 s at LivenessDefault; README "quiclink's watch").
ASSUME MaxSleep + K + Rtt + Late < B

Idle == IF Mutant = "backstop-low" THEN To ELSE B

VARIABLES
    now,    \* the clock
    rx,     \* when quic-go last processed a packet (lastPacketReceivedTime)
    fresh,  \* PacketsReceived moved since the last tick (n != last)
    pend,   \* a packet waits in the socket, not yet processed
    ka,     \* when quic-go sent its keep-alive PING (-1: not since rx)
    seen,   \* watch's seen
    prev,   \* watch's prev: the time of the last tick
    due,    \* when the ticker's next tick is due
    alive,  \* the peer and the path
    closed, \* "none", "watch" (CONNECTION_CLOSE) or "idle" (silent)
    run,    \* the process runs
    since,  \* when the process last stopped or started running
    slept   \* Sleeps taken

vars == <<now, rx, fresh, pend, ka, seen, prev, due, alive, closed, run, since, slept>>

Max(a, b) == IF a > b THEN a ELSE b

Init ==
    /\ now = 0 /\ rx = 0 /\ fresh = FALSE /\ pend = FALSE /\ ka = -1
    /\ seen = 0 /\ prev = 0 /\ due = G
    /\ alive = TRUE /\ closed = "none" /\ run = TRUE /\ since = 0 /\ slept = 0

\* Time passes: not past a running process's due tick, keep-alive or idle
\* timer by more than Late, not while it has a packet to process, not past
\* a live peer's answer to the keep-alive (Rtt after it went out), not
\* past a stop's MaxSleep. (The peer's own PINGs may come sooner: Arrive.)
Tick ==
    /\ closed = "none" /\ now < Horizon
    /\ run => /\ now + 1 <= due + Late /\ now + 1 <= rx + Idle + Late /\ ~pend
              /\ (ka < 0 => now + 1 <= rx + K + Late)
    /\ ~run => now + 1 <= since + MaxSleep
    /\ ~(alive /\ ~pend /\ ka >= 0 /\ now + 1 > ka + Rtt)
    /\ now' = now + 1
    /\ UNCHANGED <<rx, fresh, pend, ka, seen, prev, due, alive, closed, run, since, slept>>

\* A live peer's packet reaches the socket (a PING, an ACK, data).
Arrive ==
    /\ alive /\ closed = "none" /\ ~pend
    /\ pend' = TRUE
    /\ UNCHANGED <<now, rx, fresh, ka, seen, prev, due, alive, closed, run, since, slept>>

\* quic-go's run loop processes it: lastPacketReceivedTime, PacketsReceived.
Recv ==
    /\ run /\ pend /\ closed = "none"
    /\ rx' = now /\ fresh' = TRUE /\ pend' = FALSE /\ ka' = -1
    /\ UNCHANGED <<now, seen, prev, due, alive, closed, run, since, slept>>

\* quic-go's keep-alive: a PING K after the last packet it processed
\* (nextKeepAliveTime), once.
KeepAlive ==
    /\ run /\ closed = "none" /\ ka < 0 /\ now >= rx + K
    /\ ka' = now
    /\ UNCHANGED <<now, rx, fresh, pend, seen, prev, due, alive, closed, run, since, slept>>

Die ==
    /\ alive /\ closed = "none"
    /\ alive' = FALSE
    /\ UNCHANGED <<now, rx, fresh, pend, ka, seen, prev, due, closed, run, since, slept>>

Sleep ==
    /\ run /\ closed = "none" /\ slept < Sleeps
    /\ run' = FALSE /\ since' = now /\ slept' = slept + 1
    /\ UNCHANGED <<now, rx, fresh, pend, ka, seen, prev, due, alive, closed>>

Wake ==
    /\ ~run /\ closed = "none" /\ now >= since + MinSleep
    /\ run' = TRUE /\ since' = now
    /\ UNCHANGED <<now, rx, fresh, pend, ka, seen, prev, due, alive, closed, slept>>

\* One tick of watch at n (watchTick): the ticker's next tick is the first
\* multiple of G after n (a Go Ticker drops the ticks it could not deliver).
WatchAt(n) ==
    /\ run /\ closed = "none" /\ n >= due
    /\ prev' = n /\ due' = (n \div G + 1) * G
    /\ IF fresh
       THEN seen' = n /\ fresh' = FALSE /\ UNCHANGED closed
       ELSE IF n - prev > G + G \div 4 /\ Mutant # "count-gap"
       THEN seen' = seen + (n - prev) /\ UNCHANGED <<fresh, closed>>
       ELSE IF n - seen >= Quiet /\ Mutant # "never-close"
       THEN closed' = "watch" /\ UNCHANGED <<seen, fresh>>
       ELSE UNCHANGED <<seen, fresh, closed>>
    /\ UNCHANGED <<now, rx, pend, ka, alive, run, since, slept>>

Watch == WatchAt(now)

\* quic-go's idle timer: by the clock since the last packet it processed,
\* possibly before it processes the ones waiting (its timer case first).
IdleClose ==
    /\ run /\ closed = "none" /\ now >= rx + Idle
    /\ closed' = "idle"
    /\ UNCHANGED <<now, rx, fresh, pend, ka, seen, prev, due, alive, run, since, slept>>

Next == Tick \/ Arrive \/ Recv \/ KeepAlive \/ Die \/ Sleep \/ Wake \/ Watch \/ IdleClose

Spec == Init /\ [][Next]_vars

TypeOK == /\ now \in 0..Horizon /\ rx \in 0..Horizon /\ ka \in -1..Horizon
          /\ closed \in {"none", "watch", "idle"} /\ fresh \in BOOLEAN
          /\ pend \in BOOLEAN /\ run \in BOOLEAN

\* A live peer is never taken for dead, by the watch or the backstop,
\* whatever its keep-alive period and however long (up to MaxSleep) the
\* process does not run.
NoFalseDead == closed # "none" => ~alive

\* A running process ends a silent connection within Quiet + To/2 (that
\* is Interval + Timeout when K = Interval) of the later of the last packet
\* it processed and the moment it ran again, plus one lateness (the seen tick's lateness is inside To/2: WitnessTight).
DetectBound == run /\ ~alive /\ closed = "none" => now - Max(rx, since) <= Quiet + To \div 2 + Late

\* The watch closes (with CONNECTION_CLOSE, the peer told) before QUIC's
\* silent idle timeout, on a process that keeps running.
WatchFirst == closed # "idle"

\* Witnesses (make spec-mutants: TLC must violate each).
WitnessWatch == closed # "watch"
\* A live peer's tick due with nothing processed for Quiet: the verdict the
\* discount withholds (reachable only with Sleep).
WitnessStarvedLive == ~(run /\ alive /\ pend /\ now - rx >= Quiet /\ now >= due)
\* the backstop's silent close is reachable (a dead peer, the stop discounted)
WitnessIdle == closed # "idle"
WitnessTight == ~(run /\ ~alive /\ closed = "none" /\ now - Max(rx, since) = Quiet + To \div 2 + Late)
=============================================================================
