------------------------------ MODULE Liveness ------------------------------
(* Conn.keepAlive (conn.go) in discrete time: the bound within which it    *)
(* ends a connection whose peer went silent, and that it never ends one    *)
(* whose peer answers its pings. Its one timer fires at its deadline or up *)
(* to Late ticks after (a loaded machine: the Go timer and the goroutine   *)
(* that reads it). The process may also not run at all for a while         *)
(* (Sleep: a starved process, a stopped VM): its timers and its receive    *)
(* loop wait, the peer's frames wait in the socket. spec/README.md,        *)
(* "Liveness" and "Starvation".                                            *)
EXTENDS Integers

CONSTANTS
    Iv, To,  \* Liveness.Interval, Timeout (To >= Iv)
    Late,    \* how late a running process's timer may fire
    Rtt,     \* a live peer's pong arrives within Rtt of the ping (Rtt < To)
    Gap,     \* a look re-arms the verdict at least Gap later (>= 1)
    Sleeps,  \* how many times the process may stop running
    Horizon, \* the ticks TLC explores
    Mutant   \* "none" or a bug put back (spec-mutants)

VARIABLES
    now,     \* time.Since(c.t0)
    rx,      \* c.rxAt: when the receive loop last read bytes
    pingAt,  \* keepAlive's pingAt (-1: no ping waiting for an answer)
    looked,  \* keepAlive's looked: a fire since the ping saw nothing read
    timer,   \* when the timer is due
    alive,   \* the peer and the path: FALSE once the path died
    dead,    \* keepAlive ended the connection (ErrDeadPeer)
    stuck,   \* the receive loop waits on a write (finding F7, fixed)
    run,     \* the process runs (FALSE: starved, Sleep)
    pend,    \* bytes arrived that the receive loop has not read
    woke,    \* when the process last started running again
    slept    \* Sleeps taken

vars == <<now, rx, pingAt, looked, timer, alive, dead, stuck, run, pend, woke, slept>>

Max(a, b) == IF a > b THEN a ELSE b

Init ==
    /\ now = 0 /\ rx = 0 /\ pingAt = -1 /\ looked = FALSE /\ timer = Iv
    /\ alive = TRUE /\ dead = FALSE /\ stuck = FALSE
    /\ run = TRUE /\ pend = FALSE /\ woke = 0 /\ slept = 0

\* Time passes: not past a running process's due timer by more than Late,
\* not while its receive loop has bytes to read, and not past a live
\* peer's pong (it arrives, read or not).
Tick ==
    /\ ~dead /\ now < Horizon
    /\ run => now + 1 <= timer + Late
    /\ run => ~(pend /\ ~stuck)
    /\ ~(alive /\ ~pend /\ pingAt >= 0 /\ rx < pingAt /\ now + 1 > pingAt + Rtt)
    /\ now' = now + 1
    /\ UNCHANGED <<rx, pingAt, looked, timer, alive, dead, stuck, run, pend, woke, slept>>

\* A frame of a live peer reaches the socket (data, a ping, a pong).
Arrive ==
    /\ alive /\ ~dead /\ ~pend
    /\ pend' = TRUE
    /\ UNCHANGED <<now, rx, pingAt, looked, timer, alive, dead, stuck, run, woke, slept>>

\* The receive loop reads it: Conn.read stores rxAt (the time of the read).
Recv ==
    /\ run /\ pend /\ ~dead /\ ~stuck
    /\ rx' = now /\ pend' = FALSE
    /\ UNCHANGED <<now, pingAt, looked, timer, alive, dead, stuck, run, woke, slept>>

Die ==
    /\ alive /\ ~dead
    /\ alive' = FALSE
    /\ UNCHANGED <<now, rx, pingAt, looked, timer, dead, stuck, run, pend, woke, slept>>

\* The process stops running (starved). Not between a look and the read of
\* what had arrived by then: once keepAlive blocks after a look the runtime
\* polls the network and runs the receive loop (README, "Starvation", A1).
Sleep ==
    /\ run /\ ~dead /\ slept < Sleeps
    /\ ~(looked /\ pend /\ ~stuck)
    /\ run' = FALSE /\ slept' = slept + 1
    /\ UNCHANGED <<now, rx, pingAt, looked, timer, alive, dead, stuck, pend, woke>>

Wake ==
    /\ ~run /\ ~dead
    /\ run' = TRUE /\ woke' = now
    /\ UNCHANGED <<now, rx, pingAt, looked, timer, alive, dead, stuck, pend, slept>>

\* reader-writes (finding F7, the code before lane LINKREVIEW): the receive
\* loop writes a refusal itself and waits for room; with both sides
\* refusing and both directions full, neither reads again, though both are
\* alive. The design is that the receive loop never waits on the network.
Stall ==
    /\ Mutant = "reader-writes" /\ alive /\ ~dead /\ ~stuck
    /\ stuck' = TRUE
    /\ UNCHANGED <<now, rx, pingAt, looked, timer, alive, dead, run, pend, woke, slept>>

\* The timer fires at n, the last read at r: keepAlive's loop, one pass
\* (liveFire). (TraceLive.tla gives n and r from a recorded run.)
\* Nothing read since the ping: dead only once Timeout passed since the
\* ping AND an earlier fire already saw nothing read (a look): the receive
\* loop could run after that look, so nothing is waiting in the socket. A
\* fire that sees nothing read but is not yet the verdict is a look; it
\* re-arms for the verdict at pingAt + To, and never sooner than Gap after
\* itself (a fire late past the verdict's time after a starvation).
FireAt(n, r) ==
    /\ ~dead /\ n >= timer
    /\ IF pingAt >= 0 /\ r < pingAt \* nothing read since the ping
       THEN IF (looked \/ Mutant = "verdict-unlooked") /\ n - pingAt >= To
               /\ Mutant # "never-dead"
            THEN dead' = TRUE /\ UNCHANGED <<pingAt, looked, timer>>
            ELSE /\ looked' = TRUE
                 /\ timer' = IF Mutant = "look-no-gap" THEN pingAt + To
                             ELSE Max(pingAt + To, n + Gap)
                 /\ UNCHANGED <<pingAt, dead>>
       ELSE IF n - r < Iv
       THEN timer' = r + Iv /\ pingAt' = -1 /\ looked' = FALSE /\ UNCHANGED dead
       ELSE \* ping, and look again an interval on
            /\ pingAt' = n /\ looked' = FALSE
            /\ timer' = n + (IF Mutant = "rearm-timeout" THEN To ELSE Iv)
            /\ UNCHANGED dead
    /\ now' = n /\ rx' = r
    /\ UNCHANGED <<alive, stuck, run, pend, woke, slept>>

Fire == run /\ FireAt(now, rx)

\* deadline-from-rx: dead once Iv + To passed since the last frame, however
\* late the ping went out (its pong had less than Timeout to come).
FireMutant ==
    /\ Mutant = "deadline-from-rx"
    /\ run /\ ~dead /\ now >= timer /\ pingAt >= 0 /\ rx < pingAt
    /\ now - rx >= Iv + To
    /\ dead' = TRUE
    /\ UNCHANGED <<now, rx, pingAt, looked, timer, alive, stuck, run, pend, woke, slept>>

Next == Tick \/ Arrive \/ Recv \/ Die \/ Sleep \/ Wake \/ Stall \/ Fire \/ FireMutant

Spec == Init /\ [][Next]_vars

TypeOK == now \in 0..Horizon /\ rx \in 0..Horizon /\ pingAt \in -1..Horizon
          /\ looked \in BOOLEAN /\ run \in BOOLEAN /\ pend \in BOOLEAN

\* A peer that answers within Rtt is never taken for dead, however late the
\* timer fires and however long the process does not run.
NoFalseDead == dead => ~alive

\* Dead, or not yet: a running process ends the connection by Iv + To
\* after the last frame it read or the moment it started running again,
\* plus the lateness of the two timer fires that decide it (the ping's and
\* the verdict's; earlier fires are re-anchored at rx, so their lateness
\* does not add up). Time the process did not run does not count.
DetectBound == run /\ ~alive /\ ~dead => now - Max(rx, woke) <= Iv + To + 2 * Late

\* Never sooner than Iv + To after the last frame.
NeverEarly == dead => now - rx >= Iv + To

\* Witnesses (make spec-mutants: TLC must violate each): a dead peer
\* declared (NoFalseDead's and NeverEarly's case), a live peer's timer
\* fired late (NoFalseDead's), DetectBound reached exactly (it is tight),
\* a live peer's fire due with its ping unread for Timeout (the verdict the
\* look withholds: a starved process; LivenessStarve.cfg).
WitnessDead == ~dead
WitnessLateAlive == ~(alive /\ pingAt >= 0 /\ now > timer)
WitnessTight == ~(run /\ ~alive /\ ~dead /\ now - Max(rx, woke) = Iv + To + 2 * Late)
WitnessStarved == ~(run /\ alive /\ pingAt >= 0 /\ rx < pingAt /\ now - pingAt >= To /\ now >= timer)
=============================================================================
