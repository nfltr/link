------------------------------ MODULE Liveness ------------------------------
(* Conn.keepAlive (conn.go) in discrete time: the bound within which it    *)
(* ends a connection whose peer went silent, and that it never ends one    *)
(* whose peer answers its pings. Its one timer fires at its deadline or up *)
(* to Late ticks after (a starved process, a loaded machine: the Go timer  *)
(* and the goroutine that reads it). spec/README.md, "Liveness".           *)
EXTENDS Integers

CONSTANTS
    Iv, To,  \* Liveness.Interval, Timeout (To >= Iv)
    Late,    \* how late the timer may fire
    Rtt,     \* a live peer's pong arrives within Rtt of the ping (Rtt < To)
    Horizon, \* the ticks TLC explores
    Mutant   \* "none" or a bug put back (spec-mutants)

VARIABLES
    now,     \* time.Since(c.t0)
    rx,      \* c.rxAt: the last frame received
    pingAt,  \* keepAlive's pingAt (-1: no ping waiting for an answer)
    timer,   \* when the timer is due
    alive,   \* the peer and the path: FALSE once the path died
    dead     \* keepAlive ended the connection (ErrDeadPeer)

vars == <<now, rx, pingAt, timer, alive, dead>>

Init ==
    /\ now = 0 /\ rx = 0 /\ pingAt = -1 /\ timer = Iv
    /\ alive = TRUE /\ dead = FALSE

\* Time passes, but not past a due timer by more than Late, and not past a
\* live peer's pong.
Tick ==
    /\ ~dead /\ now < Horizon
    /\ now + 1 <= timer + Late
    /\ ~(alive /\ pingAt >= 0 /\ rx < pingAt /\ now + 1 > pingAt + Rtt)
    /\ now' = now + 1
    /\ UNCHANGED <<rx, pingAt, timer, alive, dead>>

\* A frame arrives (data, a ping, a pong): Conn.next stores rxAt.
Recv ==
    /\ alive /\ ~dead /\ rx < now
    /\ rx' = now
    /\ UNCHANGED <<now, pingAt, timer, alive, dead>>

Die ==
    /\ alive /\ ~dead
    /\ alive' = FALSE
    /\ UNCHANGED <<now, rx, pingAt, timer, dead>>

\* The timer fires at n, the last frame at r: keepAlive's loop, one pass.
\* (TraceLive.tla gives n and r from a recorded run.)
FireAt(n, r) ==
    /\ ~dead /\ n >= timer
    /\ IF pingAt >= 0 /\ r < pingAt \* nothing since the ping
       THEN IF n - pingAt >= To
            THEN dead' = TRUE /\ UNCHANGED <<pingAt, timer>>
            ELSE timer' = pingAt + To /\ UNCHANGED <<pingAt, dead>>
       ELSE IF n - r < Iv
       THEN timer' = r + Iv /\ pingAt' = -1 /\ UNCHANGED dead
       ELSE \* ping, and look again an interval on
            /\ pingAt' = n
            /\ timer' = n + (IF Mutant = "rearm-timeout" THEN To ELSE Iv)
            /\ UNCHANGED dead
    /\ now' = n /\ rx' = r
    /\ UNCHANGED alive

Fire == FireAt(now, rx)

\* deadline-from-rx: dead once Iv + To passed since the last frame, however
\* late the ping went out (its pong had less than Timeout to come).
FireMutant ==
    /\ Mutant = "deadline-from-rx"
    /\ ~dead /\ now >= timer /\ pingAt >= 0 /\ rx < pingAt
    /\ now - rx >= Iv + To
    /\ dead' = TRUE
    /\ UNCHANGED <<now, rx, pingAt, timer, alive>>

Next == Tick \/ Recv \/ Die \/ Fire \/ FireMutant

Spec == Init /\ [][Next]_vars

TypeOK == now \in 0..Horizon /\ rx \in 0..Horizon /\ pingAt \in -1..Horizon

\* A peer that answers within Rtt (< Timeout) is never taken for dead,
\* however late the timer fires.
NoFalseDead == dead => ~alive

\* Dead, or not yet: the connection ends by Iv + To after the last frame,
\* plus the lateness of the two timer fires that decide it (the ping's and
\* the verdict's; earlier fires are re-anchored at rx, so their lateness
\* does not add up).
DetectBound == ~alive /\ ~dead => now - rx <= Iv + To + 2 * Late

\* Never sooner than Iv + To after the last frame.
NeverEarly == dead => now - rx >= Iv + To
=============================================================================
