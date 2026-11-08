-------------------------------- MODULE Pool --------------------------------
(* A model of pool.go: the connections a Pool keeps to one peer, the       *)
(* sessions Open places on them, maintain's Min, Drain, Close, and what    *)
(* the peer and the network do to them (GOAWAY, a dead path). Sessions are *)
(* modelled only by what Link.tla guarantees of them: a connection that    *)
(* ends ends its sessions with an error, an OPEN after the opener read the *)
(* peer's GOAWAY never goes out (ErrGoAway), one past the peer's max is    *)
(* refused (ErrFull), and a side that sent GOAWAY closes the connection    *)
(* once its sessions ended. A dead path is detected within the liveness    *)
(* bound (Liveness.tla): here, eventually. spec/README.md, "The Pool".     *)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Ids,        \* connection slots (a removed connection's slot is reused)
    Opens,      \* the application's concurrent Open calls
    Min, Max,   \* PoolConfig.Min, Max (Max already max(Max, Min, 1))
    Cap,        \* sessions a connection takes (the peer's max_sessions)
    MaxEnv,     \* GOAWAYs, path deaths, failed dials and Drains, in total
    Mutant      \* "none" or a bug put back (spec-mutants)

VARIABLES
    cs,      \* cs[c]: "free", "up", "closed" (Done; not yet removed)
    ga,      \* ga[c]: either side sent GOAWAY (Conn.GoingAway)
    dead,    \* dead[c]: the path died; liveness has yet to notice
    sess,    \* sess[c]: sessions open on c
    dials,   \* Pool.dials: dials reserved or in flight
    mtw,     \* p.changed was closed since maintain took it
    mt,      \* maintain: "check" | "wait" | "dial" | "stopped"
    op,      \* op[o]: an Open: [st, c] st "idle" | "pick" | "place" | "dial" | "wait" | "open"
             \* (| "late": wait-takes-late's decided wait, p.changed not yet taken)
    opw,     \* opw[o]: p.changed was closed since Open o took it (waitChange)
    dr,      \* Drain: [st |-> "idle" | "reserve" | "dial" | "back" | "goaway" | "done", old, left,
             \*   rep: its replacements so far, stay: old ones counted against Max]
    pc,      \* Close: "open" | "cancelled" (p.ctx) | "done" (conns closed)
             \* (| "snapped": snapshot-before-cancel's snapshot taken first)
    env,     \* environment events so far (bounded by MaxEnv)
    bad,     \* a session placed on a connection already going away or closed
    gr       \* Pool.growing: the Open whose pick asked for a background growth
             \* dial (PoolConfig.Grow) in flight, 0 when none (review r2 M5)

vars == <<cs, ga, dead, sess, dials, mtw, opw, mt, op, dr, pc, env, bad, gr>>

Up(c) == cs[c] = "up"
\* counts against Max (stayingLocked): open, its writes not failed, no GOAWAY.
Stays(c) == Up(c) /\ ~ga[c]
\* takes (pool.go): one that stays, below the peer's max sessions.
Takes(c) == Stays(c) /\ sess[c] < Cap
Taking == Cardinality({c \in Ids : Takes(c)})
\* stayingLocked: connections that count against Max, and dials.
Staying == dials + Cardinality({c \in Ids : Stays(c)})
\* p.ctx is cancelled: Open fails with net.ErrClosed, maintain stops.
Closed == pc \in {"cancelled", "done"}
\* p.changed is closed: every goroutine waiting on it wakes.
Changed == mtw' = TRUE /\ opw' = [o \in Opens |-> TRUE]

Init ==
    /\ cs = [c \in Ids |-> "free"]
    /\ ga = [c \in Ids |-> FALSE]
    /\ dead = [c \in Ids |-> FALSE]
    /\ sess = [c \in Ids |-> 0]
    /\ dials = 0
    /\ mtw = FALSE
    /\ mt = "check"
    /\ op = [o \in Opens |-> [st |-> "idle", c |-> 0]]
    /\ opw = [o \in Opens |-> FALSE]
    /\ dr = [st |-> "idle", old |-> {}, left |-> 0, rep |-> {}, stay |-> 0]
    /\ pc = "open"
    /\ env = 0
    /\ bad = FALSE
    /\ gr = 0

FreeId == {c \in Ids : cs[c] = "free"}

(***************************************************************************)
(* maintain                                                                *)
(***************************************************************************)
Need == Taking + dials < Min /\ Staying < Max

\* The loop's check under p.mu: reserve a dial, or wait on p.changed.
MaintainCheck ==
    /\ mt = "check"
    /\ IF Closed THEN mt' = "stopped" /\ UNCHANGED <<dials, mtw>>
       ELSE IF Need /\ FreeId # {}
       THEN /\ IF Mutant = "maintain-no-recheck"
               THEN mt' = "reserve" /\ UNCHANGED dials \* decided, the lock let go
               ELSE mt' = "dial" /\ dials' = dials + 1
            /\ UNCHANGED mtw
       ELSE /\ mt' = "wait"
            /\ mtw' = FALSE
            /\ UNCHANGED dials
    /\ UNCHANGED <<cs, ga, dead, sess, opw, op, dr, pc, env, bad, gr>>

\* maintain-no-recheck: it reserves what it decided before (pool.go takes
\* p.mu again and re-checks Need: an Open may have dialled meanwhile).
MaintainReserve ==
    /\ mt = "reserve"
    /\ mt' = "dial" /\ dials' = dials + 1
    /\ UNCHANGED <<cs, ga, dead, sess, mtw, opw, op, dr, pc, env, bad, gr>>

\* p.changed closed, or p.ctx done.
MaintainWake ==
    /\ mt = "wait" /\ (mtw \/ Closed)
    /\ mt' = "check"
    /\ UNCHANGED <<cs, ga, dead, sess, dials, mtw, opw, op, dr, pc, env, bad, gr>>

\* dial: the connection comes up (or fails) and dials ends its reservation.
\* Once p.ctx is cancelled it closes the new connection under p.mu, so
\* Close's snapshot misses none (dial-no-closed-check drops that).
NewConn(c) == cs' = [cs EXCEPT ![c] = IF Closed /\ Mutant # "dial-no-closed-check" THEN "closed" ELSE "up"]

DialUp(c) ==
    /\ cs[c] = "free"
    /\ NewConn(c)
    /\ dials' = dials - 1
    /\ Changed
    /\ UNCHANGED env

\* A failed dial closes p.changed too: a waiter whose room that dial held
\* (maintain below Min, an Open at Max) looks again (finding H2).
DialFails ==
    /\ env < MaxEnv
    /\ env' = env + 1
    /\ dials' = dials - 1
    /\ IF Mutant = "no-wake-on-dial-fail" THEN UNCHANGED <<mtw, opw>> ELSE Changed
    /\ UNCHANGED cs

MaintainDial ==
    /\ mt = "dial"
    /\ \/ \E c \in FreeId : DialUp(c)
       \/ DialFails
    /\ mt' = "check"
    /\ UNCHANGED <<ga, dead, sess, op, dr, pc, bad, gr>>

(***************************************************************************)
(* Open (one per o): pick, then OpenChain on the pick, or dial and wait    *)
(***************************************************************************)
OpenStart(o) ==
    /\ op[o].st = "idle"
    /\ op' = [op EXCEPT ![o].st = "pick"]
    /\ UNCHANGED <<cs, ga, dead, sess, dials, mtw, opw, mt, dr, pc, env, bad, gr>>

Pick(o) ==
    /\ op[o].st = "pick"
    /\ IF Closed \* net.ErrClosed
       THEN op' = [op EXCEPT ![o] = [st |-> "idle", c |-> 0]] /\ UNCHANGED <<dials, opw, gr>>
       ELSE IF \E c \in Ids : Takes(c)
       THEN /\ \E c \in Ids : Takes(c) /\ op' = [op EXCEPT ![o] = [st |-> "place", c |-> c]]
            /\ UNCHANGED opw
            /\ \/ UNCHANGED <<dials, gr>>
               \/ \* PoolConfig.Grow asks for one more connection (growBG): reserved
                  \* against Max like any dial, one at a time, dialled in the
                  \* background while the Open places on its pick
                  /\ (Staying < Max \/ Mutant = "grow-past-max") /\ gr = 0 /\ FreeId # {}
                  /\ dials' = dials + 1 /\ gr' = o
       ELSE IF Staying < Max /\ FreeId # {}
               \* full-below-max: a full connection fails the Open (one
               \* connection only, tcp-connect before lane CONNPERF)
               /\ ~(Mutant = "full-below-max" /\ Staying > dials)
       THEN /\ dials' = dials + 1
            /\ op' = [op EXCEPT ![o].st = "dial"]
            /\ UNCHANGED <<opw, gr>>
       ELSE IF Staying > dials \* ErrFull: a connection at its most sessions holds the room
       THEN op' = [op EXCEPT ![o] = [st |-> "idle", c |-> 0]] /\ UNCHANGED <<dials, opw, gr>>
       ELSE \* every room held by dials in flight: pick returns the
            \* p.changed it read under p.mu, and Open waits on that one
            \* (finding H1).
            /\ IF Mutant = "wait-takes-late"
               THEN op' = [op EXCEPT ![o].st = "late"] /\ UNCHANGED opw
               ELSE op' = [op EXCEPT ![o].st = "wait"] /\ opw' = [opw EXCEPT ![o] = FALSE]
            /\ UNCHANGED <<dials, gr>>
    /\ UNCHANGED <<cs, ga, dead, sess, mtw, mt, dr, pc, env, bad>>

\* OpenChain: ErrGoAway, ErrFull or net.ErrClosed send the Open back to pick.
Place(o) ==
    /\ op[o].st = "place"
    /\ LET c == op[o].c IN
       IF Up(c) /\ (~ga[c] \/ Mutant = "open-on-goaway") /\ sess[c] < Cap
       THEN /\ sess' = [sess EXCEPT ![c] = @ + 1]
            /\ op' = [op EXCEPT ![o].st = "open"]
            /\ bad' = (bad \/ ga[c])
       ELSE /\ op' = [op EXCEPT ![o].st = "pick"]
            /\ UNCHANGED <<sess, bad>>
    /\ UNCHANGED <<cs, ga, dead, dials, mtw, opw, mt, dr, pc, env, gr>>

\* wait-takes-late (the code before): waitChange takes p.changed after pick
\* let go of p.mu; a dial that ended in between closed the one before.
OpenTakeLate(o) ==
    /\ op[o].st = "late"
    /\ op' = [op EXCEPT ![o].st = "wait"]
    /\ opw' = [opw EXCEPT ![o] = FALSE]
    /\ UNCHANGED <<cs, ga, dead, sess, dials, mtw, mt, dr, pc, env, bad, gr>>

\* A failed dial: Open sleeps Backoff and picks again, or returns the dial's
\* error (Backoff nil, or its ctx ended).
OpenDial(o) ==
    /\ op[o].st = "dial"
    /\ \/ \E c \in FreeId :
            /\ DialUp(c)
            /\ op' = [op EXCEPT ![o] = [st |-> "place", c |-> c]]
       \/ DialFails /\ \E st \in {"pick", "idle"} : op' = [op EXCEPT ![o].st = st]
    /\ UNCHANGED <<ga, dead, sess, mt, dr, pc, bad, gr>>

\* grow: the background growth dial ends, up or failed (a failure leaves the
\* next Open to ask again). With GrowWait the Open that asked takes the new
\* connection, if it has yet to place its session.
GrowDial ==
    /\ gr # 0
    /\ \/ \E c \in FreeId :
            /\ DialUp(c)
            /\ \/ UNCHANGED op
               \/ op[gr].st = "place" /\ op' = [op EXCEPT ![gr].c = c]
       \/ DialFails /\ UNCHANGED op
    /\ gr' = 0
    /\ UNCHANGED <<ga, dead, sess, mt, dr, pc, bad>>

\* p.changed closed, or p.ctx done.
OpenWake(o) ==
    /\ op[o].st = "wait" /\ (opw[o] \/ Closed)
    /\ op' = [op EXCEPT ![o].st = "pick"]
    /\ UNCHANGED <<cs, ga, dead, sess, dials, mtw, opw, mt, dr, pc, env, bad, gr>>

\* The application ends its session; it may also keep it forever.
SessionClose(o) ==
    /\ op[o].st = "open"
    /\ sess' = [sess EXCEPT ![op[o].c] = @ - 1]
    /\ op' = [op EXCEPT ![o] = [st |-> "idle", c |-> 0]]
    /\ UNCHANGED <<cs, ga, dead, dials, mtw, opw, mt, dr, pc, env, bad, gr>>

(***************************************************************************)
(* Drain: dial replacements (past Max), then GOAWAY the old ones          *)
(***************************************************************************)
\* The old connections: all of them. Replacements: max(Min, 1), at most one
\* per old connection that counts against Max (one at its most sessions
\* included: finding M1); one already going away counts against nothing,
\* and replacing it would dial past Max.
DrainOld == {c \in Ids : cs[c] # "free"}
DrainStay == Cardinality({c \in DrainOld : IF Mutant = "drain-counts-takes" THEN Takes(c) ELSE Stays(c)})
DrainN == LET k == IF Min > 1 THEN Min ELSE 1 IN
          IF Mutant = "drain-past-max" \/ k < DrainStay THEN k ELSE DrainStay

DrainStart ==
    /\ dr.st = "idle" /\ env < MaxEnv
    /\ DrainOld # {}
    /\ env' = env + 1
    \* its first replacement reserved under the snapshot's p.mu (finding C6:
    \* drain-reserve-late, the code before, reserves it after)
    /\ LET late == Mutant = "drain-reserve-late" /\ DrainN > 0 IN
       /\ dials' = IF DrainN > 0 /\ ~late THEN dials + 1 ELSE dials
       /\ dr' = [st |-> IF late THEN "reserve" ELSE IF DrainN > 0 THEN "dial" ELSE "goaway",
                 old |-> DrainOld, left |-> DrainN - 1, rep |-> {}, stay |-> Cardinality({c \in DrainOld : Stays(c)})]
    /\ UNCHANGED <<cs, ga, dead, sess, mtw, opw, mt, op, pc, bad, gr>>

\* drain-reserve-late: the reservation, under p.mu again; a connection that
\* ended in between was replaced by maintain too.
DrainReserve ==
    /\ dr.st = "reserve"
    /\ dials' = dials + 1
    /\ dr' = [dr EXCEPT !.st = "dial"]
    /\ UNCHANGED <<cs, ga, dead, sess, mtw, opw, mt, op, pc, env, bad, gr>>

\* A failed replacement: the old ones stay, and those already dialled go
\* away, so the Pool is back within Max (finding M2; drain-fail-keeps is
\* the code before).
DrainDial ==
    /\ dr.st = "dial"
    /\ \/ \E c \in FreeId :
            /\ NewConn(c)
            /\ Changed
            /\ IF dr.left > 0 \* the next replacement reserved as this one ends (dial's next)
               THEN dr' = [dr EXCEPT !.left = @ - 1, !.rep = @ \cup {c}] /\ UNCHANGED dials
               ELSE dr' = [dr EXCEPT !.st = "goaway", !.rep = @ \cup {c}] /\ dials' = dials - 1
            /\ UNCHANGED env
       \/ DialFails /\ dr' = [dr EXCEPT !.st = "back"]
    /\ UNCHANGED <<ga, dead, sess, mt, op, pc, bad, gr>>

\* goAway(reps): after a failed replacement, GOAWAY on those already
\* dialled, a second change after the dial's own.
DrainBack ==
    /\ dr.st = "back"
    /\ dr' = [dr EXCEPT !.st = "done"]
    /\ IF Mutant = "drain-fail-keeps" THEN UNCHANGED ga
       ELSE ga' = [c \in Ids |-> ga[c] \/ (c \in dr.rep /\ Up(c))]
    /\ Changed
    /\ UNCHANGED <<cs, dead, sess, dials, mt, op, pc, env, bad, gr>>

DrainGoAway ==
    /\ dr.st = "goaway"
    /\ ga' = [c \in Ids |-> ga[c] \/ (c \in dr.old /\ Up(c))]
    /\ dr' = [dr EXCEPT !.st = "done"]
    /\ IF Mutant = "no-wake-on-goaway" THEN UNCHANGED <<mtw, opw>> ELSE Changed
    /\ UNCHANGED <<cs, dead, sess, dials, mt, op, pc, env, bad, gr>>

(***************************************************************************)
(* Close: cancel p.ctx, then close every connection of a snapshot          *)
(***************************************************************************)
\* closeIdle on each: the connection closes, its sessions end.
CloseSnap ==
    /\ cs' = [c \in Ids |-> IF Up(c) THEN "closed" ELSE cs[c]]
    /\ sess' = [c \in Ids |-> IF Up(c) THEN 0 ELSE sess[c]]
    /\ op' = [o \in Opens |-> IF op[o].st = "open" /\ Up(op[o].c) THEN [st |-> "idle", c |-> 0] ELSE op[o]]

\* The application calls Close (once). snapshot-before-cancel takes the
\* snapshot first: a dial ending in between adds a connection that no one
\* closes.
CloseStart ==
    /\ pc = "open"
    /\ pc' = IF Mutant = "snapshot-before-cancel" THEN "snapped" ELSE "cancelled"
    /\ IF Mutant = "snapshot-before-cancel" THEN CloseSnap ELSE UNCHANGED <<cs, sess, op>>
    /\ UNCHANGED <<ga, dead, dials, mtw, opw, mt, dr, env, bad, gr>>

CloseEnd ==
    /\ pc \in {"cancelled", "snapped"}
    /\ pc' = "done"
    /\ IF pc = "cancelled" THEN CloseSnap ELSE UNCHANGED <<cs, sess, op>>
    /\ UNCHANGED <<ga, dead, dials, mtw, opw, mt, dr, env, bad, gr>>

(***************************************************************************)
(* The peer and the network                                                *)
(***************************************************************************)
\* The peer sends GOAWAY: the connection takes no more sessions. The Pool
\* hears of it (PeerGoAway closes p.changed): maintain replaces it at once,
\* not when it closes, which may be never (a session that runs on).
PeerGoAway(c) ==
    /\ Up(c) /\ ~ga[c] /\ env < MaxEnv
    /\ env' = env + 1
    /\ ga' = [ga EXCEPT ![c] = TRUE]
    /\ IF Mutant = "no-wake-on-goaway" THEN UNCHANGED <<mtw, opw>> ELSE Changed
    /\ UNCHANGED <<cs, dead, sess, dials, mt, op, dr, pc, bad, gr>>

PathDies(c) ==
    /\ Up(c) /\ ~dead[c] /\ env < MaxEnv
    /\ env' = env + 1
    /\ dead' = [dead EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<cs, ga, sess, dials, mtw, opw, mt, op, dr, pc, bad, gr>>

\* The connection closes: liveness noticed the dead path, or the side that
\* sent GOAWAY closes it once its sessions ended. Its sessions end with it.
CloseConn(c) ==
    /\ Up(c)
    /\ dead[c] \/ (ga[c] /\ sess[c] = 0)
    /\ cs' = [cs EXCEPT ![c] = "closed"]
    /\ sess' = [sess EXCEPT ![c] = 0]
    /\ op' = [o \in Opens |-> IF op[o].st = "open" /\ op[o].c = c THEN [st |-> "idle", c |-> 0] ELSE op[o]]
    /\ UNCHANGED <<ga, dead, dials, mtw, opw, mt, dr, pc, env, bad, gr>>

\* Serve returned: the Pool drops it and closes p.changed.
Remove(c) ==
    /\ cs[c] = "closed"
    /\ cs' = [cs EXCEPT ![c] = "free"]
    /\ ga' = [ga EXCEPT ![c] = FALSE]
    /\ dead' = [dead EXCEPT ![c] = FALSE]
    /\ Changed
    /\ UNCHANGED <<sess, dials, mt, op, dr, pc, env, bad, gr>>

Pool ==
    \/ MaintainCheck \/ MaintainReserve \/ MaintainWake \/ MaintainDial
    \/ \E o \in Opens : Pick(o) \/ Place(o) \/ OpenDial(o) \/ OpenWake(o) \/ OpenTakeLate(o)
    \/ DrainReserve \/ DrainDial \/ DrainBack \/ DrainGoAway \/ CloseEnd \/ GrowDial
    \/ \E c \in Ids : CloseConn(c) \/ Remove(c)

App ==
    \/ \E o \in Opens : OpenStart(o) \/ SessionClose(o)
    \/ DrainStart \/ CloseStart
    \/ \E c \in Ids : PeerGoAway(c) \/ PathDies(c)

Next == Pool \/ App

\* Every step of the Pool's goroutines and of the connections is fair; the
\* application's and the environment's are not (a session may stay open
\* forever). A dial that can succeed eventually does (failures are bounded).
Fairness ==
    /\ WF_vars(MaintainCheck) /\ WF_vars(MaintainReserve) /\ WF_vars(MaintainWake) /\ WF_vars(MaintainDial)
    /\ WF_vars(DrainReserve) /\ WF_vars(DrainDial) /\ WF_vars(DrainBack) /\ WF_vars(DrainGoAway) /\ WF_vars(CloseEnd) /\ WF_vars(GrowDial)
    /\ \A o \in Opens : WF_vars(Pick(o)) /\ WF_vars(Place(o)) /\ WF_vars(OpenDial(o)) /\ WF_vars(OpenWake(o)) /\ WF_vars(OpenTakeLate(o))
    /\ \A c \in Ids : WF_vars(CloseConn(c)) /\ WF_vars(Remove(c))

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
    /\ cs \in [Ids -> {"free", "up", "closed"}]
    /\ dials \in Nat /\ env \in 0..MaxEnv
    /\ gr \in Opens \cup {0}

\* No session goes on a connection that is going away or closed.
NoSessionOnLeaving == ~bad

\* Concurrent Opens and maintain never dial past Max; only Drain's
\* replacements do, until it sent its GOAWAYs (or, failing, sent them on
\* the replacements).
MaxHolds == dr.st \notin {"reserve", "dial", "back", "goaway"} => Staying <= Max

\* Make before break: a Drain whose old connections counted against Max
\* has a replacement before it sends them GOAWAY.
MakeBeforeBreak == dr.st = "goaway" => dr.stay = 0 \/ dr.rep # {}

\* No connection is left open once Close returned.
NoLeak == pc = "done" => \A c \in Ids : ~Up(c)

\* Min: once the environment stops, the Pool has Min connections that take
\* sessions, whatever sessions run on the ones going away, until Close.
\* Sessions held open forever may fill them (Cap): those count as taking.
MinHeld == <>[](Cardinality({c \in Ids : Stays(c)}) >= Min \/ Closed)

\* Every Open ends: with a session (the peer keeps accepting dials) or an
\* error (its own failed dial, ErrFull, net.ErrClosed).
OpensOpen == \A o \in Opens : op[o].st \in {"pick", "dial", "wait", "late", "place"} ~> op[o].st \in {"open", "idle"}

\* With two Opens (PoolWide.cfg) OpensOpen does not hold: an Open that
\* closes its session and opens again can take the last room each time
\* the other picked it, and the other re-picks forever (nothing queues
\* Opens; review r2 L3, L5). What holds is progress for the Pool: while
\* some Open is pending, some Open ends.
OpensProgress == (\E o \in Opens : op[o].st \in {"pick", "dial", "wait", "late", "place"}) ~> (\E o \in Opens : op[o].st \in {"open", "idle"})

\* An Open fails ErrFull only with the room full: every connection that
\* counts against Max carries its most sessions and no slot is left to dial
\* one more (an Open's pick -> idle step without Close is its ErrFull). A
\* client whose peer caps sessions per connection (tcp-connect: the relay's
\* max_sessions, 1,024) opens past the cap by Max > 1 (lane CONNPERF).
ErrFullStep(o) == op[o].st = "pick" /\ op'[o].st = "idle" /\ ~Closed
FullOnlyAtMax == [][\A o \in Opens : ErrFullStep(o) => ~(Staying < Max /\ FreeId # {})]_vars

\* After Close, every connection ends and is dropped.
CloseEnds == Closed ~> \A c \in Ids : cs[c] = "free"

\* Witnesses (make spec-mutants): each must be violated, so the invariant
\* or path it names is reached, not vacuous.
WitnessOpen == \A o \in Opens : op[o].st # "open"           \* a session is placed
WitnessWait == \A o \in Opens : op[o].st # "wait"           \* an Open waits on dials (H1, H2)
WitnessReplaced == ~(dr.st = "goaway" /\ dr.stay > 0)       \* MakeBeforeBreak's case
WitnessPastMax == ~(dr.st = "goaway" /\ Staying > Max)      \* MaxHolds' exception
WitnessClosed == ~(pc = "done" /\ \E c \in Ids : cs[c] = "closed") \* NoLeak with a connection Close closed
\* NoSessionOnLeaving's case: a session placed while a connection going
\* away is still up; MaxHolds': an Open waits with the room full.
WitnessBesideLeaving == ~\E o \in Opens : op[o].st = "open" /\ \E c \in Ids : Up(c) /\ ga[c]
WitnessAtMax == ~(Staying = Max /\ \E o \in Opens : op[o].st = "wait")
\* FullOnlyAtMax's case: an Open fails ErrFull (an action: checked as a
\* PROPERTY, violated).
WitnessActFull == [][\A o \in Opens : ~ErrFullStep(o)]_vars
=============================================================================
