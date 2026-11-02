-------------------------------- MODULE Pool --------------------------------
(* A model of pool.go: the connections a Pool keeps to one peer, the       *)
(* sessions Open places on them, maintain's Min, Drain, and what the peer  *)
(* and the network do to them (GOAWAY, a dead path). Sessions are modelled *)
(* only by what Link.tla guarantees of them: a connection that ends ends   *)
(* its sessions with an error, an OPEN after the opener read the peer's    *)
(* GOAWAY never goes out (ErrGoAway), and a side that sent GOAWAY closes   *)
(* the connection once its sessions ended. A dead path is detected within  *)
(* the liveness bound (Liveness.tla): here, eventually. spec/README.md,    *)
(* "The Pool".                                                             *)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Ids,        \* connection slots (a removed connection's slot is reused)
    Opens,      \* the application's concurrent Open calls
    Min, Max,   \* PoolConfig.Min, Max (Max already max(Max, Min, 1))
    MaxEnv,     \* GOAWAYs, path deaths, failed dials and Drains, in total
    Mutant      \* "none" or a bug put back (spec-mutants)

VARIABLES
    cs,      \* cs[c]: "free", "up", "closed" (Done; not yet removed)
    ga,      \* ga[c]: either side sent GOAWAY (Conn.GoingAway)
    dead,    \* dead[c]: the path died; liveness has yet to notice
    sess,    \* sess[c]: sessions open on c
    dials,   \* Pool.dials: dials reserved or in flight
    mtw,     \* p.changed was closed since maintain took it
    mt,      \* maintain: "check" | "wait" | "dial"
    op,      \* op[o]: an Open: [st, c] st "idle" | "pick" | "place" | "dial" | "wait" | "open"
    opw,     \* opw[o]: p.changed was closed since Open o took it (waitChange)
    dr,      \* Drain: [st |-> "idle" | "dial" | "goaway" | "done", old, left]
    env,     \* environment events so far (bounded by MaxEnv)
    bad      \* a session placed on a connection already going away or closed

vars == <<cs, ga, dead, sess, dials, mtw, opw, mt, op, dr, env, bad>>

Up(c) == cs[c] = "up"
\* takes (pool.go): open, its writes not failed, no GOAWAY. Sessions per
\* connection are not bounded here (Opens < MaxSessions).
Takes(c) == Up(c) /\ ~ga[c]
Taking == Cardinality({c \in Ids : Takes(c)})
\* stayingLocked: connections that count against Max, and dials.
Staying == dials + Taking
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
    /\ dr = [st |-> "idle", old |-> {}, left |-> 0]
    /\ env = 0
    /\ bad = FALSE

FreeId == {c \in Ids : cs[c] = "free"}

(***************************************************************************)
(* maintain                                                                *)
(***************************************************************************)
Need == Taking + dials < Min /\ Staying < Max

\* The loop's check under p.mu: reserve a dial, or wait on p.changed.
MaintainCheck ==
    /\ mt = "check"
    /\ IF Need /\ FreeId # {}
       THEN /\ IF Mutant = "maintain-no-recheck"
               THEN mt' = "reserve" /\ UNCHANGED dials \* decided, the lock let go
               ELSE mt' = "dial" /\ dials' = dials + 1
            /\ UNCHANGED mtw
       ELSE /\ mt' = "wait"
            /\ mtw' = FALSE
            /\ UNCHANGED dials
    /\ UNCHANGED <<cs, ga, dead, sess, opw, op, dr, env, bad>>

\* maintain-no-recheck: it reserves what it decided before (pool.go takes
\* p.mu again and re-checks Need: an Open may have dialled meanwhile).
MaintainReserve ==
    /\ mt = "reserve"
    /\ mt' = "dial" /\ dials' = dials + 1
    /\ UNCHANGED <<cs, ga, dead, sess, mtw, opw, op, dr, env, bad>>

MaintainWake ==
    /\ mt = "wait" /\ mtw
    /\ mt' = "check"
    /\ UNCHANGED <<cs, ga, dead, sess, dials, mtw, opw, op, dr, env, bad>>

\* dial: the connection comes up (or fails) and dials ends its reservation.
DialUp(c) ==
    /\ cs[c] = "free"
    /\ cs' = [cs EXCEPT ![c] = "up"]
    /\ dials' = dials - 1
    /\ Changed
    /\ UNCHANGED env

DialFails ==
    /\ env < MaxEnv
    /\ env' = env + 1
    /\ dials' = dials - 1
    /\ UNCHANGED <<cs, mtw, opw>>

MaintainDial ==
    /\ mt = "dial"
    /\ \/ \E c \in FreeId : DialUp(c)
       \/ DialFails
    /\ mt' = "check"
    /\ UNCHANGED <<ga, dead, sess, op, dr, bad>>

(***************************************************************************)
(* Open (one per o): pick, then OpenChain on the pick, or dial and wait    *)
(***************************************************************************)
OpenStart(o) ==
    /\ op[o].st = "idle"
    /\ op' = [op EXCEPT ![o].st = "pick"]
    /\ UNCHANGED <<cs, ga, dead, sess, dials, mtw, opw, mt, dr, env, bad>>

Pick(o) ==
    /\ op[o].st = "pick"
    /\ IF \E c \in Ids : Takes(c)
       THEN /\ \E c \in Ids : Takes(c) /\ op' = [op EXCEPT ![o] = [st |-> "place", c |-> c]]
            /\ UNCHANGED <<dials, opw>>
       ELSE IF Staying < Max /\ FreeId # {}
       THEN /\ dials' = dials + 1
            /\ op' = [op EXCEPT ![o].st = "dial"]
            /\ UNCHANGED opw
       ELSE \* every room held by dials in flight (ErrFull needs a full one)
            /\ op' = [op EXCEPT ![o].st = "wait"]
            /\ opw' = [opw EXCEPT ![o] = FALSE]
            /\ UNCHANGED dials
    /\ UNCHANGED <<cs, ga, dead, sess, mtw, mt, dr, env, bad>>

\* OpenChain: ErrGoAway or net.ErrClosed send the Open back to pick.
Place(o) ==
    /\ op[o].st = "place"
    /\ LET c == op[o].c IN
       IF Up(c) /\ (~ga[c] \/ Mutant = "open-on-goaway")
       THEN /\ sess' = [sess EXCEPT ![c] = @ + 1]
            /\ op' = [op EXCEPT ![o].st = "open"]
            /\ bad' = (bad \/ ga[c])
       ELSE /\ op' = [op EXCEPT ![o].st = "pick"]
            /\ UNCHANGED <<sess, bad>>
    /\ UNCHANGED <<cs, ga, dead, dials, mtw, opw, mt, dr, env>>

OpenDial(o) ==
    /\ op[o].st = "dial"
    /\ \/ \E c \in FreeId :
            /\ DialUp(c)
            /\ op' = [op EXCEPT ![o] = [st |-> "place", c |-> c]]
       \/ DialFails /\ op' = [op EXCEPT ![o].st = "pick"] \* Backoff, again
    /\ UNCHANGED <<ga, dead, sess, mt, dr, bad>>

OpenWake(o) ==
    /\ op[o].st = "wait" /\ opw[o]
    /\ op' = [op EXCEPT ![o].st = "pick"]
    /\ UNCHANGED <<cs, ga, dead, sess, dials, mtw, opw, mt, dr, env, bad>>

\* The application ends its session; it may also keep it forever.
SessionClose(o) ==
    /\ op[o].st = "open"
    /\ sess' = [sess EXCEPT ![op[o].c] = @ - 1]
    /\ op' = [op EXCEPT ![o] = [st |-> "idle", c |-> 0]]
    /\ UNCHANGED <<cs, ga, dead, dials, mtw, opw, mt, dr, env, bad>>

(***************************************************************************)
(* Drain: dial replacements (past Max), then GOAWAY the old ones          *)
(***************************************************************************)
\* The old connections are those that take sessions: one already going
\* away counts against nothing, and replacing it would dial past Max.
\* Replacements: max(Min, 1), at most one per old connection.
DrainOld == IF Mutant = "drain-past-max" THEN {c \in Ids : cs[c] # "free"} ELSE {c \in Ids : Takes(c)}
DrainN == IF Mutant = "drain-past-max" THEN IF Min > 1 THEN Min ELSE 1
          ELSE LET k == IF Min > 1 THEN Min ELSE 1 IN IF k < Cardinality(DrainOld) THEN k ELSE Cardinality(DrainOld)

DrainStart ==
    /\ dr.st = "idle" /\ env < MaxEnv
    /\ DrainOld # {}
    /\ env' = env + 1
    /\ dials' = dials + 1 \* reserve, one replacement at a time
    /\ dr' = [st |-> "dial", old |-> DrainOld, left |-> DrainN - 1]
    /\ UNCHANGED <<cs, ga, dead, sess, mtw, opw, mt, op, bad>>

DrainDial ==
    /\ dr.st = "dial"
    /\ \/ \E c \in FreeId :
            /\ cs' = [cs EXCEPT ![c] = "up"]
            /\ Changed
            /\ IF dr.left > 0 \* the next replacement reserved as this one ends
               THEN dr' = [dr EXCEPT !.left = @ - 1] /\ UNCHANGED dials
               ELSE dr' = [dr EXCEPT !.st = "goaway"] /\ dials' = dials - 1
            /\ UNCHANGED env
       \/ DialFails /\ dr' = [dr EXCEPT !.st = "done"] \* the old ones stay
    /\ UNCHANGED <<ga, dead, sess, mt, op, bad>>

DrainGoAway ==
    /\ dr.st = "goaway"
    /\ ga' = [c \in Ids |-> ga[c] \/ (c \in dr.old /\ Up(c))]
    /\ dr' = [dr EXCEPT !.st = "done"]
    /\ IF Mutant = "no-wake-on-goaway" THEN UNCHANGED <<mtw, opw>> ELSE Changed
    /\ UNCHANGED <<cs, dead, sess, dials, mt, op, env, bad>>

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
    /\ UNCHANGED <<cs, dead, sess, dials, mt, op, dr, bad>>

PathDies(c) ==
    /\ Up(c) /\ ~dead[c] /\ env < MaxEnv
    /\ env' = env + 1
    /\ dead' = [dead EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<cs, ga, sess, dials, mtw, opw, mt, op, dr, bad>>

\* The connection closes: liveness noticed the dead path, or the side that
\* sent GOAWAY closes it once its sessions ended. Its sessions end with it.
CloseConn(c) ==
    /\ Up(c)
    /\ dead[c] \/ (ga[c] /\ sess[c] = 0)
    /\ cs' = [cs EXCEPT ![c] = "closed"]
    /\ sess' = [sess EXCEPT ![c] = 0]
    /\ op' = [o \in Opens |-> IF op[o].st = "open" /\ op[o].c = c THEN [st |-> "idle", c |-> 0] ELSE op[o]]
    /\ UNCHANGED <<ga, dead, dials, mtw, opw, mt, dr, env, bad>>

\* Serve returned: the Pool drops it and closes p.changed.
Remove(c) ==
    /\ cs[c] = "closed"
    /\ cs' = [cs EXCEPT ![c] = "free"]
    /\ ga' = [ga EXCEPT ![c] = FALSE]
    /\ dead' = [dead EXCEPT ![c] = FALSE]
    /\ Changed
    /\ UNCHANGED <<sess, dials, mt, op, dr, env, bad>>

Pool ==
    \/ MaintainCheck \/ MaintainReserve \/ MaintainWake \/ MaintainDial
    \/ \E o \in Opens : Pick(o) \/ Place(o) \/ OpenDial(o) \/ OpenWake(o)
    \/ DrainDial \/ DrainGoAway
    \/ \E c \in Ids : CloseConn(c) \/ Remove(c)

App ==
    \/ \E o \in Opens : OpenStart(o) \/ SessionClose(o)
    \/ DrainStart
    \/ \E c \in Ids : PeerGoAway(c) \/ PathDies(c)

Next == Pool \/ App

\* Every step of the Pool's goroutines and of the connections is fair; the
\* application's and the environment's are not (a session may stay open
\* forever). A dial that can succeed eventually does (failures are bounded).
Fairness ==
    /\ WF_vars(MaintainCheck) /\ WF_vars(MaintainReserve) /\ WF_vars(MaintainWake) /\ WF_vars(MaintainDial)
    /\ WF_vars(DrainDial) /\ WF_vars(DrainGoAway)
    /\ \A o \in Opens : WF_vars(Pick(o)) /\ WF_vars(Place(o)) /\ WF_vars(OpenDial(o)) /\ WF_vars(OpenWake(o))
    /\ \A c \in Ids : WF_vars(CloseConn(c)) /\ WF_vars(Remove(c))

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
    /\ cs \in [Ids -> {"free", "up", "closed"}]
    /\ dials \in Nat /\ env \in 0..MaxEnv

\* No session goes on a connection that is going away or closed.
NoSessionOnLeaving == ~bad

\* Concurrent Opens and maintain never dial past Max; only Drain's
\* replacements do, until it sent its GOAWAYs.
MaxHolds == dr.st \notin {"dial", "goaway"} => Staying <= Max

\* Min: once the environment stops, the Pool has Min connections that take
\* sessions, whatever sessions run on the ones going away.
MinHeld == <>[](Taking >= Min)

\* Every Open ends up with a session (the peer keeps accepting dials).
OpensOpen == \A o \in Opens : op[o].st \in {"pick", "dial", "wait", "place"} ~> op[o].st \in {"open", "idle"}
=============================================================================
