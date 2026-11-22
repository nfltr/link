---------------------------- MODULE CreditGrow ----------------------------
(* A session's window grown with the path (lane RELAYWIN, 2026-11-22;
   spec/README.md "Growing windows"; WIRE.md section 7). One connection,
   the sessions Sids each sending Total bytes from the dialer to the
   listener; Link.tla is the rest of the session protocol with fixed
   windows, and this model is what FeatureGrow adds to its credit.

   A session's window starts at W (the hello's window, as before). A sender
   that must wait for credit (Credit.acquire) says so once, with a window
   frame of increment 0 for the session (BLOCKED; RFC 9000's
   STREAM_DATA_BLOCKED), behind every data frame it sent. When the receiver
   reads it, every byte the sender could send arrived; if its reader keeps
   up (less than half the window unread) and it received a whole window
   since the window last grew, the window doubles (TCP's dynamic right
   sizing, quic-go's and Chromium's receive window auto-tuning), at most to
   Max and within the connection's growth budget G (the growth summed over
   the sessions). The receive loop sends nothing: the bytes the window grew
   by go out with the session's next window update (Credit.Consumed). A
   session's end gives its growth back to the budget.

   Mutants (scripts/spec.sh mutants): "no-keepup" (growth whatever the
   reader left unread), "no-epoch" (growth at every BLOCKED), "no-cap",
   "no-budget", "drop-keeps-growth" (the end gives nothing back),
   "grant-not-counted" (the update carries the growth, the window does not
   grow: data past the window), "fixed-cap-sender" (a sender that caps its
   credit at the hello's window: growth toward a peer without FeatureGrow). *)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Sids,   \* the sessions
    W,      \* the hello's window: every session's first
    Max,    \* Config.WindowMax: a session's window at most
    G,      \* Config.WindowBudget: the connection's growth, summed, at most
    Total,  \* bytes each session sends
    Q,      \* an update per 1/Q window read: 4 in the code, 1 here keeps both branches of Read reachable at small windows
    Mutant

VARIABLES
    cred,    \* sender: credit (Credit.sessions[s].n)
    sent,    \* sender: bytes sent
    bsent,   \* sender: a BLOCKED out since the last window update
    ab,      \* dialer -> listener frames, in order
    ba,      \* listener -> dialer frames, in order
    win,     \* receiver: the session's window (sessionCredit.win)
    held,    \* receiver: received, not yet acknowledged
    cons,    \* receiver: read, not yet acknowledged
    owed,    \* receiver: growth not yet sent (sessionCredit.owed)
    rcvd,    \* receiver: bytes received
    rd,      \* receiver: bytes its reader took
    epoch,   \* receiver: rcvd when the window last grew
    grown,   \* receiver: the connection's growth (Credit.grown)
    stopped, \* the reader stopped for good
    stopWin, \* the window when it stopped
    live,    \* the session not ended
    err      \* the receiver saw data past the window, or the sender a grant past its cap

vars == <<cred, sent, bsent, ab, ba, win, held, cons, owed, rcvd, rd, epoch, grown, stopped, stopWin, live, err>>

Thr(s) == win[s] \div Q                \* Credit.Consumed: an update per quarter window (Q = 4)
Unread(s) == rcvd[s] - rd[s]
SendCap == IF Mutant = "fixed-cap-sender" THEN W ELSE 2 * Max   \* the sender's check: MaxWindow with FeatureGrow
Sum(f(_)) == LET S[X \in SUBSET Sids] == IF X = {} THEN 0 ELSE LET x == CHOOSE y \in X : TRUE IN f(x) + S[X \ {x}]
             IN S[Sids]

Init ==
    /\ cred = [s \in Sids |-> W]
    /\ sent = [s \in Sids |-> 0]
    /\ bsent = [s \in Sids |-> FALSE]
    /\ ab = <<>>
    /\ ba = <<>>
    /\ win = [s \in Sids |-> W]
    /\ held = [s \in Sids |-> 0]
    /\ cons = [s \in Sids |-> 0]
    /\ owed = [s \in Sids |-> 0]
    /\ rcvd = [s \in Sids |-> 0]
    /\ rd = [s \in Sids |-> 0]
    /\ epoch = [s \in Sids |-> 0]
    /\ grown = 0
    /\ stopped = [s \in Sids |-> FALSE]
    /\ stopWin = [s \in Sids |-> 0]
    /\ live = [s \in Sids |-> TRUE]
    /\ err = FALSE

\* Credit.acquire takes a byte and the frame goes out.
Send(s) ==
    /\ live[s] /\ sent[s] < Total /\ cred[s] >= 1
    /\ cred' = [cred EXCEPT ![s] = @ - 1]
    /\ sent' = [sent EXCEPT ![s] = @ + 1]
    /\ ab' = Append(ab, [t |-> "data", s |-> s])
    /\ UNCHANGED <<bsent, ba, win, held, cons, owed, rcvd, rd, epoch, grown, stopped, stopWin, live, err>>

\* Credit.acquire must wait: its BLOCKED (once until the next update). The
\* receiver must hold against any peer, so a BLOCKED may also come at any
\* other time (one of a session's on the way at once: the model's bound).
Block(s) ==
    /\ live[s] /\ sent[s] < Total
    /\ \/ cred[s] = 0 /\ ~bsent[s]
       \/ \A i \in 1..Len(ab) : ab[i] # [t |-> "blocked", s |-> s]
    /\ bsent' = [bsent EXCEPT ![s] = TRUE]
    /\ ab' = Append(ab, [t |-> "blocked", s |-> s])
    /\ UNCHANGED <<cred, sent, ba, win, held, cons, owed, rcvd, rd, epoch, grown, stopped, stopWin, live, err>>

\* Credit.Grant of a window update (Conn's receive loop at the dialer).
RecvWin ==
    /\ ba # <<>>
    /\ LET f == Head(ba) IN
       /\ ba' = Tail(ba)
       /\ IF live[f.s]
          THEN IF cred[f.s] + f.n > SendCap
               THEN err' = TRUE /\ UNCHANGED <<cred, bsent>>
               ELSE /\ cred' = [cred EXCEPT ![f.s] = @ + f.n]
                    /\ bsent' = [bsent EXCEPT ![f.s] = FALSE]
                    /\ UNCHANGED err
          ELSE UNCHANGED <<cred, bsent, err>>
    /\ UNCHANGED <<sent, ab, win, held, cons, owed, rcvd, rd, epoch, grown, stopped, stopWin, live>>

\* The growth BLOCKED earns: double, within Max and the budget.
KeepsUp(s) == Mutant = "no-keepup" \/ 2 * Unread(s) < win[s]
WholeWindow(s) == Mutant = "no-epoch" \/ rcvd[s] - epoch[s] >= win[s]
Room(s) == LET a == IF Mutant = "no-cap" THEN win[s] ELSE Max - win[s]
               b == IF Mutant = "no-budget" THEN win[s] ELSE G - grown
           IN IF win[s] < a THEN (IF win[s] < b THEN win[s] ELSE b) ELSE (IF a < b THEN a ELSE b)

\* The listener's receive loop: a data frame charged (Credit.receivedSID),
\* a BLOCKED (Credit.peerBlocked) grows the window or does nothing.
RecvAB ==
    /\ ab # <<>>
    /\ LET f == Head(ab) IN
       /\ ab' = Tail(ab)
       /\ CASE ~live[f.s] -> UNCHANGED <<win, held, rcvd, owed, epoch, grown, err>>
            [] f.t = "data" ->
                 IF held[f.s] + 1 > win[f.s]
                 THEN err' = TRUE /\ UNCHANGED <<win, held, rcvd, owed, epoch, grown>>
                 ELSE /\ held' = [held EXCEPT ![f.s] = @ + 1]
                      /\ rcvd' = [rcvd EXCEPT ![f.s] = @ + 1]
                      /\ UNCHANGED <<win, owed, epoch, grown, err>>
            [] OTHER ->
                 LET g == Room(f.s) IN
                 IF KeepsUp(f.s) /\ WholeWindow(f.s) /\ g > 0
                 THEN /\ win' = [win EXCEPT ![f.s] = IF Mutant = "grant-not-counted" THEN @ ELSE @ + g]
                      /\ owed' = [owed EXCEPT ![f.s] = @ + g]
                      /\ epoch' = [epoch EXCEPT ![f.s] = rcvd[f.s]]
                      /\ grown' = grown + g
                      /\ UNCHANGED <<held, rcvd, err>>
                 ELSE UNCHANGED <<win, held, rcvd, owed, epoch, grown, err>>
    /\ UNCHANGED <<cred, sent, bsent, ba, cons, rd, stopped, stopWin, live>>

\* The reader takes a byte (Credit.Consumed): an update once a quarter
\* window is due, carrying what the window grew by.
Read(s) ==
    /\ live[s] /\ ~stopped[s] /\ Unread(s) > 0
    /\ rd' = [rd EXCEPT ![s] = @ + 1]
    /\ IF cons[s] + 1 + owed[s] >= Thr(s)
       THEN /\ ba' = Append(ba, [s |-> s, n |-> cons[s] + 1 + owed[s]])
            /\ held' = [held EXCEPT ![s] = @ - (cons[s] + 1)]
            /\ cons' = [cons EXCEPT ![s] = 0]
            /\ owed' = [owed EXCEPT ![s] = 0]
       ELSE /\ cons' = [cons EXCEPT ![s] = @ + 1]
            /\ UNCHANGED <<ba, held, owed>>
    /\ UNCHANGED <<cred, sent, bsent, ab, win, rcvd, epoch, grown, stopped, stopWin, live, err>>

\* The reader stops for good (a peer that reads no more).
Stop(s) ==
    /\ live[s] /\ ~stopped[s]
    /\ stopped' = [stopped EXCEPT ![s] = TRUE]
    /\ stopWin' = [stopWin EXCEPT ![s] = win[s]]
    /\ UNCHANGED <<cred, sent, bsent, ab, ba, win, held, cons, owed, rcvd, rd, epoch, grown, live, err>>

\* The session ends (a close, a reset, the peer gone with its connection):
\* Credit.dropLocked gives its growth back.
Drop(s) ==
    /\ live[s]
    /\ live' = [live EXCEPT ![s] = FALSE]
    /\ grown' = IF Mutant = "drop-keeps-growth" THEN grown ELSE grown - (win[s] - W)
    /\ UNCHANGED <<cred, sent, bsent, ab, ba, win, held, cons, owed, rcvd, rd, epoch, stopped, stopWin, err>>

Next ==
    \/ \E s \in Sids : Send(s) \/ Block(s) \/ Read(s) \/ Stop(s) \/ Drop(s)
    \/ RecvWin
    \/ RecvAB

Fair == \A s \in Sids : WF_vars(Send(s)) /\ WF_vars(Block(s)) /\ WF_vars(Read(s))
Spec == Init /\ [][Next]_vars /\ Fair /\ WF_vars(RecvWin) /\ WF_vars(RecvAB)

-----------------------------------------------------------------------------
InFlightData(s) == Len(SelectSeq(ab, LAMBDA f : f.t = "data" /\ f.s = s))
InFlightGrant(s) == LET F[i \in 0..Len(ba)] == IF i = 0 THEN 0 ELSE F[i - 1] + (IF ba[i].s = s THEN ba[i].n ELSE 0)
                    IN F[Len(ba)]

TypeOK ==
    /\ cred \in [Sids -> Nat] /\ win \in [Sids -> Nat] /\ held \in [Sids -> Nat]
    /\ cons \in [Sids -> Nat] /\ owed \in [Sids -> Nat] /\ grown \in Nat
    /\ err \in BOOLEAN

\* Never data past the window, never a grant past the sender's cap.
NoErr == ~err

\* Every byte of the window is somewhere: credit, a data frame on the way,
\* held (queued or read and not acknowledged), or a window update on the
\* way, or growth not yet sent.
Conservation == \A s \in Sids : live[s] =>
    cred[s] + InFlightData(s) + held[s] + InFlightGrant(s) + owed[s] = win[s]

\* Credit is never negative and never past the window.
CreditBounds == \A s \in Sids : live[s] => cred[s] <= win[s]
WinBounds == \A s \in Sids : W <= win[s] /\ win[s] <= Max

\* The connection's growth is within its budget, and is the live
\* sessions' growth (an ended session's came back).
Budget == grown <= G
GrownAccount == grown = Sum(LAMBDA s : IF live[s] THEN win[s] - W ELSE 0)

\* Memory: a session's queue holds at most its window; the connection's,
\* every session's first window and the budget.
QueueBound == \A s \in Sids : Unread(s) <= held[s] /\ held[s] <= win[s]
ConnBound == Sum(LAMBDA s : IF live[s] THEN Unread(s) ELSE 0) <= Cardinality(Sids) * W + G

\* A reader that never read leaves its window as it was: a stopped
\* reader grows no one's memory.
NeverReadNoGrowth == \A s \in Sids : rd[s] = 0 => win[s] = W
\* A reader that stopped lets its window grow at most once more (a BLOCKED
\* on the way when it stopped).
StoppedGrowsOnce == \A s \in Sids : stopped[s] => win[s] <= 2 * stopWin[s]

\* No growth ahead of use: a window at most doubles per window received
\* (epoch: the bytes received when it last grew).
NoGrowthAhead == \A s \in Sids : win[s] = W \/ (win[s] <= 2 * epoch[s] /\ epoch[s] <= rcvd[s])

\* Liveness: a session whose reader keeps reading gets every byte.
AllDelivered == \A s \in Sids : <>(~live[s] \/ stopped[s] \/ rd[s] = Total)

\* Witnesses (scripts/spec.sh mutants): each must be violated.
WitnessGrew == \A s \in Sids : win[s] = W
WitnessCapped == \A s \in Sids : win[s] < Max
WitnessBudgetFull == grown < G
WitnessStoppedFull == \A s \in Sids : ~(stopped[s] /\ held[s] = win[s])
WitnessBlockedNoGrowth == \A s \in Sids : ~(bsent[s] /\ ab = <<>> /\ win[s] = W /\ rcvd[s] >= W /\ 2 * Unread(s) >= W)
WitnessGrownDelivered == \A s \in Sids : ~(win[s] > W /\ rd[s] = Total)
=============================================================================
