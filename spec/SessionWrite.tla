------------------------- MODULE SessionWrite -------------------------
(* Session.Write waiting for credit while the write deadline is set
   (lane P2PLEFT, 2026-11-15; spec/README.md "Session writes").
   Link.tla takes a Write as one step and CreditWaiters.tla the wait for
   a grant; neither models the write deadline. This model is the wait in
   Session.acquire: a Write takes the deadline's channel (writeWait),
   looks at the flags, then waits in Credit.acquire for a grant or that
   channel's close; SetWriteDeadline (past, future or zero) and the
   deadline's timer run at any time meanwhile, and the peer may grant.

   The code before: writeWait returned s.wdCh, nil until a first
   SetWriteDeadline. A Write already waiting for credit when the first
   deadline was set waited on that nil channel: SetWriteDeadline closed
   nothing (wakeWriters wakes only the network write's waiters) and the
   Write hung until a grant came, past its deadline ("nil-channel").
   nettest's PresentTimeout found it (nfnet TestConnIsANetConn).

   The design: writeWait makes the channel when there is none, under
   wdMu, so every waiting Write holds a channel the next
   SetWriteDeadline or timer closes.

   Channels are numbered; 0 is nil. *)
EXTENDS Naturals

CONSTANTS
    Writers, \* goroutines writing the session
    Sets,    \* SetWriteDeadline calls
    Mutant   \* "none", "nil-channel"

VARIABLES
    ch,     \* s.wdCh: the current channel (0: none yet)
    nextCh, \* the next channel's number
    shut,   \* the channels closed
    past,   \* s.wd.past
    armed,  \* a future deadline's timer is pending
    sets,   \* SetWriteDeadline calls made
    granted,\* the peer granted credit: every waiting Write may take it
    snap,   \* [Writers -> the channel its wait holds]
    pc,     \* [Writers -> step]
    res     \* [Writers -> {"", "ok", "timeout"}]

vars == <<ch, nextCh, shut, past, armed, sets, granted, snap, pc, res>>

Init ==
    /\ ch = 0 /\ nextCh = 1 /\ shut = {} /\ past = FALSE /\ armed = FALSE
    /\ sets = 0 /\ granted = FALSE
    /\ snap = [w \in Writers |-> 0]
    /\ pc = [w \in Writers |-> "take"]
    /\ res = [w \in Writers |-> ""]

\* SetWriteDeadline(t): the old channel closed (when there is one), a new
\* one; past, a timer for a future t, or neither for zero.
Set(kind) ==
    /\ sets < Sets
    /\ sets' = sets + 1
    /\ shut' = IF ch = 0 THEN shut ELSE shut \cup {ch}
    /\ ch' = nextCh /\ nextCh' = nextCh + 1
    /\ past' = (kind = "past")
    /\ armed' = (kind = "future")
    /\ UNCHANGED <<granted, snap, pc, res>>

\* The timer of a future deadline (its generation still current).
Fire ==
    /\ armed /\ armed' = FALSE /\ past' = TRUE
    /\ shut' = shut \cup {ch} /\ ch' = nextCh /\ nextCh' = nextCh + 1
    /\ UNCHANGED <<sets, granted, snap, pc, res>>

\* The peer's window update.
Grant == ~granted /\ granted' = TRUE /\ UNCHANGED <<ch, nextCh, shut, past, armed, sets, snap, pc, res>>

\* writeWait, under wdMu: the design makes a channel when there is none.
Take(w) ==
    /\ pc[w] = "take"
    /\ IF ch = 0 /\ Mutant # "nil-channel"
       THEN /\ ch' = nextCh /\ nextCh' = nextCh + 1 /\ snap' = [snap EXCEPT ![w] = nextCh]
       ELSE /\ snap' = [snap EXCEPT ![w] = ch] /\ UNCHANGED <<ch, nextCh>>
    /\ pc' = [pc EXCEPT ![w] = "check"]
    /\ UNCHANGED <<shut, past, armed, sets, granted, res>>

\* acquire's look at wd.past, then Credit.acquire's look at the credit.
Check(w) ==
    /\ pc[w] = "check"
    /\ CASE past -> /\ res' = [res EXCEPT ![w] = "timeout"] /\ pc' = [pc EXCEPT ![w] = "done"]
         [] granted -> /\ res' = [res EXCEPT ![w] = "ok"] /\ pc' = [pc EXCEPT ![w] = "done"]
         [] OTHER -> /\ pc' = [pc EXCEPT ![w] = "wait"] /\ UNCHANGED res
    /\ UNCHANGED <<ch, nextCh, shut, past, armed, sets, granted, snap>>

\* Credit.acquire's select: a grant (taken), or its channel closed (stopped:
\* acquire takes the channel again and looks again). Never on channel 0.
Wake(w) ==
    /\ pc[w] = "wait"
    /\ \/ granted /\ res' = [res EXCEPT ![w] = "ok"] /\ pc' = [pc EXCEPT ![w] = "done"]
       \/ snap[w] # 0 /\ snap[w] \in shut /\ pc' = [pc EXCEPT ![w] = "take"] /\ UNCHANGED res
    /\ UNCHANGED <<ch, nextCh, shut, past, armed, sets, granted, snap>>

Next ==
    \/ Set("past") \/ Set("future") \/ Set("zero") \/ Fire \/ Grant
    \/ \E w \in Writers : Take(w) \/ Check(w) \/ Wake(w)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ ch \in Nat /\ nextCh \in Nat /\ shut \subseteq Nat
    /\ past \in BOOLEAN /\ armed \in BOOLEAN /\ granted \in BOOLEAN
    /\ snap \in [Writers -> Nat]
    /\ pc \in [Writers -> {"take", "check", "wait", "done"}]
    /\ res \in [Writers -> {"", "ok", "timeout"}]

\* A Write waiting for credit past its deadline is woken: the channel it
\* holds is closed, so it looks again and returns os.ErrDeadlineExceeded.
NoStrandedWriter == \A w \in Writers : (pc[w] = "wait" /\ past) => snap[w] \in shut

\* Witnesses (make spec-mutants: TLC must violate each).
WitnessTimeout == ~\E w \in Writers : res[w] = "timeout"
WitnessWokenWaiter == ~\E w \in Writers : pc[w] = "wait" /\ past /\ snap[w] \in shut
WitnessGranted == ~\E w \in Writers : res[w] = "ok"
=============================================================================
