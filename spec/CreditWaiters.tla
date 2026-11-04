------------------------- MODULE CreditWaiters -------------------------
(* Several senders waiting in Credit.Acquire on one session (lane
   LINKWAKE, 2026-11-05; spec/README.md "Credit's waiters"). Credit's
   methods are safe from any goroutine, so two goroutines may Acquire on
   one session at once (relay's serverStream.Send did: lane FLAKECC).
   Link.tla gives a session one writer and takes Acquire as one step; this
   model is the wait inside it.

   The code before: Acquire woke its waiters through one token of capacity
   one (sessionCredit.wake), which Grant filled and a waiter emptied. A
   grant woke one waiter; it took its bytes and left the rest, or found
   too little for itself and waited again, and the others slept with
   credit there for them. The receiver grants only for bytes it read, so
   no grant came to wake them.

   The design: a grant (and a refund) wakes every waiter. A waiter takes
   the session's current wake channel under the lock (made when the first
   waiter needs one); Grant closes it and drops it, and the next waiter
   makes a new one. Each woken waiter checks again under the lock.

   One session; Waiters each acquire K times Need[w] bytes and send them;
   a window of W bytes; the receiver reads the bytes sent one at a time
   and grants back each quarter window it read (Credit.Consumed), the
   grants travelling in order.

   Mutants (scripts/spec.sh mutants): "one-slot" (the code before),
   "pass-on" (one-slot, and a waiter that takes its bytes and leaves some
   credit fills the token: right only while every waiter needs the same). *)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Waiters, \* the goroutines acquiring on the session
    Need,    \* [Waiters -> bytes]: each Acquire's n
    K,       \* Acquires per waiter
    W,       \* the peer's window
    Mutant

\* CreditWaiters.cfg's Need (a cfg has no tuples): one waiter needs more
\* than the others, so a waiter can find too little for itself.
CfgNeed == <<5, 2, 1>>

T == IF W \div 4 > 1 THEN W \div 4 ELSE 1 \* a window update's least size
\* Below T bytes the receiver grants nothing, so a waiter needing more than
\* W - T + 1 could wait for good in any design.
ASSUME K >= 1 /\ \A w \in Waiters : Need[w] \in 1..(W - T + 1)

VARIABLES cr, left, pc, token, woken, wire, cons, grants
vars == <<cr, left, pc, token, woken, wire, cons, grants>>

\* pc: "run" (between Acquires, done once left = 0), "wait" (in Acquire's
\* select). woken[w]: the channel w waits on is closed (the design);
\* token: the one-slot channel is full (the mutants).
TypeOK ==
    /\ cr \in 0..W /\ left \in [Waiters -> 0..K]
    /\ pc \in [Waiters -> {"run", "wait"}]
    /\ token \in BOOLEAN /\ woken \in [Waiters -> BOOLEAN]
    /\ wire \in 0..W /\ cons \in 0..T

Init ==
    /\ cr = W /\ left = [w \in Waiters |-> K] /\ pc = [w \in Waiters |-> "run"]
    /\ token = FALSE /\ woken = [w \in Waiters |-> FALSE]
    /\ wire = 0 /\ cons = 0 /\ grants = <<>>

OneSlot == Mutant \in {"one-slot", "pass-on"}

\* Credit.acquire's check under its lock: take the bytes and send them
\* (Conn.sendData); or wait, on the channel taken under the lock.
Check(w) ==
    LET n == Need[w] IN
    /\ (pc[w] = "run" /\ left[w] > 0) \/ (pc[w] = "wait" /\ (IF OneSlot THEN token ELSE woken[w]))
    /\ token' = IF Mutant = "pass-on" /\ cr > n THEN TRUE
                ELSE IF pc[w] = "wait" /\ OneSlot THEN FALSE ELSE token
    /\ woken' = [woken EXCEPT ![w] = FALSE]
    /\ IF cr >= n
       THEN /\ cr' = cr - n /\ wire' = wire + n
            /\ left' = [left EXCEPT ![w] = @ - 1]
            /\ pc' = [pc EXCEPT ![w] = "run"]
       ELSE /\ pc' = [pc EXCEPT ![w] = "wait"]
            /\ UNCHANGED <<cr, wire, left>>
    /\ UNCHANGED <<cons, grants>>

\* The receiver reads a byte (Credit.Consumed) and grants what it read once
\* that is a quarter window.
Read ==
    /\ wire > 0
    /\ wire' = wire - 1
    /\ IF cons + 1 >= T
       THEN grants' = Append(grants, cons + 1) /\ cons' = 0
       ELSE cons' = cons + 1 /\ UNCHANGED grants
    /\ UNCHANGED <<cr, left, pc, token, woken>>

\* Credit.Grant: the credit grows, and the waiters wake (the design: every
\* one waiting; the mutants: the token is filled).
Grant ==
    /\ grants # <<>>
    /\ cr' = cr + Head(grants) /\ grants' = Tail(grants)
    /\ IF OneSlot
       THEN token' = TRUE /\ UNCHANGED woken
       ELSE woken' = [w \in Waiters |-> woken[w] \/ pc[w] = "wait"] /\ UNCHANGED token
    /\ UNCHANGED <<left, pc, wire, cons>>

Next == \/ \E w \in Waiters : Check(w)
        \/ Read \/ Grant

Spec == Init /\ [][Next]_vars
    /\ \A w \in Waiters : WF_vars(Check(w))
    /\ WF_vars(Read) /\ WF_vars(Grant)

----------------------------------------------------------------------------
Sum(s) == LET f[i \in 0..Len(s)] == IF i = 0 THEN 0 ELSE f[i-1] + s[i] IN f[Len(s)]

\* Credit, bytes on the wire, read and not yet granted, and grants on the
\* way make up the window.
Conservation == cr + wire + cons + Sum(grants) = W

Woken(w) == IF OneSlot THEN token ELSE woken[w]

\* No waiter sleeps while the session has the credit it waits for, unless
\* it is woken (its check comes).
NoStrandedWaiter ==
    \A w \in Waiters : pc[w] = "wait" /\ cr >= Need[w] => Woken(w)

\* Every Acquire completes.
AllDone == <>(\A w \in Waiters : left[w] = 0)

\* Witnesses (make spec-mutants: TLC must violate each): two waiters at
\* once; a waiter with credit there, too little for it; one grant waking
\* two waiters.
WitnessTwoWait == ~(Cardinality({w \in Waiters : pc[w] = "wait"}) >= 2)
WitnessTooLittle == ~(\E w \in Waiters : pc[w] = "wait" /\ 0 < cr /\ cr < Need[w])
WitnessWakeAll == ~(Cardinality({w \in Waiters : woken[w]}) >= 2)
=============================================================================
