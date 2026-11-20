------------------------- MODULE SessionRead -------------------------
(* Session.Read inside (lane LINKREAD, 2026-11-14; spec/README.md
   "Session reads"). Link.tla gives a session one reader and takes a Read
   as one pop of its queue; a read deadline changes nothing it models.
   This model is the Read itself: several goroutines reading one session
   at once (net.Conn allows it), the read deadline's flag (dlPast) set by
   its timer and cleared or moved by SetReadDeadline at any time, and the
   peer's slices and end arriving meanwhile. With Copier set, that reader
   runs WriteTo instead (lane LINKREADWRITE, 2026-11-20): io.Copy(dst,
   sess) beside a Read, which WriteTo's doc calls misuse but nothing
   stopped; Pump's drain takes the tail the same way (takeRest).

   The code before had three bugs, all silent:
   - Read took the unread tail of the last slice (s.rest) with no lock:
     two readers copied the same bytes, and each cut the tail once, so
     bytes went twice and others never ("no-read-lock").
   - PopUnless gave up when the flag was set, and Read then looked at the
     flag again, outside the queue's lock, to tell a deadline from the
     end: a flag cleared in between read as io.EOF on a live session
     ("flag-outside-lock").
   - WriteTo (and Pump) wrote s.rest, then cut it and released s.cur,
     all without rmu: a Read copying that tail meanwhile handed the same
     bytes out again, read a buffer already back in the pool, and a tail
     a Read left after WriteTo's write was cut and never read
     ("writeto-no-lock").

   The design: Read holds the session's read lock (rmu) from its look at
   s.rest to the end of its copy; PopUnless tells why it gave up (the
   flag or the queue's end), decided under the queue's lock in one look.
   WriteTo and Pump take s.rest and s.cur under rmu (takeRest) and own
   them from there: written, then released, by them alone.

   One session; the peer pushes Slices slices of two bytes each (slice k
   is bytes 2k-1 and 2k), then may end; each Read takes one byte. *)
EXTENDS Naturals, Sequences

CONSTANTS
    Readers, \* goroutines reading the session
    Slices,  \* slices the peer pushes, two bytes each
    Copier,  \* the reader that runs WriteTo instead of Read, or None (0)
    Mutant   \* "none", "no-read-lock", "flag-outside-lock", "writeto-no-lock"

None == 0
Bytes == 1..(2 * Slices)
SliceOf(b) == (b + 1) \div 2
Range(s) == {s[i] : i \in DOMAIN s}
ASSUME Copier \in Readers \cup {None}

VARIABLES
    q,      \* the session's queue: slices (sequences of byte ids)
    closed, \* the queue closed: the peer's end
    dl,     \* dlPast: the read deadline passed
    next,   \* the next byte id the peer sends
    rest,   \* s.rest: the unread tail of the last slice popped
    cur,    \* s.cur: that slice (its id), released once read; None if nil
    lock,   \* the reader holding rmu, or None
    pc,     \* [Readers -> step]
    res,    \* [Readers -> {"", "data", "timeout", "eof"}]: a Read's result
    cnt,    \* [Bytes -> Nat]: times each byte was handed to a reader
    got,    \* [Readers -> Seq(Bytes)]: the bytes each reader was handed
    ref,    \* [Readers -> set of slice ids]: the slices a reader reads from
    tail,   \* [Readers -> Seq(Bytes)]: what WriteTo took, to write
    rel     \* [1..Slices -> Nat]: times each slice went back to the pool

vars == <<q, closed, dl, next, rest, cur, lock, pc, res, cnt, got, ref, tail, rel>>

Init ==
    /\ q = <<>> /\ closed = FALSE /\ dl = FALSE /\ next = 1 /\ rest = <<>>
    /\ cur = None /\ lock = None
    /\ pc = [r \in Readers |-> "idle"]
    /\ res = [r \in Readers |-> ""]
    /\ cnt = [b \in Bytes |-> 0]
    /\ got = [r \in Readers |-> <<>>]
    /\ ref = [r \in Readers |-> {}]
    /\ tail = [r \in Readers |-> <<>>]
    /\ rel = [k \in 1..Slices |-> 0]

Locked == Mutant # "no-read-lock"
Plain == Readers \ {Copier}

\* r is handed the bytes of s.
Hand(r, s) ==
    /\ LET S == Range(s) IN cnt' = [b \in Bytes |-> IF b \in S THEN cnt[b] + 1 ELSE cnt[b]]
    /\ got' = [got EXCEPT ![r] = @ \o s]
\* The slice k as a set (None: none); releaseData of the slices S.
Ids(k) == IF k = None THEN {} ELSE {k}
Release(S) == rel' = [k \in 1..Slices |-> IF k \in S THEN rel[k] + 1 ELSE rel[k]]
\* The k bytes the peer sends next, and the slices s1 .. sk as one.
Fresh(k) == [i \in 1..k |-> next + i - 1]
Flat(s) == LET f[i \in 0..Len(s)] == IF i = 0 THEN <<>> ELSE f[i - 1] \o s[i] IN f[Len(s)]

\* The peer: a slice of k bytes (Mux.data's Push), its end (the queue's
\* Close). The model's slices are two bytes; a trace's are its own
\* (TraceSessionRead.tla), and it also takes Grow: Mux.data's Append,
\* k bytes onto the newest slice still queued.
PushOf(k) ==
    /\ ~closed /\ next + k - 1 \in Bytes
    /\ q' = Append(q, Fresh(k)) /\ next' = next + k
    /\ UNCHANGED <<closed, dl, rest, cur, lock, pc, res, cnt, got, ref, tail, rel>>
Push == PushOf(2)
Grow(k) ==
    /\ ~closed /\ q # <<>> /\ next + k - 1 \in Bytes
    /\ q' = [q EXCEPT ![Len(q)] = @ \o Fresh(k)] /\ next' = next + k
    /\ UNCHANGED <<closed, dl, rest, cur, lock, pc, res, cnt, got, ref, tail, rel>>
Close ==
    /\ ~closed /\ closed' = TRUE
    /\ UNCHANGED <<q, dl, next, rest, cur, lock, pc, res, cnt, got, ref, tail, rel>>

\* The deadline: its timer fires (or SetReadDeadline sets a past one) and
\* Wake; SetReadDeadline clears it or moves it ahead.
Fire == ~dl /\ dl' = TRUE /\ UNCHANGED <<q, closed, next, rest, cur, lock, pc, res, cnt, got, ref, tail, rel>>
Clear == dl /\ dl' = FALSE /\ UNCHANGED <<q, closed, next, rest, cur, lock, pc, res, cnt, got, ref, tail, rel>>

\* Read's start: rmu taken, then s.rest looked at.
Begin(r) ==
    /\ r \in Plain /\ pc[r] = "idle"
    /\ IF Locked THEN lock = None /\ lock' = r ELSE UNCHANGED lock
    /\ pc' = [pc EXCEPT ![r] = IF rest # <<>> THEN "copy" ELSE "pop"]
    /\ ref' = [ref EXCEPT ![r] = IF rest # <<>> THEN Ids(cur) ELSE {}]
    /\ UNCHANGED <<q, closed, dl, next, rest, cur, res, cnt, got, tail, rel>>

\* PopUnless: under the queue's lock, it waits (not enabled) until the
\* flag, a slice or the end; the design decides there, in one look; the
\* code before only gave up there and decided later (Decide).
Take(r) == /\ rest' = Head(q) /\ cur' = SliceOf(Head(Head(q))) /\ q' = Tail(q)
           /\ ref' = [ref EXCEPT ![r] = {SliceOf(Head(Head(q)))}]
           /\ pc' = [pc EXCEPT ![r] = "copy"]
Pop(r) ==
    /\ pc[r] = "pop" /\ (dl \/ q # <<>> \/ closed)
    /\ IF Mutant = "flag-outside-lock"
       THEN IF ~dl /\ q # <<>>
            THEN Take(r) /\ UNCHANGED res
            ELSE /\ pc' = [pc EXCEPT ![r] = "decide"] /\ UNCHANGED <<q, rest, cur, ref, res>>
       ELSE CASE dl -> /\ res' = [res EXCEPT ![r] = "timeout"] /\ pc' = [pc EXCEPT ![r] = "end"]
                       /\ UNCHANGED <<q, rest, cur, ref>>
              [] q # <<>> -> Take(r) /\ UNCHANGED res
              [] OTHER -> /\ res' = [res EXCEPT ![r] = "eof"] /\ pc' = [pc EXCEPT ![r] = "end"]
                          /\ UNCHANGED <<q, rest, cur, ref>>
    /\ UNCHANGED <<closed, dl, next, lock, cnt, got, tail, rel>>

\* The code before: Read looks at the flag again, the queue unlocked.
Decide(r) ==
    /\ pc[r] = "decide"
    /\ res' = [res EXCEPT ![r] = IF dl THEN "timeout" ELSE "eof"]
    /\ pc' = [pc EXCEPT ![r] = "end"]
    /\ UNCHANGED <<q, closed, dl, next, rest, cur, lock, cnt, got, ref, tail, rel>>

\* copy(b, s.rest) of n bytes (the model's: one; a trace's: its own).
\* Under rmu, with s.rest = s.rest[n:] and, when that empties it,
\* releaseData(s.cur); with no lock that cut is a step of its own (Cut),
\* from s.rest as it is then.
CutRest(n) == /\ rest' = IF Len(rest) <= n THEN <<>> ELSE SubSeq(rest, n + 1, Len(rest))
              /\ IF Len(rest) = n THEN Release(Ids(cur)) /\ cur' = None ELSE UNCHANGED <<cur, rel>>
CopyOf(r, n) ==
    /\ pc[r] = "copy"
    /\ IF rest = <<>>
       THEN /\ res' = [res EXCEPT ![r] = "data"] /\ UNCHANGED <<cnt, got>>   \* n = 0
       ELSE /\ Hand(r, SubSeq(rest, 1, n)) /\ res' = [res EXCEPT ![r] = "data"]
    /\ IF Locked
       THEN /\ CutRest(n)
            /\ pc' = [pc EXCEPT ![r] = "end"] /\ ref' = [ref EXCEPT ![r] = {}]
       ELSE /\ pc' = [pc EXCEPT ![r] = "cut"] /\ UNCHANGED <<rest, cur, rel, ref>>
    /\ UNCHANGED <<q, closed, dl, next, lock, tail>>
Copy(r) == CopyOf(r, 1)
Cut(r) ==
    /\ pc[r] = "cut"
    /\ CutRest(1)
    /\ pc' = [pc EXCEPT ![r] = "end"] /\ ref' = [ref EXCEPT ![r] = {}]
    /\ UNCHANGED <<q, closed, dl, next, lock, res, cnt, got, tail>>

\* Read returns, rmu let go. A reader that read the end stops; one that
\* read a byte or the deadline reads again.
End(r) ==
    /\ pc[r] = "end"
    /\ lock' = IF lock = r THEN None ELSE lock
    /\ pc' = [pc EXCEPT ![r] = IF res[r] = "eof" THEN "done" ELSE "idle"]
    /\ UNCHANGED <<q, closed, dl, next, rest, cur, res, cnt, got, ref, tail, rel>>

\* WriteTo's start, takeRest: under rmu (free: a Read holding it returns
\* first), s.rest and s.cur become WriteTo's, both nil on the session.
\* The code before looked at s.rest with no lock and left it there while
\* it wrote it (WRel cut it after).
WTake(c) ==
    /\ c = Copier /\ pc[c] = "idle"
    /\ IF Mutant = "writeto-no-lock"
       THEN /\ UNCHANGED <<rest, cur>>
       ELSE /\ lock = None /\ rest' = <<>> /\ cur' = None
    /\ tail' = [tail EXCEPT ![c] = rest]
    /\ ref' = [ref EXCEPT ![c] = IF rest # <<>> THEN Ids(cur) ELSE {}]
    /\ pc' = [pc EXCEPT ![c] = IF rest # <<>> THEN "write-rest" ELSE "batch"]
    /\ UNCHANGED <<q, closed, dl, next, lock, res, cnt, got, rel>>

\* w.Write of what WriteTo took: the tail ("rest"), or a slice of a
\* batch from the queue.
WWrite(c) ==
    /\ pc[c] \in {"write", "write-rest"}
    /\ Hand(c, tail[c]) /\ tail' = [tail EXCEPT ![c] = <<>>]
    /\ pc' = [pc EXCEPT ![c] = IF pc[c] = "write" THEN "rel" ELSE "rel-rest"]
    /\ UNCHANGED <<q, closed, dl, next, rest, cur, lock, res, ref, rel>>

\* releaseData of what it wrote. The code before, for the tail: s.rest
\* cut and s.cur released only now, whatever a Read did with them meanwhile.
WRel(c) ==
    /\ pc[c] \in {"rel", "rel-rest"}
    /\ IF pc[c] = "rel-rest" /\ Mutant = "writeto-no-lock"
       THEN /\ rest' = <<>> /\ cur' = None /\ Release(Ids(cur))
       ELSE /\ Release(ref[c]) /\ UNCHANGED <<rest, cur>>
    /\ ref' = [ref EXCEPT ![c] = {}]
    /\ pc' = [pc EXCEPT ![c] = "batch"]
    /\ UNCHANGED <<q, closed, dl, next, lock, res, cnt, got, tail>>

\* PopBatch: under the queue's lock, it waits until a slice or the end
\* (WriteTo ignores the read deadline); a batch of k slices (the model's:
\* one; a trace's: its own).
WPopOf(c, k) ==
    /\ pc[c] = "batch" /\ (q # <<>> \/ closed) /\ k <= Len(q)
    /\ IF q # <<>>
       THEN /\ tail' = [tail EXCEPT ![c] = Flat(SubSeq(q, 1, k))] /\ q' = SubSeq(q, k + 1, Len(q))
            /\ ref' = [ref EXCEPT ![c] = {SliceOf(Head(q[i])) : i \in 1..k}]
            /\ pc' = [pc EXCEPT ![c] = "write"] /\ UNCHANGED res
       ELSE /\ res' = [res EXCEPT ![c] = "eof"] /\ pc' = [pc EXCEPT ![c] = "done"]
            /\ UNCHANGED <<q, tail, ref>>
    /\ UNCHANGED <<closed, dl, next, rest, cur, lock, cnt, got, rel>>
WPop(c) == WPopOf(c, IF q = <<>> THEN 0 ELSE 1)

Next ==
    \/ Push \/ Close \/ Fire \/ Clear
    \/ \E r \in Readers : Begin(r) \/ Pop(r) \/ Decide(r) \/ Copy(r) \/ Cut(r) \/ End(r)
    \/ \E c \in Readers : WTake(c) \/ WWrite(c) \/ WRel(c) \/ WPop(c)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ closed \in BOOLEAN /\ dl \in BOOLEAN /\ next \in 1..(2 * Slices + 1)
    /\ cur \in 0..Slices /\ lock \in Readers \cup {None}
    /\ pc \in [Readers -> {"idle", "pop", "decide", "copy", "cut", "end", "done",
                           "write", "write-rest", "rel", "rel-rest", "batch"}]
    /\ res \in [Readers -> {"", "data", "timeout", "eof"}]
    /\ cnt \in [Bytes -> Nat]
    /\ ref \in [Readers -> SUBSET (1..Slices)]
    /\ rel \in [1..Slices -> Nat]

\* Bytes sent and in no reader's hands yet: queued, a Read's unread tail,
\* or taken by WriteTo and not yet written.
Queued == UNION {Range(q[i]) : i \in DOMAIN q}
InHands == Range(rest) \cup UNION {Range(tail[r]) : r \in Readers}
Held == Queued \cup InHands

\* Each byte the peer sent goes to one reader at most once.
Once == \A b \in Bytes : cnt[b] <= 1

\* Each reader is handed its bytes in the order sent.
InOrder == \A r \in Readers : \A i \in 1..(Len(got[r]) - 1) : got[r][i] < got[r][i + 1]

\* No byte sent is lost: it was handed to a reader or is still held.
NoLoss == LET h == Held IN \A b \in 1..(next - 1) : cnt[b] >= 1 \/ b \in h

\* A slice goes back to the pool once, and never while a reader reads it.
ReleaseOnce == \A k \in 1..Slices : rel[k] <= 1
NoUseAfterRelease == \A r \in Readers : \A k \in ref[r] : rel[k] = 0

\* io.EOF only at the peer's end, once every byte it sent was read (or is
\* in another reader's hands: its unread tail, WriteTo's write): a
\* deadline, set or cleared, never reads as the end.
NoForgedEOF ==
    \A r \in Readers : res[r] = "eof" =>
        /\ closed
        /\ LET h == InHands IN \A b \in 1..(next - 1) : cnt[b] = 1 \/ b \in h

\* Witnesses (make spec-mutants: TLC must violate each).
WitnessTimeout == ~\E r \in Readers : res[r] = "timeout"
WitnessEOF == ~\E r \in Readers : res[r] = "eof"
WitnessBothRead == ~\A r \in Readers : res[r] = "data"
WitnessSharedTail == ~\E r \in Readers : pc[r] = "copy" /\ Len(rest) = 1
\* WriteTo takes a Read's tail (one byte: a batch is two), and both kinds
\* of reader are handed bytes.
WitnessWriteToTail == ~\E c \in Readers : pc[c] = "write-rest" /\ Len(tail[c]) = 1
WitnessBothKinds == ~(Copier # None /\ got[Copier] # <<>> /\ \E r \in Plain : got[r] # <<>>)
=============================================================================
