------------------------- MODULE SessionRead -------------------------
(* Session.Read inside (lane LINKREAD, 2026-11-14; spec/README.md
   "Session reads"). Link.tla gives a session one reader and takes a Read
   as one pop of its queue; a read deadline changes nothing it models.
   This model is the Read itself: several goroutines reading one session
   at once (net.Conn allows it), the read deadline's flag (dlPast) set by
   its timer and cleared or moved by SetReadDeadline at any time, and the
   peer's slices and end arriving meanwhile.

   The code before had two bugs, both silent:
   - Read took the unread tail of the last slice (s.rest) with no lock:
     two readers copied the same bytes, and each cut the tail once, so
     bytes went twice and others never ("no-read-lock").
   - PopUnless gave up when the flag was set, and Read then looked at the
     flag again, outside the queue's lock, to tell a deadline from the
     end: a flag cleared in between read as io.EOF on a live session
     ("flag-outside-lock").

   The design: Read holds the session's read lock (rmu) from its look at
   s.rest to the end of its copy; PopUnless tells why it gave up (the
   flag or the queue's end), decided under the queue's lock in one look.

   One session; the peer pushes Slices slices of two bytes each, then may
   end; each Read takes one byte. *)
EXTENDS Naturals, Sequences

CONSTANTS
    Readers, \* goroutines reading the session
    Slices,  \* slices the peer pushes, two bytes each
    Mutant   \* "none", "no-read-lock", "flag-outside-lock"

None == 0
Bytes == 1..(2 * Slices)

VARIABLES
    q,      \* the session's queue: slices (sequences of byte ids)
    closed, \* the queue closed: the peer's end
    dl,     \* dlPast: the read deadline passed
    next,   \* the next byte id the peer sends
    rest,   \* s.rest: the unread tail of the last slice popped
    lock,   \* the reader holding rmu, or None
    pc,     \* [Readers -> step]
    res,    \* [Readers -> {"", "data", "timeout", "eof"}]: a Read's result
    cnt     \* [Bytes -> Nat]: times each byte was handed to a reader

vars == <<q, closed, dl, next, rest, lock, pc, res, cnt>>

Init ==
    /\ q = <<>> /\ closed = FALSE /\ dl = FALSE /\ next = 1 /\ rest = <<>>
    /\ lock = None
    /\ pc = [r \in Readers |-> "idle"]
    /\ res = [r \in Readers |-> ""]
    /\ cnt = [b \in Bytes |-> 0]

Locked == Mutant # "no-read-lock"

\* The peer: a slice (Mux.data's Push), its end (the queue's Close).
Push ==
    /\ ~closed /\ next + 1 \in Bytes
    /\ q' = Append(q, <<next, next + 1>>) /\ next' = next + 2
    /\ UNCHANGED <<closed, dl, rest, lock, pc, res, cnt>>
Close ==
    /\ ~closed /\ closed' = TRUE
    /\ UNCHANGED <<q, dl, next, rest, lock, pc, res, cnt>>

\* The deadline: its timer fires (or SetReadDeadline sets a past one) and
\* Wake; SetReadDeadline clears it or moves it ahead.
Fire == ~dl /\ dl' = TRUE /\ UNCHANGED <<q, closed, next, rest, lock, pc, res, cnt>>
Clear == dl /\ dl' = FALSE /\ UNCHANGED <<q, closed, next, rest, lock, pc, res, cnt>>

\* Read's start: rmu taken, then s.rest looked at.
Begin(r) ==
    /\ pc[r] = "idle"
    /\ IF Locked THEN lock = None /\ lock' = r ELSE UNCHANGED lock
    /\ pc' = [pc EXCEPT ![r] = IF rest # <<>> THEN "copy" ELSE "pop"]
    /\ UNCHANGED <<q, closed, dl, next, rest, res, cnt>>

\* PopUnless: under the queue's lock, it waits (not enabled) until the
\* flag, a slice or the end; the design decides there, in one look; the
\* code before only gave up there and decided later (Decide).
Pop(r) ==
    /\ pc[r] = "pop" /\ (dl \/ q # <<>> \/ closed)
    /\ IF Mutant = "flag-outside-lock"
       THEN IF ~dl /\ q # <<>>
            THEN /\ rest' = Head(q) /\ q' = Tail(q) /\ pc' = [pc EXCEPT ![r] = "copy"]
                 /\ UNCHANGED res
            ELSE /\ pc' = [pc EXCEPT ![r] = "decide"] /\ UNCHANGED <<q, rest, res>>
       ELSE CASE dl -> /\ res' = [res EXCEPT ![r] = "timeout"] /\ pc' = [pc EXCEPT ![r] = "end"]
                       /\ UNCHANGED <<q, rest>>
              [] q # <<>> -> /\ rest' = Head(q) /\ q' = Tail(q) /\ pc' = [pc EXCEPT ![r] = "copy"]
                             /\ UNCHANGED res
              [] OTHER -> /\ res' = [res EXCEPT ![r] = "eof"] /\ pc' = [pc EXCEPT ![r] = "end"]
                          /\ UNCHANGED <<q, rest>>
    /\ UNCHANGED <<closed, dl, next, lock, cnt>>

\* The code before: Read looks at the flag again, the queue unlocked.
Decide(r) ==
    /\ pc[r] = "decide"
    /\ res' = [res EXCEPT ![r] = IF dl THEN "timeout" ELSE "eof"]
    /\ pc' = [pc EXCEPT ![r] = "end"]
    /\ UNCHANGED <<q, closed, dl, next, rest, lock, cnt>>

\* copy(b, s.rest) of one byte. Under rmu, with s.rest = s.rest[n:]; with
\* no lock that cut is a step of its own (Cut), from s.rest as it is then.
Copy(r) ==
    /\ pc[r] = "copy"
    /\ IF rest = <<>>
       THEN /\ res' = [res EXCEPT ![r] = "data"] /\ UNCHANGED cnt   \* n = 0
       ELSE /\ cnt' = [cnt EXCEPT ![Head(rest)] = @ + 1] /\ res' = [res EXCEPT ![r] = "data"]
    /\ IF Locked
       THEN /\ rest' = IF rest = <<>> THEN rest ELSE Tail(rest)
            /\ pc' = [pc EXCEPT ![r] = "end"]
       ELSE /\ pc' = [pc EXCEPT ![r] = "cut"] /\ UNCHANGED rest
    /\ UNCHANGED <<q, closed, dl, next, lock>>
Cut(r) ==
    /\ pc[r] = "cut"
    /\ rest' = IF rest = <<>> THEN rest ELSE Tail(rest)
    /\ pc' = [pc EXCEPT ![r] = "end"]
    /\ UNCHANGED <<q, closed, dl, next, lock, res, cnt>>

\* Read returns, rmu let go. A reader that read the end stops; one that
\* read a byte or the deadline reads again.
End(r) ==
    /\ pc[r] = "end"
    /\ lock' = IF lock = r THEN None ELSE lock
    /\ pc' = [pc EXCEPT ![r] = IF res[r] = "eof" THEN "done" ELSE "idle"]
    /\ UNCHANGED <<q, closed, dl, next, rest, res, cnt>>

Next ==
    \/ Push \/ Close \/ Fire \/ Clear
    \/ \E r \in Readers : Begin(r) \/ Pop(r) \/ Decide(r) \/ Copy(r) \/ Cut(r) \/ End(r)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ closed \in BOOLEAN /\ dl \in BOOLEAN /\ next \in 1..(2 * Slices + 1)
    /\ lock \in Readers \cup {None}
    /\ pc \in [Readers -> {"idle", "pop", "decide", "copy", "cut", "end", "done"}]
    /\ res \in [Readers -> {"", "data", "timeout", "eof"}]
    /\ cnt \in [Bytes -> Nat]

\* Each byte the peer sent goes to one reader at most once.
Once == \A b \in Bytes : cnt[b] <= 1

\* io.EOF only at the peer's end, once every byte it sent was read: a
\* deadline, set or cleared, never reads as the end.
NoForgedEOF ==
    \A r \in Readers : res[r] = "eof" =>
        /\ closed
        /\ \A b \in 1..(next - 1) : cnt[b] = 1

\* Witnesses (make spec-mutants: TLC must violate each).
WitnessTimeout == ~\E r \in Readers : res[r] = "timeout"
WitnessEOF == ~\E r \in Readers : res[r] = "eof"
WitnessBothRead == ~\A r \in Readers : res[r] = "data"
WitnessSharedTail == ~\E r \in Readers : pc[r] = "copy" /\ Len(rest) = 1
=============================================================================
