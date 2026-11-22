--------------------------- MODULE AutoWindow ---------------------------
(* One QUIC stream's receive window auto-tuned from W0 up to MaxW (lane
   LONGFAT, 2026-11-22; spec/README.md "Auto-tuned windows"). quiclink
   Config.MaxWindow sets the quic-go fork's MaxStreamReceiveWindow above
   its initial one; the fork's flow controller (flow_controller_base.go
   maybeAdjustWindowSize, flow_controller_stream.go) then doubles a
   stream's window at a window update when the reader took the last one
   fast (within a few RTTs: here the environment's choice, `fast`), never
   past MaxW, and advertises read + window (MAX_STREAM_DATA, RFC 9000
   sections 4.1-4.2: a receiver never lowers a limit it advertised).

   One stream of Total bytes; the sender sends while below the limit it
   heard; the network carries bytes in order (a stream's bytes are
   reassembled in order) and window updates in any order (the sender keeps
   the largest); the app reads one byte at a time; the receiver sends an
   update once a quarter of its window was read since its last one.

   What the design guarantees: the sender never sends past the receiver's
   limit (Credit), the receiver buffers at most MaxW unread bytes whatever
   the window grew to (MemoryBound: quiclink's per-session memory bound),
   the window never exceeds MaxW and the limit never falls (Monotone), and
   every byte is read (AllRead).

   Mutants (scripts/spec.sh mutants): "no-clamp" (the window doubles past
   MaxW), "grant-unread" (an update counts bytes received, not read),
   "shrink" (a slow epoch halves the window: the limit can fall below what
   was sent), "stale-update" (an update re-sends the old limit). *)
EXTENDS Naturals, FiniteSets

CONSTANTS Total, W0, MaxW, Mutant

ASSUME W0 >= 1 /\ MaxW >= W0 /\ Total >= 1

VARIABLES
    sent,    \* bytes the sender sent
    heard,   \* the largest limit the sender heard (MAX_STREAM_DATA)
    recv,    \* bytes arrived at the receiver
    read,    \* bytes the app read
    win,     \* the receiver's window
    limit,   \* the receiver's advertised limit: read + win at its last update
    upd      \* updates in flight (values)

vars == <<sent, heard, recv, read, win, limit, upd>>

TypeOK ==
    /\ sent \in 0..Total /\ recv \in 0..Total /\ read \in 0..Total
    /\ heard \in Nat /\ limit \in Nat /\ win \in Nat /\ upd \subseteq Nat

Init ==
    /\ sent = 0 /\ recv = 0 /\ read = 0
    /\ win = W0 /\ limit = W0 /\ heard = W0 /\ upd = {}

Send ==
    /\ sent < Total /\ sent < heard
    /\ sent' = sent + 1
    /\ UNCHANGED <<heard, recv, read, win, limit, upd>>

Arrive ==
    /\ recv < sent
    /\ recv' = recv + 1
    /\ UNCHANGED <<sent, heard, read, win, limit, upd>>

Read ==
    /\ read < recv
    /\ read' = read + 1
    /\ UNCHANGED <<sent, heard, recv, win, limit, upd>>

\* quic-go: an update once at most three quarters of the window remain
\* (protocol.WindowUpdateThreshold 0.25).
UpdateDue == 4 * (read + win - limit) >= win

NewWin(fast) ==
    CASE fast /\ Mutant = "no-clamp" -> 2 * win
      [] fast -> IF 2 * win < MaxW THEN 2 * win ELSE MaxW
      [] Mutant = "shrink" /\ win > W0 -> win \div 2
      [] OTHER -> win

Update(fast) ==
    /\ UpdateDue
    /\ win' = NewWin(fast)
    /\ limit' = CASE Mutant = "grant-unread" -> recv + win'
                  [] Mutant = "stale-update" -> limit
                  [] OTHER -> read + win'
    /\ limit' > limit \/ Mutant \in {"shrink", "stale-update"}
    /\ upd' = upd \cup {limit'}
    /\ UNCHANGED <<sent, heard, recv, read>>

Hear(u) ==
    /\ u \in upd
    /\ heard' = IF u > heard THEN u ELSE heard
    /\ upd' = upd \ {u}
    /\ UNCHANGED <<sent, recv, read, win, limit>>

\* Every byte read: the stream is over.
Done == read = Total /\ UNCHANGED vars

Next ==
    \/ Send \/ Arrive \/ Read
    \/ \E fast \in BOOLEAN : Update(fast)
    \/ \E u \in upd : Hear(u)
    \/ Done

Spec == Init /\ [][Next]_vars
    /\ WF_vars(Send) /\ WF_vars(Arrive) /\ WF_vars(Read)
    /\ WF_vars(\E fast \in BOOLEAN : Update(fast))
    /\ \A u \in 0..(Total + 2 * MaxW) : WF_vars(Hear(u))

\* The sender never sends past what the receiver advertised.
Credit == sent <= heard /\ \A u \in upd : u <= limit /\ heard <= limit /\ sent <= limit
\* The receiver buffers at most MaxW unread bytes.
MemoryBound == recv - read <= MaxW
WindowBound == win <= MaxW
\* Every byte is read.
AllRead == <>(read = Total)
\* A limit once advertised never falls.
Monotone == [][limit' >= limit]_vars

\* Witnesses (make spec-mutants: TLC must violate each): the window grew
\* to MaxW; the receiver held MaxW unread bytes (the bound is tight).
WitnessGrown == ~(win = MaxW)
WitnessFull == ~(recv - read = MaxW)
=============================================================================
