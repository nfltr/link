-------------------------- MODULE TraceAutoWindow --------------------------
(* Trace validation of one QUIC stream direction's auto-tuned window       *)
(* against AutoWindow.tla (spec/README.md "Auto-tuned windows", lane       *)
(* LINKLEFT): the lines quiclink's recorder wrote for it (-tags linktrace, *)
(* LINKTRACE_AUTOWIN; peerlink/quiclink/trace_autowin_on.go, from both     *)
(* ends' qlog events and the receiver's reads), in order, must be          *)
(* AutoWindow steps at the code's grain: a STREAM frame sent or received   *)
(* (its end), a read of n bytes, a MAX_STREAM_DATA sent or heard.          *)
(*                                                                          *)
(* The receiver's reads at an update are hidden: the fork computes an      *)
(* update's limit (its reads + its window) when it packs the packet, the   *)
(* recorder logs a read after the stream counted it. With one reader per   *)
(* stream, the reads counted at an update (r) lie between the last         *)
(* update's and both what arrived and the reads logged so far plus the     *)
(* next one's (the one that may have been under way). An update is then    *)
(* accepted when one of AutoWindow's windows (kept, or doubled up to MaxW) *)
(* gives its limit from such an r at which an update was due              *)
(* (UpdateDue). A MAX_STREAM_DATA of a value sent before is a resend (a    *)
(* lost frame's, kept as it was), one of 0 the fork's queued update no     *)
(* longer due: neither changes the state. Every invariant of AutoWindow    *)
(* holds at each line; accepted when TLC reaches the trace's end           *)
(* (TraceUnfinished violated).                                              *)
EXTENDS TraceLog, AutoWindow, Integers, Sequences, TLC

ASSUME Log[1].ev = "Up"
TraceW0 == Log[1].n
TraceMaxW == Log[1].max
\* TypeOK's bound on a stream's bytes: TLC's largest integer (a test's
\* streams are far shorter; the trace's own maximum costs a pass per state).
TraceTotal == 2147483647

VARIABLES l, lread

E == Log[l]

\* The n of the first Read line after line i (0: none).
RECURSIVE ReadAfter(_)
ReadAfter(i) ==
    IF i > Len(Log) THEN 0 ELSE IF Log[i].ev = "Read" THEN Log[i].n ELSE ReadAfter(i + 1)
NextRead == ReadAfter(l + 1)

\* Send: a STREAM frame ending at n (a resend ends at or below sent).
TSend ==
    /\ sent' = IF E.n > sent THEN E.n ELSE sent
    /\ UNCHANGED <<heard, recv, read, win, limit, upd, lread>>

\* Arrive: in order, only what was sent.
TData ==
    /\ E.n <= sent
    /\ recv' = IF E.n > recv THEN E.n ELSE recv
    /\ UNCHANGED <<sent, heard, read, win, limit, upd, lread>>

TRead ==
    /\ lread' = lread + E.n
    /\ UNCHANGED <<sent, heard, recv, read, win, limit, upd>>

\* Update(fast) at the hidden reads r (read: the last update's r).
TUpdate ==
    IF E.n = 0 \/ E.n \in upd
    THEN UNCHANGED <<sent, heard, recv, read, win, limit, upd, lread>>
    ELSE \E fast \in BOOLEAN :
        LET w == NewWin(fast)
            r == E.n - w
        IN /\ read <= r /\ r <= recv /\ r <= lread + NextRead
           /\ 4 * (r + win - limit) >= win
           /\ E.n > limit
           /\ win' = w /\ limit' = E.n /\ read' = r
           /\ upd' = upd \cup {E.n}
           /\ UNCHANGED <<sent, heard, recv, lread>>

\* Hear: a limit the receiver sent (or the fork's 0), the largest kept.
THear ==
    /\ E.n \in upd \cup {0}
    /\ heard' = IF E.n > heard THEN E.n ELSE heard
    /\ UNCHANGED <<sent, recv, read, win, limit, upd, lread>>

Step ==
    CASE E.ev = "Send" -> TSend
      [] E.ev = "Data" -> TData
      [] E.ev = "Read" -> TRead
      [] E.ev = "Update" -> TUpdate
      [] E.ev = "Hear" -> THear
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ l = 2 /\ lread = 0
TraceNext == l <= Len(Log) /\ Step /\ l' = l + 1

TraceUnfinished == l <= Len(Log)
=============================================================================
