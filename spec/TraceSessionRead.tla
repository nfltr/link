--------------------------- MODULE TraceSessionRead ---------------------------
(* Trace validation of Session.Read and WriteTo (spec/README.md "Session   *)
(* reads", lane LINKTRACE): the steps one session's readers recorded      *)
(* (-tags linktrace, LINKTRACE_READ; trace_read_on.go) must be steps of     *)
(* SessionRead.tla, in order, with the trace's sizes: its slices          *)
(* (PushOf, and Grow for Mux.data's Append), its copies (CopyOf) and its   *)
(* batches (WPopOf). Every step is logged, so nothing happens between two   *)
(* lines; a line that maps to no step rejects the trace. The trace is     *)
(* accepted when TLC takes its every line (TraceAccepted) with every       *)
(* invariant of SessionRead.tla held at each.                              *)
EXTENDS TraceLog, SessionRead, TLC, FiniteSets

Lines(evs) == {i \in DOMAIN Log : Log[i].ev \in evs}
Grows == SelectSeq(Log, LAMBDA e : e.ev \in {"Push", "Append"})
\* The trace's constants: its readers (WriteTo's are negative), its slices.
TraceReaders == {Log[i].r : i \in Lines({"Begin", "WTake"})}
TraceCopier == IF Lines({"WTake"}) = {} THEN 0 ELSE Log[CHOOSE i \in Lines({"WTake"}) : TRUE].r
TraceSlices == Cardinality(Lines({"Push"}))
\* byte b's slice, in send order: a Push starts the next one
SliceSeq ==
    LET f[i \in 0..Len(Grows)] ==
          IF i = 0 THEN <<0, <<>>>>
          ELSE LET p == f[i - 1] \* once: TLC does not memoize f
                   k == p[1] + (IF Grows[i].ev = "Push" THEN 1 ELSE 0)
               IN <<k, p[2] \o [j \in 1..Grows[i].n |-> k]>>
    IN f[Len(Grows)][2]
TraceBytes == 1..Len(SliceSeq)
TraceSliceOf(b) == SliceSeq[b]

VARIABLE l
traceVars == <<vars, l>>

E == Log[l]

Step ==
    CASE E.ev = "Push" -> PushOf(E.n)
      [] E.ev = "Append" -> Grow(E.n)
      [] E.ev = "Close" -> Close
      [] E.ev = "Fire" -> Fire
      [] E.ev = "Clear" -> Clear
      [] E.ev = "Begin" -> Begin(E.r) /\ Len(rest) = E.n
      \* WriteTo's pops: PopBatch, or writeOwned's Pop (one slice)
      [] E.ev = "Pop" /\ E.r = Copier -> WPopOf(E.r, E.k) /\ Len(tail'[E.r]) = E.n /\ (E.res = "eof") = (E.k = 0)
      [] E.ev = "Pop" -> Pop(E.r) /\ CASE E.res = "data" -> pc'[E.r] = "copy" /\ Len(rest') = E.n
                                       [] OTHER -> res'[E.r] = E.res
      [] E.ev = "Copy" -> CopyOf(E.r, E.n) /\ (E.rel = 1) = (rel' # rel)
      [] E.ev = "End" -> End(E.r) /\ res[E.r] = E.res
      [] E.ev = "WTake" -> WTake(E.r) /\ Len(tail'[E.r]) = E.n
      [] E.ev = "WWrite" -> Len(tail[E.r]) = E.n /\ WWrite(E.r)
      [] E.ev = "WRel" -> WRel(E.r)
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ l = 1
TraceNext == l <= Len(Log) /\ Step /\ l' = l + 1

\* The longest behaviour TLC found took every line: the trace is a
\* behaviour of SessionRead.
TraceAccepted ==
    LET d == TLCGet("stats").diameter IN
    IF d - 1 = Len(Log) THEN TRUE
    ELSE Print(<<"trace rejected at line", d, Log[d]>>, FALSE)
=============================================================================
