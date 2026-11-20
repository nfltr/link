--------------------------- MODULE TraceSessionRead ---------------------------
(* Trace validation of Session.Read, WriteTo and Pump (spec/README.md     *)
(* "Session reads"; Pump's drain, a Read refused after it: lane           *)
(* LINKTRACE2).                                                           *)
(* Lane LINKTRACE: the steps one session's readers recorded              *)
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
\* The trace's constants: its readers (WriteTo's are negative, Pump's -1),
\* its slices, whether its copier is a Pump.
TraceReaders == {Log[i].r : i \in Lines({"Begin", "Refuse", "WTake", "Pump"})}
TraceCopier == IF Lines({"WTake", "Pump"}) = {} THEN 0 ELSE Log[CHOOSE i \in Lines({"WTake", "Pump"}) : TRUE].r
TracePumps == Lines({"Pump"}) # {}
TraceSlices == Cardinality(Lines({"Push"}))
\* byte b's slice: a Push or Append line carries the slices (s) and bytes
\* (b) pushed so far, so no recursion over the trace (TLC's stack)
TraceBytes == 1..(IF Grows = <<>> THEN 0 ELSE Grows[Len(Grows)].b)
SliceOfF == [b \in TraceBytes |->
               LET i == CHOOSE i \in DOMAIN Grows : Grows[i].b - Grows[i].n < b /\ b <= Grows[i].b
               IN Grows[i].s]
TraceSliceOf(b) == SliceOfF[b]

VARIABLE l
traceVars == <<vars, l>>

E == Log[l]

Step ==
    CASE E.ev = "Push" -> PushOf(E.n) /\ next' = E.b + 1
      [] E.ev = "Append" -> Grow(E.n) /\ next' = E.b + 1
      [] E.ev = "Close" -> Close
      [] E.ev = "Fire" -> Fire
      [] E.ev = "Clear" -> Clear
      [] E.ev = "Begin" -> Begin(E.r) /\ Len(rest) = E.n
      [] E.ev = "Refuse" -> Refuse(E.r)
      [] E.ev = "Pump" -> PumpOn(E.r)
      \* WriteTo's pops: PopBatch, or writeOwned's Pop (one slice); Pump's
      \* TryPopBatch
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
