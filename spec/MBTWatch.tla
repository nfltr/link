------------------------------- MODULE MBTWatch ------------------------------
(* Behaviours of QuicWatch for the model-based test (spec/README.md        *)
(* "Model-based tests"): each TLC -simulate walk, one state per line, to    *)
(* MBTDir/watch-<n>.ndjson; peerlink/quiclink/watch_mbt_test.go replays    *)
(* each WatchAt step on watchState.tick (with the idle start, idle) and    *)
(* each Look on watchState.look, and compares seen, prev and the verdict   *)
(* (look: its first look, closed: its second).                             *)
EXTENDS QuicWatch, TLC, TLCExt, Json, Sequences

CONSTANT MBTDir

Row(x) == [a |-> x._action.name, now |-> x.now, fresh |-> x.fresh, seen |-> x.seen,
           prev |-> x.prev, closed |-> x.closed, idle |-> IF x.ka >= 0 THEN x.ka ELSE x.rx, look |-> x.look]
\* the step being taken too (an ACTION_CONSTRAINT sees it primed), so a
\* walk's last step, the verdict, is in its file; named by what it
\* changed (only WatchAt moves prev, only Look ends a look; TLCGet("action")
\* there is not it).
Last == [a |-> IF prev' # prev THEN "WatchAt" ELSE IF look >= 0 /\ look' < 0 THEN "Look" ELSE "step",
         now |-> now', fresh |-> fresh', seen |-> seen',
         prev |-> prev', closed |-> closed', idle |-> IF ka' >= 0 THEN ka' ELSE rx', look |-> look']
MBTDump == ndJsonSerialize(MBTDir \o "/watch-" \o ToString(TLCGet("stats").traces) \o ".ndjson",
                           [i \in DOMAIN Trace |-> Row(Trace[i])] \o <<Last>>)
=============================================================================
