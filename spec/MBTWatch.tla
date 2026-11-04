------------------------------- MODULE MBTWatch ------------------------------
(* Behaviours of QuicWatch for the model-based test (spec/README.md        *)
(* "Model-based tests"): each TLC -simulate walk, one state per line, to    *)
(* MBTDir/watch-<n>.ndjson; peerlink/quiclink/watch_mbt_test.go replays    *)
(* each WatchAt step on watchState.tick (with the idle start, idle) and    *)
(* compares seen, prev and the verdict.                                    *)
EXTENDS QuicWatch, TLC, TLCExt, Json, Sequences

CONSTANT MBTDir

Row(x) == [a |-> x._action.name, now |-> x.now, fresh |-> x.fresh, seen |-> x.seen,
           prev |-> x.prev, closed |-> x.closed, idle |-> IF x.ka >= 0 THEN x.ka ELSE x.rx]
\* the step being taken too (an ACTION_CONSTRAINT sees it primed), so a
\* walk's last step, the verdict, is in its file; named by what it
\* changed (only WatchAt moves prev; TLCGet("action") there is not it).
Last == [a |-> IF prev' # prev THEN "WatchAt" ELSE "step", now |-> now', fresh |-> fresh', seen |-> seen',
         prev |-> prev', closed |-> closed', idle |-> IF ka' >= 0 THEN ka' ELSE rx']
MBTDump == ndJsonSerialize(MBTDir \o "/watch-" \o ToString(TLCGet("stats").traces) \o ".ndjson",
                           [i \in DOMAIN Trace |-> Row(Trace[i])] \o <<Last>>)
=============================================================================
