------------------------------- MODULE MBTLive -------------------------------
(* Behaviours of Liveness for the model-based test (spec/README.md         *)
(* "Model-based tests"): each TLC -simulate walk, one state per line, to    *)
(* MBTDir/live-<n>.ndjson; liveness_mbt_test.go replays each FireAt step   *)
(* on keepAlive's decision (liveFire) and compares the timer and pingAt.  *)
EXTENDS Liveness, TLC, TLCExt, Json, Sequences

CONSTANT MBTDir

Row(x) == [a |-> x._action.name, now |-> x.now, rx |-> x.rx, pingAt |-> x.pingAt, looked |-> x.looked,
           timer |-> x.timer, dead |-> x.dead]
MBTDump == ndJsonSerialize(MBTDir \o "/live-" \o ToString(TLCGet("stats").traces) \o ".ndjson",
                           [i \in DOMAIN Trace |-> Row(Trace[i])])
=============================================================================
