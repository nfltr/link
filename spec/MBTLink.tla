------------------------------- MODULE MBTLink -------------------------------
(* Behaviours of Link for the model-based test (spec/README.md "Model-based *)
(* tests"): TLC -simulate walks MCLink's configurations at random and this  *)
(* action constraint writes each walk, one state per line, to              *)
(* MBTDir/link-<n>.ndjson: the action that led to it (name and arguments),  *)
(* the credit state (cr, pend, gpend) and the type of the frame at the     *)
(* head of each side's queue (what a Recv reads). credit_mbt_test.go       *)
(* replays each step on two Credits and compares their state with cr.      *)
EXTENDS MCLink, TLCExt, Json

CONSTANT MBTDir

Head1(q) == IF q = <<>> THEN [t |-> "", s |-> 0] ELSE [t |-> Head(q).t, s |-> Head(q).s]
Row(x) == [a |-> x._action.name, c |-> IF "context" \in DOMAIN x._action THEN x._action.context ELSE <<>>,
           cr |-> x.cr, pend |-> x.pend, gpend |-> x.gpend,
           h |-> [p \in Sides |-> Head1(x.out[p])]]
Steps == SelectSeq(Trace, LAMBDA x : x._action.name # "Terminated")
MBTDump == ndJsonSerialize(MBTDir \o "/link-" \o ToString(TLCGet("stats").traces) \o ".ndjson",
                           [i \in DOMAIN Steps |-> Row(Steps[i])])
=============================================================================
