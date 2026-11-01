------------------------------- MODULE TraceLog -------------------------------
(* The recorded trace, read once: TLC evaluates a module's constant          *)
(* definitions before those of the modules extending it, so Trace.tla's     *)
(* definitions over Log see the value instead of reading the file again.   *)
EXTENDS Json

CONSTANT TraceFile

Log == ndJsonDeserialize(TraceFile)
=============================================================================
