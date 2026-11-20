---------------------------- MODULE RelayedClose ----------------------------
(* A relayed session's Close (spec/README.md "RelayedClose.tla", lane      *)
(* RELAYCLOSE): an agent A writes K bytes on a session of its link to the  *)
(* relay, closes it and its program exits; the relay's joint J (link.Join) *)
(* passes the bytes and the end to the client C on C's own connection.     *)
(* Bytes are counts; each connection is one FIFO.                          *)
(*                                                                         *)
(* The design. A's Close returns clean once J's Mux read every byte and    *)
(* A's FIN: the CloseWrite's ping (Session.CloseWriteSync) answered, as    *)
(* peerlink.Join syncs (Join.tla); an error once A's connection ended     *)
(* first, never a wait past it. After that A may exit (its connection      *)
(* ends, its unread frames lost) or its stop may reset the session: J then *)
(* holds the whole stream, and a session whose peer's FIN was read before  *)
(* the session failed (a reset, the connection's end) reads its bytes and  *)
(* then io.EOF, at J as at C (LinkSession.tla GEnds: the first end a      *)
(* reader learns is its end). J's direction A -> C passes the tail, then  *)
(* the FIN, then the failure (an ABORT, so C's writes fail): C reads every *)
(* byte and io.EOF. J's direction C -> A, whose write onto A's failed     *)
(* session fails, leaves C's abort to the other direction while that one  *)
(* still runs (held), so the abort never overtakes the tail.               *)
EXTENDS Naturals, Sequences

CONSTANTS
    K,       \* A's bytes
    Mutant   \* "none"; "close-before-ack": Close returns once the FIN is
             \* buffered (the code before: an accepted relayed Conn's Close
             \* returned at once); "ack-lost-hang": Close waits for the
             \* ping's answer only, not for the connection's end;
             \* "fail-drops-fin": a failure after the peer's FIN makes
             \* the reads end with the error (the code before: C read every
             \* byte, then a reset); "abort-at-write-fail": J's failed write
             \* toward A aborts C at once, before the tail (the code before)

VARIABLES
    aw,      \* bytes A wrote
    ret,     \* A's Close: "no" (not called), "wait", "clean", "err"
    rst,     \* A's stop reset the session after its Close returned
    exited,  \* A's program exited
    toJ,     \* frames A -> J: "d", "fin", "ping", "rst"
    fromJ,   \* frames J -> A: "pong"
    lost1,   \* A's connection to the relay ended
    gone,    \* the relay crashed
    jq,      \* A's bytes J's Mux read, not yet written to C
    jfin,    \* J's Mux read A's FIN
    jfail,   \* A's session failed at J (A's reset, its connection's end)
    d1,      \* J: A -> C, "run" or "done"
    d0,      \* J: C -> A, "run", "held" (its abort of C waits for d1), "done"
    cw,      \* C wrote a byte (toward A)
    cabort,  \* J aborted C's session: J's writes onto it fail
    toC,     \* frames J -> C: "d", "fin", "abort"
    cq,      \* bytes C's Mux read, not yet read by C
    cfin,    \* C's Mux read the FIN
    cfail,   \* C's session failed (the abort, its connection's end)
    crd,     \* bytes C read
    cend     \* how C's reads ended: "", "eof", "err"

vars == <<aw, ret, rst, exited, toJ, fromJ, lost1, gone, jq, jfin, jfail, d1, d0,
          cw, cabort, toC, cq, cfin, cfail, crd, cend>>
aVars == <<aw, ret, rst, exited>>
jVars == <<jq, jfin, jfail, d1, d0, cabort>>
cVars == <<cw, cq, cfin, cfail, crd, cend>>

Init ==
    /\ aw = 0 /\ ret = "no" /\ rst = FALSE /\ exited = FALSE
    /\ toJ = <<>> /\ fromJ = <<>> /\ lost1 = FALSE /\ gone = FALSE
    /\ jq = 0 /\ jfin = FALSE /\ jfail = FALSE /\ d1 = "run" /\ d0 = "run"
    /\ cw = FALSE /\ cabort = FALSE /\ toC = <<>>
    /\ cq = 0 /\ cfin = FALSE /\ cfail = FALSE /\ crd = 0 /\ cend = ""

\* A reader's end once its queue drained: io.EOF when the peer's FIN came
\* (before any failure: frames are read in order), else the failure.
ReadsEof(fin, fail) == fin /\ (Mutant # "fail-drops-fin" \/ ~fail)

\* ---- A ----

AWrite ==
    /\ aw < K /\ ret = "no" /\ ~lost1
    /\ aw' = aw + 1 /\ toJ' = Append(toJ, "d")
    /\ UNCHANGED <<ret, rst, exited, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars

\* Close: the FIN with its ping (CloseWriteSync), then wait for the pong.
AClose ==
    /\ aw = K /\ ret = "no" /\ ~lost1
    /\ toJ' = toJ \o <<"fin", "ping">>
    /\ ret' = IF Mutant = "close-before-ack" THEN "clean" ELSE "wait"
    /\ UNCHANGED <<aw, rst, exited, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars

APong ==
    /\ fromJ # <<>> /\ ~lost1
    /\ fromJ' = Tail(fromJ)
    /\ ret' = IF ret = "wait" THEN "clean" ELSE ret
    /\ UNCHANGED <<aw, rst, exited, toJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars

\* The connection's end ends the wait (waitSync: c.Done; a dead relay is
\* found by the link's liveness).
ADown ==
    /\ lost1 /\ ret = "wait" /\ Mutant # "ack-lost-hang"
    /\ ret' = "err"
    /\ UNCHANGED <<aw, rst, exited, toJ, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars

\* The agent's stop resets the session after its Close returned.
AReset ==
    /\ ret \in {"clean", "err"} /\ ~rst /\ ~lost1
    /\ rst' = TRUE /\ toJ' = Append(toJ, "rst")
    /\ UNCHANGED <<aw, ret, exited, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars

\* The program exits once Close returned: its connection ends, the frames
\* J had not read are lost.
Exit ==
    /\ ret \in {"clean", "err"} /\ ~exited
    /\ exited' = TRUE /\ lost1' = TRUE /\ toJ' = <<>> /\ fromJ' = <<>>
    /\ UNCHANGED <<aw, ret, rst, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars

\* The network cuts A's connection.
Cut ==
    /\ ~lost1
    /\ lost1' = TRUE /\ toJ' = <<>> /\ fromJ' = <<>>
    /\ UNCHANGED <<aw, ret, rst, exited, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars

\* ---- J ----

JRecv ==
    /\ ~gone /\ toJ # <<>>
    /\ toJ' = Tail(toJ)
    /\ CASE Head(toJ) = "d"    -> jq' = jq + 1 /\ UNCHANGED <<jfin, jfail, fromJ>>
         [] Head(toJ) = "fin"  -> jfin' = TRUE /\ UNCHANGED <<jq, jfail, fromJ>>
         [] Head(toJ) = "ping" -> fromJ' = Append(fromJ, "pong") /\ UNCHANGED <<jq, jfin, jfail>>
         [] Head(toJ) = "rst"  -> jfail' = TRUE /\ UNCHANGED <<jq, jfin, fromJ>>
    /\ UNCHANGED aVars /\ UNCHANGED <<lost1, gone, d1, d0, cabort, toC>> /\ UNCHANGED cVars

\* J's Mux finds A's connection ended (every frame left was lost).
JDown ==
    /\ ~gone /\ lost1 /\ toJ = <<>> /\ ~jfail
    /\ jfail' = TRUE
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, jq, jfin, d1, d0, cabort, toC>> /\ UNCHANGED cVars

abortC == IF cabort THEN UNCHANGED <<toC, cabort>> ELSE toC' = Append(toC, "abort") /\ cabort' = TRUE

\* d1: a byte of A onto C; after C's abort the write fails (lost).
D1Copy ==
    /\ ~gone /\ d1 = "run" /\ jq > 0
    /\ jq' = jq - 1
    /\ IF cabort THEN d1' = "done" /\ UNCHANGED toC ELSE toC' = Append(toC, "d") /\ UNCHANGED d1
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, jfin, jfail, d0, cabort>> /\ UNCHANGED cVars

\* d1's end: A's reads ended. A clean end passes the FIN on, then A's
\* failure (or d0's held one) as C's abort; a failed one aborts C.
D1End ==
    /\ ~gone /\ d1 = "run" /\ jq = 0 /\ (jfin \/ jfail)
    /\ d1' = "done"
    /\ IF ReadsEof(jfin, jfail)
       THEN IF jfail \/ d0 = "held"
            THEN /\ toC' = (IF cabort THEN toC ELSE toC \o <<"fin", "abort">>)
                 /\ cabort' = TRUE
            ELSE /\ toC' = (IF cabort THEN toC ELSE Append(toC, "fin"))
                 /\ UNCHANGED cabort
       ELSE abortC
    /\ d0' = IF d0 = "held" THEN "done" ELSE d0
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, jq, jfin, jfail>> /\ UNCHANGED cVars

\* d0: C's byte onto A's failed session fails; C's abort waits for d1.
D0Fail ==
    /\ ~gone /\ cw /\ d0 = "run" /\ jfail
    /\ IF d1 = "run" /\ Mutant # "abort-at-write-fail"
       THEN d0' = "held" /\ UNCHANGED <<toC, cabort>>
       ELSE d0' = "done" /\ abortC
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, jq, jfin, jfail, d1>> /\ UNCHANGED cVars

\* The relay crashes: both its connections end.
RelayGone ==
    /\ ~gone
    /\ gone' = TRUE /\ lost1' = TRUE /\ toJ' = <<>> /\ fromJ' = <<>> /\ toC' = <<>> /\ cfail' = TRUE
    /\ UNCHANGED aVars /\ UNCHANGED jVars /\ UNCHANGED <<cw, cq, cfin, crd, cend>>

\* ---- C ----

CWrite ==
    /\ ~cw /\ cend = ""
    /\ cw' = TRUE
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, toC>> /\ UNCHANGED jVars
    /\ UNCHANGED <<cq, cfin, cfail, crd, cend>>

CRecv ==
    /\ toC # <<>>
    /\ toC' = Tail(toC)
    /\ CASE Head(toC) = "d"     -> cq' = cq + 1 /\ UNCHANGED <<cfin, cfail>>
         [] Head(toC) = "fin"   -> cfin' = TRUE /\ UNCHANGED <<cq, cfail>>
         [] Head(toC) = "abort" -> cfail' = TRUE /\ UNCHANGED <<cq, cfin>>
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<cw, crd, cend>>

CRead ==
    /\ cq > 0 /\ cend = ""
    /\ cq' = cq - 1 /\ crd' = crd + 1
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, toC>> /\ UNCHANGED jVars /\ UNCHANGED <<cw, cfin, cfail, cend>>

CEnd ==
    /\ cend = "" /\ cq = 0 /\ (cfin \/ cfail)
    /\ cend' = IF ReadsEof(cfin, cfail) THEN "eof" ELSE "err"
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, toC>> /\ UNCHANGED jVars /\ UNCHANGED <<cw, cq, cfin, cfail, crd>>

Next ==
    \/ AWrite \/ AClose \/ APong \/ ADown \/ AReset \/ Exit \/ Cut
    \/ JRecv \/ JDown \/ D1Copy \/ D1End \/ D0Fail \/ RelayGone
    \/ CWrite \/ CRecv \/ CRead \/ CEnd

\* A, J and C take their own steps; the program's exit, its stop's reset,
\* the network's cut, the relay's crash and C's write are the
\* environment's (no fairness).
Fairness ==
    /\ WF_vars(AWrite) /\ WF_vars(AClose) /\ WF_vars(APong) /\ WF_vars(ADown)
    /\ WF_vars(JRecv) /\ WF_vars(JDown) /\ WF_vars(D1Copy) /\ WF_vars(D1End) /\ WF_vars(D0Fail)
    /\ WF_vars(CRecv) /\ WF_vars(CRead) /\ WF_vars(CEnd)

Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK ==
    /\ aw \in 0..K /\ ret \in {"no", "wait", "clean", "err"}
    /\ d1 \in {"run", "done"} /\ d0 \in {"run", "held", "done"}
    /\ cend \in {"", "eof", "err"} /\ crd \in 0..K /\ jq \in 0..K /\ cq \in 0..K

\* C reads io.EOF only after every byte A wrote.
NoSilentLoss == cend = "eof" => crd = K

\* A's Close returns clean only once the relay read every byte and the
\* FIN: a program that exits then loses nothing it wrote.
CloseTruth == ret = "clean" => jfin

\* After a clean Close, C reads every byte and io.EOF, whatever A does
\* next (exit, reset), unless the relay itself crashed.
TailDelivered == (ret = "clean") ~> ((cend = "eof" /\ crd = K) \/ gone)

\* A Close ends: never a wait past its connection's end.
CloseEnds == (ret = "wait") ~> (ret \in {"clean", "err"})

\* Witnesses (make spec-mutants: TLC must violate each): a clean Close,
\* the program's exit, and C's clean end; C's FIN then its abort read as
\* io.EOF; d0's abort held for d1; a Close that ends with an error.
WitnessExitThenEof == ~(ret = "clean" /\ exited /\ cend = "eof")
WitnessFinThenAbort == ~(cfin /\ cfail /\ cend = "eof" /\ ~gone)
WitnessHeld == d0 # "held"
WitnessCloseErr == ret # "err"
=============================================================================
