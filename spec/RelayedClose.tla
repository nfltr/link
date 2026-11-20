---------------------------- MODULE RelayedClose ----------------------------
(* A relayed session's Close (spec/README.md "RelayedClose.tla", lane      *)
(* RELAYCLOSE): an agent A writes K bytes on a session of its link to the  *)
(* relay, closes it and its program exits; the relay's joint J (link.Join) *)
(* passes the bytes and the end to the client C on C's own connection.     *)
(* Bytes are counts; each connection is one FIFO.                          *)
(*                                                                         *)
(* The design (FeatureEndAck, WIRE.md section 13). A's Close              *)
(* (CloseWriteSync) returns clean once the far end acknowledged its end:  *)
(* C's Mux received the FIN after every byte, and C sent an endack; J,    *)
(* whose onward CloseWriteSync onto C returns at that endack, sends its    *)
(* own endack to A only then. An error once the session failed or A's     *)
(* connection ended first, never a wait past it: C's cut ends J's onward  *)
(* wait and J's other direction aborts A's session. After a clean Close A *)
(* may exit or reset: C already has everything. A session whose peer's    *)
(* FIN was read before the session failed reads its bytes, then io.EOF,   *)
(* at J as at C (LinkSession.tla GEnds), and J passes the tail, the FIN,  *)
(* then the failure (an ABORT); J's direction C -> A, whose write onto A's *)
(* failed session fails, leaves C's abort to the other direction while    *)
(* that one runs (held), so the abort never overtakes the tail.           *)
(*                                                                         *)
(* Progress bound (lane RELAYCLOSE, V1READY's finding): a far end that    *)
(* answers the link's pings but never acknowledges (mute) holds no wait   *)
(* past the idle bound: a wait (A's Close, J's onward one) gives up N s   *)
(* after its last progress (the session's bytes acknowledged advancing),  *)
(* whatever the connection's liveness says. Time is abstract: the idle    *)
(* timer may fire while no acknowledgement is under way (Stalled).        *)
EXTENDS Naturals, Sequences

CONSTANTS
    K,       \* A's bytes
    Mutant   \* "none"; "close-before-ack": Close returns once the FIN is
             \* buffered (the code before: an accepted relayed Conn's Close
             \* returned at once); "relay-read-ack": J acknowledges A's end
             \* once its own Mux read it (a Sync's pong, lane RELAYCLOSE's
             \* first fix: pipe TestRelayedEndDropAfterLastByte);
             \* "ack-lost-hang": Close waits for the endack only, not for
             \* the connection's end; "sync-hang": J's onward wait ignores
             \* C's connection's end;
             \* "fail-drops-fin": a failure after the peer's FIN makes
             \* the reads end with the error (the code before: C read every
             \* byte, then a reset); "abort-at-write-fail": J's failed write
             \* toward A aborts C at once, before the tail (the code before);
             \* "liveness-counts-as-progress": the idle timer restarts at
             \* every frame heard (pings), so it never fires on a live
             \* connection (relay psConn.Close before: V1READY's 600 s hang);
             \* "reset-keeps-endack-wait": A's own reset while its Close
             \* waits leaves the wait for an endack that never comes (link
             \* v0.12.19: Session.Reset kept the ENDACK wait; nfltr connect's
             \* Pump, EOF then a local failure, waited the 30 s idle bound);
             \* "close-after-reset-waits": a Close after A's own reset
             \* (its other direction failed first: a CloseWrite racing the
             \* Reset) waits for an endack (link v0.12.19: closeWrite
             \* registered the wait after the Reset had ended the waits);
             \* "close-acks": a whole close ends a wait for the end clean
             \* (link's closeAckErr before lane ENDACKTRUTH: C's close before
             \* A's FIN reached it ended J's onward wait nil, and J's Close
             \* of A's session acknowledged an end it never passed on);
             \* "close-resets": J's clean close of A's session (C closed
             \* whole first) fails A's Close also when A wrote nothing
             \* (closeAckErr, closedFirst after lane ENDACKTRUTH: relay-core
             \* TestConnIsANetConn/BasicIO; lane PIPERESET2;
             \* RelayedCloseEmpty.cfg)

VARIABLES
    aw,      \* bytes A wrote
    ret,     \* A's Close: "no" (not called), "wait", "clean", "err"
    rst,     \* A reset the session (its stop after its Close returned, or
             \* its other direction's failure while its Close waits)
    exited,  \* A's program exited
    toJ,     \* frames A -> J: "d", "fin", "rst"
    fromJ,   \* frames J -> A: "eack" (endack), "abort", "close" (J's
             \* clean whole close of A's session: C closed it whole)
    afail,   \* A's session failed (J's abort, or its close)
    aclosed, \* J closed A's session whole, cleanly
    lost1,   \* A's connection to the relay ended
    gone,    \* the relay crashed
    jq,      \* A's bytes J's Mux read, not yet written to C
    jfin,    \* J's Mux read A's FIN
    jfail,   \* A's session failed at J (A's reset, its connection's end)
    d1,      \* J: A -> C, "run", "sync" (C's endack awaited), "done"
    d0,      \* J: C -> A, "run", "held" (its abort of C waits for d1), "done"
    cw,      \* C wrote a byte (toward A)
    cabort,  \* J aborted C's session: J's writes onto it fail
    toC,     \* frames J -> C: "d", "fin", "abort"
    cq,      \* bytes C's Mux read, not yet read by C
    cfin,    \* C's Mux read the FIN
    cfail,   \* C's session failed (the abort, its connection's end)
    crd,     \* bytes C read
    cend,    \* how C's reads ended: "", "eof", "err", "closed" (C closed
             \* the session whole before its reads ended)
    fromC,   \* frames C -> J: "eack", "close" (C's whole close)
    ccut,    \* C's connection to the relay ended
    mute     \* C answers pings but never acknowledges (broken or hostile)

vars == <<aw, ret, rst, exited, toJ, fromJ, afail, lost1, gone, jq, jfin, jfail, d1, d0,
          cw, cabort, toC, cq, cfin, cfail, crd, cend, fromC, ccut, mute, aclosed>>
aVars == <<aw, ret, rst, exited, afail, aclosed>>

\* J's clean whole close of A's session loses nothing of a stream that
\* carried no byte (as a TCP close(2) in FIN_WAIT_2 still ACKs a bare
\* FIN): A's Close reads it clean, and errs with a byte written (C may
\* not have taken it).
EmptyClosed == aclosed /\ aw = 0 /\ ~rst /\ Mutant # "close-resets"
jVars == <<jq, jfin, jfail, d1, d0, cabort>>
cVars == <<cw, cq, cfin, cfail, crd, cend, fromC, ccut, mute>>

Init ==
    /\ aw = 0 /\ ret = "no" /\ rst = FALSE /\ exited = FALSE /\ afail = FALSE /\ aclosed = FALSE
    /\ toJ = <<>> /\ fromJ = <<>> /\ lost1 = FALSE /\ gone = FALSE
    /\ jq = 0 /\ jfin = FALSE /\ jfail = FALSE /\ d1 = "run" /\ d0 = "run"
    /\ cw = FALSE /\ cabort = FALSE /\ toC = <<>>
    /\ cq = 0 /\ cfin = FALSE /\ cfail = FALSE /\ crd = 0 /\ cend = ""
    /\ fromC = <<>> /\ ccut = FALSE /\ mute = FALSE

\* A reader's end once its queue drained: io.EOF when the peer's FIN came
\* (before any failure: frames are read in order), else the failure.
ReadsEof(fin, fail) == fin /\ (Mutant # "fail-drops-fin" \/ ~fail)

\* ---- A ----

AWrite ==
    /\ aw < K /\ ret = "no" /\ ~lost1 /\ ~afail /\ ~rst
    /\ aw' = aw + 1 /\ toJ' = Append(toJ, "d")
    /\ UNCHANGED <<ret, rst, exited, afail, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars /\ UNCHANGED aclosed

\* Close: the FIN (CloseWriteSync), then wait for J's endack; on a failed
\* session at once an error.
AClose ==
    /\ (aw = K \/ afail \/ rst) /\ ret = "no" /\ ~lost1
    /\ IF afail \/ rst
       THEN /\ ret' = IF rst /\ ~afail /\ Mutant = "close-after-reset-waits" THEN "wait"
                       ELSE IF EmptyClosed THEN "clean" ELSE "err"
            /\ UNCHANGED toJ
       ELSE /\ toJ' = Append(toJ, "fin")
            /\ ret' = IF Mutant = "close-before-ack" THEN "clean" ELSE "wait"
    /\ UNCHANGED <<aw, rst, exited, afail, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars /\ UNCHANGED aclosed

\* J's endack ends the wait clean; its abort fails the session.
ARecv ==
    /\ fromJ # <<>> /\ ~lost1
    /\ fromJ' = Tail(fromJ)
    /\ CASE Head(fromJ) = "eack"
           -> ret' = (IF ret = "wait" THEN "clean" ELSE ret) /\ UNCHANGED <<afail, aclosed>>
         [] Head(fromJ) = "close"
           -> /\ afail' = TRUE /\ aclosed' = TRUE
              /\ ret' = (IF ret = "wait" THEN (IF aw = 0 /\ ~rst /\ Mutant # "close-resets" THEN "clean" ELSE "err") ELSE ret)
         [] OTHER -> afail' = TRUE /\ ret' = (IF ret = "wait" THEN "err" ELSE ret) /\ UNCHANGED aclosed
    /\ UNCHANGED <<aw, rst, exited, toJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars

\* The connection's end ends the wait (c.Done; a dead relay is found by
\* the link's liveness).
ADown ==
    /\ lost1 /\ ret = "wait" /\ Mutant # "ack-lost-hang"
    /\ ret' = "err"
    /\ UNCHANGED <<aw, rst, exited, afail, toJ, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars /\ UNCHANGED aclosed

\* No acknowledgement is under way: the far end is mute and none is in
\* flight. The idle timer fires only then (N s with no progress); with
\* the mutant only once the connection is gone (pings restart it). An
\* abort in flight is no acknowledgement: a wait may give up while J's
\* abort travels to A (waitAck counts only the session's acknowledged
\* bytes; TraceRelayedClose.tla found it: JIdle, then AIdle).
Stalled(alive) == mute /\ fromC = <<>> /\ (\A i \in DOMAIN fromJ : fromJ[i] # "eack")
                  /\ (Mutant = "liveness-counts-as-progress" => ~alive)
\* A's idle bound: its Close gives up (resets the session).
AIdle ==
    /\ ret = "wait" /\ ~lost1 /\ Stalled(~gone)
    /\ ret' = "err" /\ toJ' = Append(toJ, "rst")
    /\ UNCHANGED <<aw, rst, exited, afail, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars /\ UNCHANGED aclosed
\* The agent's stop resets the session after its Close returned.
AReset ==
    /\ ret \in {"clean", "err"} /\ ~rst /\ ~lost1
    /\ rst' = TRUE /\ toJ' = Append(toJ, "rst")
    /\ UNCHANGED <<aw, ret, exited, afail, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars /\ UNCHANGED aclosed

\* A's other direction fails while its Close waits (a local side that
\* took the EOF, then failed): A resets the session, which ends its own
\* wait with an error (Session.Reset ends the ENDACK wait).
AResetWaiting ==
    /\ ret = "wait" /\ ~rst /\ ~lost1
    /\ rst' = TRUE /\ toJ' = Append(toJ, "rst")
    /\ ret' = IF Mutant = "reset-keeps-endack-wait" THEN ret ELSE "err"
    /\ UNCHANGED <<aw, exited, afail, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars /\ UNCHANGED aclosed

\* A's other direction fails before its Close: A resets the session; the
\* Close after it (a CloseWrite racing the Reset) waits for nothing.
AResetFirst ==
    /\ ret = "no" /\ ~rst /\ ~lost1
    /\ rst' = TRUE /\ toJ' = Append(toJ, "rst")
    /\ UNCHANGED <<aw, ret, exited, afail, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars /\ UNCHANGED aclosed

\* The program exits once Close returned: its connection ends, the frames
\* J had not read are lost.
Exit ==
    /\ ret \in {"clean", "err"} /\ ~exited
    /\ exited' = TRUE /\ lost1' = TRUE /\ toJ' = <<>> /\ fromJ' = <<>>
    /\ UNCHANGED <<aw, ret, rst, afail, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars /\ UNCHANGED aclosed

\* The network cuts A's connection.
Cut ==
    /\ ~lost1
    /\ lost1' = TRUE /\ toJ' = <<>> /\ fromJ' = <<>>
    /\ UNCHANGED <<aw, ret, rst, exited, afail, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<toC>> /\ UNCHANGED cVars /\ UNCHANGED aclosed

\* ---- J ----

JRecv ==
    /\ ~gone /\ toJ # <<>>
    /\ toJ' = Tail(toJ)
    /\ CASE Head(toJ) = "d"    -> jq' = jq + 1 /\ UNCHANGED <<jfin, jfail, fromJ>>
         [] Head(toJ) = "fin"  -> /\ jfin' = TRUE /\ UNCHANGED <<jq, jfail>>
                                  /\ fromJ' = IF Mutant = "relay-read-ack" THEN Append(fromJ, "eack") ELSE fromJ
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

\* d1's end: A's reads ended. A clean end passes the FIN on and, with A
\* still there, awaits C's endack (the onward CloseWriteSync); with A's
\* session failed (or d0's abort held) it passes the FIN, then the failure
\* as C's abort. A failed end aborts C.
D1End ==
    /\ ~gone /\ d1 = "run" /\ jq = 0 /\ (jfin \/ jfail)
    /\ IF ReadsEof(jfin, jfail)
       THEN IF cabort
            THEN d1' = "done" /\ UNCHANGED <<toC, cabort>>
            ELSE IF jfail \/ d0 = "held"
            THEN /\ toC' = toC \o <<"fin", "abort">> /\ cabort' = TRUE /\ d1' = "done"
            ELSE /\ toC' = Append(toC, "fin") /\ UNCHANGED cabort /\ d1' = "sync"
       ELSE d1' = "done" /\ abortC
    /\ d0' = IF d0 = "held" /\ d1' = "done" THEN "done" ELSE d0
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, jq, jfin, jfail>> /\ UNCHANGED cVars

\* C's endack: J's onward wait returns; J acknowledges A's end (when A's
\* connection is still there), then passes a failure since (A's, or d0's
\* held one) as C's abort. C's whole close before it held A's end: J's
\* onward wait (or its copy onto C, whose writes now fail) ends with an
\* error, and J ends A's session so that A's Close says so: an error,
\* never an acknowledgement (lane ENDACKTRUTH; link closeAckErr, Session.Close
\* on a hop).
JAck ==
    /\ ~gone /\ fromC # <<>>
    /\ fromC' = Tail(fromC)
    /\ IF Head(fromC) = "close" /\ d1 \in {"run", "sync"}
       THEN /\ d1' = "done"
            /\ fromJ' = IF lost1 THEN fromJ ELSE Append(fromJ, IF Mutant = "close-acks" THEN "eack" ELSE "close")
            /\ cabort' = TRUE /\ UNCHANGED toC
            /\ d0' = IF d0 = "held" THEN "done" ELSE d0
       ELSE IF Head(fromC) = "eack" /\ d1 = "sync"
       THEN /\ d1' = "done"
            /\ fromJ' = IF lost1 \/ Mutant = "relay-read-ack" THEN fromJ ELSE Append(fromJ, "eack")
            /\ IF jfail \/ d0 = "held" THEN abortC ELSE UNCHANGED <<toC, cabort>>
            /\ d0' = IF d0 = "held" THEN "done" ELSE d0
       ELSE UNCHANGED <<d1, d0, fromJ, toC, cabort>>
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, lost1, gone, jq, jfin, jfail>>
    /\ UNCHANGED <<cw, cq, cfin, cfail, crd, cend, ccut, mute>>

\* J's idle bound: its onward wait gives up; J aborts A's session.
JIdle ==
    /\ ~gone /\ d1 = "sync" /\ Stalled(~ccut)
    /\ d1' = "done"
    /\ fromJ' = IF lost1 THEN fromJ ELSE Append(fromJ, "abort")
    /\ abortC
    /\ d0' = IF d0 = "held" THEN "done" ELSE d0
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, lost1, gone, jq, jfin, jfail>> /\ UNCHANGED cVars
\* C's connection ended: the onward wait fails (link: the session's end).
JSyncFail ==
    /\ ~gone /\ d1 = "sync" /\ ccut /\ fromC = <<>> /\ Mutant # "sync-hang"
    /\ d1' = "done"
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, jq, jfin, jfail, d0, cabort, toC>> /\ UNCHANGED cVars

\* d0 reads C's failure (its connection's end) and aborts A's session.
D0CFail ==
    /\ ~gone /\ ccut /\ d0 \in {"run", "held"}
    /\ d0' = "done"
    /\ fromJ' = IF lost1 THEN fromJ ELSE Append(fromJ, "abort")
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, lost1, gone, jq, jfin, jfail, d1, cabort, toC>> /\ UNCHANGED cVars

\* d0: C's byte onto A's failed session fails; C's abort waits for d1
\* while d1 still copies (run). Once d1 passed the FIN on (sync), the abort
\* goes at once, behind the FIN, and ends d1's onward wait (link joint.ended:
\* cutLater holds only while the other direction runs; lane LINKTRACE2,
\* the recorder's TestEndAckCutThenTheFarEndWrites).
D0Fail ==
    /\ ~gone /\ cw /\ d0 = "run" /\ jfail
    /\ IF d1 = "run" /\ Mutant # "abort-at-write-fail"
       THEN d0' = "held" /\ UNCHANGED <<toC, cabort, d1>>
       ELSE d0' = "done" /\ abortC /\ d1' = IF d1 = "sync" THEN "done" ELSE d1
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, jq, jfin, jfail>> /\ UNCHANGED cVars

\* The relay crashes: both its connections end.
RelayGone ==
    /\ ~gone
    /\ gone' = TRUE /\ lost1' = TRUE /\ toJ' = <<>> /\ fromJ' = <<>> /\ toC' = <<>> /\ cfail' = TRUE
    /\ fromC' = <<>> /\ ccut' = TRUE
    /\ UNCHANGED aVars /\ UNCHANGED jVars /\ UNCHANGED <<cw, cq, cfin, crd, cend, mute>>

\* ---- C ----

CWrite ==
    /\ ~cw /\ cend = "" /\ ~ccut
    /\ cw' = TRUE
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, toC>> /\ UNCHANGED jVars
    /\ UNCHANGED <<cq, cfin, cfail, crd, cend, fromC, ccut, mute>>

CRecv ==
    /\ toC # <<>>
    /\ toC' = Tail(toC)
    /\ cend # "closed"
    /\ CASE Head(toC) = "d"     -> cq' = cq + 1 /\ UNCHANGED <<cfin, cfail>>
         [] Head(toC) = "fin"   -> cfin' = TRUE /\ UNCHANGED <<cq, cfail>>
         [] Head(toC) = "abort" -> cfail' = TRUE /\ UNCHANGED <<cq, cfin>>
    \* the far end's Mux acknowledges the FIN on receipt (in order: it
    \* holds every byte before it), unless mute or failed
    /\ fromC' = IF Head(toC) = "fin" /\ ~cfail /\ ~mute THEN Append(fromC, "eack") ELSE fromC
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED <<cw, crd, cend, ccut, mute>>

CRead ==
    /\ cq > 0 /\ cend = ""
    /\ cq' = cq - 1 /\ crd' = crd + 1
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, toC>> /\ UNCHANGED jVars /\ UNCHANGED <<cw, cfin, cfail, cend, fromC, ccut, mute>>

\* C's reads end.
CEnd ==
    /\ cend = "" /\ cq = 0 /\ (cfin \/ cfail)
    /\ cend' = IF ReadsEof(cfin, cfail) THEN "eof" ELSE "err"
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, toC>> /\ UNCHANGED jVars /\ UNCHANGED <<cw, cq, cfin, cfail, crd, fromC, ccut, mute>>
\* C closes the session whole before A's FIN reached it (an app that ends
\* the session early): J reads its close; C's Mux drops what comes after.
CClose ==
    /\ cend = "" /\ ~cfin /\ ~cfail /\ ~ccut /\ ~gone
    /\ cend' = "closed" /\ fromC' = Append(fromC, "close")
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, toC>> /\ UNCHANGED jVars /\ UNCHANGED <<cw, cq, cfin, cfail, crd, ccut, mute>>
\* What comes for C's session after its close is dropped.
CDrop ==
    /\ toC # <<>> /\ cend = "closed"
    /\ toC' = Tail(toC)
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone>> /\ UNCHANGED jVars /\ UNCHANGED cVars

\* C turns mute (a broken or hostile far end): its connection stays up.
CMute ==
    /\ ~mute
    /\ mute' = TRUE
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, toC>> /\ UNCHANGED jVars
    /\ UNCHANGED <<cw, cq, cfin, cfail, crd, cend, fromC, ccut>>

\* The network cuts C's connection: its unread frames are lost both ways,
\* C's session fails, J's writes onto it fail.
CCut ==
    /\ ~ccut /\ ~gone
    /\ ccut' = TRUE /\ toC' = <<>> /\ fromC' = <<>> /\ cfail' = TRUE /\ cabort' = TRUE
    /\ UNCHANGED aVars /\ UNCHANGED <<toJ, fromJ, lost1, gone, jq, jfin, jfail, d1, d0>>
    /\ UNCHANGED <<cw, cq, cfin, crd, cend, mute>>

Next ==
    \/ AWrite \/ AClose \/ ARecv \/ ADown \/ AIdle \/ AReset \/ AResetWaiting \/ AResetFirst \/ Exit \/ Cut
    \/ JRecv \/ JDown \/ D1Copy \/ D1End \/ JAck \/ JIdle \/ JSyncFail \/ D0CFail \/ D0Fail \/ RelayGone
    \/ CWrite \/ CRecv \/ CRead \/ CEnd \/ CCut \/ CMute \/ CClose \/ CDrop

\* A, J and C take their own steps; the program's exit, its stop's reset,
\* A's reset before or while its Close, the network's cuts, the relay's
\* crash, C's write, C's whole close and C turning mute are the environment's (no
\* fairness); the idle timers are A's and J's.
Fairness ==
    /\ WF_vars(AWrite) /\ WF_vars(AClose) /\ WF_vars(ARecv) /\ WF_vars(ADown)
    /\ WF_vars(JRecv) /\ WF_vars(JDown) /\ WF_vars(D1Copy) /\ WF_vars(D1End) /\ WF_vars(D0Fail)
    /\ WF_vars(JAck) /\ WF_vars(JSyncFail) /\ WF_vars(D0CFail) /\ WF_vars(AIdle) /\ WF_vars(JIdle)
    /\ WF_vars(CRecv) /\ WF_vars(CRead) /\ WF_vars(CEnd) /\ WF_vars(CDrop)

Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK ==
    /\ aw \in 0..K /\ ret \in {"no", "wait", "clean", "err"} /\ aclosed \in BOOLEAN
    /\ d1 \in {"run", "sync", "done"} /\ d0 \in {"run", "held", "done"}
    /\ cend \in {"", "eof", "err", "closed"} /\ crd \in 0..K /\ jq \in 0..K /\ cq \in 0..K /\ mute \in BOOLEAN

\* C reads io.EOF only after every byte A wrote.
NoSilentLoss == cend = "eof" => crd = K

\* A's Close returns clean only once C holds every byte and the FIN (its
\* Mux received them; C's reads need no network then): a program that
\* exits then loses nothing it wrote (pipe spec/Pipe.tla CleanExit).
CloseTruth == ret = "clean" => ((cfin /\ crd + cq = K) \/ (aclosed /\ aw = 0))

\* J's clean close fails no Close of an empty stream: an error there only
\* from A's reset, a cut, the idle bound or the relay's crash.
NoFalseReset == (ret = "err" /\ aclosed /\ aw = 0) => (rst \/ lost1 \/ mute \/ ccut \/ gone)

\* Witness (RelayedCloseEmpty.cfg): an empty stream's Close clean at J's close.
WitnessEmptyClosed == ~(ret = "clean" /\ aclosed)

\* Once J read A's FIN, C reads every byte and io.EOF whatever A does
\* next (exit, a cut, a reset), unless C's connection or the relay ended
\* or C closed the session first.
TailDelivered == jfin ~> ((cend = "eof" /\ crd = K) \/ gone \/ ccut \/ cend = "closed")

\* A Close ends: never a wait past its connection's or the far end's end,
\* nor past the idle bound when the far end is mute.
CloseEnds == (ret = "wait") ~> (ret \in {"clean", "err"})

\* J's onward wait ends (unless the relay crashed).
SyncEnds == (d1 = "sync") ~> (d1 = "done" \/ gone)

\* Witnesses (make spec-mutants: TLC must violate each): a clean Close,
\* the program's exit, and C's clean end; C's FIN then its abort read as
\* io.EOF; d0's abort held for d1; a Close that ends with an error
\* because C's connection ended (pipe's TestRelayedEndDropAfterLastByte).
WitnessExitThenEof == ~(ret = "clean" /\ exited /\ cend = "eof")
WitnessFinThenAbort == ~(cfin /\ cfail /\ cend = "eof" /\ ~gone)
WitnessHeld == d0 # "held"
WitnessCloseErr == ~(ret = "err" /\ ccut /\ ~lost1)
\* A Close that gives up at the idle bound on a live connection.
WitnessIdle == ~(ret = "err" /\ mute /\ ~lost1 /\ ~ccut /\ ~gone /\ ~afail)
\* A Close that ends with an error at A's own reset while it waits.
\* A Close after A's own reset, ended at once with an error.
WitnessResetFirst == ~(ret = "err" /\ rst /\ aw < K /\ ~mute /\ ~lost1 /\ ~ccut /\ ~gone /\ ~afail)
WitnessResetWaiting == ~(ret = "err" /\ rst /\ ~mute /\ ~lost1 /\ ~ccut /\ ~gone /\ ~afail)
\* A Close that ends with an error because C closed the session whole
\* before A's FIN reached it, on live connections.
WitnessCClose == ~(ret = "err" /\ cend = "closed" /\ ~rst /\ ~mute /\ ~lost1 /\ ~ccut /\ ~gone)
=============================================================================
