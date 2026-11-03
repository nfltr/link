------------------------------- MODULE MCLink -------------------------------
(* Link with the small sizes TLC checks (spec/README.md "What is           *)
(* abstracted"): window 4, chunk 2 (MinWindow = 2 x MaxChunk), a grant per *)
(* quarter window; sessions 1 and 3 are the dialer's, 2 the listener's.  *)
(* Refines: Link implements the session interface (LinkSession.tla).     *)
EXTENDS Link, TLC

MCOwner == 1 :> "d" @@ 2 :> "l" @@ 3 :> "d"
MCWin == [p \in Sides |-> 4]
\* Link.cfg: one session, the dialer writes past the window (credit returns).
MCCredit == [p \in Sides |-> [s \in Sids |-> IF p = "d" THEN 6 ELSE 0]]
\* LinkCrash.cfg: one byte of the dialer's (the crash on fewer states).
MCCrash == [p \in Sides |-> [s \in Sids |-> IF p = "d" THEN 1 ELSE 0]]
\* LinkDrain.cfg: two sessions, no data (Link.cfg checks credit).
MCDrain == [p \in Sides |-> [s \in Sids |-> 0]]

-----------------------------------------------------------------------------
(* The refinement mapping (spec/README.md "The session interface").         *)
WEnd(code) == CASE code = Half -> "fin" [] code = Full -> "close"
                [] code \in {RST, ABT} -> "reset" [] OTHER -> ""
REnd(p, s) ==
    LET g == ss[p][s].got IN
    IF g \in {Half, Full} THEN "eof"
    ELSE IF g \in {RST, ABT} THEN "reset"
    ELSE IF down[p] /\ ss[p][s].st # "none" THEN "lost"
    ELSE ""

LS == INSTANCE LinkSession WITH
    st <- [p \in Sides |-> [s \in Sids |-> ss[p][s].st]],
    w <- sent,
    d <- dlv,
    c <- [p \in Sides |-> [s \in Sids |-> dlv[p][s] - ss[p][s].q]],
    wend <- [p \in Sides |-> [s \in Sids |-> WEnd(ss[p][s].code)]],
    fin <- [p \in Sides |-> [s \in Sids |-> ss[p][s].fin]],
    shut <- [p \in Sides |-> [s \in Sids |-> ss[p][s].closing # 0 \/ ss[p][s].code \in {Full, RST, ABT}]],
    rend <- [p \in Sides |-> [s \in Sids |-> REnd(p, s)]],
    up <- [p \in Sides |-> ~down[p]],
    fault <- err # {}

Refines == LS!Spec

\* Witnesses (make spec-mutants: TLC must violate each, so the interface's
\* guarantees constrain steps Link takes): a reader's io.EOF, reset and
\* lost end; a writer past one window (credit came back); an open refused
\* by GOAWAY.
WitnessEof == \A p \in Sides, s \in Sids : REnd(p, s) # "eof"
WitnessReset == \A p \in Sides, s \in Sids : REnd(p, s) # "reset"
WitnessLost == \A p \in Sides, s \in Sids : REnd(p, s) # "lost"
WitnessCredit == \A p \in Sides, s \in Sids : sent[p][s] <= Win[Peer(p)]
\* Not a guarantee (spec/README.md "The session interface"): a session that
\* ended cleanly at p (FIN each way) had its FIN read by the peer. A crash
\* breaks it (make spec-mutants expects the violation: race:clean-end-tail-lost).
CleanEndReceived ==
    \A p \in Sides, s \in Sids :
        (down["d"] /\ down["l"] /\ ss[p][s].code = Half /\ ss[p][s].got = Half) =>
            ss[Peer(p)][s].got \in {Half, Full}
WitnessRefused == \A p \in Sides, s \in Sids : ~(ga[p] /\ ss[p][s].st = "gone" /\ sent[p][s] = 0 /\ ss[p][s].code = NoCode /\ ss[p][s].got = NoCode /\ ~down[p])
\* A witness per invariant of Link (make spec-mutants: TLC must violate
\* each, so the invariant's case is reached, not vacuous):
\* NoProtocolError: an OPEN on its way to a side at max_sessions (the close
\* ahead of it frees the count: LinkSess.cfg); CreditBounds: a writer out of
\* credit; BufferBound: a reader's window full; QueueWithinHeld: a queue as
\* large as its charge; Conservation: data and a window update of one
\* session on the wire at once.
WitnessOpenAtMax ==
    ~\E p \in Sides : Count(p, Peer(p)) >= MaxSess /\ \E i \in 1..Len(out[Peer(p)]) : out[Peer(p)][i].t = "open"
WitnessNoCredit == \A p \in Sides, s \in Sids : ~(ss[p][s].st = "open" /\ cr[p][s].on /\ cr[p][s].n = 0)
WitnessWindowFull == \A p \in Sides, s \in Sids : cr[p][s].held < Win[p]
WitnessQueueHeld == \A p \in Sides, s \in Sids : ~(cr[p][s].on /\ ss[p][s].q > 0 /\ ss[p][s].q = cr[p][s].held)
WitnessInFlight == \A p \in Sides, s \in Sids : ~(SumIn(out[p], "data", s) > 0 /\ SumIn(out[Peer(p)], "win", s) > 0)
=============================================================================
