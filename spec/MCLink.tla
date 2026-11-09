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
\* LinkRefuse.cfg: the accepter writes 2 bytes (a Refuse racing a Write).
MCRefuse == [p \in Sides |-> [s \in Sids |-> IF p = "l" THEN 2 ELSE 0]]
\* LinkBoth.cfg: a session of each side, opened at once (crossed OPENs),
\* one byte each way on each (review r2 of LinkSession, finding 7).
MCBoth == [p \in Sides |-> [s \in Sids |-> 1]]
\* LinkCut.cfg: the accepter writes a byte (a Write racing the cut's ABORT).
MCCut == [p \in Sides |-> [s \in Sids |-> IF p = "l" THEN 1 ELSE 0]]
\* LinkDrain.cfg: two sessions, no data (Link.cfg checks credit).
MCDrain == [p \in Sides |-> [s \in Sids |-> 0]]

-----------------------------------------------------------------------------
(* The refinement mapping (spec/README.md "The session interface").         *)
WEnd(code) == CASE code = Half -> "fin" [] code = Full -> "close"
                [] code \in {RST, ABT, DDL} -> "reset" [] OTHER -> ""
REnd(p, s) ==
    LET g == ss[p][s].got IN
    IF g \in {Half, Full} THEN "eof"
    ELSE IF g \in {RST, ABT, DDL} THEN "reset"
    ELSE IF down[p] /\ ss[p][s].st # "none" THEN "lost"
    ELSE ""

LS == INSTANCE LinkSession WITH
    st <- [p \in Sides |-> [s \in Sids |-> ss[p][s].st]],
    w <- sent,
    d <- dlv,
    c <- [p \in Sides |-> [s \in Sids |-> dlv[p][s] - ss[p][s].q]],
    wend <- [p \in Sides |-> [s \in Sids |-> WEnd(ss[p][s].code)]],
    fin <- [p \in Sides |-> [s \in Sids |-> ss[p][s].fin]],
    shut <- [p \in Sides |-> [s \in Sids |-> ss[p][s].closing # 0 \/ ss[p][s].code \in {Full, RST, ABT, DDL}]],
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
\* FinBeforeShut: a CloseWrite's CLOSE still due when the peer's FIN came.
WitnessFinRace == \A p \in Sides, s \in Sids : ~(ss[p][s].wfin /\ ~ss[p][s].fin /\ ss[p][s].rfin)
\* DataOK's refusal clause: a Write holds credit while a Refuse's ABORT is due.
WitnessRefusing == \A p \in Sides, s \in Sids : ~(Refusing(p, s) /\ pend[p][s] > 0)
\* The drain's cut (lane LINKATOMIC): two calls of cut, one waiting for the
\* other (DrainReturnsAfterCut's case); Session.Abort's ABORT read before
\* the cut's, though the cut began (CONNDRAINFLAKE's open question: the
\* peer reads the first ABORT buffered, either); the cut's ABORT of a
\* session that ended first (sendAbort does not look; the peer drops it).
WitnessCutWaits == \A p \in Sides : ~(cut[p].in = 2 /\ cut[p].st # "done")
WitnessAbortBeforeCut == \A p \in Sides, s \in Sids : ~(cut[p].st # "no" /\ ss[Peer(p)][s].got = ABT)
WitnessCutAbortsEnded ==
    \A p \in Sides, s \in Sids : ~(ss[p][s].st = "gone" /\ \E i \in 1..Len(out[p]) : out[p][i] = [Frame("close", s, DDL) EXCEPT !.at = sent[p][s]])
\* Not a guarantee (README "The drain's cut"): once the cut closed the
\* connection (its write side shut first, or at once with no ABORT to
\* send), every session open here had its ABORT sent or ended at the peer.
\* An OPEN that crossed the GOAWAY and arrived after the cut's snapshot gets
\* none: its opener reads lost (spec-mutants expects the violation).
CutAbortsAll ==
    \A p \in Sides, s \in Sids :
        (cut[p].st \in {"flushed", "close"} /\ closed[p] /\ ss[p][s].st = "open") =>
            \/ \E i \in 1..Len(out[p]) : out[p][i].t = "close" /\ out[p][i].s = s
            \/ ss[Peer(p)][s].st # "open"
WitnessInFlight == \A p \in Sides, s \in Sids : ~(SumIn(out[p], "data", s) > 0 /\ SumIn(out[Peer(p)], "win", s) > 0)
=============================================================================
