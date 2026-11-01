------------------------------- MODULE MCLink -------------------------------
(* Link with the small sizes TLC checks (spec/README.md "What is           *)
(* abstracted"): window 4, chunk 2 (MinWindow = 2 x MaxChunk), a grant per *)
(* quarter window; session 1 is the dialer's, 2 the listener's.            *)
EXTENDS Link, TLC

MCOwner == 1 :> "d" @@ 2 :> "l"
MCWin == [p \in Sides |-> 4]
\* Link.cfg: one session, the dialer writes past the window (credit returns).
MCCredit == [p \in Sides |-> [s \in Sids |-> IF p = "d" THEN 6 ELSE 0]]
\* LinkDrain.cfg: two sessions, no data (Link.cfg checks credit).
MCDrain == [p \in Sides |-> [s \in Sids |-> 0]]
=============================================================================
