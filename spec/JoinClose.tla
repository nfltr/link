------------------------------ MODULE JoinClose ------------------------------
(* A joint (link.Join, peerlink.Join) whose onward side B is closed whole *)
(* by its peer while the origin side A still writes (nfltr lane          *)
(* REFUSALFLAKE: an agent's TLS alert, then its close; the refused client *)
(* read EOF without the alert, exit 6 for 10). B's peer writes K bytes,   *)
(* optionally its half close (FIN), then its whole close; the joint's     *)
(* direction 1 copies B's bytes to A and ends at B's end; direction 0    *)
(* copies A's peer's byte to B, and that write fails once B's whole close *)
(* reached the joint (the peer takes no more).                            *)
(*                                                                        *)
(* The design: the failed write ends direction 0 only. A is closed whole *)
(* by the later of the two directions to end: by direction 1 at B's end  *)
(* (whole, not its half close, once direction 0 failed), or by direction *)
(* 0 when direction 1 ended first. So B's bytes reach A before A's close *)
(* (TailDelivered), and A is closed whole in the end (AClosedInTheEnd):  *)
(* A's peer, still writing, is never left on a half-open session.        *)
EXTENDS Naturals, Sequences

CONSTANTS
    K,       \* B's peer's bytes
    Mutant   \* "none"; "close-at-write-fail": the code before: the failed
             \* write closes A at once, dropping what direction 1 still
             \* had to write; "no-whole": the failed write leaves A to
             \* direction 1, whose end after B's half close is A's half
             \* close only

VARIABLES
    bs,      \* bytes B's peer wrote
    bhalf,   \* B's peer wrote its half close
    bclose,  \* B's peer wrote its whole close
    toJ,     \* frames toward the joint: "d", "fin", "close"
    jq,      \* B's bytes the joint read, not yet written to A
    beof,    \* B's reads ended (its FIN or close read, its bytes drained)
    bgone,   \* B's whole close read at the joint: writes to B fail
    d0,      \* A -> B: "run", "done"
    d1,      \* B -> A: "run", "done"
    whole,   \* direction 0 failed first: direction 1's end closes A whole
    aw,      \* A's peer wrote its byte
    toA,     \* frames toward A's peer: "d", "fin", "close"
    ard,     \* bytes A's peer read
    aend,    \* what A's peer read last: "", "fin", "close"
    aclosed  \* the joint closed A whole (writes to A fail)

vars == <<bs, bhalf, bclose, toJ, jq, beof, bgone, d0, d1, whole, aw, toA, ard, aend, aclosed>>

Init ==
    /\ bs = 0 /\ bhalf = FALSE /\ bclose = FALSE /\ toJ = <<>>
    /\ jq = 0 /\ beof = FALSE /\ bgone = FALSE
    /\ d0 = "run" /\ d1 = "run" /\ whole = FALSE
    /\ aw = FALSE /\ toA = <<>> /\ ard = 0 /\ aend = "" /\ aclosed = FALSE

\* B's peer.
BWrite == bs < K /\ ~bhalf /\ ~bclose /\ bs' = bs + 1 /\ toJ' = Append(toJ, "d")
          /\ UNCHANGED <<bhalf, bclose, jq, beof, bgone, d0, d1, whole, aw, toA, ard, aend, aclosed>>
BHalf == bs = K /\ ~bhalf /\ ~bclose /\ bhalf' = TRUE /\ toJ' = Append(toJ, "fin")
         /\ UNCHANGED <<bs, bclose, jq, beof, bgone, d0, d1, whole, aw, toA, ard, aend, aclosed>>
BClose == bs = K /\ ~bclose /\ bclose' = TRUE /\ toJ' = Append(toJ, "close")
          /\ UNCHANGED <<bs, bhalf, jq, beof, bgone, d0, d1, whole, aw, toA, ard, aend, aclosed>>

\* The joint's Mux reads B's frames in order.
JRecv ==
    /\ toJ # <<>> /\ toJ' = Tail(toJ)
    /\ CASE Head(toJ) = "d" -> jq' = jq + 1 /\ UNCHANGED <<beof, bgone>>
         [] Head(toJ) = "fin" -> beof' = TRUE /\ UNCHANGED <<jq, bgone>>
         [] Head(toJ) = "close" -> beof' = TRUE /\ bgone' = TRUE /\ UNCHANGED jq
    /\ UNCHANGED <<bs, bhalf, bclose, d0, d1, whole, aw, toA, ard, aend, aclosed>>

\* closeA: the joint's Close of A (a CLOSE frame once).
closeA == IF aclosed THEN UNCHANGED <<toA, aclosed>>
          ELSE toA' = Append(toA, "close") /\ aclosed' = TRUE

\* Direction 1: a byte of B onto A; a write after A's close fails (the
\* byte is lost, the direction ends).
D1Copy ==
    /\ d1 = "run" /\ jq > 0 /\ jq' = jq - 1
    /\ IF aclosed THEN d1' = "done" /\ UNCHANGED toA
       ELSE toA' = Append(toA, "d") /\ UNCHANGED d1
    /\ UNCHANGED <<bs, bhalf, bclose, toJ, beof, bgone, d0, whole, aw, ard, aend, aclosed>>
\* B's reads ended: A's half close after B's half close, else its close;
\* whole once direction 0 failed.
D1End ==
    /\ d1 = "run" /\ jq = 0 /\ beof /\ d1' = "done"
    /\ IF bhalf /\ ~whole
       THEN (IF aclosed THEN UNCHANGED <<toA, aclosed>> ELSE toA' = Append(toA, "fin") /\ UNCHANGED aclosed)
       ELSE closeA
    /\ UNCHANGED <<bs, bhalf, bclose, toJ, jq, beof, bgone, d0, whole, aw, ard, aend>>

\* A's peer writes its byte; direction 0 tries it onto B.
AWrite == ~aw /\ ~aclosed /\ aw' = TRUE
          /\ UNCHANGED <<bs, bhalf, bclose, toJ, jq, beof, bgone, d0, d1, whole, toA, ard, aend, aclosed>>
\* The write onto B fails: B's peer closed it whole.
D0Fail ==
    /\ aw /\ d0 = "run" /\ bgone /\ d0' = "done"
    /\ CASE Mutant = "close-at-write-fail" -> closeA /\ UNCHANGED whole
         [] d1 = "done" -> closeA /\ UNCHANGED whole
         [] OTHER -> whole' = (Mutant # "no-whole") /\ UNCHANGED <<toA, aclosed>>
    /\ UNCHANGED <<bs, bhalf, bclose, toJ, jq, beof, bgone, d1, aw, ard, aend>>

\* A's peer reads.
ARead ==
    /\ toA # <<>> /\ toA' = Tail(toA)
    /\ IF Head(toA) = "d" THEN ard' = ard + 1 /\ UNCHANGED aend ELSE aend' = Head(toA) /\ UNCHANGED ard
    /\ UNCHANGED <<bs, bhalf, bclose, toJ, jq, beof, bgone, d0, d1, whole, aw, aclosed>>

Next == BWrite \/ BHalf \/ BClose \/ JRecv \/ D1Copy \/ D1End \/ AWrite \/ D0Fail \/ ARead

\* B's peer closes in the end (an agent's refusal); A's peer writes its
\* byte (a client); the joint and the connections take their steps.
Fairness == WF_vars(BWrite) /\ WF_vars(BClose) /\ WF_vars(AWrite) /\ WF_vars(JRecv)
            /\ WF_vars(D1Copy) /\ WF_vars(D1End) /\ WF_vars(D0Fail) /\ WF_vars(ARead)

Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK == d0 \in {"run", "done"} /\ d1 \in {"run", "done"} /\ aend \in {"", "fin", "close"}
          /\ ard <= K /\ jq <= K

\* A's peer reads every byte B's peer wrote before it reads A's end.
TailDelivered == aend # "" => ard = K

\* A is closed whole in the end, and A's peer reads that close.
AClosedInTheEnd == <>[](aend = "close")

\* Witnesses (make spec-mutants: TLC must violate each): direction 0 fails
\* while direction 1 still has bytes to write; B's half close, then A's
\* close after direction 0 failed.
WitnessFailFirst == ~(d0 = "done" /\ d1 = "run" /\ jq > 0)
WitnessHalfThenWhole == ~(bhalf /\ whole /\ aend = "close")
=============================================================================
