------------------------------ MODULE EndAckLeg ------------------------------
(* One leg's end acknowledgement, per transport (spec/README.md "One leg's *)
(* end acknowledgement", lane ENDACK2D): a sender S writes K bytes and its *)
(* half close and waits; the receiver R acknowledges the end once it holds *)
(* every byte and the FIN in order. RelayedClose.tla and HopHold.tla model  *)
(* each leg as a FIFO whose far end acknowledges on in-order receipt; this  *)
(* spec checks that each transport gives that guarantee (Kind):             *)
(*   "link": link FeatureEndAck (WIRE.md 13), one FIFO each way;            *)
(*   "grpc": a ConnectTCP stream's EndAckFrame (relay dataplane), one FIFO; *)
(*   "tcp":  a TCP peer's kernel ACK of the FIN (TCP_INFO tcpi_unacked and  *)
(*           tcpi_notsent_bytes 0, RFC 9293 3.10.4), one FIFO;              *)
(*   "quic": a QUIC stream (RFC 9000): its frames arrive in any order (lost *)
(*           and resent packets) and are reassembled; the acknowledgement   *)
(*           travels on the connection's control stream (quiclink's 'E'),   *)
(*           which is not ordered with the session's stream, so a          *)
(*           RESET_STREAM of the session may arrive before it; the receiver *)
(*           sends its later failure on the control stream too ('A').       *)
(* Progress (the idle bound, link's waitAck): the wait gives up only while  *)
(* nothing of the session is acknowledged (the far end is mute), whatever   *)
(* the connection's liveness says; time is abstract (Stalled).             *)
EXTENDS Naturals, Sequences

CONSTANTS
    K,       \* S's bytes
    Kind,    \* "link", "grpc", "tcp", "quic"
    Mutant   \* "none"; "ack-at-write": S's wait returns once its end is
             \* written (quiclink's CloseWrite, the relay's TCP visitor leg
             \* "once passed on", the relay SDK's Close on a gRPC stream:
             \* the code today); "ack-at-final-size": R acknowledges when the
             \* FIN's frame arrives, before the bytes before it (quic-go's
             \* SetReceiveFinalSizeCallback); "wait-on-stream-reset": S's wait
             \* also ends at the session stream's reset, which may overtake
             \* the acknowledgement (quic); "liveness-counts-as-progress": the
             \* idle bound restarts at every frame heard; "total-bound": the
             \* wait gives up after a fixed time whatever the progress
             \* (pathswitch Conn.Close today: waitHeard(Detach) with no stall
             \* bound); "close-acks": R's whole close acknowledges the end
             \* whatever it holds (quiclink's 'E' at Close; link's
             \* closeAckErr before lane ENDACKTRUTH: a peer's clean whole
             \* close ended the wait nil); "close-resets": R's clean whole
             \* close ends the wait with an error also when S wrote nothing
             \* (link's closeAckErr and closedFirst after lane ENDACKTRUTH:
             \* relay-core TestConnIsANetConn/BasicIO, whose second Close
             \* read a reset; lane PIPERESET2)

VARIABLES
    sw,      \* bytes S wrote
    ret,     \* S's wait: "no" (not closed), "wait", "clean", "err"
    ch,      \* frames S -> R: "d", "fin"
    got,     \* bytes R received
    finSeen, \* R received the FIN's frame
    back,    \* frames R -> S on the acknowledgement's channel: "eack", "abort"
    sreset,  \* quic: R's RESET_STREAM of the session is in flight
    eackSent,\* history: R acknowledged the end
    rfail,   \* R's side failed (its app reset the session)
    mute,    \* R never acknowledges; its connection stays up
    cut,     \* the connection ended (every frame in flight lost)
    sfail,   \* S's session failed (R's abort read)
    idled,   \* history: S's wait gave up at the idle bound
    rclosed  \* R's app closed the session whole, cleanly (RClose, not RFail)

vars == <<sw, ret, ch, got, finSeen, back, sreset, eackSent, rfail, mute, cut, sfail, idled, rclosed>>

Init ==
    /\ sw = 0 /\ ret = "no" /\ ch = <<>> /\ got = 0 /\ finSeen = FALSE
    /\ back = <<>> /\ sreset = FALSE /\ eackSent = FALSE /\ rfail = FALSE
    /\ mute = FALSE /\ cut = FALSE /\ sfail = FALSE /\ idled = FALSE /\ rclosed = FALSE

InOrder == finSeen /\ got = sw  \* R holds every byte and the FIN in order

\* A clean whole close of R loses nothing of a stream that carried no byte
\* (link; as a TCP close(2) in FIN_WAIT_2 still ACKs a bare FIN): S's wait
\* reads it clean. With a byte written, R may not have taken it: an error.
EmptyClosed == Kind = "link" /\ rclosed /\ sw = 0 /\ Mutant # "close-resets"

\* The index R's transport hands up next: the head on a FIFO; on QUIC any
\* frame in flight (reassembly is R's).
Idx == IF Kind = "quic" THEN 1..Len(ch) ELSE IF ch = <<>> THEN {} ELSE {1}
Drop(s, i) == SubSeq(s, 1, i - 1) \o SubSeq(s, i + 1, Len(s))

SWrite ==
    /\ sw < K /\ ret = "no" /\ ~cut /\ ~sfail
    /\ sw' = sw + 1 /\ ch' = Append(ch, "d")
    /\ UNCHANGED <<ret, got, finSeen, back, sreset, eackSent, rfail, mute, cut, sfail, idled, rclosed>>

\* S's half close; on a failed session (or a cut one) an error at once.
SClose ==
    /\ ret = "no"
    /\ IF sfail \/ cut
       THEN ret' = (IF EmptyClosed /\ ~cut THEN "clean" ELSE "err") /\ UNCHANGED ch
       ELSE /\ ch' = Append(ch, "fin")
            /\ ret' = IF Mutant = "ack-at-write" THEN "clean" ELSE "wait"
    /\ UNCHANGED <<sw, got, finSeen, back, sreset, eackSent, rfail, mute, cut, sfail, idled, rclosed>>

\* R takes a frame; it acknowledges once it holds the end in order (with
\* the mutant at the FIN's frame), unless mute or failed.
RRecv ==
    \E i \in Idx :
       /\ ~cut
       /\ ch' = Drop(ch, i)
       /\ got' = IF ch[i] = "d" THEN got + 1 ELSE got
       /\ finSeen' = (finSeen \/ ch[i] = "fin")
       /\ LET ready == IF Mutant = "ack-at-final-size" THEN finSeen' ELSE finSeen' /\ got' = sw
          IN IF ready /\ ~eackSent /\ ~mute /\ ~rfail
             THEN back' = Append(back, "eack") /\ eackSent' = TRUE
             ELSE UNCHANGED <<back, eackSent>>
       /\ UNCHANGED <<sw, ret, sreset, rfail, mute, cut, sfail, idled, rclosed>>

\* R's side fails (its app reset the session): an abort on the
\* acknowledgement's channel; on QUIC also the stream's RESET_STREAM.
RFail ==
    /\ ~rfail /\ ~cut
    /\ rfail' = TRUE /\ back' = Append(back, "abort")
    /\ sreset' = (Kind = "quic")
    /\ UNCHANGED <<sw, ret, ch, got, finSeen, eackSent, mute, cut, sfail, idled, rclosed>>

\* R's app closes the session whole before acknowledging (its FIN had not
\* reached R): the end is not known delivered (as a TCP close with data
\* unread is an RST). QUIC: quiclink Session.Close says 'A' on the control
\* stream (STOP_SENDING); link: the CLOSE frame, which S's wait reads as an
\* error (closeAckErr: ErrReset, lane ENDACKTRUTH). With the mutant the
\* close acknowledges ('E'; link's closeAckErr before: a clean whole close
\* ended the wait nil). A mute R closes too: its Close still ends the wait.
RClose ==
    /\ ~eackSent /\ ~rfail /\ ~cut
    /\ rfail' = TRUE /\ rclosed' = TRUE
    /\ IF Mutant = "close-acks"
       THEN back' = Append(back, "eack") /\ eackSent' = TRUE
       ELSE back' = Append(back, IF Kind = "link" THEN "close" ELSE "abort") /\ UNCHANGED eackSent
    /\ UNCHANGED <<sw, ret, ch, got, finSeen, sreset, mute, cut, sfail, idled>>

\* S reads its acknowledgement channel (in order).
SRecv ==
    /\ back # <<>> /\ ~cut
    /\ back' = Tail(back)
    /\ ret' = IF ret = "wait"
              THEN (IF Head(back) = "eack" \/ (Head(back) = "close" /\ EmptyClosed) THEN "clean" ELSE "err")
              ELSE ret
    /\ sfail' = (sfail \/ Head(back) \in {"abort", "close"})
    /\ UNCHANGED <<sw, ch, got, finSeen, sreset, eackSent, rfail, mute, cut, idled, rclosed>>

\* QUIC: the session stream's reset arrives (in any order with the control
\* stream); the wait ignores it, with the mutant it ends it.
SReset ==
    /\ sreset /\ ~cut
    /\ sreset' = FALSE
    /\ ret' = IF ret = "wait" /\ Mutant = "wait-on-stream-reset" THEN "err" ELSE ret
    /\ UNCHANGED <<sw, ch, got, finSeen, back, eackSent, rfail, mute, cut, sfail, idled, rclosed>>

\* The connection ends: the wait fails.
Cut ==
    /\ ~cut
    /\ cut' = TRUE /\ ch' = <<>> /\ back' = <<>> /\ sreset' = FALSE
    /\ ret' = IF ret = "wait" THEN "err" ELSE ret
    /\ UNCHANGED <<sw, got, finSeen, eackSent, rfail, mute, sfail, idled, rclosed>>

\* R turns mute (broken or hostile): its connection stays up.
RMute ==
    /\ ~mute
    /\ mute' = TRUE
    /\ UNCHANGED <<sw, ret, ch, got, finSeen, back, sreset, eackSent, rfail, cut, sfail, idled, rclosed>>

\* Nothing is acknowledged and nothing is under way: the far end is mute
\* and no acknowledgement or abort is in flight. With the mutant pings
\* restart the timer: it fires only once the connection is gone, which
\* has failed the wait already.
Stalled == mute /\ back = <<>> /\ (Mutant = "liveness-counts-as-progress" => cut)

SIdle ==
    /\ ret = "wait" /\ (Stalled \/ Mutant = "total-bound")
    /\ ret' = "err" /\ idled' = TRUE
    /\ UNCHANGED <<sw, ch, got, finSeen, back, sreset, eackSent, rfail, mute, cut, sfail, rclosed>>

Next == SWrite \/ SClose \/ RRecv \/ RFail \/ RClose \/ SRecv \/ SReset \/ Cut \/ RMute \/ SIdle

\* S's and R's steps are fair; R's failure, the cut and muteness are the
\* environment's.
Fairness ==
    /\ WF_vars(SWrite) /\ WF_vars(SClose) /\ WF_vars(RRecv) /\ WF_vars(SRecv)
    /\ WF_vars(SReset) /\ WF_vars(SIdle)

Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK ==
    /\ sw \in 0..K /\ got \in 0..K /\ ret \in {"no", "wait", "clean", "err"} /\ rclosed \in BOOLEAN
    /\ Kind \in {"link", "grpc", "tcp", "quic"}

\* A clean end only once R holds every byte and the FIN in order, or R
\* closed cleanly a stream that carried none.
AckTruth == ret = "clean" => (InOrder \/ (rclosed /\ sw = 0))

\* R's clean whole close fails no wait of an empty stream (link): an error
\* there only from a cut, the idle bound or R's failure.
NoFalseReset == (ret = "err" /\ Kind = "link" /\ rclosed /\ sw = 0) => (cut \/ idled)

\* An error on a live connection only when R acknowledged nothing: never
\* once the acknowledgement was sent (it is read first).
ErrTruth == (ret = "err" /\ ~cut) => ~eackSent

\* The idle bound fires only on a far end that acknowledges nothing.
IdleTruth == idled => mute

\* The wait ends: never past the connection's end, nor past the idle bound
\* when the far end is mute.
WaitEnds == (ret = "wait") ~> (ret \in {"clean", "err"})

\* Witnesses (make spec-mutants: TLC must violate each): a clean end; the
\* idle bound on a live connection; on QUIC the FIN's frame before a byte
\* (EndAckLegQuic.cfg only).
WitnessClean == ret # "clean"
WitnessIdle == ~(idled /\ ~cut)
WitnessFinFirst == ~(finSeen /\ got < K)
WitnessEmptyClosed == ~(ret = "clean" /\ rclosed)
=============================================================================
