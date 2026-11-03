-------------------------------- MODULE Link --------------------------------
(* link's session protocol (WIRE.md sections 5, 7, 9, 12's answers) as the  *)
(* Go code implements it. spec/README.md is the design: what is modelled,   *)
(* the Go each action stands for, the invariants and the liveness.          *)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Sids,        \* session ids
    Owner,       \* [Sids -> Sides]: the side that opens each (Mux: odd ids the dialer's)
    Win,         \* [Sides -> Nat]: the bytes of one session each side buffers (its window)
    Chunk,       \* the most credit taken at once (MaxChunk)
    Chain,       \* both sides announced FeatureChain: opens are answered
    NoSid,       \* the sid of frames that carry none
    Mutant,      \* "none", or a bug put back on purpose (README "Mutants")
    MaxBytes,    \* model checking: [Sides -> [Sids -> Nat]], bytes each side writes on each session
    Drain,       \* model checking: the sides that may send GOAWAY
    Ends,        \* model checking: the close codes used (Half, Full, RST; ABT: Mux.cut after a GOAWAY)
    MaxOut,      \* model checking: frames on the way each way
    MaxPings,    \* model checking: pings per side
    Crash,       \* model checking: a side may close its connection abruptly
    MaxSess      \* each side's max_sessions (WIRE.md section 5): sessions the peer opened, not yet closed

Sides == {"d", "l"}
Peer(p) == IF p = "d" THEN "l" ELSE "d"
Thr(p) == Win[p] \div 4                       \* Credit.Consumed: a grant per quarter window

\* Close frame codes (WIRE.md section 5); 3 stands for an ABORT (section 12).
Full == 0
Half == 1
RST == 2
ABT == 3
NoCode == 4    \* no close sent or received yet

\* at: for a close, the bytes of the session its sender sent before it.
Frame(t, s, n) == [t |-> t, s |-> s, n |-> n, at |-> 0]

VARIABLES
    out,      \* [Sides -> Seq(Frame)]: frames p sent that the peer has not read
    cr,       \* [Sides -> [Sids -> credit]]: Credit.sessions
    ss,       \* [Sides -> [Sids -> session]]: Mux.sess
    pend,     \* credit taken by a Write, its frame not buffered yet
    gpend,    \* a window update Consumed decided, not buffered yet
    sent,     \* bytes p buffered for s (data frames)
    dlv,      \* bytes p queued for its reader of s
    ga,       \* Conn.goingAway: either side sent GOAWAY
    sentGA,   \* p sent GOAWAY (goAwayAt set)
    gaPinged, \* p pinged after its GOAWAY
    peerGA,   \* p read the peer's GOAWAY
    drained,  \* the pong of a ping p sent after its GOAWAY came
    pong,     \* the ping p has to answer: 0 none, 1 one sent before the peer's GOAWAY, 2 after
    pings,    \* pings p sent
    wshut,    \* closeFlushed: p's frames all went out, its write side closed
    closed,   \* Conn.Close ran at p
    down,     \* Mux.close ran at p (Serve returned)
    appDone,  \* p's application ended s (Close, Reset or CloseWrite)
    crashes,  \* abrupt closes so far
    err,      \* the protocol errors and broken invariants seen by receivers
    wt        \* [Sides -> [Sids -> {"no", "data", "open"}]]: a frame checked, waiting in frameLocked (wmu let go)

vars == <<out, cr, ss, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA,
          drained, pong, pings, wshut, closed, down, appDone, crashes, err, wt>>

NoCredit == [on |-> FALSE, n |-> 0, held |-> 0, cons |-> 0]
\* closing: a Close, Reset or abort that forgot the credit and has yet to buffer its frame (code + 1).
\* code, got, fin: history for the interface (LinkSession.tla): the code of the last close
\* this side buffered (a reset kept), the first one it received while the session was
\* open (NoCode: none), and whether it buffered a FIN (code Half or Full).
NoSession == [st |-> "none", wfin |-> FALSE, rfin |-> FALSE, pclosed |-> FALSE, q |-> 0, acc |-> FALSE, closing |-> 0,
              code |-> NoCode, got |-> NoCode, fin |-> FALSE]

Init ==
    /\ out = [p \in Sides |-> <<>>]
    /\ cr = [p \in Sides |-> [s \in Sids |-> NoCredit]]
    /\ ss = [p \in Sides |-> [s \in Sids |-> NoSession]]
    /\ pend = [p \in Sides |-> [s \in Sids |-> 0]]
    /\ gpend = [p \in Sides |-> [s \in Sids |-> 0]]
    /\ sent = [p \in Sides |-> [s \in Sids |-> 0]]
    /\ dlv = [p \in Sides |-> [s \in Sids |-> 0]]
    /\ ga = [p \in Sides |-> FALSE]
    /\ sentGA = [p \in Sides |-> FALSE]
    /\ gaPinged = [p \in Sides |-> FALSE]
    /\ peerGA = [p \in Sides |-> FALSE]
    /\ drained = [p \in Sides |-> FALSE]
    /\ pong = [p \in Sides |-> 0]
    /\ pings = [p \in Sides |-> 0]
    /\ wshut = [p \in Sides |-> FALSE]
    /\ closed = [p \in Sides |-> FALSE]
    /\ down = [p \in Sides |-> FALSE]
    /\ appDone = [p \in Sides |-> [s \in Sids |-> FALSE]]
    /\ crashes = 0
    /\ err = {}
    /\ wt = [p \in Sides |-> [s \in Sids |-> "no"]]

-----------------------------------------------------------------------------
(* Sending: a frame joins out[p] unless p's connection is closed or its     *)
(* write side shut, when the network never takes it.                        *)
CanSend(p) == ~closed[p] /\ ~wshut[p]
Put(p, seq) == out' = [out EXCEPT ![p] = IF CanSend(p) THEN seq ELSE @]
Send(p, f) == Put(p, Append(out[p], f))

\* Mutant fin-skips-queue: a close goes in before the session's data
\* frames still queued, instead of after them.
RECURSIVE Before(_, _)
Before(seq, f) ==
    IF seq = <<>> THEN <<f>>
    ELSE IF Head(seq).t = "data" /\ Head(seq).s = f.s THEN <<f>> \o seq
    ELSE <<Head(seq)>> \o Before(Tail(seq), f)
SendClose(p, s, code) ==
    LET f == [Frame("close", s, code) EXCEPT !.at = sent[p][s]] IN
    IF Mutant = "fin-skips-queue" THEN Put(p, Before(out[p], f)) ELSE Send(p, f)

Forget(p, s) == [cr EXCEPT ![p][s] = NoCredit]
Answered(p, s) == ~Chain \/ Owner[s] = p \/ ss[p][s].acc   \* autoAccept ran
\* A Refuse took the session's answer (Session.ans) and its ABORT is not
\* buffered yet (its credit lives): review r2 of LinkSession, finding 2.
\* Refuse is checked where Session.Abort is (ABT in Ends, no drain:
\* LinkSess.cfg, LinkRefuse.cfg).
Refuse == Drain = {}
Refusing(p, s) == ss[p][s].closing = ABT + 1 /\ cr[p][s].on

-----------------------------------------------------------------------------
(* The application's steps.                                                 *)

\* Conn.sendOpen checks under wmu, then frameLocked may wait for its turn
\* (a ticket: wmu let go) and checks again just before the OPEN is buffered
\* (review F2: a pong that never waits goes out meanwhile). The count of
\* this side's sessions includes one whose CLOSE or ABORT is not buffered
\* yet: its credit is forgotten under wmu, after its frame took its place
\* in line (review F3; mutant abort-forget-early: Conn.sendAbort forgot
\* before it took wmu). A Mux never reuses an id; a Conn refuses one its
\* Credit knows (review F5; mutant reopen-known: it did not look).
Count(p, o) == Cardinality({x \in Sids : cr[p][x].on /\ Owner[x] = o})
OpenOK(p) == ~ga[p] \/ (Mutant = "open-after-goaway" /\ ~sentGA[p])
OpenCheck(p, s) ==
    /\ Owner[s] = p /\ ~down[p] /\ wt[p][s] = "no" /\ OpenOK(p)
    /\ \/ ss[p][s].st = "none" /\ Count(p, p) < MaxSess
       \/ Mutant = "reopen-known" /\ ss[p][s].st = "open" /\ cr[p][s].on
    /\ Mutant = "abort-forget-early" \/ \A x \in Sids : ss[p][x].closing = 0
    /\ cr' = [cr EXCEPT ![p][s] = [NoCredit EXCEPT !.on = TRUE, !.n = Win[Peer(p)]]]
    /\ wt' = [wt EXCEPT ![p][s] = "open"]
    /\ UNCHANGED <<out, ss, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err>>

\* An OPEN buffered once the connection closed never goes out: Mux.OpenChain's
\* Flush fails and the open is undone, as OpenRefused (finding I2).
OpenBuffer(p, s) ==
    /\ wt[p][s] = "open" /\ ~closed[p]
    /\ Mutant = "open-no-recheck" \/ OpenOK(p)
    /\ ss' = [ss EXCEPT ![p][s] = [NoSession EXCEPT !.st = "open"]]
    /\ Send(p, Frame("open", s, 0))
    /\ wt' = [wt EXCEPT ![p][s] = "no"]
    /\ UNCHANGED <<cr, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err>>

\* The check after the wait failed: ErrGoAway, the credit undone (Mux.drop);
\* or the connection closed meanwhile.
OpenRefused(p, s) ==
    /\ wt[p][s] = "open" /\ ((~OpenOK(p) /\ Mutant # "open-no-recheck") \/ closed[p])
    /\ cr' = Forget(p, s)
    /\ ss' = [ss EXCEPT ![p][s].st = "gone"]
    /\ wt' = [wt EXCEPT ![p][s] = "no"]
    /\ UNCHANGED <<out, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err>>

\* Trace.tla: an open whose frame did not wait (OpenCheck, OpenBuffer at once).
Open(p, s) ==
    /\ Owner[s] = p /\ ~down[p] /\ wt[p][s] = "no" /\ OpenOK(p)
    /\ ss[p][s].st = "none" /\ Count(p, p) < MaxSess
    /\ \A x \in Sids : ss[p][x].closing = 0
    /\ cr' = [cr EXCEPT ![p][s] = [NoCredit EXCEPT !.on = TRUE, !.n = Win[Peer(p)]]]
    /\ ss' = [ss EXCEPT ![p][s].st = "open"]
    /\ Send(p, Frame("open", s, 0))
    /\ UNCHANGED <<pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err, wt>>

Accept(p, s) ==                                  \* Session.autoAccept -> Conn.sendAccept
    /\ Chain /\ Owner[s] # p /\ ss[p][s].st = "open" /\ ~ss[p][s].acc
    /\ ~Refusing(p, s)                            \* ansOnce: the refusal answered it
    /\ ss' = [ss EXCEPT ![p][s].acc = TRUE]
    /\ Send(p, Frame("accept", s, 0))
    /\ UNCHANGED <<cr, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err, wt>>

\* Session.acquire looks at halfClosed before it waits, not after: credit
\* may come after this side's FIN (SendData then refuses it).
Acquire(p, s, n) ==                              \* Session.acquire -> Credit.acquire
    /\ ss[p][s].st = "open" /\ pend[p][s] = 0 /\ (Answered(p, s) \/ Refusing(p, s))
    /\ cr[p][s].on /\ n > 0 /\ cr[p][s].n >= n
    /\ cr' = [cr EXCEPT ![p][s].n = @ - n]
    /\ pend' = [pend EXCEPT ![p][s] = n]
    /\ UNCHANGED <<out, ss, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err, wt>>

\* Conn.sendData buffers nothing once the write side closed or the credit
\* ended, checked under wmu; frameLocked may then wait (turns or a ticket:
\* wmu let go) while a CloseWrite, Close, Reset or Abort buffers its frame,
\* so it checks again just before the frame is buffered. Mutants:
\* write-race, no check at all (finding 1); turns-no-recheck, the first
\* check only (review F1).
\* On FeatureChain, no data of a session the peer opened before its ACCEPT:
\* Credit.unanswered, which a Refuse's taken answer does not pass (review
\* r2 of LinkSession, finding 2; mutant refuse-then-write: the check left
\* out, the code before: the opener's Credit took the data for a protocol
\* error and the whole connection ended).
DataOK(p, s) == ~ss[p][s].wfin /\ cr[p][s].on /\ (Answered(p, s) \/ Mutant = "refuse-then-write")
SendCheck(p, s) ==                               \* Conn.sendData, then frameLocked waits
    /\ pend[p][s] > 0 /\ wt[p][s] = "no"
    /\ Mutant = "write-race" \/ DataOK(p, s)
    /\ wt' = [wt EXCEPT ![p][s] = "data"]
    /\ UNCHANGED <<out, cr, ss, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err>>

Buffer(p, s) ==
    /\ Send(p, Frame("data", s, pend[p][s]))
    /\ sent' = [sent EXCEPT ![p][s] = @ + (IF CanSend(p) THEN pend[p][s] ELSE 0)]
    /\ pend' = [pend EXCEPT ![p][s] = 0]

SendBuffer(p, s) ==                              \* frameLocked -> dataLocked
    /\ wt[p][s] = "data"
    /\ Mutant \in {"write-race", "turns-no-recheck"} \/ DataOK(p, s)
    /\ Buffer(p, s)
    /\ wt' = [wt EXCEPT ![p][s] = "no"]
    /\ UNCHANGED <<cr, ss, gpend, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err>>

\* Trace.tla: a data frame that did not wait (SendCheck, SendBuffer at once).
SendData(p, s) ==
    /\ pend[p][s] > 0 /\ wt[p][s] = "no" /\ DataOK(p, s)
    /\ Buffer(p, s)
    /\ UNCHANGED <<cr, ss, gpend, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err, wt>>

\* A Write gives back the credit it took and sends nothing: the write side
\* closed or the credit ended meanwhile (Conn.sendData refused it, before
\* or after a wait), or its deadline passed (not modelled otherwise).
\* Credit.refund.
Refund(p, s) ==
    /\ pend[p][s] > 0
    /\ cr' = IF cr[p][s].on THEN [cr EXCEPT ![p][s].n = @ + pend[p][s]] ELSE cr
    /\ pend' = [pend EXCEPT ![p][s] = 0]
    /\ wt' = [wt EXCEPT ![p][s] = "no"]
    /\ UNCHANGED <<out, ss, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err>>

\* Session.CloseWrite in two steps, as the code takes them (review r2 of
\* LinkSession, finding 1): halfClosed set (Mark: no data after it), then
\* the CLOSE buffered (Send, finSent); the session is dropped once both
\* halves are closed and this side's CLOSE is buffered, by whichever of
\* Send and the receive of the peer's FIN comes second. Mutant
\* drop-on-mark (the code before): the receive dropped it on the mark, and
\* a leaving connection then shut its write side before the CLOSE.
CloseWriteMark(p, s) ==
    /\ ss[p][s].st = "open" /\ ~ss[p][s].wfin /\ ss[p][s].closing = 0
    /\ Answered(p, s)
    /\ ss' = [ss EXCEPT ![p][s].wfin = TRUE, ![p][s].code = IF @ = ABT THEN @ ELSE Half]
    /\ appDone' = [appDone EXCEPT ![p][s] = TRUE]
    /\ UNCHANGED <<out, cr, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, crashes, err, wt>>
CloseWriteSend(p, s) ==
    /\ ss[p][s].wfin /\ ~ss[p][s].fin
    /\ SendClose(p, s, Half)
    /\ IF ss[p][s].rfin /\ ss[p][s].st = "open"   \* both halves: drop
       THEN /\ ss' = [ss EXCEPT ![p][s].st = "gone", ![p][s].pclosed = TRUE, ![p][s].fin = TRUE]
            /\ cr' = Forget(p, s)
       ELSE /\ ss' = [ss EXCEPT ![p][s].fin = TRUE]
            /\ UNCHANGED cr
    /\ UNCHANGED <<pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err, wt>>

\* The two at once (Trace.tla: the recorder logs the CLOSE, not the mark).
CloseWrite(p, s) ==
    /\ ss[p][s].st = "open" /\ ~ss[p][s].wfin /\ ss[p][s].closing = 0
    /\ Answered(p, s)
    /\ SendClose(p, s, Half)
    /\ IF ss[p][s].rfin                          \* both halves: drop
       THEN /\ ss' = [ss EXCEPT ![p][s].wfin = TRUE, ![p][s].st = "gone", ![p][s].pclosed = TRUE, ![p][s].code = IF @ = ABT THEN @ ELSE Half, ![p][s].fin = TRUE]
            /\ cr' = Forget(p, s)
       ELSE /\ ss' = [ss EXCEPT ![p][s].wfin = TRUE, ![p][s].code = IF @ = ABT THEN @ ELSE Half, ![p][s].fin = TRUE]
            /\ UNCHANGED cr
    /\ appDone' = [appDone EXCEPT ![p][s] = TRUE]
    /\ UNCHANGED <<pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, crashes, err, wt>>

\* Session.Close (code Full), Session.Reset (RST) and Mux.cut's abort
\* (ABT, Conn.sendAbort: no answer first) end a session in two steps:
\* Conn.sendClose forgets the credit, then buffers the frame, which may wait
\* for room in the write buffer meanwhile; then Mux.drop (not after an
\* abort: Mux.cut leaves the session to the connection's close).
Closing(p, s, code) ==
    /\ ss[p][s].st = "open" /\ ss[p][s].closing = 0
    /\ IF code = ABT THEN cr[p][s].on ELSE Answered(p, s)
    /\ cr' = Forget(p, s)
    /\ ss' = [ss EXCEPT ![p][s].closing = code + 1]
    /\ appDone' = [appDone EXCEPT ![p][s] = @ \/ code # ABT]
    /\ UNCHANGED <<out, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, crashes, err, wt>>

\* Session.Refuse (AbortWith, Refused) of a session the peer opened, not
\* answered: the answer taken (ansOnce), then the ABORT (End, which forgets
\* the credit as Conn.sendAbort does, under wmu, and drops the session).
RefuseMark(p, s) ==
    /\ Chain /\ Owner[s] # p /\ ss[p][s].st = "open" /\ ~ss[p][s].acc /\ ss[p][s].closing = 0
    /\ ~ss[p][s].wfin
    /\ ss' = [ss EXCEPT ![p][s].closing = ABT + 1]
    /\ appDone' = [appDone EXCEPT ![p][s] = TRUE]
    /\ UNCHANGED <<out, cr, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, crashes, err, wt>>

\* Mutant abort-as-fin: a Reset or Abort goes out as a Close (the peer
\* reads io.EOF, not the reset).
End(p, s) ==
    LET code == ss[p][s].closing - 1 IN
    /\ ss[p][s].closing # 0
    /\ SendClose(p, s, IF Mutant = "abort-as-fin" /\ code \in {RST, ABT} THEN Full ELSE code)
    /\ ss' = [ss EXCEPT ![p][s].closing = 0, ![p][s].st = IF code = ABT /\ ~cr[p][s].on THEN @ ELSE "gone",
                                 ![p][s].code = IF @ \in {RST, ABT} THEN @ ELSE code,
                                 ![p][s].fin = @ \/ code = Full]   \* a Close after Mux.cut's abort
    /\ cr' = IF cr[p][s].on THEN Forget(p, s) ELSE cr   \* a refusal's (RefuseMark)
    /\ UNCHANGED <<pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err, wt>>

\* A read takes k bytes off the session's queue (bytequeue's OnPop ->
\* Credit.Consumed): a quarter window read is granted back (gpend).
Read(p, s, k) ==
    /\ k > 0 /\ ss[p][s].q >= k /\ gpend[p][s] = 0
    /\ ss' = [ss EXCEPT ![p][s].q = @ - k]
    /\ IF cr[p][s].on
       THEN LET c == cr[p][s].cons + k
                r == IF Mutant = "grant-early" \/ (Mutant = "release-early" /\ cr[p][s].held > c)
                     THEN c + 1 ELSE c
                h == IF Mutant = "release-early" THEN r ELSE c   \* a byte still queued released
            IN IF c >= Thr(p)
               THEN /\ cr' = [cr EXCEPT ![p][s].cons = 0, ![p][s].held = IF @ > h THEN @ - h ELSE 0]
                    /\ gpend' = [gpend EXCEPT ![p][s] = r]
               ELSE /\ cr' = [cr EXCEPT ![p][s].cons = c]
                    /\ UNCHANGED gpend
       ELSE UNCHANGED <<cr, gpend>>
    /\ UNCHANGED <<out, pend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err, wt>>

SendWindow(p, s) ==                              \* Credit.grant -> Conn.sendWindow
    /\ gpend[p][s] > 0
    /\ Send(p, Frame("win", s, gpend[p][s]))
    /\ gpend' = [gpend EXCEPT ![p][s] = 0]
    /\ UNCHANGED <<cr, ss, pend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err, wt>>

GoAway(p) ==                                     \* Mux.goAway -> Conn.GoAway
    /\ ~down[p]
    /\ ga' = [ga EXCEPT ![p] = TRUE]
    /\ sentGA' = [sentGA EXCEPT ![p] = TRUE]
    /\ Send(p, Frame("goaway", NoSid, 0))
    /\ UNCHANGED <<cr, ss, pend, gpend, sent, dlv, gaPinged, peerGA, drained, pong,
                   pings, wshut, closed, down, appDone, crashes, err, wt>>

\* Conn.ping: g is 1 when the ping's time is past goAwayAt (it counts for
\* the drain barrier); a ping timed before the GOAWAY went out may be
\* buffered after it, with g 0.
Ping(p, g) ==
    /\ g \in {0, 1} /\ (g = 1 => sentGA[p])
    /\ Send(p, Frame("ping", NoSid, g))
    /\ pings' = [pings EXCEPT ![p] = @ + 1]
    /\ gaPinged' = [gaPinged EXCEPT ![p] = @ \/ g = 1]
    /\ UNCHANGED <<cr, ss, pend, gpend, sent, dlv, ga, sentGA, peerGA, drained, pong,
                   wshut, closed, down, appDone, crashes, err, wt>>

\* keepAlive answers the latest ping (Conn.control); a pong still last in
\* the write buffer is overwritten in place (rep) rather than followed.
Pong(p, rep) ==
    /\ pong[p] # 0
    /\ IF rep
       THEN /\ out[p] # <<>> /\ out[p][Len(out[p])].t = "pong"
            /\ Put(p, [out[p] EXCEPT ![Len(out[p])] = Frame("pong", NoSid, pong[p] - 1)])
       ELSE Send(p, Frame("pong", NoSid, pong[p] - 1))
    /\ pong' = [pong EXCEPT ![p] = 0]
    /\ UNCHANGED <<cr, ss, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained,
                   pings, wshut, closed, down, appDone, crashes, err, wt>>

-----------------------------------------------------------------------------
(* The receive loop of p applies the peer's next frame (Conn.recv, then     *)
(* Mux.serve or Mux.data).                                                  *)

Flag(e) == err' = err \cup {e}

RecvOpen(p, f) ==
    LET s == f.s IN
    IF ss[p][s].st # "none" THEN Flag("open of an id in use") /\ UNCHANGED <<cr, ss>>
    ELSE IF peerGA[p] \/ drained[p]
    THEN Flag("open after GOAWAY") /\ UNCHANGED <<cr, ss>>   \* the code refuses it with a reset
    ELSE IF Count(p, Peer(p)) >= MaxSess
    THEN Flag("open past max_sessions") /\ UNCHANGED <<cr, ss>>   \* ErrFull
    ELSE /\ cr' = [cr EXCEPT ![p][s] = [NoCredit EXCEPT !.on = TRUE, !.n = Win[Peer(p)]]]
         /\ ss' = [ss EXCEPT ![p][s].st = "open"]
         /\ UNCHANGED err

RecvData(p, f) ==                                \* Conn.next charges, Mux.data queues
    LET s == f.s
        c == cr[p][s]
        queue ==                                 \* the Mux's session, whether or not the Credit knows it
            IF ss[p][s].st = "open" /\ ~ss[p][s].rfin
            THEN /\ ss' = [ss EXCEPT ![p][s].q = @ + f.n]
                 /\ dlv' = [dlv EXCEPT ![p][s] = @ + f.n]
                 /\ UNCHANGED err
            ELSE /\ UNCHANGED <<ss, dlv>>
                 /\ IF ss[p][s].st = "open" THEN Flag("data after FIN") ELSE UNCHANGED err
    IN IF ~c.on THEN UNCHANGED <<cr, ss, dlv, err>>      \* no credit here: dropped, not charged
       ELSE IF Chain /\ Owner[s] = p /\ ~ss[p][s].acc
       THEN Flag("data before ACCEPT") /\ UNCHANGED <<cr, ss, dlv>>
       ELSE IF f.n > Win[p] - c.held
       THEN Flag("data past the window") /\ UNCHANGED <<cr, ss, dlv>>
       ELSE cr' = [cr EXCEPT ![p][s].held = @ + f.n] /\ queue

RecvWindow(p, f) ==
    LET c == cr[p][f.s] IN
    IF ~c.on THEN UNCHANGED <<cr, err>>                 \* an unknown session's: ignored
    ELSE IF f.n > Win[Peer(p)] - c.n THEN Flag("credit past the window") /\ UNCHANGED cr
    ELSE cr' = [cr EXCEPT ![p][f.s].n = @ + f.n] /\ UNCHANGED err

RecvAccept(p, f) ==
    LET s == f.s IN
    IF ~cr[p][s].on THEN UNCHANGED <<ss, err>>
    ELSE IF Owner[s] # p \/ ss[p][s].acc THEN Flag("ACCEPT not owed") /\ UNCHANGED ss
    ELSE ss' = [ss EXCEPT ![p][s].acc = TRUE] /\ UNCHANGED err

RecvClose(p, f) ==
    LET s == f.s
        x == ss[p][s]
        cr1 == IF f.n # Half THEN Forget(p, s) ELSE cr  \* Conn.recv
    IN IF x.st # "open" THEN cr' = cr1 /\ UNCHANGED <<ss, err>>
       ELSE /\ IF dlv[p][s] # f.at /\ cr[p][s].on THEN Flag("FIN overtook data") ELSE UNCHANGED err
            /\ LET g == IF x.got = NoCode THEN f.n ELSE x.got IN
               IF f.n # Half
               THEN ss' = [ss EXCEPT ![p][s].st = "gone", ![p][s].pclosed = TRUE, ![p][s].got = g] /\ cr' = cr1
               ELSE IF x.wfin /\ (x.fin \/ Mutant = "drop-on-mark")
               THEN /\ ss' = [ss EXCEPT ![p][s].rfin = TRUE, ![p][s].st = "gone", ![p][s].pclosed = TRUE, ![p][s].got = g]
                    /\ cr' = Forget(p, s)
               ELSE ss' = [ss EXCEPT ![p][s].rfin = TRUE, ![p][s].got = g] /\ cr' = cr1

Recv(p) ==
    LET q == Peer(p)
        f == Head(out[q])
    IN /\ ~down[p] /\ out[q] # <<>>
       /\ out' = [out EXCEPT ![q] = Tail(@)]
       /\ CASE f.t = "open" -> RecvOpen(p, f) /\ UNCHANGED <<dlv, ga, peerGA, drained, pong>>
            [] f.t = "data" -> RecvData(p, f) /\ UNCHANGED <<ga, peerGA, drained, pong>>
            [] f.t = "win" -> RecvWindow(p, f) /\ UNCHANGED <<ss, dlv, ga, peerGA, drained, pong>>
            [] f.t = "accept" -> RecvAccept(p, f) /\ UNCHANGED <<cr, dlv, ga, peerGA, drained, pong>>
            [] f.t = "close" -> RecvClose(p, f) /\ UNCHANGED <<dlv, ga, peerGA, drained, pong>>
            [] f.t = "goaway" -> /\ ga' = [ga EXCEPT ![p] = TRUE]
                                 /\ peerGA' = [peerGA EXCEPT ![p] = TRUE]
                                 /\ UNCHANGED <<cr, ss, dlv, drained, pong, err>>
            [] f.t = "ping" -> /\ pong' = [pong EXCEPT ![p] = f.n + 1]
                               /\ UNCHANGED <<cr, ss, dlv, ga, peerGA, drained, err>>
            [] f.t = "pong" -> /\ drained' = [drained EXCEPT ![p] = @ \/ (f.n = 1 /\ sentGA[p])]
                               /\ UNCHANGED <<cr, ss, dlv, ga, peerGA, pong, err>>
       /\ UNCHANGED <<pend, gpend, sent, sentGA, gaPinged, pings, wshut, closed, down,
                      appDone, crashes, wt>>

-----------------------------------------------------------------------------
(* The connection's end.                                                    *)

Leave(p) ==                                      \* closeFlushed: every frame out, write side shut
    /\ ~wshut[p] /\ ~closed[p]
    /\ wshut' = [wshut EXCEPT ![p] = TRUE]
    /\ UNCHANGED <<out, cr, ss, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA,
                   drained, pong, pings, closed, down, appDone, crashes, err, wt>>

\* Conn.Close: the peer reads the first k frames p sent (all after Leave).
CloseAt(p, k) ==
    /\ ~closed[p]
    /\ k \in (IF wshut[p] THEN {Len(out[p])} ELSE 0..Len(out[p]))
    /\ closed' = [closed EXCEPT ![p] = TRUE]
    /\ out' = [out EXCEPT ![p] = SubSeq(@, 1, k)]
    /\ UNCHANGED <<cr, ss, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained,
                   pong, pings, wshut, down, appDone, err, wt>>
\* Crashed counts its closes: CloseConn left crashes unchanged once, so no
\* crash was ever taken (WitnessLost, MCLink.tla, now guards it).
CloseConn(p, k) == CloseAt(p, k) /\ UNCHANGED crashes

\* Mutant lost-as-eof: a session the connection's end cut reads io.EOF
\* (as if the peer had closed it), not ErrConnClosed.
Down(p) ==                                       \* Mux.close: every session at p cut
    /\ closed[p] /\ ~down[p]
    /\ down' = [down EXCEPT ![p] = TRUE]
    /\ ss' = [ss EXCEPT ![p] = [s \in Sids |->
                IF @[s].st = "open"
                THEN [@[s] EXCEPT !.st = "gone",
                                  !.got = IF Mutant = "lost-as-eof" /\ @ = NoCode THEN Full ELSE @]
                ELSE @[s]]]
    /\ UNCHANGED <<out, cr, pend, gpend, sent, dlv, ga, sentGA, gaPinged, peerGA, drained,
                   pong, pings, wshut, closed, appDone, crashes, err, wt>>

-----------------------------------------------------------------------------
(* Model checking: the steps above, bounded, with the Mux's own guards on   *)
(* when it drains and leaves.                                               *)

OpenAt(p) == {s \in Sids : ss[p][s].st = "open"}
Ended(s) == \A p \in Sides : ss[p][s].st # "open"

\* Mux.leave: leaving, the peer read the GOAWAY, no session left.
LeaveWhenDrained(p) == sentGA[p] /\ drained[p] /\ OpenAt(p) = {} /\ Leave(p)
\* The peer's end reached p: its read sees EOF once it read every frame.
EOFClose(p) == (closed[Peer(p)] \/ wshut[Peer(p)]) /\ out[Peer(p)] = <<>> /\ CloseConn(p, Len(out[p]))
DrainPing(p) == sentGA[p] /\ ~gaPinged[p] /\ Ping(p, 1)
Crashed(p) == crashes < Crash /\ \E k \in 0..Len(out[p]) : CloseAt(p, k) /\ crashes' = crashes + 1
\* The ends TLC's deadlock check allows: both sides down, or an idle
\* connection (no session open, nothing on the way, nothing to answer).
Idle == \A p \in Sides : OpenAt(p) = {} /\ out[p] = <<>> /\ pong[p] = 0
                         /\ \A s \in Sids : pend[p][s] = 0 /\ gpend[p][s] = 0 /\ ss[p][s].closing = 0
                                                 /\ (ss[p][s].wfin => ss[p][s].fin)
                                                 /\ wt[p][s] = "no"
Terminated == ((down["d"] /\ down["l"]) \/ Idle) /\ UNCHANGED vars

AppStep(p, s) ==
    \/ OpenCheck(p, s)
    \/ Accept(p, s)
    \/ \E n \in 1..Chunk : sent[p][s] + pend[p][s] + n <= MaxBytes[p][s] /\ Acquire(p, s, n)
    \/ Half \in Ends /\ CloseWriteMark(p, s)
    \/ \E code \in Ends \ {Half, ABT} : Closing(p, s, code)
    \* Mux.cut's abort after a GOAWAY; Session.Abort any time where no side
    \* drains (LinkSess.cfg: LinkDrain.cfg's state space stays small).
    \/ ABT \in Ends /\ (sentGA[p] \/ Drain = {}) /\ Closing(p, s, ABT)
    \/ ABT \in Ends /\ Refuse /\ RefuseMark(p, s)
    \/ \E k \in 1..ss[p][s].q : Read(p, s, k)

Next ==
    \/ \E p \in Sides, s \in Sids :
        \/ AppStep(p, s)
        \/ SendCheck(p, s)
        \/ SendBuffer(p, s)
        \/ OpenBuffer(p, s)
        \/ OpenRefused(p, s)
        \/ Refund(p, s)
        \/ SendWindow(p, s)
        \/ End(p, s)
        \/ CloseWriteSend(p, s)
    \/ \E p \in Sides :
        \/ p \in Drain /\ ~sentGA[p] /\ GoAway(p)
        \/ pings[p] < MaxPings /\ Ping(p, 0)
        \/ DrainPing(p)
        \/ \E rep \in BOOLEAN : Pong(p, rep)
        \/ Recv(p)
        \/ LeaveWhenDrained(p)
        \/ EOFClose(p)
        \/ Crashed(p)
        \/ Down(p)
    \/ Terminated

\* Every step that is not the application's is taken once it can be.
Fairness ==
    /\ \A p \in Sides :
        /\ WF_vars(Recv(p))
        /\ WF_vars(\E rep \in BOOLEAN : Pong(p, rep))
        /\ WF_vars(DrainPing(p))
        /\ WF_vars(LeaveWhenDrained(p))
        /\ WF_vars(EOFClose(p))
        /\ WF_vars(Down(p))
        /\ \A s \in Sids : WF_vars(SendCheck(p, s)) /\ WF_vars(SendBuffer(p, s))
                          /\ WF_vars(OpenBuffer(p, s)) /\ WF_vars(OpenRefused(p, s))
                          /\ WF_vars(Refund(p, s))
                          /\ WF_vars(SendWindow(p, s)) /\ WF_vars(End(p, s))
                          /\ WF_vars(CloseWriteSend(p, s))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Invariants (README "Invariants").                                        *)

NoProtocolError == err = {}

\* The write side shuts (closeFlushed) only after every CloseWrite's CLOSE
\* is buffered while the peer still reads the session (review r2 of
\* LinkSession, finding 1): a graceful end is never read as lost. A CLOSE
\* still due after this side's reset or abort is no FIN (the peer reads the
\* reset), nor one to a peer that closed the session whole.
FinBeforeShut ==
    \A p \in Sides, s \in Sids :
        (wshut[p] /\ ss[p][s].wfin /\ ~ss[p][s].fin /\ ss[p][s].code = Half) => ss[Peer(p)][s].st # "open"

CreditBounds == \A p \in Sides, s \in Sids : cr[p][s].n >= 0 /\ cr[p][s].n <= Win[Peer(p)]

BufferBound == \A p \in Sides, s \in Sids : ss[p][s].q <= Win[p] /\ cr[p][s].held <= Win[p]

\* Why the receive loop never waits on a session's full queue (review F6):
\* the queue holds only bytes still charged, and the charge stays within
\* the window (bytequeue.Push waits only past the window).
QueueWithinHeld == \A p \in Sides, s \in Sids : cr[p][s].on => ss[p][s].q <= cr[p][s].held

RECURSIVE SumFrom(_, _, _, _)
SumFrom(seq, i, t, s) ==                          \* by index: Tail copies the sequence
    IF i > Len(seq) THEN 0
    ELSE (IF seq[i].t = t /\ seq[i].s = s THEN seq[i].n ELSE 0) + SumFrom(seq, i + 1, t, s)
SumIn(seq, t, s) == SumFrom(seq, 1, t, s)

\* The peer's window is split between p's credit, p's Write in flight,
\* data on the wire, the peer's buffer and its grants on their way back.
Conservation ==
    \A p \in Sides, s \in Sids :
        LET q == Peer(p) IN
        (cr[p][s].on /\ cr[q][s].on /\ ~closed[p] /\ ~closed[q]) =>
            cr[p][s].n + pend[p][s] + SumIn(out[p], "data", s)
              + cr[q][s].held + gpend[q][s] + SumIn(out[q], "win", s) = Win[q]

TypeOK ==
    /\ \A p \in Sides, s \in Sids : ss[p][s].st \in {"none", "open", "gone"}
    /\ \A p \in Sides : pong[p] \in 0..2
    /\ \A p \in Sides, s \in Sids : ss[p][s].closing \in 0..ABT + 1
    /\ \A p \in Sides, s \in Sids : wt[p][s] \in {"no", "data", "open"}

\* Liveness (README "Liveness").
SessionsEnd == \A s \in Sids : (appDone["d"][s] /\ appDone["l"][s]) ~> Ended(s)
ConnEnds == \A p \in Sides : closed[p] ~> (down["d"] /\ down["l"])
DrainEnds == \A p \in Sides : (sentGA[p] /\ drained[p] /\ \A s \in Sids : Ended(s)) ~> down[p]

\* Model checking bound: frames in flight.
Bounded == \A p \in Sides : Len(out[p]) <= MaxOut
=============================================================================
