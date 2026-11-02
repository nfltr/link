----------------------------- MODULE LinkSession -----------------------------
(* What a layer above link may assume of its sessions: the interface.      *)
(* Link.tla (the protocol as the Go code implements it) refines it: TLC    *)
(* checks Refines (MCLink.tla) on every configuration of make spec, and    *)
(* make spec-mutants shows each guarantee broken in Link.tla fails it.     *)
(* spec/README.md "The session interface" says it in words.                *)
(*                                                                         *)
(* The interface is a transition constraint: Next allows any step in       *)
(* which every guarantee below holds (several fields may change at once:   *)
(* one receive of a close ends a session and types its end). Bytes are     *)
(* counts; a reader is handed a prefix of what its peer wrote, in order    *)
(* (one FIFO connection), so a count says which bytes.                     *)
(*                                                                         *)
(* Timed guarantees are Liveness.tla's (dead peer: a silent peer is        *)
(* declared dead within Interval + Timeout + 2 x Late of its last frame,    *)
(* a peer that answers within Timeout never, DetectBound and NoFalseDead); *)
(* the ends of sessions and connections are Link.tla's liveness            *)
(* (SessionsEnd, ConnEnds, DrainEnds). Here they show as their effect: a   *)
(* connection that ends (dead peer or closed) ends its sessions "lost".    *)
EXTENDS Naturals

CONSTANTS
    Sides,   \* the connection's two ends
    Sids,    \* session ids
    Owner,   \* [Sids -> Sides]: the side that opens each
    Win      \* [Sides -> Nat]: each side's receive window per session

Peer(p) == CHOOSE x \in Sides : x # p

VARIABLES
    st,     \* [Sides -> [Sids -> {"none", "open", "gone"}]]: the session at p
    w,      \* [Sides -> [Sids -> Nat]]: bytes p wrote on s that went out
    d,      \* [Sides -> [Sids -> Nat]]: bytes handed to p's reader of s (queued)
    c,      \* [Sides -> [Sids -> Nat]]: bytes p's reader took
    wend,   \* [Sides -> [Sids -> {"", "fin", "close", "reset"}]]: how p ended its writes:
            \*   CloseWrite ("fin"), Close ("close": both ways), Reset or Abort ("reset")
    fin,    \* [Sides -> [Sids -> BOOLEAN]]: p sent its FIN (CloseWrite or Close), perhaps reset after
    shut,   \* [Sides -> [Sids -> BOOLEAN]]: p ended its own reads (Close, Reset, Abort)
    rend,   \* [Sides -> [Sids -> {"", "eof", "reset", "lost"}]]: how p's reader learned
            \*   the peer's end: io.EOF, ErrReset or *Abort, ErrConnClosed
    ga,     \* [Sides -> BOOLEAN]: p sent or read GOAWAY
    up,     \* [Sides -> BOOLEAN]: p's connection serves (Mux.Serve has not returned)
    fault   \* the connection failed on a protocol error (ErrFrame, ErrCredit)

ivars == <<st, w, d, c, wend, fin, shut, rend, ga, up, fault>>

Init ==
    /\ st = [p \in Sides |-> [s \in Sids |-> "none"]]
    /\ w = [p \in Sides |-> [s \in Sids |-> 0]]
    /\ d = [p \in Sides |-> [s \in Sids |-> 0]]
    /\ c = [p \in Sides |-> [s \in Sids |-> 0]]
    /\ wend = [p \in Sides |-> [s \in Sids |-> ""]]
    /\ fin = [p \in Sides |-> [s \in Sids |-> FALSE]]
    /\ shut = [p \in Sides |-> [s \in Sids |-> FALSE]]
    /\ rend = [p \in Sides |-> [s \in Sids |-> ""]]
    /\ ga = [p \in Sides |-> FALSE]
    /\ up = [p \in Sides |-> TRUE]
    /\ fault = FALSE

Changed(v, p, s) == v'[p][s] # v[p][s]

\* 1. Sessions. An id goes none -> open -> gone once: it is never reused.
\* The opener opens only before GOAWAY (after it, Open returns ErrGoAway:
\* none -> gone); the peer's end opens only an id its opener opened. A
\* session ends with a typed reason: its own whole close, the connection's
\* end, the peer's reset, or a FIN each way (or the peer's Close).
Opened(p, s) ==
    IF Owner[s] = p THEN ~ga[p] ELSE st[Peer(p)][s] # "none"
Reason(p, s) ==
    \/ shut'[p][s] \/ ~up'[p] \/ rend'[p][s] = "reset"
    \/ rend'[p][s] = "eof" /\ (wend'[p][s] # "" \/ wend'[Peer(p)][s] = "close")
GSessions ==
    \A p \in Sides, s \in Sids : Changed(st, p, s) =>
        \/ st[p][s] = "none" /\ st'[p][s] = "open" /\ Opened(p, s)
        \/ st[p][s] = "none" /\ st'[p][s] = "gone" /\ Owner[s] = p /\ ga'[p]   \* ErrGoAway
        \/ st[p][s] = "open" /\ st'[p][s] = "gone" /\ Reason(p, s)

\* 2. Writes: only on a session p opened or accepted whose write side p
\* has not ended (no data after its FIN, Close or reset: the peer's EOF
\* is exact), and credit: the bytes p wrote that the peer's reader has
\* not taken never exceed the peer's window. A write may still go out
\* after the peer's whole close ended the session here (the peer drops
\* it: its reader has ended); link refuses it, the interface allows it.
GWrites ==
    \A p \in Sides, s \in Sids : Changed(w, p, s) =>
        /\ w'[p][s] > w[p][s]
        /\ st[p][s] # "none" /\ wend[p][s] = "" /\ ~shut[p][s]
        /\ w'[p][s] - c[Peer(p)][s] <= Win[Peer(p)]

\* 3. Reads: in order, a prefix of what the peer wrote, never past this
\* side's window (a stalled reader holds only its own session), never after
\* the reader's end.
GReads ==
    \A p \in Sides, s \in Sids :
        /\ Changed(d, p, s) =>
            /\ d'[p][s] > d[p][s] /\ d'[p][s] <= w[Peer(p)][s]
            /\ d'[p][s] - c[p][s] <= Win[p]
            /\ rend[p][s] = "" /\ ~shut[p][s]
        /\ Changed(c, p, s) => c'[p][s] > c[p][s] /\ c'[p][s] <= d'[p][s]

\* 4. Ends, typed. p ends its writes once (a CloseWrite may be followed by
\* a Close or a Reset); a reader's end is typed by the peer's: io.EOF only
\* after the peer's FIN (CloseWrite or Close) and every byte it wrote
\* before (unless this side dropped them: its own whole close), a reset
\* only from the peer's Reset or Abort, lost only with the connection's
\* end. The first end a reader learns is its end (a FIN then a reset
\* reads io.EOF).
GEnds ==
    \A p \in Sides, s \in Sids :
        /\ Changed(wend, p, s) =>
            /\ \/ wend[p][s] = ""
               \/ wend[p][s] = "fin" /\ wend'[p][s] \in {"close", "reset"}
            /\ st[p][s] = "open" \/ shut[p][s]
        /\ Changed(shut, p, s) => ~shut[p][s] /\ st[p][s] = "open"
        /\ Changed(rend, p, s) =>
            /\ rend[p][s] = ""
            /\ CASE rend'[p][s] = "eof" ->
                      /\ fin[Peer(p)][s]
                      /\ shut[p][s] \/ d[p][s] = w[Peer(p)][s]
                 [] rend'[p][s] = "reset" -> wend[Peer(p)][s] = "reset"
                 [] rend'[p][s] = "lost" -> ~up'[p]
                 [] OTHER -> FALSE

\* 5. The connection: GOAWAY is final; its end is final and ends every
\* session at that side; a correct peer never makes it fail.
GConn ==
    /\ \A p \in Sides : ga[p] => ga'[p]
    /\ \A p \in Sides : ~up[p] => ~up'[p]
    /\ \A p \in Sides : ~up'[p] => \A s \in Sids : st'[p][s] # "open"
    /\ ~fault'

Next == GSessions /\ GWrites /\ GReads /\ GEnds /\ GConn

Spec == Init /\ [][Next]_ivars
=============================================================================
