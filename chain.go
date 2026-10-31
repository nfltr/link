// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

// FeatureChain is feature bit 1 (WIRE.md section 6): the side reads
// ACCEPT, ABORT and CHAIN-OPEN frames, answers every session opened to it
// (Accept or Refuse) and opens sessions with CHAIN-OPEN. A connection
// where both sides announced it carries answers and typed aborts
// (Session.Answers); one where either did not is byte for byte the
// v0.9.4 connection.
const FeatureChain uint32 = 1 << 1

// Trace identifies a path of joined sessions end to end: the origin picks
// it and every hop carries it unchanged. It is for correlation (logs,
// Observer.Hop) only: not a secret, not an authenticator.
type Trace [16]byte

// Chain is where an open is on its path.
type Chain struct {
	Trace Trace // the origin's; zero in OpenChain: link picks one (crypto/rand)
	Hops  uint8 // joins the open crossed: 0 from the origin
}

type chainKey struct{}

// ContextWithChain returns ctx carrying c: Pool.Open (and peerlink.Node's
// Open) open with it (Mux.OpenChain). Forward hands its Route a ctx
// carrying the inbound session's Chain with Hops+1.
func ContextWithChain(ctx context.Context, c Chain) context.Context {
	return context.WithValue(ctx, chainKey{}, c)
}

// ChainFromContext returns the Chain ctx carries (ContextWithChain).
func ChainFromContext(ctx context.Context) (Chain, bool) {
	c, ok := ctx.Value(chainKey{}).(Chain)
	return c, ok
}

// Codes of an Abort: link's below CodeApp, the application's from it.
const (
	CodeUnspecified   uint32 = 0
	CodeRefused       uint32 = 1 // not taking sessions: no accept function, or no room
	CodeDraining      uint32 = 2 // GOAWAY or Drain: open elsewhere
	CodeHopLimit      uint32 = 3 // MaxHops reached: a loop, or a path too long
	CodeNoRoute       uint32 = 4 // the Route failed with an error that is no *Abort
	CodeConnLost      uint32 = 5 // a hop's connection ended (dead peer, network)
	CodeTimeout       uint32 = 6 // no answer within ForwardConfig.AnswerTimeout, or a write deadline
	CodeDrainDeadline uint32 = 7 // a draining hop's deadline cut the session
	CodeApp           uint32 = 1024
)

var codeNames = [...]string{"unspecified", "refused", "draining", "hop limit", "no route", "connection lost", "timeout", "drain deadline"}

// MaxAbortMsg bounds an Abort's message: Refuse and Abort cut a longer
// one at a UTF-8 boundary, decoders refuse it.
const MaxAbortMsg = 1024

// Abort is a session's typed end on a FeatureChain connection: a refusal
// (the accepter's answer instead of its accept) or a reset (either side,
// any time). Reads of the session, and Answered, return it.
// errors.Is(a, ErrOpenRefused) for refusals; errors.Is(a, ErrReset) for
// both (a refused open ends as a reset did before v0.10.0).
type Abort struct {
	Refused bool
	Code    uint32 // link's below CodeApp, the application's from CodeApp
	Hops    uint8  // joins the abort crossed: 0 = this side's peer itself
	Msg     string // at most MaxAbortMsg bytes of UTF-8
}

// Error says what ended the session, where on the path and why.
func (a *Abort) Error() string {
	what := "reset"
	if a.Refused {
		what = "refused"
	}
	code := fmt.Sprintf("code %d", a.Code)
	switch {
	case int(a.Code) < len(codeNames):
		code = codeNames[a.Code]
	case a.Code >= CodeApp:
		code = fmt.Sprintf("application code %d", a.Code-CodeApp)
	}
	s := fmt.Sprintf("link: session %s (%s) %d hop(s) beyond the peer", what, code, a.Hops)
	if a.Hops == 0 {
		s = fmt.Sprintf("link: session %s (%s) by the peer", what, code)
	}
	if a.Msg != "" {
		s += ": " + a.Msg
	}
	return s
}

// Is matches ErrOpenRefused for a refusal and ErrReset for any Abort.
func (a *Abort) Is(target error) bool {
	return target == ErrReset || target == ErrOpenRefused && a.Refused
}

// ErrOpenRefused is what a refused session's Answered and reads match
// (errors.Is): the *Abort says who and why.
var ErrOpenRefused = errors.New("link: session refused")

// ErrNoAnswers is Answered on a session whose connection did not
// negotiate FeatureChain: its peer does not answer opens.
var ErrNoAnswers = errors.New("link: the peer does not answer opens (no FeatureChain)")

// ErrDrainDeadline is matched (errors.Is) by the error of a session that
// a draining Mux's deadline cut, on the side that drained: a splice
// (Join, peerlink.Forward) sends it on as CodeDrainDeadline.
var ErrDrainDeadline = errors.New("link: the drain's deadline cut the session")

// clipMsg makes msg valid UTF-8 of at most MaxAbortMsg bytes.
func clipMsg(msg string) string {
	msg = strings.ToValidUTF8(msg, "�")
	if len(msg) <= MaxAbortMsg {
		return msg
	}
	n := MaxAbortMsg
	for n > 0 && !utf8.RuneStart(msg[n]) {
		n--
	}
	return msg[:n]
}

// abort kinds on the wire.
const (
	abortRefuse = 0
	abortReset  = 1
)

// appendAbort appends an ABORT body after its sid: kind, code, hops,
// msg_len, msg (clipped).
func appendAbort(b []byte, a *Abort) []byte {
	kind := byte(abortReset)
	if a.Refused {
		kind = abortRefuse
	}
	msg := clipMsg(a.Msg)
	b = append(b, kind)
	b = binary.BigEndian.AppendUint32(b, a.Code)
	b = append(b, a.Hops)
	b = binary.BigEndian.AppendUint16(b, uint16(len(msg))) // #nosec G115 -- clipped to MaxAbortMsg
	return append(b, msg...)
}

// parseAbort reads an ABORT body after its sid.
func parseAbort(b []byte) (*Abort, error) {
	if len(b) < 8 || b[0] > abortReset {
		return nil, ErrFrame
	}
	n := int(binary.BigEndian.Uint16(b[6:]))
	msg := b[8:]
	if n != len(msg) || n > MaxAbortMsg || !utf8.Valid(msg) {
		return nil, ErrFrame
	}
	return &Abort{Refused: b[0] == abortRefuse, Code: binary.BigEndian.Uint32(b[1:]), Hops: b[5], Msg: string(msg)}, nil
}

// passed is a, received from one side of a join, as the join sends it on
// to the other: one hop further (Hops saturate at 255).
func (a *Abort) passed() *Abort {
	p := *a
	if p.Hops < 255 {
		p.Hops++
	}
	return &p
}

// abortFor is the Abort a join sends on after err ended one of its
// sides: an *Abort passed on, a peer's plain reset one hop further, or a
// local cause (Hops 0): a deadline, a drain's deadline, the connection.
func abortFor(err error) *Abort {
	var a *Abort
	switch {
	case errors.As(err, &a):
		return a.passed()
	case errors.Is(err, ErrReset):
		return &Abort{Code: CodeUnspecified, Hops: 1}
	case errors.Is(err, ErrDrainDeadline):
		return &Abort{Code: CodeDrainDeadline, Msg: "drain deadline"}
	case !errors.Is(err, ErrConnClosed) && errors.Is(err, os.ErrDeadlineExceeded):
		return &Abort{Code: CodeTimeout, Msg: "write deadline"}
	default:
		return &Abort{Code: CodeConnLost, Msg: err.Error()}
	}
}

// HopKind is a HopEvent's kind.
type HopKind uint8

// Hop event kinds.
const (
	HopOpened   HopKind = iota // Forward took an open
	HopAnswered                // the onward answer came (or Forward refused)
	HopEnded                   // the joined sessions ended
)

// HopEvent is one hop's event on a path of joined sessions (Observer.Hop).
type HopEvent struct {
	Kind    HopKind
	Chain   Chain         // the trace and this hop's Hops (the inbound session's)
	Latency time.Duration // HopAnswered: Route called -> onward answer
	Err     error         // HopAnswered: the refusal; HopEnded: the abort, nil on a clean end
	In, Out int64         // HopEnded: bytes origin->far and far->origin
	// FirstOut is, on HopEnded, when the far end's first byte was written
	// toward the origin (zero when none was): a spliced session's time to
	// first byte, which no caller sees when the join moves the bytes
	// (since v0.12.0).
	FirstOut time.Time
}

// Answers reports whether the session's connection negotiated
// FeatureChain: the accepter answers the open (Accept, Refuse), aborts
// carry a reason, and the open carried a Chain.
func (s *Session) Answers() bool { return s.answers }

// Chain is where the session is on its path: the CHAIN-OPEN's for a
// session the peer opened, OpenChain's (its Trace picked) for one this
// side opened. Zero on a connection without FeatureChain, but for
// OpenChain's own.
func (s *Session) Chain() Chain { return s.chain }

// PathLen is, once Answered returned nil, the hops beyond the peer to the
// session's far end (the ACCEPT's): 0 when the peer is the far end.
func (s *Session) PathLen() uint8 {
	if s.answered != nil && closed(s.answered) {
		return s.pathLen
	}
	return 0
}

// Answered waits for the peer's answer to a session this side opened:
// nil once it accepted (at the end of a path of Forward hops: once the far
// end accepted), the *Abort of its refusal (errors.Is ErrOpenRefused), or
// the error that ended the session before an answer (a reset's *Abort,
// the connection's end), or ctx's error. ErrNoAnswers on a connection
// without FeatureChain; nil on a session the peer opened. Bytes written
// before the answer are sent (no round trip per hop); toward a refused
// session they are discarded.
func (s *Session) Answered(ctx context.Context) error {
	switch {
	case !s.answers:
		return ErrNoAnswers
	case s.accepter:
		return nil
	}
	select {
	case <-s.answered:
		return nil
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if closed(s.answered) {
		return nil
	}
	if err := s.end(); err != io.EOF {
		return err
	}
	return s.closedPipe()
}

// Accept answers a session the peer opened: accepted. The first Write,
// WriteBuffers, CloseWrite or Close answers so too; a side that may wait
// before writing (a far end that dialled its target) calls it, since the
// opener's Answered waits for it. It does nothing on a session this side
// opened, on a connection without FeatureChain, or once answered.
func (s *Session) Accept() error { return s.answer(0, true) }

// AcceptPath is Accept for a hop that joined the session to a path
// pathLen hops long beyond it (Forward: the onward session's PathLen
// plus one): the opener's PathLen.
func (s *Session) AcceptPath(pathLen uint8) error { return s.answer(pathLen, true) }

// Refuse answers a session the peer opened: no, with code (CodeApp and
// up are the application's) and msg (cut to MaxAbortMsg bytes of UTF-8).
// The opener's Answered and reads return the *Abort. The session ends.
// It fails once the session was answered (Abort it instead). On a
// connection without FeatureChain it is Reset.
func (s *Session) Refuse(code uint32, msg string) error {
	return s.AbortWith(&Abort{Refused: true, Code: code, Msg: msg})
}

// Abort resets the session with a reason, either side, any time: the
// peer reads an *Abort with code and msg (errors.Is ErrReset). Reads here
// end as after Close. On a connection without FeatureChain it is Reset.
func (s *Session) Abort(code uint32, msg string) error {
	return s.AbortWith(&Abort{Code: code, Msg: msg})
}

// AbortWith is Refuse (a.Refused) or Abort with every field of a: a hop
// passing on an abort from the other side of a join sets Hops (the
// abort's own Hops plus one).
func (s *Session) AbortWith(a *Abort) error {
	if debugOn {
		debugReleased(s)
	}
	if a.Refused {
		if !s.accepter {
			return errors.New("link: Refuse of a session this side opened (Abort it)")
		}
		refused := false
		s.ansOnce.Do(func() { refused = true; s.ans.Store(true) })
		if !refused {
			return errors.New("link: Refuse of a session already answered (Abort it)")
		}
	}
	s.once.Do(func() {
		defer s.m.drop(s) // after the frame: a leaving connection closes after it
		if s.peerClosed.Load() {
			return
		}
		var err error
		if s.answers {
			err = s.m.c.sendAbort(s.id, a)
		} else {
			err = s.m.c.SendReset(s.id)
		}
		if err == nil {
			_ = s.m.c.Flush()
		}
	})
	return nil
}

// autoAccept answers an accepter's session before its first frame.
func (s *Session) autoAccept() {
	if s.accepter && s.answers && !s.ans.Load() {
		_ = s.answer(0, false)
	}
}

// answer sends the accepter's ACCEPT once (flush: write it now).
func (s *Session) answer(hops uint8, flush bool) error {
	if !s.accepter || !s.answers {
		return nil
	}
	var err error
	s.ansOnce.Do(func() {
		defer s.ans.Store(true)
		if s.closed.Load() {
			err = s.closedPipe()
			return
		}
		if err = s.m.c.sendAccept(s.id, hops); err == nil && flush {
			err = s.flush()
		}
	})
	return err
}
