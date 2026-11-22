// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Per-session credit. Each side tells the other how many bytes of one
// session it buffers (its window). A sender may have at most the peer's
// window of a session's data unacknowledged (Acquire); the receiver
// acknowledges bytes once they left its buffer (Consumed), in window
// updates of at least a quarter window (often enough that a session on a
// long path keeps its window in flight). A receive loop therefore never waits
// on one session's full buffer, and a session whose reader is slow stalls
// only itself.
//
// Both caps are enforced, so a peer cannot make this side buffer or send
// more than the windows allow: data past a session's window, or a window
// update that would raise a session's credit past the peer's own window, is
// ErrCredit and ends the connection. A connection's memory is then at most
// its sessions times the window.
//
// On a connection where both sides announced FeatureGrow a session's window
// grows with the path (spec/CreditGrow.tla, WIRE.md section 7): a sender
// that must wait for credit sends a window update of 0 bytes (BLOCKED,
// RFC 9000's STREAM_DATA_BLOCKED) once until its next update; it arrives
// behind every byte the sender could send, so when the receiver reads it
// with less than half the window unread and a whole window received since
// the window last grew, the path held the sender back, and the window
// doubles, at most to Config.WindowMax and within the connection's
// Config.WindowBudget (growth past Window, summed over its sessions; a
// session's end gives its growth back). The growth goes out with the
// session's next update. A connection's memory is then at most its
// sessions times the window plus the budget.

// ErrCredit is the receive error of a peer that broke the credit rules:
// it sent a session more data than this side's window, or granted more
// credit than its own window.
var ErrCredit = errors.New("link: peer exceeded the session window")

// Credit is the per-session flow control of one connection. A Conn has
// one and carries its window updates in window frames; a caller whose
// sessions travel on a transport of its own (a gRPC stream) makes one with
// NewCredit and carries the updates itself (AppendWindow, ParseWindow).
// Its methods are safe from any goroutine.
type Credit struct {
	window, peerWindow int
	grant              func(sid string, n int) // sends a window update
	// FeatureGrow (both sides announced it): blocked sends a BLOCKED (a
	// window update of 0) and the peer's window may grow; this side's
	// grow to winMax (none when it is at most window) within budget.
	grows          bool
	blocked        func(sid string)
	winMax, budget int
	grown          int          // the sessions' growth past window, summed (under mu)
	growths        uint64       // windows grown (Stats; under mu)
	flush          func() error // before Acquire waits; nil: none
	obs            Observer     // nil: none
	tr             traceState   // linktrace builds only (trace_on.go)

	waits     atomic.Uint64 // senders' waits for credit (Stats)
	waitNanos atomic.Int64  // and how long, in all

	mu       sync.Mutex
	sessions map[string]*sessionCredit
	queued   int    // received and not yet consumed, every session's (Stats)
	openedN  uint64 // link-managed sessions opened (Stats)
	opened   [2]int // sessions open by side: [0] this side's, [1] the peer's
	cut      bool   // the connection ended: sessions are no longer observed (cutSessions)
	closed   chan struct{}
	once     sync.Once
}

// Who opened a session: link-managed sessions (v2 OPEN frames) are counted
// against the opener's limit; ones a caller opened with Open are not.
const (
	byCaller = iota
	byUs
	byPeer
)

type sessionCredit struct {
	n        int           // bytes this side may still send
	pend     int           // taken by a Session's Write, not yet buffered (Grant's bound)
	wake     chan struct{} // the waiting senders': closed when n grows or the credit is forgotten (wakeAll); nil while none waits
	held     int           // received and not yet acknowledged: at most win
	consumed int           // left the buffer, not yet acknowledged
	win      int           // this side's window of the session: Credit.window, more once grown
	owed     int           // growth not yet sent: goes with the next window update
	rcvd     int           // bytes received in all
	epoch    int           // rcvd when the window last grew
	bsent    bool          // a BLOCKED sent since the peer's last update (FeatureGrow)
	by       int           // byCaller, byUs or byPeer
	answered bool          // FeatureChain connections: byUs, the peer's ACCEPT or refusal came; byPeer, this side's went
	onGrant  func(int)     // Session.OnGrant's; nil: none
	fins     uint8         // the half CLOSEs: finSent (this side's buffered), finRecv (the peer's read)
	wt       waitTrace     // linktrace builds only (trace_wait_on.go); zero size otherwise
	wtr      creditTrace   // linktrace builds only (trace_write_on.go); zero size otherwise
	granted  uint64        // bytes the peer acknowledged, in all (Grant): an ENDACK wait's progress
}

const (
	finSent = 1 << iota
	finRecv
)

// wakeAll wakes every sender waiting in Acquire: each checks the credit
// again, so a grant enough for some of them is taken by each it is enough
// for (spec/CreditWaiters.tla). One wakeup for all waiters lost one: the
// sender woken could find too little for itself, or take its bytes and
// leave the rest, and the others slept with credit there (lane LINKWAKE).
// A waiter makes the next channel.
func (s *sessionCredit) wakeAll() {
	if s.wake != nil {
		close(s.wake)
		s.wake = nil
	}
}

// NewCredit returns the credit of a connection whose side buffers window
// bytes per session and whose peer buffers peerWindow. grant sends the
// peer a window update for n bytes of sid (AppendWindow); it is called
// from Consumed, never with a lock held.
func NewCredit(window, peerWindow int, grant func(sid string, n int)) *Credit {
	return &Credit{
		window: window, peerWindow: peerWindow, grant: grant,
		sessions: map[string]*sessionCredit{}, closed: make(chan struct{}),
	}
}

// SetObserver sets the observer of a Credit made with NewCredit: it gets
// CreditWait for each wait of a sender (Acquire). Call it before the
// Credit is used. A Conn's Credit has its Config.Observer.
func (c *Credit) SetObserver(obs Observer) { c.obs = obs }

// Window is this side's window.
func (c *Credit) Window() int { return c.window }

// PeerWindow is the other side's window.
func (c *Credit) PeerWindow() int { return c.peerWindow }

// Open starts the credit of a session on both sides: the opener before it
// sends the open frame, the other side when it receives it. Opening an id
// that is open starts it again: its sender waiting in Acquire gives up.
func (c *Credit) Open(sid string) { c.open(sid, byCaller) }

func (c *Credit) open(sid string, by int) {
	c.mu.Lock()
	old := c.sessions[sid]
	if old != nil && old.by == byCaller && by == byUs && !c.cut {
		// SendOpen of an id whose credit its caller opened first (Open's
		// order): the link takes that credit over, what was acquired of
		// it still counted.
		old.by = byUs
		c.opened[byUs-1]++
		c.openedN++
		c.mu.Unlock()
		if c.obs != nil {
			c.obs.SessionOpen()
		}
		return
	}
	if old != nil {
		old.wakeAll() // a reused id: the old session's sender gives up
		if by == byCaller {
			by = old.by // the caller's own Open of a link-opened session
		}
		if old.by != byCaller {
			c.opened[old.by-1]--
		}
	}
	if c.cut {
		by = byCaller // the connection ended: SessionClose was reported for its sessions
	}
	c.sessions[sid] = &sessionCredit{n: c.peerWindow, by: by, win: c.window} // no wake channel until a sender waits (TestCreditOpenMakesNoWakeChannel)
	if traceOn {
		traceWaitUp(c.sessions[sid], c.peerWindow)
	}
	if by != byCaller {
		c.opened[by-1]++
		if old == nil {
			c.openedN++
		}
	}
	c.mu.Unlock()
	if old == nil && by != byCaller && c.obs != nil {
		c.obs.SessionOpen()
	}
}

// cutSessions reports SessionClose(err) for each link-managed session
// still open: the connection ended under them. A later Forget of one
// reports nothing more, nor a session opened after.
func (c *Credit) cutSessions(err error) {
	c.mu.Lock()
	c.cut = true
	n := 0
	for _, s := range c.sessions {
		if s.by != byCaller {
			c.opened[s.by-1]--
			s.by = byCaller
			n++
		}
	}
	c.mu.Unlock()
	for range n {
		c.obs.SessionClose(err)
	}
}

// Forget ends a session's credit: a sender waiting in Acquire gives up.
func (c *Credit) Forget(sid string) { c.forget(sid, -1) }

// halfClosed records one direction's half CLOSE of sid (e, the trace's
// event for it, under this lock); the second ends its credit, under this
// lock, while its CLOSE is buffered under wmu and not yet written (finSent)
// or before the next frame is read (finRecv): the peer may open another
// session once it reads the CLOSE that ended this one, and this side
// counts its sessions by their credit (max_sessions). Mux.drop, after the
// frame, forgot it too late (lane LINKDROPSPLIT, spec/Link.tla
// CloseWriteSend, RecvClose).
func (c *Credit) halfClosed(sid string, fin uint8, e traceEvent) {
	c.mu.Lock()
	if traceOn {
		traceEv(c.tr, e)
	}
	s := c.sessions[sid]
	if s == nil || s.fins|fin != finSent|finRecv {
		if s != nil {
			s.fins |= fin
		}
		c.mu.Unlock()
		return
	}
	c.dropLocked(sid)
	c.mu.Unlock()
	c.dropped(s)
}

// forget is Forget; code, when this side ends the session (a CLOSE or
// ABORT it is about to buffer: 0, 2, 3, 4 for a refusal), goes to the trace.
func (c *Credit) forget(sid string, code int) {
	c.mu.Lock()
	if traceOn && code >= 0 {
		traceEv(c.tr, traceEvent{ev: "Forget", sid: sid, n: code})
	}
	s := c.dropLocked(sid)
	c.mu.Unlock()
	c.dropped(s)
}

// dropLocked ends sid's credit, under c.mu; nil when it had none.
func (c *Credit) dropLocked(sid string) *sessionCredit {
	s := c.sessions[sid]
	if s != nil {
		s.wakeAll()
		delete(c.sessions, sid)
		c.queued -= s.held - s.consumed
		c.grown -= s.win - c.window // its growth back to the budget (spec/CreditGrow.tla Drop)
		if s.by != byCaller {
			c.opened[s.by-1]--
		}
	}
	return s
}

// dropped reports a link-managed session's end (dropLocked's) to the Observer.
func (c *Credit) dropped(s *sessionCredit) {
	if s != nil && s.by != byCaller && c.obs != nil {
		c.obs.SessionClose(nil)
	}
}

// count is how many link-managed sessions side by (byUs, byPeer) has open.
func (c *Credit) count(by int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opened[by-1]
}

// Acquire waits until sid may send n more bytes and takes them. n must not
// exceed the peer's window (MaxChunk does not). It reports false once the
// session is forgotten or the credit closed.
//
// Before it waits it flushes: the peer returns credit only for bytes it
// received, so a sender waiting with its own frames still buffered (a
// Session.Write of several chunks) would wait for good.
func (c *Credit) Acquire(sid string, n int) bool {
	ok, _ := c.acquire(sid, n, c.flush, nil, false, writeStep{})
	return ok
}

// acquire is Acquire flushing with flush, and giving up (stopped) once
// stop is closed. With track the bytes count as pending until sent or
// refund (a Session's Write); ws is its writer in the session's write
// trace (spec/TraceSessionWrite.tla).
func (c *Credit) acquire(sid string, n int, flush func() error, stop <-chan struct{}, track bool, ws writeStep) (ok, stopped bool) {
	var waited time.Time
	var tw int // the trace's waiter (spec/TraceCreditWaiters.tla)
	if traceOn {
		tw = traceWaiter()
	}
	for {
		c.mu.Lock()
		s := c.sessions[sid]
		if s == nil {
			c.mu.Unlock()
			return false, false
		}
		if s.n >= n {
			s.n -= n
			if track {
				s.pend += n
			}
			if traceOn {
				traceEv(c.tr, traceEvent{ev: "Acquire", sid: sid, n: n, credit: s.n})
				s.traceWait("Check", tw, n, true)
				s.traceCheck(ws, n, true)
			}
			c.mu.Unlock()
			if !waited.IsZero() {
				d := time.Since(waited)
				c.waits.Add(1)
				c.waitNanos.Add(int64(d))
				if c.obs != nil {
					c.obs.CreditWait(d)
				}
			}
			return true, false
		}
		if s.wake == nil {
			s.wake = make(chan struct{})
		}
		wake := s.wake
		if traceOn {
			s.traceWait("Check", tw, n, false)
			s.traceCheck(ws, n, false)
		}
		block := c.grows && !s.bsent // once until the peer's next update (spec/CreditGrow.tla Block)
		s.bsent = s.bsent || block
		c.mu.Unlock()
		if block {
			c.blocked(sid) // buffered: the flush below sends it
		}
		if waited.IsZero() {
			waited = time.Now()
		}
		if flush != nil && flush() != nil { // a grant meanwhile closes wake
			return false, false
		}
		select {
		case <-wake:
		case <-c.closed:
			return false, false
		case <-stop:
			if traceOn {
				ws.traceWake()
			}
			return false, true
		}
	}
}

// sent records n bytes a Session's Write acquired as buffered.
func (c *Credit) sent(sid string, n int) {
	c.mu.Lock()
	if s := c.sessions[sid]; s != nil {
		s.pend -= n
	}
	c.mu.Unlock()
}

// refund gives back n bytes of sid's credit that a Session's Write
// acquired and did not send.
func (c *Credit) refund(sid string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.sessions[sid]
	if traceOn {
		e := traceEvent{ev: "Refund", sid: sid, n: n, credit: -1}
		if s != nil {
			e.credit = s.n + n
		}
		traceEv(c.tr, e)
	}
	if s != nil {
		s.n += n
		s.pend -= n
		if traceOn {
			s.traceWait("Grant", 0, n, false)
			s.traceGrant(n)
		}
		s.wakeAll()
	}
}

// Consumed acknowledges n bytes of sid that left the receive buffer; a
// window update goes out once a quarter window has.
func (c *Credit) Consumed(sid string, n int) {
	c.mu.Lock()
	s := c.sessions[sid]
	if s == nil {
		c.mu.Unlock()
		return
	}
	s.consumed += n
	c.queued -= n
	r := s.consumed
	if r+s.owed < s.win/4 {
		if traceOn {
			traceEv(c.tr, traceEvent{ev: "Read", sid: sid, n: n, held: s.held})
		}
		c.mu.Unlock()
		return
	}
	s.consumed = 0
	s.held = max(s.held-r, 0) // the peer may send r more from now on
	g := r + s.owed           // and what the window grew by (Credit.blocked)
	s.owed = 0
	if traceOn {
		traceEv(c.tr, traceEvent{ev: "Read", sid: sid, n: n, held: s.held, grant: g})
	}
	c.mu.Unlock()
	c.grant(sid, g)
}

// peerBlocked is the peer's BLOCKED for sid (a window update of 0 bytes on a
// FeatureGrow connection): the window doubles when the reader keeps up
// (less than half of it unread) and a whole window arrived since it last
// grew, at most to winMax and within the budget; the bytes go out with
// the next update (Consumed). The receive loop calls it: it sends nothing.
func (c *Credit) peerBlocked(sid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.sessions[sid]
	if s == nil || 2*(s.held-s.consumed) >= s.win || s.rcvd-s.epoch < s.win {
		return
	}
	g := min(s.win, c.winMax-s.win, c.budget-c.grown)
	if g <= 0 {
		return
	}
	s.win += g
	s.owed += g
	s.epoch = s.rcvd
	c.grown += g
	c.growths++
}

// queueLimit is the most a session's receive queue holds: its window, as
// it may grow.
func (c *Credit) queueLimit() int {
	if c.grows {
		return max(c.winMax, c.window)
	}
	return c.window
}

// Received charges n bytes of data for sid against this side's window:
// a Conn does for data frames, a caller for data it carries otherwise.
// Data of a session this side does not know is not buffered by anyone and
// is not charged.
func (c *Credit) Received(sid string, n int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.chargeLocked(c.sessions[sid], n, false)
}

// sendable reports whether sid's data may go out: its credit lives and,
// on a FeatureChain connection, a session the peer opened was answered
// with an ACCEPT (a Refuse that took the answer sends none: review r2 of
// LinkSession, finding 2). With n > 0 it is the check just before a data
// frame of n bytes is buffered, and the trace's SendData is logged with it.
func (c *Credit) sendable(sid string, chain bool, n int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.sessions[sid]
	ok := s != nil && (!chain || s.by != byPeer || s.answered)
	if traceOn && ok && n > 0 { // the frame of n bytes is buffered next, under wmu (finding C12)
		traceEv(c.tr, traceEvent{ev: "SendData", sid: sid, n: n})
	}
	return ok
}

// inUse reports whether sid is a session the link opened (an OPEN sent or
// received) whose credit lives: its id is not free (WIRE.md section 5).
// A caller's own Credit.Open, before its SendOpen, is no session yet.
func (c *Credit) inUse(sid string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.sessions[sid]
	return s != nil && s.by != byCaller
}

// receivedSID is Received of a received frame's sid (no copy of it);
// answers: data on a session this side opened is ErrFrame before the
// peer's answer (FeatureChain). known is false for a session this side
// keeps no credit for (the data is not charged).
func (c *Credit) receivedSID(sid []byte, n int, answers bool) (known bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.sessions[string(sid)]
	err = c.chargeLocked(s, n, answers)
	if traceOn && err == nil {
		e := traceEvent{ev: "Recv", t: "data", sid: string(sid), n: n, credit: -1}
		if s != nil {
			e.credit, e.held = s.n, s.held
		}
		traceEv(c.tr, e)
	}
	return s != nil, err
}

// chargeLocked charges n bytes of data to s (nil: a session this side
// does not know).
func (c *Credit) chargeLocked(s *sessionCredit, n int, answers bool) error {
	if s == nil {
		return nil
	}
	if answers && s.by == byUs && !s.answered {
		return ErrFrame
	}
	if n > s.win-s.held {
		return ErrCredit
	}
	s.held += n
	s.rcvd += n
	c.queued += n
	return nil
}

// answered records the peer's answer (ACCEPT, or ABORT refuse) to sid:
// known is false for a session this side does not know (the frame is
// ignored); ErrFrame for one it did not open, or a second answer.
func (c *Credit) answered(sid string) (known bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.sessions[sid]
	switch {
	case s == nil:
		return false, nil
	case s.by != byUs || s.answered:
		return true, ErrFrame
	}
	s.answered = true
	return true, nil
}

// answering reports whether sid is a session the peer opened that this
// side has not answered yet, and records that it now is (FeatureChain).
func (c *Credit) answering(sid string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.sessions[sid]
	if s == nil || s.by != byPeer || s.answered {
		return false
	}
	s.answered = true
	return true
}

// Grant adds a window update's bytes to sid's credit, which never exceeds
// the peer's window, counting the credit a Write took and has not sent
// yet (WIRE.md section 7, review F8).
func (c *Credit) Grant(sid string, n int) error {
	c.mu.Lock()
	s := c.sessions[sid]
	if s == nil {
		if traceOn {
			traceEv(c.tr, traceEvent{ev: "Recv", t: "win", sid: sid, n: n, credit: -1})
		}
		c.mu.Unlock()
		return nil
	}
	limit := c.peerWindow
	if c.grows {
		limit = MaxWindow // the peer's window grows: the protocol's bound
	}
	if n < 0 || n > limit-s.n-s.pend {
		c.mu.Unlock()
		return ErrCredit
	}
	s.n += n
	s.bsent = false
	s.granted += uint64(n) // #nosec G115 -- n >= 0
	if traceOn {
		traceEv(c.tr, traceEvent{ev: "Recv", t: "win", sid: sid, n: n, credit: s.n})
		s.traceWait("Grant", 0, n, false)
		s.traceGrant(n)
	}
	s.wakeAll()
	f := s.onGrant
	c.mu.Unlock()
	if f != nil {
		f(n)
	}
	return nil
}

// progress is the bytes of sid the peer acknowledged so far (0 once the
// session is forgotten).
func (c *Credit) progress(sid string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.sessions[sid]; s != nil {
		return s.granted
	}
	return 0
}

// setOnGrant sets sid's grant callback (Session.OnGrant).
func (c *Credit) setOnGrant(sid string, f func(int)) {
	c.mu.Lock()
	if s := c.sessions[sid]; s != nil {
		s.onGrant = f
	}
	c.mu.Unlock()
}

// Close makes every sender waiting in Acquire, and every later Acquire,
// give up.
func (c *Credit) Close() { c.once.Do(func() { close(c.closed) }) }

// AppendWindow appends a window update's body, sid_len(1) sid
// increment(4), to b: the body of a window frame, and what a caller with a
// transport of its own carries for a Credit's grant. sid is at most
// MaxSID bytes and n at most a window.
func AppendWindow(b []byte, sid string, n int) []byte {
	b = append(append(b, byte(len(sid))), sid...)      // #nosec G115 -- the caller's sid fits (MaxSID)
	return binary.BigEndian.AppendUint32(b, uint32(n)) // #nosec G115 -- n <= a window
}

// ParseWindow reads a window update's body (AppendWindow).
func ParseWindow(b []byte) (sid string, n int, err error) {
	if len(b) < 5 || int(b[0]) != len(b)-5 {
		return "", 0, ErrFrame
	}
	return string(b[1 : len(b)-4]), int(binary.BigEndian.Uint32(b[len(b)-4:])), nil
}
