// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"encoding/binary"
	"errors"
	"sync"
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
	flush              func() error            // before Acquire waits; nil: none
	obs                Observer                // nil: none

	mu       sync.Mutex
	sessions map[string]*sessionCredit
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
	wake     chan struct{} // signalled when n grows; closed when forgotten
	held     int           // received and not yet acknowledged: at most the window
	consumed int           // left the buffer, not yet acknowledged
	by       int           // byCaller, byUs or byPeer
	answered bool          // FeatureChain connections: byUs, the peer's ACCEPT or refusal came; byPeer, this side's went
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
	if old != nil {
		close(old.wake) // a reused id: the old session's sender gives up
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
	c.sessions[sid] = &sessionCredit{n: c.peerWindow, wake: make(chan struct{}, 1), by: by}
	if by != byCaller {
		c.opened[by-1]++
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
func (c *Credit) Forget(sid string) {
	c.mu.Lock()
	s := c.sessions[sid]
	if s != nil {
		close(s.wake)
		delete(c.sessions, sid)
		if s.by != byCaller {
			c.opened[s.by-1]--
		}
	}
	c.mu.Unlock()
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
	ok, _ := c.acquire(sid, n, c.flush, nil)
	return ok
}

// acquire is Acquire flushing with flush, and giving up (stopped) once
// stop is closed.
func (c *Credit) acquire(sid string, n int, flush func() error, stop <-chan struct{}) (ok, stopped bool) {
	var waited time.Time
	for {
		c.mu.Lock()
		s := c.sessions[sid]
		if s == nil {
			c.mu.Unlock()
			return false, false
		}
		if s.n >= n {
			s.n -= n
			c.mu.Unlock()
			if !waited.IsZero() && c.obs != nil {
				c.obs.CreditWait(time.Since(waited))
			}
			return true, false
		}
		wake := s.wake
		c.mu.Unlock()
		if waited.IsZero() {
			waited = time.Now()
		}
		if flush != nil && flush() != nil { // a grant meanwhile is kept in wake
			return false, false
		}
		select {
		case <-wake:
		case <-c.closed:
			return false, false
		case <-stop:
			return false, true
		}
	}
}

// refund gives back n bytes of sid's credit that were acquired and not
// sent.
func (c *Credit) refund(sid string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.sessions[sid]; s != nil {
		s.n += n
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// Consumed acknowledges n bytes of sid that left the receive buffer; a
// window update goes out once half a window has.
func (c *Credit) Consumed(sid string, n int) {
	c.mu.Lock()
	s := c.sessions[sid]
	if s == nil {
		c.mu.Unlock()
		return
	}
	s.consumed += n
	r := s.consumed
	if r < c.window/4 {
		c.mu.Unlock()
		return
	}
	s.consumed = 0
	s.held = max(s.held-r, 0) // the peer may send r more from now on
	c.mu.Unlock()
	c.grant(sid, r)
}

// Received charges n bytes of data for sid against this side's window:
// a Conn does for data frames, a caller for data it carries otherwise.
// Data of a session this side does not know is not buffered by anyone and
// is not charged.
func (c *Credit) Received(sid string, n int) error { return c.received(sid, n, false) }

// received is Received; answers: data on a session this side opened is
// ErrFrame before the peer's answer (FeatureChain).
func (c *Credit) received(sid string, n int, answers bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.sessions[sid]
	if s == nil {
		return nil
	}
	if answers && s.by == byUs && !s.answered {
		return ErrFrame
	}
	if n > c.window-s.held {
		return ErrCredit
	}
	s.held += n
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
// the peer's window.
func (c *Credit) Grant(sid string, n int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.sessions[sid]
	if s == nil {
		return nil
	}
	if n < 0 || n > c.peerWindow-s.n {
		return ErrCredit
	}
	s.n += n
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return nil
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
