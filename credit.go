// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"encoding/binary"
	"errors"
	"sync"
)

// Per-session credit. Each side tells the other, in the hello and its
// reply, how many bytes of one session it buffers (its window). A sender
// may have at most the peer's window of a session's data unacknowledged
// (Acquire); the receiver acknowledges bytes once they left its buffer
// (Consumed), in window frames of at least half a window. A receive loop
// therefore never waits on one session's full buffer, and a session whose
// reader is slow stalls only itself.
//
// Both caps are enforced, so a peer cannot make this side buffer or send
// more than the windows allow: data past a session's window, or a window
// frame that would raise a session's credit past the peer's own window, is
// ErrCredit and ends the connection. A connection's memory is then at most
// its sessions times the window.

// ErrCredit is the receive error of a peer that broke the credit rules:
// it sent a session more data than this side's window, or granted more
// credit than its own window.
var ErrCredit = errors.New("link: peer exceeded the session window")

type credit struct {
	n    int           // bytes the sender may still send
	wake chan struct{} // signalled when n grows; closed when forgotten
}

// recvCredit is a session's receive side.
type recvCredit struct {
	held     int // received and not yet acknowledged: at most the window
	consumed int // left the buffer, not yet acknowledged
}

type credits struct {
	mu     sync.Mutex
	send   map[string]*credit
	recv   map[string]*recvCredit
	closed chan struct{}
	once   sync.Once
}

// Open starts the credit of a session on both sides: the opener before it
// sends the open frame, the other side when it receives it.
func (c *Conn) Open(sid string) {
	c.cr.mu.Lock()
	if old := c.cr.send[sid]; old != nil {
		close(old.wake) // a reused id: the old session's sender gives up
	}
	c.cr.send[sid] = &credit{n: c.peerWindow, wake: make(chan struct{}, 1)}
	c.cr.recv[sid] = &recvCredit{}
	c.cr.mu.Unlock()
}

// Forget ends a session's credit: a sender waiting in Acquire gives up.
func (c *Conn) Forget(sid string) {
	c.cr.mu.Lock()
	if cr := c.cr.send[sid]; cr != nil {
		close(cr.wake)
		delete(c.cr.send, sid)
	}
	delete(c.cr.recv, sid)
	c.cr.mu.Unlock()
}

// Acquire waits until sid may send n more bytes and takes them. n must not
// exceed the peer's window (MaxChunk does not). It reports false once the
// session is forgotten or the connection closed or failed.
//
// Before it waits it flushes: the peer returns credit only for bytes it
// received, so a sender waiting with its own frames still buffered (a
// Session.Write of several chunks) would wait for good.
func (c *Conn) Acquire(sid string, n int) bool {
	for {
		c.cr.mu.Lock()
		cr := c.cr.send[sid]
		if cr == nil {
			c.cr.mu.Unlock()
			return false
		}
		if cr.n >= n {
			cr.n -= n
			c.cr.mu.Unlock()
			return true
		}
		wake := cr.wake
		c.cr.mu.Unlock()
		if c.Flush() != nil { // a grant meanwhile is kept in wake
			return false
		}
		select {
		case <-wake:
		case <-c.cr.closed:
			return false
		}
	}
}

// Consumed acknowledges n bytes of sid that left the receive buffer; a
// window frame goes out once half a window has.
func (c *Conn) Consumed(sid string, n int) {
	c.cr.mu.Lock()
	rc := c.cr.recv[sid]
	if rc == nil {
		c.cr.mu.Unlock()
		return
	}
	rc.consumed += n
	r := rc.consumed
	if r < c.window/2 {
		c.cr.mu.Unlock()
		return
	}
	rc.consumed = 0
	rc.held = max(rc.held-r, 0) // the peer may send r more from now on
	c.cr.mu.Unlock()
	b := make([]byte, 0, 1+len(sid)+4)
	b = append(append(append(b, byte(len(sid))), sid...), 0, 0, 0, 0) // #nosec G115 -- the caller's sid fits (MaxSID)
	binary.BigEndian.PutUint32(b[len(b)-4:], uint32(r))               // #nosec G115 -- r <= the window
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.frameLocked(typeWindow, "", b) == nil {
		c.flushLocked()
	}
}

// Received charges n bytes of data for sid against this side's window:
// Recv does for data frames, the caller for data it carries in messages.
// Data of a session this side does not know is not buffered by anyone and
// is not charged.
func (c *Conn) Received(sid string, n int) error {
	c.cr.mu.Lock()
	defer c.cr.mu.Unlock()
	rc := c.cr.recv[sid]
	if rc == nil {
		return nil
	}
	if n > c.window-rc.held {
		return ErrCredit
	}
	rc.held += n
	return nil
}

// granted adds a window frame's bytes to sid's credit, which never exceeds
// the peer's window.
func (c *Conn) granted(sid string, n int) error {
	c.cr.mu.Lock()
	defer c.cr.mu.Unlock()
	cr := c.cr.send[sid]
	if cr == nil {
		return nil
	}
	if n > c.peerWindow-cr.n {
		return ErrCredit
	}
	cr.n += n
	select {
	case cr.wake <- struct{}{}:
	default:
	}
	return nil
}
