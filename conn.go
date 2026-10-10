// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// MaxChunk is the most data a sender acquires credit for at once: larger
// writes are split.
const MaxChunk = 64 << 10

// MaxSID is the longest session id.
const MaxSID = 255

const (
	typeData   = 0
	typeMsg    = 1
	typeWindow = 2
	// Version 2 only.
	typeOpen   = 3
	typeClose  = 4
	typePing   = 5
	typePong   = 6
	typeGoAway = 7

	// maxData is the largest payload of one data frame; SendData splits
	// larger.
	maxData = 1 << 20
	// maxFrame bounds the body Recv accepts for one frame.
	maxFrame = 1 + MaxSID + maxData
	// maxHeader bounds an OPEN frame's header.
	maxHeader = 64 << 10
	// maxReason bounds a GOAWAY frame's reason.
	maxReason = 1024
	// pingLen is a PING's and a PONG's body.
	pingLen = 8

	// writeBuf is the write batch: over TLS one write syscall. Pooled,
	// held only while frames wait for Flush.
	writeBuf = 256 << 10
)

// CLOSE frame codes (version 2).
const (
	closeFull = 0 // the sender neither sends nor takes more of the session
	closeHalf = 1 // the sender sends no more; it still takes the peer's
)

var writeBufs = sync.Pool{New: func() any { b := make([]byte, 0, writeBuf); return &b }}

// ErrFrame reports bytes that break the frame format.
var ErrFrame = errors.New("link: malformed frame")

var (
	// errSID is a send for a session id longer than MaxSID.
	errSID = errors.New("link: session id longer than 255 bytes")
	// errEmpty is a send of an empty message (a frame body is never empty).
	errEmpty = errors.New("link: empty message")
	// errV1 is a version-2 frame sent on a version-1 connection.
	errV1 = errors.New("link: frame of wire version 2 on a version-1 connection")
	// errHeader is an OPEN header longer than the format allows.
	errHeader = errors.New("link: session header too long")
)

// Frame is a received frame: a session's data (SID, Data), a message
// (Msg, the caller's bytes), or in version 2 a session's open (SID, Open,
// Header: the opener's bytes) or close (SID, Close; Half when the peer
// only stopped sending). Window, ping, pong and goaway frames never
// surface: Recv applies them.
type Frame struct {
	SID    string
	Data   []byte
	Msg    []byte
	Open   bool
	Header []byte
	Close  bool
	Half   bool
}

// Conn sends and receives frames on a bound connection. One goroutine
// Receives; the sends, Flush and the credit calls (its Credit's) are safe
// from any.
//
// Reads go straight to the connection: over TLS, tls.Conn's record buffer
// serves the small header reads, and an idle connection pins no buffer.
// ponytail: a plaintext connection (loopback, dev) reads the socket for
// each header; give it a bufio.Reader if one carries real traffic.
type Conn struct {
	*Credit
	nc   net.Conn
	rhdr [5 + 1 + MaxSID + 4]byte // a header, and a window or close frame or a data frame's sid

	wmu     sync.Mutex
	wcond   sync.Cond // on wmu: a write finished or took the buffer
	wbuf    *[]byte   // frames not yet written; nil when none
	writing bool      // a write is in flight, wmu released
	werr    error

	// Version 2.
	version         int
	dialer          bool // this side dialed: Mux picks odd session ids
	peerMaxSessions int
	maxSessions     int // the peer may have this many open toward this side
	live            Liveness
	id              Identity
	t0              time.Time
	rxAt            atomic.Int64 // time.Since(t0) of the last frame header received
	pong            chan [pingLen]byte
	goingAway       atomic.Bool // either side sent GOAWAY
	peerGoAway      chan struct{}
	peerGoAwayOnce  sync.Once

	errMu   sync.Mutex
	err     error // why the connection ended (ErrDeadPeer), set before it closes
	downOne sync.Once
}

// NewConn returns a version-1 Conn over nc, whose hello is done: window
// is this side's window, peerWindow the other side's.
func NewConn(nc net.Conn, window, peerWindow int) *Conn {
	c := &Conn{nc: nc, version: 1}
	c.Credit = NewCredit(window, peerWindow, c.sendWindow)
	c.Credit.flush = c.Flush
	c.wcond.L = &c.wmu
	return c
}

// newConnV2 returns a version-2 Conn over nc, bound by Client (dialer) or
// Server, and starts its liveness.
func newConnV2(nc net.Conn, cfg Config, peerWindow, peerMaxSessions int, dialer bool) *Conn {
	c := NewConn(nc, cfg.Window, peerWindow)
	c.version, c.dialer, c.peerMaxSessions, c.live = 2, dialer, peerMaxSessions, cfg.Liveness
	c.maxSessions = cfg.MaxSessions
	c.t0 = time.Now()
	c.pong = make(chan [pingLen]byte, 1)
	c.peerGoAway = make(chan struct{})
	go c.keepAlive()
	return c
}

// up records the connection's peer and observer and reports it up.
func (c *Conn) up(obs Observer, id Identity) {
	c.id, c.obs = id, obs
	if obs != nil {
		obs.ConnUp(id)
	}
}

// Version is the connection's wire version. allow:wrapper
func (c *Conn) Version() int { return c.version }

// Identity is who the peer is, as the listener's Authenticator said (zero
// on the dialer). allow:wrapper
func (c *Conn) Identity() Identity { return c.id }

// PeerMaxSessions is how many sessions this side may have open toward the
// peer at once (version 2). allow:wrapper
func (c *Conn) PeerMaxSessions() int { return c.peerMaxSessions }

// SendMsg buffers a message frame; Flush, or a full buffer, writes it. The
// caller does not modify b afterwards.
func (c *Conn) SendMsg(b []byte) error {
	if len(b) == 0 {
		return errEmpty
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.frameLocked(typeMsg, "", b)
}

// SendData buffers sid's data as data frames, as SendMsg does. The caller
// acquired credit for it (Acquire).
func (c *Conn) SendData(sid string, data []byte) error {
	if len(sid) > MaxSID {
		return errSID
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	for len(data) > 0 {
		n := min(len(data), maxData)
		if err := c.frameLocked(typeData, sid, data[:n]); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// SendOpen buffers an OPEN frame (version 2) for session sid, with header
// (the opener's bytes, at most 64 KiB), and starts the session's credit.
// It refuses a session past the peer's max sessions (ErrFull) and any on a
// connection that is going away (ErrGoAway).
func (c *Conn) SendOpen(sid string, header []byte) error {
	switch {
	case c.version < 2:
		return errV1
	case len(sid) > MaxSID:
		return errSID
	case len(header) > maxHeader:
		return errHeader
	case c.goingAway.Load():
		return ErrGoAway
	case c.count(byUs) >= c.peerMaxSessions:
		return ErrFull
	}
	c.open(sid, byUs)
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.frameLocked(typeOpen, sid, header)
}

// SendClose buffers a CLOSE frame (version 2) for sid: half, the side
// sends no more of the session and still takes the peer's; else it
// neither sends nor takes more, and the session's credit ends.
func (c *Conn) SendClose(sid string, half bool) error {
	if c.version < 2 {
		return errV1
	}
	if len(sid) > MaxSID {
		return errSID
	}
	code := byte(closeFull)
	if half {
		code = closeHalf
	} else {
		c.Forget(sid)
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.frameLocked(typeClose, sid, []byte{code})
}

// GoAway tells the peer (version 2) to open no more sessions on this
// connection: the open ones may run for drain, then this side closes it.
// It also stops this side opening more (ErrGoAway). It writes at once.
func (c *Conn) GoAway(drain time.Duration, reason string) error {
	if c.version < 2 {
		return errV1
	}
	if len(reason) > maxReason {
		reason = reason[:maxReason]
	}
	c.goingAway.Store(true)
	b := binary.BigEndian.AppendUint32(nil, uint32(max(drain, 0).Milliseconds())) // #nosec G115 -- a drain in ms fits
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.frameLocked(typeGoAway, "", append(b, reason...)); err != nil {
		return err
	}
	c.flushLocked()
	return c.werr
}

// PeerGoAway is closed once the peer sent GOAWAY (version 2; never on
// version 1).
func (c *Conn) PeerGoAway() <-chan struct{} { return c.peerGoAway }

// GoingAway reports whether either side sent GOAWAY. allow:wrapper
func (c *Conn) GoingAway() bool { return c.goingAway.Load() }

// Done is closed once the connection is closed. allow:wrapper
func (c *Conn) Done() <-chan struct{} { return c.closed }

// Err is why the connection ended when the link ended it (ErrDeadPeer),
// else nil.
func (c *Conn) Err() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.err
}

// sendWindow is the Credit's grant: a window frame, written at once.
func (c *Conn) sendWindow(sid string, n int) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.frameLocked(typeWindow, "", AppendWindow(make([]byte, 0, 1+len(sid)+4), sid, n)) == nil {
		c.flushLocked()
	}
}

func (c *Conn) frameLocked(typ byte, sid string, body []byte) error {
	n := len(body)
	withSID := typ == typeData || typ == typeOpen || typ == typeClose
	if withSID {
		n += 1 + len(sid)
	}
	// A full buffer goes out first, by this sender or the write in flight.
	for c.werr == nil && c.wbuf != nil && len(*c.wbuf)+5+n > cap(*c.wbuf) {
		if c.writing {
			c.wcond.Wait()
		} else {
			c.writeLocked(false)
		}
	}
	if c.werr != nil {
		return c.werr
	}
	if c.wbuf == nil {
		c.wbuf = writeBufs.Get().(*[]byte)
	}
	b := binary.BigEndian.AppendUint32(*c.wbuf, uint32(n)) // #nosec G115 -- n <= maxFrame
	b = append(b, typ)
	if withSID {
		b = append(append(b, byte(len(sid))), sid...) // #nosec G115 -- len(sid) <= MaxSID
	}
	*c.wbuf = append(b, body...)
	return nil
}

// writeLocked writes the buffered frames, and those buffered while it
// writes, until none are left. wmu is released during each write: senders
// keep buffering, and their frames go out together in the next one.
// Writes stay in order because only one runs at a time (writing). With
// yield the writer first lets other runnable goroutines run: on a busy
// CPU they buffer their frames for this write instead of each writing
// its own; on an idle one it returns at once.
func (c *Conn) writeLocked(yield bool) {
	c.writing = true
	if yield {
		c.wmu.Unlock()
		runtime.Gosched()
		c.wmu.Lock()
	}
	for c.wbuf != nil && c.werr == nil {
		b := c.wbuf
		c.wbuf = nil
		c.wcond.Broadcast() // senders waiting on the full buffer start a new one
		c.wmu.Unlock()
		_, err := c.nc.Write(*b)
		if cap(*b) <= writeBuf { // not one grown for a huge frame
			*b = (*b)[:0]
			writeBufs.Put(b)
		}
		c.wmu.Lock()
		if err != nil {
			c.werr = err
		}
	}
	c.writing = false
	c.wcond.Broadcast()
}

// Flush writes the buffered frames: inline when no write is in flight,
// else the write in flight takes them next. The buffer goes back to the
// pool once written, so an idle connection holds none.
func (c *Conn) Flush() error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.flushLocked()
	return c.werr
}

func (c *Conn) flushLocked() {
	if !c.writing && c.wbuf != nil {
		c.writeLocked(true)
	}
}

// Recv returns the next data, message, open or close frame; it applies
// the others itself and charges data frames against their session's
// window. A peer that breaks the credit rules gets ErrCredit (credit.go);
// the caller closes the connection on any error. A connection the link
// ended returns why (ErrDeadPeer).
func (c *Conn) Recv() (Frame, error) {
	for {
		f, ok, err := c.recv()
		if err != nil {
			if e := c.Err(); e != nil {
				return Frame{}, e
			}
			return Frame{}, err
		}
		if ok {
			if n := len(f.Data); n > 0 {
				if err := c.Received(f.SID, n); err != nil {
					return Frame{}, err
				}
			}
			return f, nil
		}
	}
}

// recv reads one frame; ok is false for a frame it applied.
func (c *Conn) recv() (Frame, bool, error) {
	if _, err := io.ReadFull(c.nc, c.rhdr[:6]); err != nil {
		return Frame{}, false, err
	}
	if c.version >= 2 {
		c.rxAt.Store(int64(time.Since(c.t0)))
	}
	n := int(binary.BigEndian.Uint32(c.rhdr[:4]))
	if n > maxFrame || n < 1 {
		return Frame{}, false, ErrFrame
	}
	typ := c.rhdr[4]
	if typ > typeWindow && c.version < 2 {
		return Frame{}, false, ErrFrame
	}
	// rhdr[5] is the body's first byte: a data, open or close frame's sid_len.
	switch typ {
	case typeData:
		l := int(c.rhdr[5])
		if l > n-1 {
			return Frame{}, false, ErrFrame
		}
		sid := c.rhdr[6 : 6+l]
		if _, err := io.ReadFull(c.nc, sid); err != nil {
			return Frame{}, false, err
		}
		f := Frame{SID: string(sid), Data: make([]byte, n-1-l)}
		if _, err := io.ReadFull(c.nc, f.Data); err != nil {
			return Frame{}, false, err
		}
		return f, true, nil
	case typeMsg:
		b := make([]byte, n)
		b[0] = c.rhdr[5]
		if _, err := io.ReadFull(c.nc, b[1:]); err != nil {
			return Frame{}, false, err
		}
		return Frame{Msg: b}, true, nil
	case typeWindow:
		b, err := c.small(n, 5, 5+MaxSID)
		if err != nil {
			return Frame{}, false, err
		}
		sid, inc, err := ParseWindow(b)
		if err != nil {
			return Frame{}, false, err
		}
		return Frame{}, false, c.Grant(sid, inc)
	case typeOpen:
		l := int(c.rhdr[5])
		if l > n-1 || n-1-l > maxHeader {
			return Frame{}, false, ErrFrame
		}
		b := make([]byte, n-1)
		if _, err := io.ReadFull(c.nc, b); err != nil {
			return Frame{}, false, err
		}
		if c.count(byPeer) >= c.maxSessions {
			return Frame{}, false, ErrFull // the peer ignored this side's max sessions
		}
		sid := string(b[:l])
		c.open(sid, byPeer)
		return Frame{SID: sid, Open: true, Header: b[l:]}, true, nil
	case typeClose:
		b, err := c.small(n, 2, 2+MaxSID)
		if err != nil {
			return Frame{}, false, err
		}
		l := int(b[0])
		if l != n-2 || b[n-1] > closeHalf {
			return Frame{}, false, ErrFrame
		}
		sid, half := string(b[1:1+l]), b[n-1] == closeHalf
		if !half {
			c.Forget(sid)
		}
		return Frame{SID: sid, Close: true, Half: half}, true, nil
	case typePing, typePong:
		b, err := c.small(n, pingLen, pingLen)
		if err != nil {
			return Frame{}, false, err
		}
		if typ == typePing {
			select {
			case c.pong <- [pingLen]byte(b): // keepAlive answers: never this loop
			default: // one answer pending is enough
			}
		} else if c.obs != nil {
			sent := time.Duration(binary.BigEndian.Uint64(b)) // #nosec G115 -- our own ping's time
			if rtt := time.Since(c.t0) - sent; rtt >= 0 && sent > 0 {
				c.obs.PingRTT(rtt)
			}
		}
		return Frame{}, false, nil
	case typeGoAway:
		if n < 4 || n > 4+maxReason {
			return Frame{}, false, ErrFrame
		}
		b := make([]byte, n-1)
		if _, err := io.ReadFull(c.nc, b); err != nil {
			return Frame{}, false, err
		}
		c.goingAway.Store(true)
		c.peerGoAwayOnce.Do(func() {
			close(c.peerGoAway)
			if c.obs != nil {
				c.obs.GoAway(c.id)
			}
		})
		return Frame{}, false, nil
	default:
		return Frame{}, false, ErrFrame
	}
}

// small reads the rest of a frame body of n bytes, which must be within
// [lo, hi], into rhdr; its first byte (rhdr[5]) is already read.
func (c *Conn) small(n, lo, hi int) ([]byte, error) {
	if n < lo || n > hi {
		return nil, ErrFrame
	}
	b := c.rhdr[5 : 5+n]
	if _, err := io.ReadFull(c.nc, b[1:]); err != nil {
		return nil, err
	}
	return b, nil
}

// keepAlive runs a version-2 connection's liveness: it pings once nothing
// arrived for the interval, ends the connection (ErrDeadPeer) once nothing
// arrived for the timeout after the ping, and answers the peer's pings.
func (c *Conn) keepAlive() {
	iv, to := c.live.Interval, c.live.Timeout
	t := time.NewTimer(iv)
	defer t.Stop()
	pingAt := time.Duration(-1)
	for {
		select {
		case <-c.closed:
			return
		case b := <-c.pong:
			c.control(typePong, b[:])
			continue
		case <-t.C:
		}
		now := time.Since(c.t0)
		rx := time.Duration(c.rxAt.Load())
		if pingAt >= 0 && rx < pingAt { // nothing since the ping
			if now-pingAt >= to {
				c.fail(ErrDeadPeer)
				return
			}
			t.Reset(pingAt + to - now)
			continue
		}
		pingAt = -1
		if idle := now - rx; idle < iv {
			t.Reset(iv - idle)
			continue
		}
		var b [pingLen]byte
		binary.BigEndian.PutUint64(b[:], uint64(now)) // #nosec G115 -- a duration since t0 is positive
		c.control(typePing, b[:])
		pingAt = now
		t.Reset(to)
	}
}

// control writes a ping or pong at once.
func (c *Conn) control(typ byte, b []byte) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.frameLocked(typ, "", b) == nil {
		c.flushLocked()
	}
}

// fail ends the connection for err, which Recv and Err then report.
func (c *Conn) fail(err error) {
	c.errMu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.errMu.Unlock()
	_ = c.Close()
}

// Close closes the connection; senders waiting for credit give up.
func (c *Conn) Close() error {
	c.Credit.Close()
	err := c.nc.Close()
	c.downOne.Do(func() {
		if c.obs != nil {
			c.obs.ConnDown(c.id, c.Err())
		}
	})
	return err
}
