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

// CLOSE frame codes.
const (
	closeFull  = 0 // the sender neither sends nor takes more of the session
	closeHalf  = 1 // the sender sends no more; it still takes the peer's
	closeReset = 2 // as closeFull, and the session failed (FeatureReset)
)

// Feature bits a side announces in its hello or reply.
const (
	// FeatureReset: the side reads CLOSE code 2 (a reset). A side sends
	// code 2 only to a peer that announced it, else code 0.
	FeatureReset uint32 = 1 << 0

	features = FeatureReset // what this package announces
)

// dataBufs holds data frames' buffers of MaxChunk bytes: a Mux session
// returns one once its bytes were read or written on (Read, WriteTo), so a
// busy session reuses them instead of allocating and zeroing one per
// frame. A buffer handed elsewhere (Recv's caller, an OwnedWriter) is
// never returned: it is the garbage collector's.
var dataBufs = sync.Pool{New: func() any { b := make([]byte, MaxChunk); return &b }}

func newData(n int) []byte {
	if n > MaxChunk || n < MaxChunk/16 {
		return make([]byte, n)
	}
	return (*dataBufs.Get().(*[]byte))[:n]
}

// releaseData returns b, a data frame's buffer nothing references any
// more, to dataBufs.
func releaseData(b []byte) {
	if cap(b) == MaxChunk {
		b = b[:MaxChunk]
		dataBufs.Put(&b)
	}
}

var writeBufs = sync.Pool{New: func() any { b := make([]byte, 0, writeBuf); return &b }}

// ErrFrame reports bytes that break the wire format (WIRE.md): a frame, or
// a hello or reply with a field out of its bounds.
var ErrFrame = errors.New("link: malformed frame")

var (
	// errSID is a send for a session id longer than MaxSID.
	errSID = errors.New("link: session id longer than 255 bytes")
	// errEmpty is a send of an empty message (a frame body is never empty).
	errEmpty = errors.New("link: empty message")
	// errHeader is an OPEN header longer than the format allows.
	errHeader = errors.New("link: session header too long")
)

// Frame is a received frame: a session's data (SID, Data), a message
// (Msg, the caller's bytes), a session's open (SID, Open,
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
	Reset  bool // a close that says the session failed (code 2)
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
	wcond   sync.Cond       // on wmu: a write finished or took the buffer
	wbuf    *[]byte         // frames not yet written; nil when none
	writing bool            // a write is in flight, wmu released
	waiting int             // senders waiting on wcond for room or their turn
	inBuf   map[string]bool // sessions with a data frame in wbuf (turns)
	pongEnd int             // len(*wbuf) when its last frame is a pong; 0: it is not
	// Senders waiting for room in wbuf take a ticket and go in its order:
	// ticket is the next one to give, serving the one whose turn it is.
	ticket, serving uint64
	werr            error

	dialer          bool // this side dialed: Mux picks odd session ids
	peerMaxSessions int
	peerFeatures    uint32
	maxSessions     int // the peer may have this many open toward this side
	live            Liveness
	id              Identity
	t0              time.Time
	rxAt            atomic.Int64 // time.Since(t0) of the last frame header received
	pong            chan [pingLen]byte
	goingAway       atomic.Bool // either side sent GOAWAY
	peerGoAway      chan struct{}
	peerGoAwayOnce  sync.Once
	// goAwayAt is time.Since(t0) when this side sent GOAWAY (0: not yet);
	// drained is closed once a pong answered a ping sent after it:
	// the peer read the GOAWAY, and every OPEN it sent before has arrived.
	goAwayAt    atomic.Int64
	drained     chan struct{}
	drainedOnce sync.Once

	errMu   sync.Mutex
	err     error // why the connection ended (ErrDeadPeer), set before it closes
	cause   error // what ended it when the peer or the network did (ConnDown)
	closing atomic.Bool
	downOne sync.Once
}

// newConnV2 returns a Conn over nc, bound by Client (dialer) or Server;
// up starts its liveness.
func newConnV2(nc net.Conn, cfg Config, peerWindow, peerMaxSessions int, dialer bool) *Conn {
	c := &Conn{nc: nc}
	c.Credit = NewCredit(cfg.Window, peerWindow, c.sendWindow)
	c.Credit.flush = c.Flush
	c.wcond.L = &c.wmu
	c.dialer, c.peerMaxSessions, c.live = dialer, peerMaxSessions, cfg.Liveness
	c.maxSessions = cfg.MaxSessions
	c.t0 = time.Now()
	c.pong = make(chan [pingLen]byte, 1)
	c.peerGoAway = make(chan struct{})
	c.drained = make(chan struct{})
	return c
}

// up records the connection's peer and observer and reports it up.
// The connection's liveness starts here, once they are set.
func (c *Conn) up(obs Observer, id Identity) {
	c.id, c.obs = id, obs
	if obs != nil {
		obs.ConnUp(id)
	}
	go c.keepAlive()
}

// Version is the connection's wire version: 2, the only one since v0.5.0.
func (c *Conn) Version() int { return MaxVersion }

// Identity is who the peer is, as the listener's Authenticator said (zero
// on the dialer). allow:wrapper
func (c *Conn) Identity() Identity { return c.id }

// PeerMaxSessions is how many sessions this side may have open toward the
// peer at once. allow:wrapper
func (c *Conn) PeerMaxSessions() int { return c.peerMaxSessions }

// PeerFeatures is the feature bits the peer announced.
// allow:wrapper
func (c *Conn) PeerFeatures() uint32 { return c.peerFeatures }

// SendMsg buffers a message frame; Flush, or a full buffer, writes it. The
// caller does not modify b afterwards. The peer reads it from Recv
// (Frame.Msg); a Mux on the peer drops it, so messages are for callers that
// drive the Conn themselves.
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

// SendOpen buffers an OPEN frame for session sid, with header
// (the opener's bytes, at most 64 KiB), and starts the session's credit.
// It refuses a session past the peer's max sessions (ErrFull) and any on a
// connection that is going away (ErrGoAway).
func (c *Conn) SendOpen(sid string, header []byte) error {
	switch {
	case len(sid) > MaxSID:
		return errSID
	case len(header) > maxHeader:
		return errHeader
	case c.count(byUs) >= c.peerMaxSessions:
		return ErrFull
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	// Checked with the frame buffered under one lock: once the peer's
	// GOAWAY was read, no OPEN goes out after a frame written since (the
	// pong that ends the peer's drain barrier, GoAway).
	if c.goingAway.Load() {
		return ErrGoAway
	}
	c.open(sid, byUs)
	return c.frameLocked(typeOpen, sid, header)
}

// SendClose buffers a CLOSE frame for sid: half, the side
// sends no more of the session and still takes the peer's; else it
// neither sends nor takes more, and the session's credit ends.
func (c *Conn) SendClose(sid string, half bool) error {
	code := byte(closeFull)
	if half {
		code = closeHalf
	}
	return c.sendClose(sid, code)
}

// SendReset is SendClose of the whole session saying it failed: a peer
// that announced FeatureReset reads it as a reset, another as a close.
func (c *Conn) SendReset(sid string) error {
	code := byte(closeFull)
	if c.peerFeatures&FeatureReset != 0 {
		code = closeReset
	}
	return c.sendClose(sid, code)
}

func (c *Conn) sendClose(sid string, code byte) error {
	if len(sid) > MaxSID {
		return errSID
	}
	if code != closeHalf {
		c.Forget(sid)
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.frameLocked(typeClose, sid, []byte{code})
}

// GoAway tells the peer to open no more sessions on this
// connection: the open ones may run for drain, then this side closes it.
// It also stops this side opening more (ErrGoAway). It writes at once.
func (c *Conn) GoAway(drain time.Duration, reason string) error {
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
	c.goAwayAt.CompareAndSwap(0, int64(time.Since(c.t0)))
	c.flushLocked()
	return c.werr
}

// ping sends a ping now (its pong arrives after every frame the peer sent
// before it read this one).
func (c *Conn) ping() {
	var b [pingLen]byte
	binary.BigEndian.PutUint64(b[:], uint64(time.Since(c.t0))) // #nosec G115 -- a duration since t0 is positive
	c.control(typePing, b[:])
}

// Drained is closed once the peer answered a ping this side
// sent after its GOAWAY (Mux.GoAway sends one at once): the peer has read
// the GOAWAY, so it opens nothing more here, and every OPEN it sent
// before has arrived (frames keep their order). An OPEN that crossed the
// GOAWAY is a session like any other; one after Drained breaks the
// protocol. A peer before v0.4.2 answers only one of two pings that
// arrive together: then a later ping of the liveness's closes it, or the
// drain deadline ends the connection.
func (c *Conn) Drained() <-chan struct{} { return c.drained }

// PeerGoAway is closed once the peer sent GOAWAY.
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
	// Turns: while senders wait for room, a session whose data is already
	// in the buffer waits for the next one, so they each get a frame into
	// every write instead of one busy session filling buffer after buffer
	// (a waiter wakes only when a write takes the buffer, a running sender
	// at once; on a slow link one of 8 sessions moved nothing for seconds).
	for typ == typeData && c.werr == nil && c.waiting > 0 && c.wbuf != nil && c.inBuf[sid] {
		if !c.writing { // no write to wait for: this sender writes the buffer
			c.writeLocked(false)
			continue
		}
		c.waiting++
		c.wcond.Wait()
		c.waiting--
	}
	// A full buffer goes out first, by this sender or the write in flight.
	// Senders that wait for room take their turns in the order they came
	// (a ticket each): woken together, they would race for the room, and
	// on a slow link one could lose for seconds. Pings and pongs never
	// wait: keepAlive must not block on a network that takes nothing more.
	full := func() bool { return c.wbuf != nil && len(*c.wbuf)+5+n > cap(*c.wbuf) }
	if typ != typePing && typ != typePong && c.werr == nil && (c.ticket != c.serving || full()) {
		my := c.ticket
		c.ticket++
		for c.werr == nil && (my != c.serving || full()) {
			if my == c.serving && !c.writing {
				c.writeLocked(false)
				continue
			}
			c.waiting++
			c.wcond.Wait()
			c.waiting--
		}
		if my == c.serving {
			c.serving++
			if c.waiting > 0 {
				c.wcond.Broadcast() // the next ticket's turn
			}
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
	if typ == typeData {
		if c.inBuf == nil {
			c.inBuf = map[string]bool{}
		}
		c.inBuf[sid] = true
	}
	return nil
}

// writeLocked writes the buffered frames, and those buffered while it
// writes, until none are left. wmu is released during each write: senders
// keep buffering, and their frames go out together in the next one.
// Writes stay in order because only one runs at a time (writing). With
// yield the writer first lets other runnable goroutines run: on a busy
// CPU they buffer their frames for this write instead of each writing
// its own; on an idle one it returns at once.
//
// The sender that starts writing writes once: what was buffered meanwhile
// goes out from a drain goroutine, so that sender gets back to its own
// session. Writing for as long as others kept buffering, a sender on a
// slow, busy connection never did (its session stalled for seconds while
// the others moved).
func (c *Conn) writeLocked(yield bool) {
	c.writing = true
	if yield {
		c.wmu.Unlock()
		runtime.Gosched()
		c.wmu.Lock()
	}
	c.writeOneLocked()
	if c.wbuf != nil && c.werr == nil {
		go c.drain() // writing stays set until it is done
		return
	}
	c.writing = false
	c.wcond.Broadcast()
}

// drain writes what was buffered while a sender wrote, and what is
// buffered meanwhile, until none is left.
func (c *Conn) drain() {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	for c.wbuf != nil && c.werr == nil {
		c.writeOneLocked()
	}
	c.writing = false
	c.wcond.Broadcast()
}

// writeOneLocked writes the buffered frames, wmu released meanwhile.
func (c *Conn) writeOneLocked() {
	if c.wbuf == nil || c.werr != nil {
		return
	}
	b := c.wbuf
	c.wbuf, c.pongEnd = nil, 0
	clear(c.inBuf)      // a new buffer: every session's turn again
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
			c.lost(err)
			return Frame{}, err
		}
		if ok {
			if n := len(f.Data); n > 0 {
				if err := c.Received(f.SID, n); err != nil {
					c.lost(err)
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
	c.rxAt.Store(int64(time.Since(c.t0)))
	n := int(binary.BigEndian.Uint32(c.rhdr[:4]))
	if n > maxFrame || n < 1 {
		return Frame{}, false, ErrFrame
	}
	typ := c.rhdr[4]
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
		f := Frame{SID: string(sid), Data: newData(n - 1 - l)}
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
		if l != n-2 || b[n-1] > closeReset {
			return Frame{}, false, ErrFrame
		}
		sid, half := string(b[1:1+l]), b[n-1] == closeHalf
		if !half {
			c.Forget(sid)
		}
		return Frame{SID: sid, Close: true, Half: half, Reset: b[n-1] == closeReset}, true, nil
	case typePing, typePong:
		b, err := c.small(n, pingLen, pingLen)
		if err != nil {
			return Frame{}, false, err
		}
		if typ == typePing {
			// keepAlive answers, never this loop; only the latest ping
			// needs an answer (a GOAWAY's ping must get one). recv is the
			// only sender, so the send finds room.
			select {
			case <-c.pong:
			default:
			}
			c.pong <- [pingLen]byte(b)
		} else {
			sent := time.Duration(binary.BigEndian.Uint64(b)) // #nosec G115 -- our own ping's time
			if at := c.goAwayAt.Load(); at > 0 && int64(sent) >= at {
				c.drainedOnce.Do(func() { close(c.drained) })
			}
			if rtt := time.Since(c.t0) - sent; c.obs != nil && rtt >= 0 && sent > 0 {
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

// keepAlive runs a connection's liveness: it pings once nothing
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
		c.ping()
		pingAt = now
		// Look again an interval on (Timeout is at least that): once the
		// pong came the next ping goes Interval after it, so a peer that
		// dies right after it is dead Interval + Timeout after its last
		// frame (not 2 x Timeout).
		t.Reset(iv)
	}
}

// control writes a ping or pong at once, from a goroutine of its own when
// no write is in flight: keepAlive never waits for the network.
//
// A pong that still waits as the last frame buffered answers a newer ping
// in its place (it is then still after every frame sent before that ping
// was read): a peer that pings faster than it reads gets one pong per
// write, never a buffer that grows with its pings.
func (c *Conn) control(typ byte, b []byte) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if typ == typePong && c.wbuf != nil && len(*c.wbuf) == c.pongEnd {
		copy((*c.wbuf)[c.pongEnd-pingLen:], b)
		return
	}
	if c.frameLocked(typ, "", b) != nil {
		return
	}
	if typ == typePong {
		c.pongEnd = len(*c.wbuf)
	}
	if !c.writing {
		c.writing = true
		go c.drain()
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

// lost records what ended the connection when this side did not close it
// (ConnDown's error): the peer's close (io.EOF), a reset, a broken frame.
func (c *Conn) lost(err error) {
	select {
	case <-c.closed:
		return
	default:
	}
	if c.closing.Load() {
		return
	}
	c.errMu.Lock()
	if c.cause == nil {
		c.cause = err
	}
	c.errMu.Unlock()
}

// Close closes the connection; senders waiting for credit give up.
// ConnDown gets why it ended: ErrDeadPeer, what the peer or the network
// did (lost), or nil when this side closed it.
func (c *Conn) Close() error {
	c.Credit.Close()
	err := c.nc.Close()
	c.downOne.Do(func() {
		if c.obs != nil {
			c.errMu.Lock()
			why := c.err
			if why == nil {
				why = c.cause
			}
			c.errMu.Unlock()
			c.obs.ConnDown(c.id, why)
		}
	})
	return err
}

// closeFlushed closes the connection once the frames buffered so far are
// written: a leaving connection's last tails and CLOSE frames reach the
// peer. Where the connection has a write side of its own to close (TCP,
// TLS) it closes that and leaves the rest to the peer's close, so frames
// the peer sends meanwhile (window updates) do not reset the connection
// under bytes it has yet to read; after wait it closes anyway.
func (c *Conn) closeFlushed(wait time.Duration) {
	c.closing.Store(true)
	c.wmu.Lock()
	for (c.writing || c.wbuf != nil) && c.werr == nil {
		if !c.writing {
			c.flushLocked()
			continue
		}
		c.wcond.Wait()
	}
	c.wmu.Unlock()
	if cw, ok := c.nc.(interface{ CloseWrite() error }); ok && cw.CloseWrite() == nil {
		t := time.AfterFunc(wait, func() { _ = c.Close() })
		<-c.closed
		t.Stop()
		return
	}
	_ = c.Close()
}
