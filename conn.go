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

	// maxData is the largest payload of one data frame; SendData splits
	// larger.
	maxData = 1 << 20
	// maxFrame bounds the body Recv accepts for one frame.
	maxFrame = 1 + MaxSID + maxData

	// writeBuf is the write batch: over TLS one write syscall. Pooled,
	// held only while frames wait for Flush.
	writeBuf = 256 << 10
)

var writeBufs = sync.Pool{New: func() any { b := make([]byte, 0, writeBuf); return &b }}

// ErrFrame reports bytes that break the frame format.
var ErrFrame = errors.New("link: malformed frame")

// errSID is a send for a session id longer than MaxSID.
var errSID = errors.New("link: session id longer than 255 bytes")

// errEmpty is a send of an empty message (a frame body is never empty).
var errEmpty = errors.New("link: empty message")

// Frame is a received frame: a session's data (SID, Data), or a message
// (Msg, the caller's bytes). Window frames never surface: Recv applies
// them.
type Frame struct {
	SID  string
	Data []byte
	Msg  []byte
}

// Conn sends and receives frames on a bound connection. One goroutine
// Receives; the sends, Flush and the credit calls are safe from any.
//
// Reads go straight to the connection: over TLS, tls.Conn's record buffer
// serves the small header reads, and an idle connection pins no buffer.
// ponytail: a plaintext connection (loopback, dev) reads the socket for
// each header; give it a bufio.Reader if one carries real traffic.
type Conn struct {
	nc   net.Conn
	rhdr [5 + 1 + MaxSID + 4]byte // a header, and a window frame or a data frame's sid

	wmu     sync.Mutex
	wcond   sync.Cond // on wmu: a write finished or took the buffer
	wbuf    *[]byte   // frames not yet written; nil when none
	writing bool      // a write is in flight, wmu released
	werr    error

	window, peerWindow int
	cr                 credits
}

// NewConn returns a Conn over nc, whose hello is done: window is this
// side's window, peerWindow the other side's.
func NewConn(nc net.Conn, window, peerWindow int) *Conn {
	c := &Conn{
		nc:     nc,
		window: window, peerWindow: peerWindow,
		cr: credits{send: map[string]*credit{}, recv: map[string]*recvCredit{}, closed: make(chan struct{})},
	}
	c.wcond.L = &c.wmu
	return c
}

// Window is this side's window. allow:wrapper
func (c *Conn) Window() int { return c.window }

// PeerWindow is the other side's window. allow:wrapper
func (c *Conn) PeerWindow() int { return c.peerWindow }

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

func (c *Conn) frameLocked(typ byte, sid string, body []byte) error {
	n := len(body)
	if typ == typeData {
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
	if typ == typeData {
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

// Recv returns the next data or message frame; it applies window frames
// itself and charges data frames against their session's window. A peer
// that breaks the credit rules gets ErrCredit (credit.go); the caller
// closes the connection on any error.
func (c *Conn) Recv() (Frame, error) {
	for {
		f, ok, err := c.recv()
		if err != nil {
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

// recv reads one frame; ok is false for a window frame, which it applied.
func (c *Conn) recv() (Frame, bool, error) {
	if _, err := io.ReadFull(c.nc, c.rhdr[:6]); err != nil {
		return Frame{}, false, err
	}
	n := int(binary.BigEndian.Uint32(c.rhdr[:4]))
	if n > maxFrame || n < 1 {
		return Frame{}, false, ErrFrame
	}
	// rhdr[5] is the body's first byte: a data frame's sid_len.
	switch c.rhdr[4] {
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
		if n < 5 || n > 5+MaxSID {
			return Frame{}, false, ErrFrame
		}
		b := c.rhdr[5 : 5+n]
		if _, err := io.ReadFull(c.nc, b[1:]); err != nil {
			return Frame{}, false, err
		}
		if int(b[0]) != n-5 {
			return Frame{}, false, ErrFrame
		}
		return Frame{}, false, c.granted(string(b[1:n-4]), int(binary.BigEndian.Uint32(b[n-4:])))
	default:
		return Frame{}, false, ErrFrame
	}
}

// Close closes the connection; senders waiting for credit give up.
func (c *Conn) Close() error {
	c.cr.once.Do(func() { close(c.cr.closed) })
	return c.nc.Close()
}
