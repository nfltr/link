// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"io"
	"time"
)

// The receive loop reads frames through a read-ahead buffer: a burst of
// small frames costs one read of the connection, not three each (header,
// sid, payload). At a frame boundary with nothing buffered it reads into
// rsmall, an array of the Conn's own, so an idle connection holds no
// pooled buffer; once a read fills rsmall (more is waiting) it reads into a
// MaxChunk buffer from dataBufs, which goes back at the next boundary with
// nothing buffered. A payload of viewMax bytes or more goes from the
// connection straight into its own buffer, never through the read-ahead.
// docs/performance.md, P3, has the measurements and what not to undo.

const (
	// rsmallLen is the read while idle: the frames of a small message, or
	// of a few, in one read.
	rsmallLen = 2 << 10
	// rheadLen is the read after a long payload went straight to its
	// buffer: the next frame's header and sid with little of its payload,
	// which a long one would only copy once more.
	rheadLen = 512
	// viewMax: a data frame whose payload is shorter is read ahead, and
	// handed to a Mux as a view of the read buffer (rframe).
	viewMax = MaxChunk / 16
)

// rframe is a frame as the receive loop reads it: a data frame's sid,
// and its payload when view is set (shorter than viewMax), are slices of
// the read buffers, valid until the next read.
type rframe struct {
	Frame
	sid   []byte // a data frame's; nil for other frames
	view  bool
	known bool // a data frame's session has credit here (Credit.receivedSID)
}

// recvIdle gives the read-ahead buffer back at a frame boundary with
// nothing buffered, and the next read goes into rsmall.
func (c *Conn) recvIdle() {
	if len(c.rbuf) > 0 {
		return
	}
	if c.rbig != nil {
		dataBufs.Put(c.rbig)
		c.rbig = nil
	}
	c.rbuf, c.rfull = nil, false
}

// readFull fills p as io.ReadFull(c.nc, p) does, taking the bytes read
// ahead first.
func (c *Conn) readFull(p []byte) error {
	if len(p) < viewMax {
		v, err := c.view(len(p))
		copy(p, v)
		return err
	}
	n := copy(p, c.rbuf)
	c.rbuf = c.rbuf[n:]
	if n == len(p) {
		return nil
	}
	var err error
	// The rest goes straight into p. The next frame's header then goes
	// into rsmall, not ahead into a buffer, and reads little more than a
	// header (rheadLen): a long payload after it would be copied twice.
	c.rfull, c.rlong = false, true
	if err = c.rerr; err != nil {
		c.rerr = nil
		return eofIn(err, n)
	}
	for err == nil && n < len(p) {
		var m int
		m, err = c.read(p[n:])
		n += m
	}
	if n == len(p) {
		return nil
	}
	return eofIn(err, n)
}

// read is one read of the connection. Every byte read is a sign of life
// (rxAt): a frame slower to arrive than Interval + Timeout, its bytes
// coming all along, is no dead peer (spec/README.md, the slow frame).
func (c *Conn) read(b []byte) (int, error) {
	m, err := c.nc.Read(b)
	if m > 0 {
		c.rx.Add(uint64(m)) // #nosec G115 -- m > 0
		c.rxAt.Store(int64(time.Since(c.t0)))
	}
	return m, err
}

// eofIn is a read's error for a call that got n bytes before it: io.EOF
// only if it got none, as io.ReadFull.
func eofIn(err error, n int) error {
	if err == io.EOF && n > 0 {
		return io.ErrUnexpectedEOF
	}
	return err
}

// view returns the next n bytes (at most MaxChunk) as a slice of the read
// buffer, valid until the next read. It reads ahead as much as the
// connection has, up to the buffer's end.
func (c *Conn) view(n int) ([]byte, error) {
	if len(c.rbuf) >= n {
		v := c.rbuf[:n:n]
		c.rbuf = c.rbuf[n:]
		return v, nil
	}
	buf := c.rsmall[:]
	if c.rlong && n <= rheadLen {
		buf = c.rsmall[:rheadLen]
	}
	c.rlong = false
	if c.rbig != nil || c.rfull || n > len(buf) {
		if c.rbig == nil {
			c.rbig = dataBufs.Get().(*[MaxChunk]byte)
		}
		buf = c.rbig[:]
	}
	got := copy(buf, c.rbuf) // what was read ahead, to the buffer's start
	err := c.rerr
	c.rerr = nil
	for got < n && err == nil {
		var m int
		m, err = c.read(buf[got:])
		got += m
	}
	if got < n {
		c.rbuf = nil
		return nil, eofIn(err, got)
	}
	c.rerr = err // with enough bytes: the next read's
	c.rfull = got == len(buf)
	c.rbuf = buf[n:got]
	return buf[:n:n], nil
}
