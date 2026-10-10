// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/link/bytequeue"
)

// MaxSessions is the most sessions one Mux carries at once: Open refuses
// more (ErrFull), Serve closes a session the peer opens past it.
const MaxSessions = 1024

// ErrFull is Open's refusal of a session past MaxSessions.
var ErrFull = errors.New("link: connection carries its most sessions")

// Codec reads and writes the messages that open and close sessions: in
// wire version 1 their encoding is the caller's.
type Codec interface {
	// Close returns the message that closes session sid.
	Close(sid string) ([]byte, error)
	// Parse reads a received message: the session it names, whether it
	// opens or closes it, and data it carries for the session.
	Parse(msg []byte) (Message, error)
}

// Message is a parsed message frame (Codec.Parse).
type Message struct {
	SID         string
	Open, Close bool
	Data        []byte
}

// Mux carries sessions over a Conn, each a net.Conn.
// One side opens sessions (Open), the other takes them (Serve's accept).
// A session's data goes on credit, so a session whose reader is slow
// stalls only itself; a Write goes to the network inline, or with the
// frames of other sessions when a write is in flight (Conn).
type Mux struct {
	c     *Conn
	codec Codec
	ids   atomic.Uint64
	mu    sync.Mutex
	sess  map[string]*Session
	done  bool // Serve returned: no more sessions
}

// NewMux returns a Mux over c whose open and close messages codec reads
// and writes.
func NewMux(c *Conn, codec Codec) *Mux {
	return &Mux{c: c, codec: codec, sess: map[string]*Session{}}
}

// Open starts a session: open returns the message that opens it, for the
// session id the Mux chose. Open sends it at once, so a peer that speaks
// first is reached without the caller's bytes.
func (m *Mux) Open(open func(sid string) ([]byte, error)) (*Session, error) {
	sid := strconv.FormatUint(m.ids.Add(1), 36)
	s, err := m.add(sid)
	if err != nil {
		return nil, err
	}
	msg, err := open(sid)
	if err == nil {
		err = m.c.SendMsg(msg)
	}
	if err == nil {
		err = m.c.Flush()
	}
	if err != nil {
		m.drop(s)
		return nil, err
	}
	return s, nil
}

func (m *Mux) add(sid string) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done {
		return nil, net.ErrClosed
	}
	if len(m.sess) >= MaxSessions {
		return nil, ErrFull
	}
	s := &Session{m: m, id: sid, q: bytequeue.New(m.c.window)}
	s.q.OnPop(func(n int) { m.c.Consumed(sid, n) })
	m.c.Open(sid)
	if old := m.sess[sid]; old != nil { // a peer reusing an id ends the old session
		old.ended()
	}
	m.sess[sid] = s
	return s, nil
}

// drop forgets s: its writes and pending credit waits fail, its reads
// return what is queued, then io.EOF.
func (m *Mux) drop(s *Session) {
	m.mu.Lock()
	if m.sess[s.id] == s {
		delete(m.sess, s.id)
		m.c.Forget(s.id)
	}
	m.mu.Unlock()
	s.ended()
}

func (m *Mux) get(sid string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sess[sid]
}

// sendClose sends the message that closes sid.
func (m *Mux) sendClose(sid string) {
	if msg, err := m.codec.Close(sid); err == nil && m.c.SendMsg(msg) == nil {
		_ = m.c.Flush()
	}
}

// Serve receives frames until the connection fails, then closes it and
// ends every session. accept, nil on the side that opens sessions, runs
// in a goroutine of its own for each session the peer opens, with the
// message that opened it; it owns the session and closes it.
func (m *Mux) Serve(accept func(open []byte, s *Session)) error {
	defer m.close()
	for {
		f, err := m.c.Recv()
		if err != nil {
			return err
		}
		msg := Message{SID: f.SID, Data: f.Data}
		if f.Msg != nil {
			if msg, err = m.codec.Parse(f.Msg); err != nil {
				return err
			}
			if n := len(msg.Data); n > 0 {
				if err := m.c.Received(msg.SID, n); err != nil {
					return err
				}
			}
		}
		id := msg.SID
		switch {
		case strings.HasPrefix(id, "_"): // control frames: the caller's
		case msg.Open:
			var s *Session
			if accept != nil {
				s, err = m.add(id)
			}
			if s == nil {
				m.sendClose(id)
				continue
			}
			go accept(f.Msg, s)
		case msg.Close:
			if s := m.get(id); s != nil {
				s.peerClosed.Store(true)
				m.drop(s)
			}
		case len(msg.Data) > 0:
			if s := m.get(id); s != nil {
				s.q.Push(msg.Data) // within its credit: does not wait
			}
		}
	}
}

// close ends the connection and every session; Open fails from then on.
func (m *Mux) close() {
	m.mu.Lock()
	m.done = true
	sess := m.sess
	m.sess = nil
	m.mu.Unlock()
	_ = m.c.Close() // senders waiting for credit give up
	for _, s := range sess {
		s.peerClosed.Store(true)
		s.ended()
	}
}

// Close closes the connection: Serve returns and every session ends.
// allow:wrapper
func (m *Mux) Close() error { return m.c.Close() }

// Session is one session of a Mux. Read and WriteTo take the peer's bytes
// in order, then io.EOF once the peer closed the session; Write sends on
// credit. Deadlines are not supported: Close ends a session.
type Session struct {
	m    *Mux
	id   string
	q    *bytequeue.Queue
	rest []byte // unread tail of the last slice taken from q

	closed     atomic.Bool // Close called, or the session ended
	peerClosed atomic.Bool // the peer closed it (or the connection ended): Close sends nothing
	once       sync.Once
}

// ended stops writes and lets reads drain the queue.
func (s *Session) ended() {
	s.closed.Store(true)
	s.q.Close()
}

func (s *Session) Read(b []byte) (int, error) {
	if len(s.rest) == 0 {
		data, ok := s.q.Pop()
		if !ok {
			return 0, io.EOF
		}
		s.rest = data
	}
	n := copy(b, s.rest)
	s.rest = s.rest[n:]
	return n, nil
}

// OwnedWriter is a writer that may keep the slices it is given: WriteTo
// hands it the received slices themselves, no copy.
type OwnedWriter interface {
	WriteOwned(b []byte) error
}

// WriteTo writes the peer's bytes to w until the session ends; a backlog
// goes in one net.Buffers write (writev on a TCP conn), or slice by slice
// to an OwnedWriter.
func (s *Session) WriteTo(w io.Writer) (int64, error) {
	if ow, ok := w.(OwnedWriter); ok {
		return s.writeOwned(ow)
	}
	var total int64
	if len(s.rest) > 0 {
		n, err := w.Write(s.rest)
		total += int64(n)
		s.rest = nil
		if err != nil {
			return total, err
		}
	}
	var bufs [][]byte
	for {
		var ok bool
		if bufs, ok = s.q.PopBatch(bufs[:0], writeBuf); !ok {
			return total, nil
		}
		nb := net.Buffers(bufs)
		n, err := nb.WriteTo(w)
		total += n
		clear(bufs)
		if err != nil {
			return total, err
		}
	}
}

func (s *Session) writeOwned(w OwnedWriter) (int64, error) {
	var total int64
	for {
		b := s.rest
		s.rest = nil
		if len(b) == 0 {
			var ok bool
			if b, ok = s.q.Pop(); !ok {
				return total, nil
			}
		}
		if err := w.WriteOwned(b); err != nil {
			return total, err
		}
		total += int64(len(b))
	}
}

// Write sends b to the peer in chunks it has credit for, waiting for
// credit, then writes them to the network (Flush).
func (s *Session) Write(b []byte) (int, error) {
	for p := b; len(p) > 0; {
		n := min(len(p), MaxChunk)
		if s.closed.Load() || !s.m.c.Acquire(s.id, n) {
			return len(b) - len(p), io.ErrClosedPipe
		}
		if err := s.m.c.SendData(s.id, p[:n]); err != nil {
			return len(b) - len(p), err
		}
		p = p[n:]
	}
	return len(b), s.m.c.Flush()
}

// Close ends the session: the peer gets a close unless it closed it; a
// pending Read returns.
func (s *Session) Close() error {
	s.once.Do(func() {
		s.m.drop(s)
		if !s.peerClosed.Load() {
			s.m.sendClose(s.id)
		}
	})
	return nil
}

// LocalAddr is the connection's. allow:wrapper
func (s *Session) LocalAddr() net.Addr { return s.m.c.nc.LocalAddr() }

// RemoteAddr is the connection's. allow:wrapper
func (s *Session) RemoteAddr() net.Addr { return s.m.c.nc.RemoteAddr() }

// SetDeadline and its siblings are no-ops: Close ends a session.
func (s *Session) SetDeadline(time.Time) error      { return nil }
func (s *Session) SetReadDeadline(time.Time) error  { return nil }
func (s *Session) SetWriteDeadline(time.Time) error { return nil }
