// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/link/bytequeue"
)

// MaxSessions is the most sessions one Mux carries at once by default:
// Open refuses more (ErrFull), Serve closes a session the peer opens past
// it. On version 2 the peer's hello sets the limit toward it.
const MaxSessions = 1024

// ErrFull is Open's refusal of a session past the most sessions allowed.
var ErrFull = errors.New("link: connection carries its most sessions")

// Codec reads and writes the messages that open and close sessions: in
// wire version 1 their encoding is the caller's. Version 2 needs none.
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

// Mux carries sessions over a Conn, each a net.Conn. Either side opens
// sessions (Open), the other takes them (Serve's accept).
// A session's data goes on credit, so a session whose reader is slow
// stalls only itself; a Write goes to the network inline, or with the
// frames of other sessions when a write is in flight (Conn).
//
// On version 1 opens and closes are the caller's messages (Codec), and
// the Mux picks session ids from 1 up. On version 2 they are OPEN and
// CLOSE frames, the open carrying the opener's header, and the Mux picks
// ids as uvarints, odd on the dialer and even on the listener, so the two
// sides never pick the same.
type Mux struct {
	c     *Conn
	codec Codec
	ids   atomic.Uint64
	mu    sync.Mutex
	sess  map[string]*Session
	done  bool // Serve returned: no more sessions
	// leaving: this side sent GOAWAY; the connection closes once its last
	// session ends.
	leaving bool
}

// NewMux returns a Mux over c whose open and close messages codec reads
// and writes (version 1; nil on version 2).
func NewMux(c *Conn, codec Codec) *Mux {
	return &Mux{c: c, codec: codec, sess: map[string]*Session{}}
}

// Conn is the Mux's connection. allow:wrapper
func (m *Mux) Conn() *Conn { return m.c }

// Sessions is how many sessions are open.
func (m *Mux) Sessions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sess)
}

// nextSID picks a session id this side has not used.
func (m *Mux) nextSID() string {
	n := m.ids.Add(1)
	if m.c.version < 2 {
		return strconv.FormatUint(n, 36)
	}
	n = 2 * n // even: the listener's
	if m.c.dialer {
		n--
	}
	return string(binary.AppendUvarint(nil, n))
}

// Open starts a session: open returns the bytes that open it for the
// session id the Mux chose (version 1: the caller's open message;
// version 2: the OPEN frame's header). Open sends it at once, so a peer
// that speaks first is reached without the caller's bytes.
func (m *Mux) Open(open func(sid string) ([]byte, error)) (*Session, error) {
	if m.c.GoingAway() {
		return nil, ErrGoAway
	}
	sid := m.nextSID()
	s, err := m.add(sid, nil)
	if err != nil {
		return nil, err
	}
	msg, err := open(sid)
	if err == nil {
		s.header = msg
		if m.c.version >= 2 {
			err = m.c.SendOpen(sid, msg)
		} else {
			err = m.c.SendMsg(msg)
		}
	}
	if err == nil {
		err = m.c.Flush()
	}
	if err != nil {
		s.peerClosed.Store(true) // the peer never heard of it
		m.drop(s)
		return nil, err
	}
	return s, nil
}

func (m *Mux) add(sid string, header []byte) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done {
		return nil, net.ErrClosed
	}
	if len(m.sess) >= MaxSessions {
		return nil, ErrFull
	}
	s := &Session{m: m, id: sid, q: bytequeue.New(m.c.window), header: header, done: make(chan struct{})}
	s.q.OnPop(func(n int) { m.c.Consumed(sid, n) })
	if m.c.version < 2 {
		m.c.Open(sid) // version 2: the OPEN frame's
	}
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
	last := m.leaving && len(m.sess) == 0
	m.mu.Unlock()
	s.ended()
	if last {
		_ = m.c.Close()
	}
}

func (m *Mux) get(sid string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sess[sid]
}

// sendClose sends the frame that closes sid (half: its write side,
// version 2).
func (m *Mux) sendClose(sid string, half bool) {
	var err error
	if m.c.version >= 2 {
		err = m.c.SendClose(sid, half)
	} else {
		var msg []byte
		if msg, err = m.codec.Close(sid); err == nil {
			err = m.c.SendMsg(msg)
		}
	}
	if err == nil {
		_ = m.c.Flush()
	}
}

// Serve receives frames until the connection fails, then closes it and
// ends every session. accept, nil on a side that takes no sessions, runs
// in a goroutine of its own for each session the peer opens, with the
// bytes that opened it (version 1: the open message; version 2: the
// header); it owns the session and closes it.
func (m *Mux) Serve(accept func(open []byte, s *Session)) error {
	defer m.close()
	for {
		f, err := m.c.Recv()
		if err != nil {
			return err
		}
		if m.c.version >= 2 {
			m.serveV2(f, accept)
			continue
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
				s, err = m.add(id, f.Msg)
			}
			if s == nil {
				m.sendClose(id, false)
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
				s.kick()
			}
		}
	}
}

// serveV2 applies a version-2 frame. Messages have no meaning to a Mux.
func (m *Mux) serveV2(f Frame, accept func([]byte, *Session)) {
	switch {
	case f.Open:
		var s *Session
		if accept != nil && !m.c.GoingAway() {
			s, _ = m.add(f.SID, f.Header)
		}
		if s == nil { // refused: the opener reads a reset, never a clean end
			if m.c.SendReset(f.SID) == nil {
				_ = m.c.Flush()
			}
			return
		}
		go accept(f.Header, s)
	case f.Close:
		s := m.get(f.SID)
		if s == nil {
			return
		}
		if !f.Half {
			if f.Reset {
				s.fail(ErrReset)
			}
			s.peerClosed.Store(true)
			m.drop(s)
			return
		}
		s.q.Close() // reads end once the queue drains
		s.kick()
		if s.peerHalf.Store(true); s.halfClosed.Load() {
			s.peerClosed.Store(true)
			m.drop(s) // both sides stopped sending
		}
	case len(f.Data) > 0:
		if s := m.get(f.SID); s != nil {
			s.q.Push(f.Data) // within its credit: does not wait
			s.kick()
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
	err := m.c.Err()
	if err == nil {
		err = ErrConnClosed
	}
	for _, s := range sess {
		s.fail(err) // a session cut with its connection never reads a clean end
		s.peerClosed.Store(true)
		s.ended()
	}
}

// GoAway (version 2) tells the peer to open no more sessions here and
// stops this side opening more; the connection closes once its last
// session ends, and at deadline at the latest (zero: no deadline). OPENs
// that arrive afterwards are refused: their opener retries elsewhere.
func (m *Mux) GoAway(deadline time.Time) error {
	var drain time.Duration
	if !deadline.IsZero() {
		drain = max(time.Until(deadline), time.Millisecond)
	}
	if err := m.c.GoAway(drain, ""); err != nil {
		return err
	}
	m.mu.Lock()
	m.leaving = true
	empty := len(m.sess) == 0
	m.mu.Unlock()
	if empty {
		return m.c.Close()
	}
	if drain > 0 {
		t := time.AfterFunc(drain, func() { _ = m.c.Close() })
		go func() { <-m.c.Done(); t.Stop() }()
	}
	return nil
}

// Close closes the connection: Serve returns and every session ends.
// allow:wrapper
func (m *Mux) Close() error { return m.c.Close() }

// Session is one session of a Mux. Read and WriteTo take the peer's bytes
// in order, then io.EOF once the peer closed the session (or its write
// side), or the error that cut it: ErrReset (the peer's Reset), the
// connection's (ErrDeadPeer, ErrConnClosed). Write sends on credit. A read
// deadline ends a pending Read with os.ErrDeadlineExceeded; write
// deadlines are not supported (Close ends a session).
type Session struct {
	m      *Mux
	id     string
	header []byte
	q      *bytequeue.Queue
	rest   []byte // unread tail of the last slice taken from q
	cur    []byte // that slice, released once read

	closed     atomic.Bool // Close called, or the session ended
	peerClosed atomic.Bool // the peer closed it (or the connection ended): Close sends nothing
	halfClosed atomic.Bool // CloseWrite called (version 2)
	peerHalf   atomic.Bool // the peer closed its write side (version 2)
	once       sync.Once

	err     atomic.Pointer[error] // why the session was cut; nil: it ends with io.EOF
	endOnce sync.Once
	done    chan struct{} // closed once the session ended

	dlMu    sync.Mutex
	dlTimer *time.Timer
	dlGen   uint64      // the deadline's generation: a stale timer does nothing
	dlPast  atomic.Bool // the read deadline passed

	pump    atomic.Pointer[pump] // set by Pump
	pumping atomic.Bool          // a pump goroutine runs, or the pump ended
}

// pump is a Session's Pump: where its bytes go.
type pump struct {
	w     io.Writer
	done  func(int64, error)
	total int64
	bufs  [][]byte // scratch, kept across runs
	bw    batchWriter
}

// ErrReset is what a session reads once the peer reset it (Reset).
var ErrReset = errors.New("link: session reset by the peer")

// ErrConnClosed is what a session reads once its connection closed with
// no error of the link's (the network's, or Close).
var ErrConnClosed = errors.New("link: connection closed")

// fail records why the session was cut (the first reason wins).
func (s *Session) fail(err error) { s.err.CompareAndSwap(nil, &err) }

// end is what Read returns once the queue is drained.
func (s *Session) end() error {
	if e := s.err.Load(); e != nil {
		return *e
	}
	return io.EOF
}

// Done is closed once the session ended: both sides closed it, the peer
// reset it, or its connection closed. Bytes already received stay
// readable.
func (s *Session) Done() <-chan struct{} { return s.done }

// Header is the bytes that opened the session: the open message on
// version 1, the OPEN frame's header on version 2. allow:wrapper
func (s *Session) Header() []byte { return s.header }

// ended stops writes and lets reads drain the queue.
func (s *Session) ended() {
	s.closed.Store(true)
	s.q.Close()
	s.kick()
	s.endOnce.Do(func() { close(s.done) })
}

func (s *Session) Read(b []byte) (int, error) {
	if len(s.rest) == 0 {
		data, ok := s.q.PopUnless(&s.dlPast)
		if !ok {
			if s.dlPast.Load() {
				return 0, os.ErrDeadlineExceeded
			}
			return 0, s.end()
		}
		s.rest, s.cur = data, data
	}
	n := copy(b, s.rest)
	if s.rest = s.rest[n:]; len(s.rest) == 0 {
		releaseData(s.cur)
		s.cur = nil
	}
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
		releaseData(s.cur)
		s.cur = nil
		if err != nil {
			return total, err
		}
	}
	var bufs [][]byte
	bw := new(batchWriter)
	for {
		var ok bool
		if bufs, ok = s.q.PopBatch(bufs[:0], writeBuf); !ok {
			return total, s.endErr()
		}
		n, err := bw.write(w, bufs)
		total += n
		if err != nil {
			return total, err
		}
	}
}

// batchWriter writes batches of received slices in one net.Buffers write
// each, then releases them (a Writer keeps none of p). Its scratch is
// reused: net.Buffers.WriteTo moves its receiver to the heap.
type batchWriter struct {
	wb [][]byte
	nb net.Buffers
}

func (bw *batchWriter) write(w io.Writer, bufs [][]byte) (int64, error) {
	// net.Buffers clears the slices it wrote (writev): it gets a copy,
	// bufs keeps them for the release.
	bw.wb = append(bw.wb[:0], bufs...)
	bw.nb = bw.wb
	n, err := bw.nb.WriteTo(w)
	for _, b := range bufs {
		releaseData(b)
	}
	clear(bufs)
	clear(bw.wb)
	return n, err
}

// Pump writes the peer's bytes to w as WriteTo does, without a goroutine
// waiting while the session is idle: one runs only while bytes are
// queued, started as they arrive (the receive loop never waits for it).
// done gets what WriteTo would return, once, from that goroutine: the
// session ended, or w failed (the session's further bytes stay unread:
// close it). Pump takes over the session's reads: no Read, WriteTo or
// second Pump after it.
func (s *Session) Pump(w io.Writer, done func(n int64, err error)) {
	s.pump.Store(&pump{w: w, done: done})
	s.kick()
}

// kick starts the pump's goroutine unless one runs.
func (s *Session) kick() {
	if s.pump.Load() != nil && s.pumping.CompareAndSwap(false, true) {
		go s.drain()
	}
}

// drain writes what is queued, then stops; a push meanwhile restarts it.
// Only one runs at a time (pumping), so bytes go out in order.
func (s *Session) drain() {
	p := s.pump.Load()
	ow, owned := p.w.(OwnedWriter)
	var err error
	if len(s.rest) > 0 { // the tail of a Read before Pump
		b := s.rest
		s.rest, s.cur = nil, nil
		if err = writeOwnedOrCopy(p.w, ow, owned, b); err != nil {
			p.done(p.total, err)
			return
		}
		p.total += int64(len(b))
	}
	for {
		bufs, ok, ended := s.q.TryPopBatch(p.bufs[:0], writeBuf)
		p.bufs = bufs
		if !ok {
			if ended {
				p.done(p.total, s.endErr()) // pumping stays set: no other runs
				return
			}
			s.pumping.Store(false)
			if !s.q.Ready() || !s.pumping.CompareAndSwap(false, true) {
				return // idle, or a push started another
			}
			continue
		}
		var n int64
		if owned {
			for _, b := range bufs {
				if err = ow.WriteOwned(b); err != nil {
					break
				}
				n += int64(len(b))
			}
			clear(bufs)
		} else {
			n, err = p.bw.write(p.w, bufs)
		}
		p.total += n
		if err != nil {
			p.done(p.total, err)
			return
		}
	}
}

// writeOwnedOrCopy writes b to an OwnedWriter (which keeps it) or w.
func writeOwnedOrCopy(w io.Writer, ow OwnedWriter, owned bool, b []byte) error {
	if owned {
		return ow.WriteOwned(b)
	}
	_, err := w.Write(b)
	return err
}

func (s *Session) writeOwned(w OwnedWriter) (int64, error) {
	var total int64
	for {
		b := s.rest
		s.rest, s.cur = nil, nil // owned by w from here
		if len(b) == 0 {
			var ok bool
			if b, ok = s.q.Pop(); !ok {
				return total, s.endErr()
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
		if s.closed.Load() || s.halfClosed.Load() || !s.m.c.Acquire(s.id, n) {
			return len(b) - len(p), io.ErrClosedPipe
		}
		if err := s.m.c.SendData(s.id, p[:n]); err != nil {
			return len(b) - len(p), err
		}
		p = p[n:]
	}
	return len(b), s.m.c.Flush()
}

// WriteBuffers is Write of each of bufs in turn, with one flush at the
// end (and before any wait for credit): a backlog goes to the network in
// one write.
func (s *Session) WriteBuffers(bufs [][]byte) (int64, error) {
	var total int64
	for _, b := range bufs {
		for p := b; len(p) > 0; {
			n := min(len(p), MaxChunk)
			if s.closed.Load() || s.halfClosed.Load() || !s.m.c.Acquire(s.id, n) {
				return total, io.ErrClosedPipe
			}
			if err := s.m.c.SendData(s.id, p[:n]); err != nil {
				return total, err
			}
			p = p[n:]
			total += int64(n)
		}
	}
	return total, s.m.c.Flush()
}

// CloseWrite (version 2) ends the session's write side: the peer reads
// io.EOF after what was written; this side still reads the peer's bytes.
// The session ends once both sides closed their write side.
func (s *Session) CloseWrite() error {
	if s.m.c.version < 2 {
		return errV1
	}
	if s.closed.Load() || s.halfClosed.Swap(true) {
		return nil
	}
	s.m.sendClose(s.id, true)
	if s.peerHalf.Load() {
		s.peerClosed.Store(true) // both halves closed: nothing more to send
		s.m.drop(s)
	}
	return nil
}

// endErr is WriteTo's error once the session ended: nil at a clean end
// (io.Copy's contract), else why it was cut.
func (s *Session) endErr() error {
	if err := s.end(); err != io.EOF {
		return err
	}
	return nil
}

// Close ends the session: the peer gets a close unless it closed it; a
// pending Read returns.
func (s *Session) Close() error {
	s.once.Do(func() {
		s.m.drop(s)
		if !s.peerClosed.Load() {
			s.m.sendClose(s.id, false)
		}
	})
	return nil
}

// Reset ends the session as failed: the peer reads ErrReset (version 2
// with a peer that announced FeatureReset; else it reads a close).
// Reads here end as after Close.
func (s *Session) Reset() error {
	s.once.Do(func() {
		s.m.drop(s)
		if s.peerClosed.Load() {
			return
		}
		var err error
		if s.m.c.version >= 2 {
			err = s.m.c.SendReset(s.id)
		} else {
			var msg []byte
			if msg, err = s.m.codec.Close(s.id); err == nil {
				err = s.m.c.SendMsg(msg)
			}
		}
		if err == nil {
			_ = s.m.c.Flush()
		}
	})
	return nil
}

// LocalAddr is the connection's. allow:wrapper
func (s *Session) LocalAddr() net.Addr { return s.m.c.nc.LocalAddr() }

// RemoteAddr is the connection's. allow:wrapper
func (s *Session) RemoteAddr() net.Addr { return s.m.c.nc.RemoteAddr() }

// SetDeadline is SetReadDeadline: write deadlines are not supported.
func (s *Session) SetDeadline(t time.Time) error { return s.SetReadDeadline(t) }

// SetReadDeadline makes a pending or later Read return
// os.ErrDeadlineExceeded once t passed; zero t clears it. WriteTo ignores
// it.
func (s *Session) SetReadDeadline(t time.Time) error {
	s.dlMu.Lock()
	defer s.dlMu.Unlock()
	s.dlGen++
	if s.dlTimer != nil {
		s.dlTimer.Stop()
		s.dlTimer = nil
	}
	d := time.Until(t)
	s.dlPast.Store(!t.IsZero() && d <= 0)
	if s.dlPast.Load() {
		s.q.Wake()
	}
	if !t.IsZero() && d > 0 {
		gen := s.dlGen
		s.dlTimer = time.AfterFunc(d, func() {
			s.dlMu.Lock()
			if s.dlGen == gen {
				s.dlPast.Store(true)
			}
			s.dlMu.Unlock()
			s.q.Wake()
		})
	}
	return nil
}

// SetWriteDeadline is a no-op: Close ends a session whose writes wait.
func (s *Session) SetWriteDeadline(time.Time) error { return nil }
