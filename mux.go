// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/link/internal/bytequeue"
)

// MaxSessions is the most sessions one Mux carries at once by default:
// Open refuses more (ErrFull), Serve closes a session the peer opens past
// it. The peer's hello sets the limit toward it.
const MaxSessions = 1024

// ErrFull is Open's refusal of a session past the most sessions allowed.
var ErrFull = errors.New("link: connection carries its most sessions")

// Mux carries sessions over a Conn, each a net.Conn. Either side opens
// sessions (Open), the other takes them (Serve's accept).
// A session's data goes on credit, so a session whose reader is slow
// stalls only itself; a Write goes to the network inline, or with the
// frames of other sessions when a write is in flight (Conn).
//
// Opens and closes are OPEN and CLOSE frames, the open carrying the
// opener's header, and the Mux picks ids as uvarints, odd on the dialer and even on the listener, so the two
// sides never pick the same.
//
// Message frames (Conn.SendMsg) are a Conn-level feature for callers that
// run their own receive loop; a Mux owns the Conn's receive loop and drops
// them. Per-peer control traffic on a Mux goes in a session whose header
// names its purpose.
type Mux struct {
	c    *Conn
	ids  atomic.Uint64
	used atomic.Bool // a session went on it, either side's
	mu   sync.Mutex
	sess map[string]*Session
	done bool // Serve returned: no more sessions
	// leaving: this side sent GOAWAY; the connection closes once its last
	// session ended and the peer read the GOAWAY (Conn.Drained).
	leaving bool
	left    bool // the close began
	cutting bool // a drain's deadline cut the sessions
}

// NewMux returns a Mux over c. Call Serve on it: Serve is the receive
// loop, which a side that takes no sessions runs too (Serve(nil)), or
// no credit returns and its writes stall.
func NewMux(c *Conn) *Mux {
	return &Mux{c: c, sess: map[string]*Session{}}
}

// Conn is the Mux's connection.
func (m *Mux) Conn() *Conn { return m.c }

// Sessions is how many sessions are open.
func (m *Mux) Sessions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sess)
}

// nextSID picks a session id this side has not used.
func (m *Mux) nextSID() string {
	n := 2 * m.ids.Add(1) // even: the listener's
	if m.c.dialer {
		n--
	}
	return string(binary.AppendUvarint(nil, n))
}

// Open starts a session with header, the bytes the peer's accept
// receives (the OPEN frame's header, at most 64 KiB). Open sends it at
// once and does not wait for the peer, so a peer that speaks first is
// reached without the caller's bytes.
//
// On a connection that negotiated FeatureChain the open is a CHAIN-OPEN
// from the origin (OpenChain with a zero Chain) and the peer answers it
// (Answered).
func (m *Mux) Open(header []byte) (*Session, error) { return m.OpenChain(header, Chain{}) }

// OpenChain is Open of a session on a path (Forward's Route opens the
// next leg with the Chain its ctx carries): a zero Trace is a new path,
// for which link picks a random one. On a connection without
// FeatureChain it is Open (the Chain is kept locally only) and the
// session does not answer (Answers is false).
func (m *Mux) OpenChain(header []byte, c Chain) (*Session, error) {
	if m.c.GoingAway() {
		return nil, ErrGoAway
	}
	if c.Trace == (Trace{}) && m.c.chain() {
		_, _ = rand.Read(c.Trace[:])
	}
	sid := m.nextSID()
	s, err := m.add(sid, header, c, false)
	if err != nil {
		return nil, err
	}
	err = m.c.sendOpen(sid, header, c)
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

func (m *Mux) add(sid string, header []byte, c Chain, accepter bool) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done {
		return nil, net.ErrClosed
	}
	if len(m.sess) >= MaxSessions {
		return nil, ErrFull
	}
	s := &Session{m: m, id: sid, q: bytequeue.New(m.c.credit.window), header: header, done: make(chan struct{}),
		chain: c, answers: m.c.chain(), accepter: accepter}
	if s.answers && !accepter {
		s.answered = make(chan struct{})
	}
	s.q.OnPop(func(n int) { m.c.credit.Consumed(sid, n) })
	if old := m.sess[sid]; old != nil { // a peer reusing an id ends the old session
		old.ended()
	}
	m.sess[sid] = s
	m.used.Store(true)
	return s, nil
}

// drop forgets s: its writes and pending credit waits fail, its reads
// return what is queued, then io.EOF.
func (m *Mux) drop(s *Session) {
	m.mu.Lock()
	if m.sess[s.id] == s {
		delete(m.sess, s.id)
		m.c.credit.Forget(s.id)
	}
	m.mu.Unlock()
	s.ended()
	m.leave()
}

// leave closes a leaving connection once it carries no session and the
// peer read the GOAWAY, after the frames buffered for it went out.
func (m *Mux) leave() {
	m.mu.Lock()
	last := m.leaving && !m.left && len(m.sess) == 0 && closed(m.c.drained)
	m.left = m.left || last
	m.mu.Unlock()
	if last {
		go m.c.closeFlushed(m.c.live.Timeout)
	}
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (m *Mux) get(sid string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sess[sid]
}

// sendClose sends the frame that closes sid (half: its write side).
func (m *Mux) sendClose(sid string, half bool) {
	if m.c.SendClose(sid, half) == nil {
		_ = m.c.Flush()
	}
}

// Serve receives frames until the connection fails, then closes it and
// ends every session. accept, nil on a side that takes no sessions, runs
// in a goroutine of its own for each session the peer opens, with the
// header that opened it; it owns the session and closes it.
func (m *Mux) Serve(accept func(open []byte, s *Session)) error {
	defer m.close()
	for {
		f, err := m.c.next()
		switch {
		case err != nil:
			return err
		case len(f.Data) > 0:
			m.data(f)
		case f.sid == nil: // not an empty data frame
			m.serve(f.Frame, accept)
		}
	}
}

// data queues a data frame's payload on its session (within its credit:
// no wait), onto the newest queued buffer when that has room: a session
// whose reader is behind holds its bytes packed, not a buffer per frame
// (docs/performance.md, P3).
// A view of the read buffer is copied; data of a session this side does
// not know is dropped.
func (m *Mux) data(f rframe) {
	m.mu.Lock()
	s := m.sess[string(f.sid)]
	m.mu.Unlock()
	switch {
	case s == nil:
	case s.q.Append(f.Data):
	case f.view:
		b := newChunk(len(f.Data))
		copy(b, f.Data)
		if !s.q.Push(b) {
			releaseData(b)
		}
	default:
		if !s.q.Push(f.Data) {
			releaseData(f.Data)
		}
		s.kick()
		return // the queue owns f.Data
	}
	if !f.view {
		releaseData(f.Data)
	}
	if s != nil {
		s.kick()
	}
}

// serve applies a frame other than data. Messages have no meaning to a
// Mux.
func (m *Mux) serve(f Frame, accept func([]byte, *Session)) {
	switch {
	case f.Open:
		var s *Session
		// An OPEN that crossed this side's GOAWAY runs like any other;
		// one after the peer's GOAWAY, or after it read ours (Drained),
		// breaks the protocol.
		refusal := &Abort{Refused: true, Code: CodeRefused, Msg: "not taking sessions"}
		switch {
		case accept == nil:
		case closed(m.c.peerGoAway) || closed(m.c.drained):
			refusal.Code, refusal.Msg = CodeDraining, "draining"
		default:
			s, _ = m.add(f.SID, f.Header, f.Chain, true)
		}
		if s == nil { // refused: the opener reads a refusal or a reset, never a clean end
			var err error
			if m.c.chain() {
				err = m.c.sendAbort(f.SID, refusal)
			} else {
				err = m.c.SendReset(f.SID)
			}
			if err == nil {
				_ = m.c.Flush()
			}
			return
		}
		go accept(f.Header, s)
	case f.Accept:
		if s := m.get(f.SID); s != nil && s.answered != nil && !closed(s.answered) {
			s.pathLen = f.Chain.Hops
			close(s.answered)
		}
	case f.Close:
		s := m.get(f.SID)
		if s == nil {
			return
		}
		if !f.Half {
			switch {
			case f.Abort != nil:
				s.fail(f.Abort)
			case f.Reset:
				s.fail(ErrReset)
			}
			s.peerClosed.Store(true)
			m.drop(s)
			return
		}
		s.peerHalf.Store(true) // before the queue's end: a Pump's done (Join) reads it
		s.q.Close()            // reads end once the queue drains
		s.kick()
		if s.halfClosed.Load() {
			s.peerClosed.Store(true)
			m.drop(s) // both sides stopped sending
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
	err := m.connErr()
	for _, s := range sess {
		s.fail(err) // a session cut with its connection never reads a clean end
		s.peerClosed.Store(true)
		s.ended()
	}
}

// connErr is what a session cut with its connection reads: ErrConnClosed
// wrapping why (a drain's deadline, ErrDeadPeer, the network's error).
func (m *Mux) connErr() error {
	m.mu.Lock()
	cutting := m.cutting
	m.mu.Unlock()
	switch why := m.c.why(); {
	case cutting:
		return fmt.Errorf("%w: %w", ErrConnClosed, ErrDrainDeadline)
	case why == nil:
		return fmt.Errorf("%w; open a new session", ErrConnClosed)
	case errors.Is(why, io.EOF): // never a session's clean end
		return fmt.Errorf("%w: the peer closed it (%v); open a new session", ErrConnClosed, why)
	default:
		return fmt.Errorf("%w: %w; open a new session", ErrConnClosed, why)
	}
}

// Drain closes the connection gracefully: it tells the peer to open no
// more sessions here (GOAWAY) and stops this side opening more (Open:
// ErrGoAway); the open sessions run to their end (an OPEN the peer sent
// before it read the GOAWAY runs like the others), then the connection
// closes with every frame written. Drain returns once it closed, nil; at
// ctx's end it closes the connection at once and returns ctx's error.
// Serve must run (it is the receive loop).
func (m *Mux) Drain(ctx context.Context) error {
	deadline, _ := ctx.Deadline()
	if err := m.goAway(deadline); err != nil {
		_ = m.Close()
		return err
	}
	select {
	case <-m.c.Done():
		return lateErr(ctx)
	case <-ctx.Done():
		m.cut()
		return ctx.Err()
	}
}

// lateErr is a drain's result once its connections closed: nil, unless
// its deadline had come, when goAway's own cut closed them (racing ctx's
// timer): then context.DeadlineExceeded, as if ctx's end were seen first.
func lateErr(ctx context.Context) error {
	if d, ok := ctx.Deadline(); ok && !time.Now().Before(d) {
		return context.DeadlineExceeded
	}
	return nil
}

// cut ends a drain at its deadline: on a FeatureChain connection each
// session still open gets an ABORT (CodeDrainDeadline) first, and reads
// here end with ErrDrainDeadline; the connection then closes its write
// side once the ABORTs are written, and the rest at the peer's close
// (closeFlushed: a reset under frames the peer still sends would drop the
// ABORTs before the peer read them), within two seconds at most.
func (m *Mux) cut() {
	m.mu.Lock()
	if m.cutting { // the drain's timer and Drain's ctx: one cut
		m.mu.Unlock()
		return
	}
	m.cutting = true
	ids := make([]string, 0, len(m.sess))
	for id, s := range m.sess {
		ids = append(ids, id)
		s.fail(fmt.Errorf("%w: %w", ErrConnClosed, ErrDrainDeadline)) // before its ABORT ends its credit (writeErr)
	}
	m.mu.Unlock()
	if m.c.chain() && len(ids) > 0 {
		closed := make(chan struct{})
		go func() {
			defer close(closed)
			for _, id := range ids {
				if m.c.sendAbort(id, &Abort{Code: CodeDrainDeadline, Msg: "drain deadline"}) != nil {
					return
				}
			}
			m.c.closeFlushed(time.Second)
		}()
		t := time.NewTimer(2 * time.Second)
		select {
		case <-closed:
		case <-t.C:
		}
		t.Stop()
	}
	_ = m.Close()
}

// goAway is Drain without the wait: the connection closes once its last
// session ended and the peer read the GOAWAY, at deadline at the latest
// (zero: none).
func (m *Mux) goAway(deadline time.Time) error {
	var drain time.Duration
	if !deadline.IsZero() {
		drain = max(time.Until(deadline), time.Millisecond)
	}
	if err := m.c.GoAway(drain, ""); err != nil {
		return err
	}
	m.c.ping() // its pong: the peer read the GOAWAY (Drained)
	m.mu.Lock()
	m.leaving = true
	m.mu.Unlock()
	var t *time.Timer
	if drain > 0 {
		t = time.AfterFunc(drain, m.cut)
	}
	go func() {
		select {
		case <-m.c.drained:
			m.leave()
		case <-m.c.Done():
		}
		if t != nil {
			<-m.c.Done()
			t.Stop()
		}
	}()
	return nil
}

// Close closes the connection: Serve returns and every session ends.
func (m *Mux) Close() error { return m.c.Close() }

// closeIdle closes m: once its buffered frames are written when it carries
// no session (Close would cut the frames of sessions that ended), else at
// once.
func (m *Mux) closeIdle() {
	m.mu.Lock()
	idle, closing := len(m.sess) == 0, m.left
	m.left = true
	m.mu.Unlock()
	switch {
	case closing: // leave closes it
	case idle:
		go m.c.closeFlushed(m.c.live.Timeout)
	default:
		_ = m.Close()
	}
}

// Session is one session of a Mux. Read and WriteTo take the peer's bytes
// in order, then io.EOF once the peer closed the session (or its write
// side), or the error that cut it: ErrReset (the peer's Reset), or
// ErrConnClosed wrapping why the connection ended (ErrDeadPeer, the
// network's error). Write sends on credit; past such an end it returns
// io.ErrClosedPipe wrapping the same error. A read deadline ends a
// pending Read, and a write deadline a pending Write, with
// os.ErrDeadlineExceeded.
type Session struct {
	m      *Mux
	id     string
	header []byte
	q      *bytequeue.Queue
	rest   []byte // unread tail of the last slice taken from q
	cur    []byte // that slice, released once read

	closed     atomic.Bool // Close called, or the session ended
	peerClosed atomic.Bool // the peer closed it (or the connection ended): Close sends nothing
	halfClosed atomic.Bool // CloseWrite called
	peerHalf   atomic.Bool // the peer closed its write side
	once       sync.Once

	err     atomic.Pointer[error] // why the session was cut; nil: it ends with io.EOF
	endOnce sync.Once
	done    chan struct{} // closed once the session ended

	dlMu    sync.Mutex
	dlTimer *time.Timer
	dlGen   uint64      // the deadline's generation: a stale timer does nothing
	dlPast  atomic.Bool // the read deadline passed

	wdMu    sync.Mutex
	wdTimer *time.Timer
	wdGen   uint64        // the write deadline's generation: a stale timer does nothing
	wdCh    chan struct{} // closed when the write deadline passes or changes; under wdMu
	wd      writeDeadline

	pump    atomic.Pointer[pump] // set by Pump
	pumping atomic.Bool          // a pump goroutine runs, or the pump ended

	// Answers (FeatureChain connections).
	chain    Chain
	answers  bool          // the connection negotiated FeatureChain
	accepter bool          // the peer opened the session
	answered chan struct{} // the opener's, with answers: closed at the peer's ACCEPT
	pathLen  uint8         // the ACCEPT's hops, set before answered closes
	ans      atomic.Bool   // the accepter's: it answered (accept or refusal)
	ansOnce  sync.Once
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

// ErrConnClosed is what a session reads once its connection closed:
// the error wraps it and why (ErrDeadPeer, the network's error; the
// peer's close in its text only, never as io.EOF; nothing when this side
// closed it), and says to open a new session.
var ErrConnClosed = errors.New("link: connection closed")

// closedPipe is what a write past the session's end returns:
// io.ErrClosedPipe, wrapping why the session was cut when it was.
func (s *Session) closedPipe() error {
	if e := s.err.Load(); e != nil {
		return fmt.Errorf("%w: %w", io.ErrClosedPipe, *e)
	}
	return io.ErrClosedPipe
}

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

// Header is the bytes that opened the session: the OPEN frame's header.
func (s *Session) Header() []byte { return s.header }

// ended stops writes and lets reads drain the queue.
func (s *Session) ended() {
	s.closed.Store(true)
	s.q.Close()
	s.kick()
	s.endOnce.Do(func() { close(s.done) })
}

// Read reads the peer's bytes in order: io.EOF once the peer ended its
// writes, or the error that cut the session (see Session).
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

// buffersWriter writes a batch at once and keeps none of it (Session).
type buffersWriter interface {
	WriteBuffers(bufs [][]byte) (int64, error)
}

func (bw *batchWriter) write(w io.Writer, bufs [][]byte) (int64, error) {
	// net.Buffers clears the slices it wrote (writev): it gets a copy,
	// bufs keeps them for the release.
	var n int64
	var err error
	if b, ok := w.(buffersWriter); ok { // a Session: one flush for the batch
		n, err = b.WriteBuffers(bufs)
	} else {
		bw.wb = append(bw.wb[:0], bufs...)
		bw.nb = bw.wb
		n, err = bw.nb.WriteTo(w)
	}
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
// credit, then writes them to the network (Flush). With a write deadline
// set, the frames go to the network from a goroutine, and Write returns
// os.ErrDeadlineExceeded, with the bytes it sent, once the deadline passed
// while it waited for credit or for room in the connection's write
// buffer.
func (s *Session) Write(b []byte) (int, error) {
	s.autoAccept()
	inline := !s.wd.set.Load()
	n, err := s.send(b, inline)
	if err == nil && !inline {
		err = s.flush()
	}
	return n, s.writeErr(err)
}

// writeErr is what a write that failed with err returns: io.ErrClosedPipe
// wrapping why the session was cut (an *Abort, ErrReset, the
// connection's end), plain only when the session was closed (by its peer
// whole, or here). A splice (Join) passes the first on as an abort and
// the second as a clean close, so a write must not fail plain before
// the cause reached the session: the credit of a session ends (the
// write fails) when the peer's ABORT or CLOSE is read, a moment before
// the receive loop applies it, so the write waits for that; a network
// write that failed says so at once (the connection's end may be long
// coming).
func (s *Session) writeErr(err error) error {
	switch {
	case err == nil || errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, errSID):
		return err
	case s.closed.Load() || s.err.Load() != nil || s.halfClosed.Load():
	case closed(s.m.c.Done()) || s.m.c.writeFailed():
		return fmt.Errorf("%w: %w", io.ErrClosedPipe, s.m.connErr())
	default:
		<-s.done // the session's end is on its way: the receive loop's next step, or Close here
	}
	return s.closedPipe()
}

// WriteBuffers is Write of each of bufs in turn, with one flush at the
// end (and before any wait for credit): a backlog goes to the network in
// one write.
func (s *Session) WriteBuffers(bufs [][]byte) (int64, error) {
	s.autoAccept()
	var total int64
	for _, b := range bufs {
		n, err := s.send(b, false)
		total += int64(n)
		if err != nil {
			return total, s.writeErr(err)
		}
	}
	return total, s.writeErr(s.flush())
}

// send buffers b as data frames, MaxChunk at a time, each once it has
// credit; it returns how many bytes it buffered. With flush the last
// frame's lock also writes them (Flush): one lock per small Write
// (docs/performance.md, P5).
func (s *Session) send(b []byte, flush bool) (int, error) {
	if len(b) == 0 && flush {
		return 0, s.m.c.Flush()
	}
	for p := b; len(p) > 0; {
		n := min(len(p), MaxChunk)
		if err := s.acquire(n); err != nil {
			return len(b) - len(p), err
		}
		flushErr, err := s.m.c.sendData(s.id, p[:n], &s.wd, flush && n == len(p))
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				s.m.c.credit.refund(s.id, n) // nothing of it was sent
			}
			return len(b) - len(p), err
		}
		if p = p[n:]; flushErr != nil {
			return len(b), flushErr
		}
	}
	return len(b), nil
}

// acquire waits for n bytes of credit, until the write deadline.
func (s *Session) acquire(n int) error {
	for {
		stop := s.writeWait() // before the check: a deadline passing after it closes stop
		if s.closed.Load() || s.halfClosed.Load() {
			return s.closedPipe()
		}
		if s.wd.past.Load() {
			return os.ErrDeadlineExceeded
		}
		ok, stopped := s.m.c.credit.acquire(s.id, n, s.flush, stop)
		if ok {
			return nil
		}
		if !stopped {
			return s.closedPipe()
		}
	}
}

// flush writes the buffered frames: from a goroutine while a write
// deadline is set, so that no write waits on the network.
func (s *Session) flush() error {
	if s.wd.set.Load() {
		return s.m.c.flushAsync()
	}
	return s.m.c.Flush()
}

// writeWait is closed once the write deadline passes or changes (nil while
// none was ever set).
func (s *Session) writeWait() <-chan struct{} {
	s.wdMu.Lock()
	defer s.wdMu.Unlock()
	return s.wdCh
}

// CloseWrite ends the session's write side: the peer reads
// io.EOF after what was written; this side still reads the peer's bytes.
// The session ends once both sides closed their write side.
func (s *Session) CloseWrite() error {
	s.autoAccept()
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
	s.autoAccept()
	s.once.Do(func() {
		if !s.peerClosed.Load() {
			s.m.sendClose(s.id, false) // before drop: a leaving connection closes after it
		}
		s.m.drop(s)
	})
	return nil
}

// Reset ends the session as failed: the peer reads ErrReset (a peer of
// link v0.3.0 or later, which reads CLOSE code 2; an older one reads a
// close).
// Reads here end as after Close.
func (s *Session) Reset() error {
	s.once.Do(func() {
		defer s.m.drop(s) // after the reset: a leaving connection closes after it
		if s.peerClosed.Load() {
			return
		}
		if s.m.c.SendReset(s.id) == nil {
			_ = s.m.c.Flush()
		}
	})
	return nil
}

// LocalAddr is the connection's.
func (s *Session) LocalAddr() net.Addr { return s.m.c.nc.LocalAddr() }

// RemoteAddr is the connection's.
func (s *Session) RemoteAddr() net.Addr { return s.m.c.nc.RemoteAddr() }

// SetDeadline sets the read and the write deadline.
func (s *Session) SetDeadline(t time.Time) error {
	_ = s.SetReadDeadline(t)
	return s.SetWriteDeadline(t)
}

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

// SetWriteDeadline makes a pending or later Write (WriteBuffers) return
// os.ErrDeadlineExceeded once t passed; zero t clears it. While one is
// set, writes leave the network write to a goroutine (see Write).
func (s *Session) SetWriteDeadline(t time.Time) error {
	s.wdMu.Lock()
	s.wdGen++
	if s.wdTimer != nil {
		s.wdTimer.Stop()
		s.wdTimer = nil
	}
	if s.wdCh != nil {
		close(s.wdCh) // a pending wait looks at the new deadline
	}
	s.wdCh = make(chan struct{})
	d := time.Until(t)
	past := !t.IsZero() && d <= 0
	s.wd.set.Store(!t.IsZero())
	s.wd.past.Store(past)
	if !t.IsZero() && d > 0 {
		gen := s.wdGen
		s.wdTimer = time.AfterFunc(d, func() {
			s.wdMu.Lock()
			if s.wdGen == gen {
				s.wd.past.Store(true)
				close(s.wdCh)
				s.wdCh = make(chan struct{})
			}
			s.wdMu.Unlock()
			s.m.c.wakeWriters()
		})
	}
	s.wdMu.Unlock()
	if past {
		s.m.c.wakeWriters()
	}
	return nil
}
