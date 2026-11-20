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

// DefaultMaxSessions is Config.DefaultMaxSessions's default: the most sessions the
// peer may have open toward this side at once. Open refuses more toward
// the peer than its hello allows (ErrFull); a peer that opens more than
// this side's ends the connection (ErrFull).
const DefaultMaxSessions = 1024

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
	acks map[string]*ackWait // half closes awaiting their ENDACK (FeatureEndAck)
	done bool                // Serve returned: no more sessions
	// leaving: this side sent GOAWAY; the connection closes once its last
	// session ended and the peer read the GOAWAY (Conn.Drained).
	leaving  bool
	left     bool          // the close began
	cutting  bool          // a drain's deadline cut the sessions
	cutEnded chan struct{} // closed once that cut ended (its ABORTs sent, the connection closed)
	// refusals of the peer's opens, sent by a goroutine while refusing
	// (refused: closed when it ends); shut: the write side shuts, so no
	// more refusals go out (shutWrite).
	refusals []refusal
	refusing bool
	refused  chan struct{}
	shut     bool
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
// ctx carries the session's path (ContextWithChain: Forward's Route opens
// the next leg with the ctx it was given); without one the session starts
// a new path. A zero Trace is a new path, for which link picks a random
// one. On a connection that negotiated FeatureChain the open is a
// CHAIN-OPEN and the peer answers it (Answered); on one without, the
// Chain is kept locally only and the session does not answer (Answers is
// false). Open does not wait, so ctx's deadline does not bound it; a ctx
// already done fails it with ctx's error.
func (m *Mux) Open(ctx context.Context, header []byte) (*Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c, _ := ChainFromContext(ctx)
	return m.openChain(header, c)
}

// openChain is Open of a session on the path c.
func (m *Mux) openChain(header []byte, c Chain) (*Session, error) {
	if m.c.isGoingAway() {
		return nil, ErrGoAway
	}
	if c.Trace == (Trace{}) && m.c.chain() {
		_, _ = rand.Read(c.Trace[:])
	}
	sid := m.nextSID()
	s, err := m.add(sid, header, c, false)
	for err == errSIDInUse { // the peer took it: a Conn picks its own ids (WIRE.md section 5)
		sid = m.nextSID()
		s, err = m.add(sid, header, c, false)
	}
	if err != nil {
		return nil, err
	}
	err = m.c.sendOpen(sid, header, c)
	if err == nil {
		err = m.c.flush()
	}
	if err != nil {
		s.peerClosed.Store(true) // the peer never heard of it
		if debugOn {
			debugReleased(s) // the caller never gets it to close
		}
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
	if m.sess[sid] != nil { // the Conn refuses a peer's open of an id in use
		return nil, errSIDInUse
	}
	// The cut took its snapshot under m.mu: a peer's OPEN read after it is
	// refused, as HTTP/2's REFUSED_STREAM (nothing ran: the opener opens
	// elsewhere), never left out of the cut's ABORTs to read the
	// connection's end (spec/Link.tla OpensAnswered).
	if accepter && m.cutting {
		return nil, errCut
	}
	s := &Session{m: m, id: sid, q: bytequeue.New(m.c.credit.window), header: header, done: make(chan struct{}),
		chain: c, answers: m.c.chain(), accepter: accepter}
	if s.answers && !accepter {
		s.answered = make(chan struct{})
	}
	s.q.OnPop(func(n int) { m.c.credit.Consumed(sid, n) })
	if traceOn {
		traceReadUp(s)
		traceWriteUp(s)
	}
	m.sess[sid] = s
	m.used.Store(true)
	if debugOn {
		debugOpened(s)
	}
	return s, nil
}

// drop forgets s: its writes and pending credit waits fail, its reads
// return what is queued, then io.EOF.
func (m *Mux) drop(s *Session) {
	m.mu.Lock()
	if m.sess[s.id] == s {
		delete(m.sess, s.id)
		m.c.credit.Forget(s.id)
		if traceOn {
			traceEv(m.c.credit.tr, traceEvent{ev: "Drop", sid: s.id})
		}
	}
	m.mu.Unlock()
	s.ended()
	m.leave()
}

// leave closes a leaving connection once it carries no session and the
// peer read the GOAWAY, after the frames buffered for it went out.
func (m *Mux) leave() {
	m.mu.Lock()
	last := m.leaving && !m.left && len(m.sess) == 0 && len(m.acks) == 0 && closed(m.c.drained)
	if last {
		// a Sync's pong still to come (CloseWriteSync's: the session ended
		// with its CLOSE): the peer's close would lose it (spec/Join.tla
		// Leave, DrainNeverResets)
		if synced := m.c.syncPending(); synced != nil {
			m.mu.Unlock()
			go func() {
				select {
				case <-synced:
					m.leave()
				case <-m.c.Done():
				}
			}()
			return
		}
	}
	m.left = m.left || last
	m.mu.Unlock()
	if last {
		go m.closeFlushed(m.c.live.Timeout)
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

// sendClose sends the frame that closes sid (half: its write side): the
// error is the frame's or its flush's.
func (m *Mux) sendClose(sid string, half bool) error {
	err := m.c.sendCloseFrame(sid, half)
	if err == nil {
		err = m.c.flush()
	}
	return err
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
			m.serve(f.rxFrame, accept)
		}
	}
}

// data queues a data frame's payload on its session (within its credit:
// no wait), onto the newest queued buffer when that has room: a session
// whose reader is behind holds its bytes packed, not a buffer per frame
// (docs/performance.md, P3).
// A view of the read buffer is copied; data of a session this side does
// not know, or keeps no credit for, is dropped.
func (m *Mux) data(f rframe) {
	m.mu.Lock()
	s := m.sess[string(f.sid)]
	m.mu.Unlock()
	switch {
	case s == nil || !f.known: // no credit: past this side's CLOSE or ABORT (WIRE.md section 5)
		s = nil
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
func (m *Mux) serve(f rxFrame, accept func([]byte, *Session)) {
	switch {
	case f.Open:
		var s *Session
		// An OPEN that crossed this side's GOAWAY runs like any other,
		// unless the drain deadline's cut began (add: errCut); one after
		// the peer's GOAWAY, or after it read ours (Drained), breaks the
		// protocol.
		refusal := &Abort{Refused: true, Code: CodeRefused, Msg: "not taking sessions"}
		switch {
		case accept == nil:
		case closed(m.c.peerGoAway) || closed(m.c.drained):
			refusal.Code, refusal.Msg = CodeDraining, "draining"
		default:
			var err error
			if s, err = m.add(f.SID, f.Header, f.Chain, true); err == errCut {
				refusal.Code, refusal.Msg = CodeDraining, "draining"
			}
		}
		if s == nil { // refused: the opener reads a refusal or a reset, never a clean end
			m.refuse(f.SID, refusal)
			return
		}
		go accept(f.Header, s)
	case f.Accept:
		if s := m.get(f.SID); s != nil && s.answered != nil && !closed(s.answered) {
			s.pathLen = f.Chain.Hops
			close(s.answered)
		}
	case f.EndAck:
		m.ackDone(f.SID, nil)
	case f.Close:
		s := m.get(f.SID)
		if s == nil {
			if !f.Half {
				m.ackDone(f.SID, closeAckErr(f))
			}
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
			// The peer takes no more: an end it had not acknowledged never
			// will be. After peerClosed: a closeWrite registering its wait
			// now sees it (closeWrite).
			m.ackDone(f.SID, closeAckErr(f))
			m.drop(s)
			return
		}
		s.peerHalf.Store(true) // before the queue's end: a Pump's done (Join) reads it
		s.q.Close()            // reads end once the queue drains
		s.kick()
		if !m.c.hop && m.c.endAcks() { // a far end: received is taken (a goroutine: the receive loop never writes)
			go s.AckEnd()
		}
		if s.finSent.Load() { // not halfClosed: its CLOSE may not be buffered yet (review r2 LinkSession finding 1)
			s.peerClosed.Store(true)
			m.drop(s) // both sides stopped sending
		}
	}
}

// refuse queues the refusal of the peer's open of sid; a goroutine sends
// it, so the receive loop never waits on a write (review F7: two sides
// refusing while both directions are full read nothing more, and each
// took the other for dead). The queue is bounded by this side's max
// sessions: a refused session keeps its credit, and counts, until its
// refusal is buffered, and an open past the max ends the connection.
func (m *Mux) refuse(sid string, a *Abort) {
	m.mu.Lock()
	if m.shut { // read after the write side's shut: no answer can go out
		m.mu.Unlock()
		return
	}
	m.refusals = append(m.refusals, refusal{sid, a})
	start := !m.refusing
	if start {
		m.refused = make(chan struct{})
	}
	m.refusing = true
	m.mu.Unlock()
	if start {
		go m.sendRefusals()
	}
}

type refusal struct {
	sid string
	a   *Abort
}

// sendRefusals sends the queued refusals, then ends.
func (m *Mux) sendRefusals() {
	for {
		m.mu.Lock()
		if len(m.refusals) == 0 {
			m.refusals, m.refusing = nil, false
			close(m.refused)
			m.mu.Unlock()
			return
		}
		r := m.refusals[0]
		m.refusals = m.refusals[1:]
		m.mu.Unlock()
		var err error
		if m.c.chain() {
			err = m.c.sendAbort(r.sid, r.a)
		} else {
			err = m.c.sendResetFrame(r.sid)
		}
		if err == nil {
			_ = m.c.flush()
		}
	}
}

// closeFlushed is Conn.closeFlushed once the refusals queued are
// buffered (shutWrite).
func (m *Mux) closeFlushed(wait time.Duration) {
	m.shutWrite()
	m.c.closeFlushed(wait)
}

// shutWrite returns once every refusal queued is buffered, and queues no
// more: the write side shuts next, so a peer's OPEN read before it is
// answered, never left to the connection's end (spec/Link.tla
// OpensAnswered, mutant shut-before-refusal).
func (m *Mux) shutWrite() {
	for {
		m.mu.Lock()
		if !m.refusing {
			m.shut = true
			m.mu.Unlock()
			return
		}
		done := m.refused
		m.mu.Unlock()
		<-done
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
	if traceOn {
		traceEv(m.c.credit.tr, traceEvent{ev: "Down"})
	}
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
// side once the ABORTs and every frame buffered before are written (with
// no session open too: an ended session's CLOSE), and the rest at the peer's close
// (closeFlushed: a reset under frames the peer still sends would drop the
// ABORTs before the peer read them), within two seconds at most.
func (m *Mux) cut() {
	m.mu.Lock()
	if m.cutting { // the drain's timer and Drain's ctx: one cut, which the other waits for
		done := m.cutEnded
		if traceOn {
			traceEv(m.c.credit.tr, traceEvent{ev: "CutWait"})
		}
		m.mu.Unlock()
		<-done // Drain returns once the peer can have read the ABORTs (TestDrainReturnsAfterTheCutItRaced)
		if traceOn {
			traceEv(m.c.credit.tr, traceEvent{ev: "CutRet"})
		}
		return
	}
	m.cutting = true
	m.cutEnded = make(chan struct{})
	defer close(m.cutEnded)
	ids := make([]string, 0, len(m.sess))
	for id, s := range m.sess {
		ids = append(ids, id)
		s.fail(fmt.Errorf("%w: %w", ErrConnClosed, ErrDrainDeadline)) // before its ABORT ends its credit (writeErr)
	}
	if traceOn {
		traceEv(m.c.credit.tr, traceEvent{ev: "Cut", n: len(ids)})
	}
	m.mu.Unlock()
	if m.c.chain() { // with no ABORT to send too: what is buffered goes out first (TestDrainDeadlineKeepsAnEndedSessionsClose)
		closed := make(chan struct{})
		go func() {
			defer close(closed)
			for _, id := range ids {
				if m.c.sendAbort(id, cutAbort) != nil {
					return
				}
			}
			m.closeFlushed(time.Second)
		}()
		t := time.NewTimer(2 * time.Second)
		select {
		case <-closed:
		case <-t.C:
		}
		t.Stop()
	}
	_ = m.Close()
	if traceOn { // before the defer closes cutEnded: CutEnd, then each call's CutRet
		traceEv(m.c.credit.tr, traceEvent{ev: "CutEnd"})
		traceEv(m.c.credit.tr, traceEvent{ev: "CutRet"})
	}
}

// errCut is Mux.add's refusal of a peer's open once the cut began.
var errCut = errors.New("link: drain deadline cut")

// cutAbort is the ABORT Mux.cut sends each session at a drain's deadline.
var cutAbort = &Abort{Code: CodeDrainDeadline, Msg: "drain deadline"}

// goAway is Drain without the wait: the connection closes once its last
// session ended and the peer read the GOAWAY, at deadline at the latest
// (zero: none).
func (m *Mux) goAway(deadline time.Time) error {
	var drain time.Duration
	if !deadline.IsZero() {
		drain = max(time.Until(deadline), time.Millisecond)
	}
	if err := m.c.goAway(drain, ""); err != nil {
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
		go m.closeFlushed(m.c.live.Timeout)
	default:
		_ = m.Close()
	}
}

// Session is one session of a Mux. Read and WriteTo take the peer's bytes
// in order, then io.EOF once the peer closed the session (or its write
// side), or the error that cut it: ErrReset (the peer's Reset), an
// *Abort (the peer's, or this side's own Reset or Abort: never io.EOF), or
// ErrConnClosed wrapping why the connection ended (ErrDeadPeer, the
// network's error). Write sends on credit; past such an end it returns
// io.ErrClosedPipe wrapping the same error. A read deadline ends a
// pending Read, and a write deadline a pending Write, with
// os.ErrDeadlineExceeded. Several goroutines may Read at once (as
// net.Conn): each byte goes to one of them. WriteTo takes the session's
// bytes for itself; a Read beside it is safe too (each byte to one of
// them, once), but their bytes interleave. Pump takes the session's reads:
// a Read once Pump was called returns an error and takes nothing.
type Session struct {
	m      *Mux
	dbg    debugState // linkdebug builds only (debug_on.go); zero size otherwise
	rt     readTrace  // linktrace builds only (trace_read_on.go); zero size otherwise
	wtr    writeTrace // linktrace builds only (trace_write_on.go); zero size otherwise
	id     string
	header []byte
	q      *bytequeue.Queue
	rmu    sync.Mutex // Read's: one reader at a time on rest and cur
	rest   []byte     // unread tail of the last slice taken from q
	cur    []byte     // that slice, released once read

	closed     atomic.Bool             // Close called, or the session ended
	peerClosed atomic.Bool             // the peer closed it (or the connection ended): Close sends nothing
	halfClosed atomic.Bool             // CloseWrite called
	heldAck    atomic.Bool             // HoldEndAck: a join sends the ENDACK (AckEnd)
	ackMu      sync.Mutex              // the ENDACK goes once, before any close after it
	acked      bool                    // the ENDACK went (ackMu)
	ack        atomic.Pointer[ackWait] // this side's half close's wait for the peer's ENDACK
	peerHalf   atomic.Bool             // the peer closed its write side
	finSent    atomic.Bool             // CloseWrite buffered its CLOSE
	once       sync.Once

	err     atomic.Pointer[error] // why the session was cut; nil: it ends with io.EOF
	finRead atomic.Bool           // the peer's FIN was read before the cut: reads still end with io.EOF
	endOnce sync.Once
	done    chan struct{} // closed once the session ended

	dlMu    sync.Mutex
	dlTimer *time.Timer
	dlGen   uint64      // the deadline's generation: a stale timer does nothing
	dlPast  atomic.Bool // the read deadline passed

	wdMu    sync.Mutex
	wdTimer *time.Timer
	wdGen   uint64        // the write deadline's generation: a stale timer does nothing
	wdCh    chan struct{} // closed when the write deadline passes or changes; under wdMu, made by the first writeWait or SetWriteDeadline
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

// errPumped is what a Read returns once Pump was called: the pump's writer
// takes the session's bytes, in order. Unexported: Pump's one caller is
// peerlink (docs/v1-api-review.md #6).
var errPumped = errors.New("link: session read by its Pump")

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

// fail records why the session was cut (the first reason wins). A cut
// after the peer's FIN leaves its reads a clean end: the peer's stream was
// complete (spec/LinkSession.tla GEnds, "a FIN then a reset reads
// io.EOF"; spec/RelayedClose.tla ReadsEof, mutant fail-drops-fin). finRead
// is stored before the cut, so a reader that sees the cut sees it.
func (s *Session) fail(err error) {
	if s.err.Load() == nil && s.peerHalf.Load() {
		s.finRead.Store(true)
	}
	s.err.CompareAndSwap(nil, &err)
}

// end is what Read returns once the queue is drained: io.EOF after the
// peer's FIN, even when the session was cut since; else the cut.
func (s *Session) end() error {
	if e := s.err.Load(); e != nil && !s.finRead.Load() {
		return *e
	}
	if s.peerHalf.Load() {
		s.readEnd()
	}
	return io.EOF
}

// Done is closed once the session ended: both sides closed it, the peer
// reset it, or its connection closed. Bytes already received stay
// readable.
func (s *Session) Done() <-chan struct{} { return s.done }

// Err is why the session ended, once Done is closed: nil for a clean end
// (both sides closed it), else ErrReset (the peer's Reset), the peer's
// *Abort (FeatureChain), this side's own Reset or Abort (an *Abort), or
// ErrConnClosed wrapping why the connection ended. A layer that joins the session to another conn watches it with
// Done and Err (peerlink.Join: an idle client whose connection died).
func (s *Session) Err() error {
	if e := s.err.Load(); e != nil {
		return *e
	}
	return nil
}

// OnGrant sets f to be called with each window update the peer sends for
// the session: n more bytes of what this side wrote left the peer's
// receive buffer (its reader took them). The peer sends one per at least a
// quarter of its window read, so f is how a writer sees its bytes still
// moving after its last Write returned. f runs on the connection's receive
// loop: it must not block. A later call replaces f; nil removes it.
func (s *Session) OnGrant(f func(n int)) { s.m.c.credit.setOnGrant(s.id, f) }

// Header is the bytes that opened the session: the OPEN frame's header.
func (s *Session) Header() []byte { return s.header }

// ended stops writes and lets reads drain the queue; its deadline
// timers stop (a pending one held the session until it fired).
func (s *Session) ended() {
	s.closed.Store(true)
	s.q.Close()
	s.kick()
	s.endOnce.Do(func() { close(s.done) })
	s.dlMu.Lock()
	if s.dlTimer != nil {
		s.dlTimer.Stop()
		s.dlTimer = nil
	}
	s.dlMu.Unlock()
	s.wdMu.Lock()
	if s.wdTimer != nil {
		s.wdTimer.Stop()
		s.wdTimer = nil
	}
	s.wdMu.Unlock()
}

// Read reads the peer's bytes in order: io.EOF once the peer ended its
// writes, or the error that cut the session (see Session).
func (s *Session) Read(b []byte) (n int, err error) {
	if debugOn {
		defer debugEnter(s, debugRead)()
	}
	s.rmu.Lock()
	defer s.rmu.Unlock()
	// Under rmu: a Read that began before Pump ends before the drain takes
	// its tail (takeRest); one after would leave a tail the drain's next
	// run wrote after newer bytes (spec/SessionRead.tla Refuse,
	// read-beside-pump).
	if s.pumpedRead() {
		return 0, errPumped
	}
	if traceOn { // spec/TraceSessionRead.tla: under rmu (Begin: pumpedRead)
		defer func() { s.traceReadEnd(err) }()
	}
	if len(s.rest) == 0 {
		data, ok, stopped := s.q.PopUnless(&s.dlPast)
		if !ok {
			if stopped {
				return 0, os.ErrDeadlineExceeded
			}
			return 0, s.end()
		}
		s.rest, s.cur = data, data
	}
	n = copy(b, s.rest)
	if s.rest = s.rest[n:]; len(s.rest) == 0 {
		releaseData(s.cur)
		s.cur = nil
	}
	if traceOn {
		s.traceRead("Copy", false, n, len(s.rest) == 0)
	}
	return n, nil
}

// OwnedWriter is a writer that may keep the slices it is given: WriteTo
// hands it the received slices themselves, no copy.
type OwnedWriter interface {
	// WriteOwned takes b, which link no longer reads or writes: the
	// writer may keep it past the call. An error ends WriteTo with it.
	WriteOwned(b []byte) error
}

// WriteTo writes the peer's bytes to w until the session ends; a backlog
// goes in one net.Buffers write (writev on a TCP conn), or slice by slice
// to an OwnedWriter.
func (s *Session) WriteTo(w io.Writer) (int64, error) {
	if debugOn {
		defer debugEnter(s, debugRead)()
	}
	if ow, ok := w.(OwnedWriter); ok {
		return s.writeOwned(ow)
	}
	var total int64
	if rest, cur := s.takeRest(); len(rest) > 0 {
		n, err := w.Write(rest)
		total += int64(n)
		releaseData(cur)
		if traceOn {
			s.traceRead("WWrite", true, n, false)
			s.traceRead("WRel", true, 0, false)
		}
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
		if traceOn {
			s.traceRead("WWrite", true, int(n), false)
			s.traceRead("WRel", true, 0, false)
		}
		if err != nil {
			return total, err
		}
	}
}

// takeRest takes the unread tail of a Read (s.rest) and its slice
// (s.cur) for WriteTo or Pump, which own them from here: under rmu, so a
// Read beside them (misuse, but nothing stops it) neither hands the same
// bytes out again nor reads a slice they released (spec/SessionRead.tla,
// writeto-no-lock).
func (s *Session) takeRest() (rest, cur []byte) {
	s.rmu.Lock()
	rest, cur = s.rest, s.cur
	s.rest, s.cur = nil, nil
	if traceOn {
		s.traceRead("WTake", true, len(rest), false)
	}
	s.rmu.Unlock()
	return rest, cur
}

// batchWriter writes batches of received slices in one net.Buffers write
// each, then releases them (a Writer keeps none of p). Its scratch is
// reused: net.Buffers.WriteTo moves its receiver to the heap.
type batchWriter struct {
	wb [][]byte
	nb net.Buffers
}

// buffersWriter writes a batch at once and keeps none of it: a writer onto
// a Session (joinWriter, peerlink's connWriter).
type buffersWriter interface {
	WriteBuffers(bufs [][]byte) (int64, error)
}

func (bw *batchWriter) write(w io.Writer, bufs [][]byte) (int64, error) {
	// net.Buffers clears the slices it wrote (writev): it gets a copy,
	// bufs keeps them for the release.
	var n int64
	var err error
	if s, ok := w.(*Session); ok { // one flush for the batch
		n, err = s.writeBuffers(bufs)
	} else if b, ok := w.(buffersWriter); ok { // a Session's writer (join, peerlink)
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

// startPump writes the peer's bytes to w as WriteTo does, without a goroutine
// waiting while the session is idle: one runs only while bytes are
// queued, started as they arrive (the receive loop never waits for it).
// done gets what WriteTo would return, once, from that goroutine: the
// session ended, or w failed (the session's further bytes stay unread:
// close it). startPump takes over the session's reads: a Read after it
// returns an error (the tail of a Read before it goes to w first); no
// WriteTo or second startPump after it.
func (s *Session) startPump(w io.Writer, done func(n int64, err error)) {
	s.setPump(&pump{w: w, done: done})
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
	if traceOn {
		s.traceDrain()
	}
	p := s.pump.Load()
	ow, owned := p.w.(OwnedWriter)
	var err error
	if b, _ := s.takeRest(); len(b) > 0 { // the tail of a Read before Pump
		err = writeOwnedOrCopy(p.w, ow, owned, b)
		if traceOn {
			s.traceRead("WWrite", true, len(b), false)
			s.traceRead("WRel", true, 0, false)
		}
		if err != nil {
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
		taken := 0 // the trace's WWrite: the bytes handed to w
		if traceOn {
			for _, b := range bufs {
				taken += len(b)
			}
		}
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
		if traceOn {
			s.traceRead("WWrite", true, taken, false)
			s.traceRead("WRel", true, 0, false)
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
	b, _ := s.takeRest() // owned by w from here
	for {
		if len(b) == 0 {
			var ok bool
			if b, ok = s.q.Pop(); !ok {
				return total, s.endErr()
			}
		}
		if err := w.WriteOwned(b); err != nil {
			return total, err
		}
		if traceOn {
			s.traceRead("WWrite", true, len(b), false)
			s.traceRead("WRel", true, 0, false)
		}
		total += int64(len(b))
		b = nil
	}
}

// Write sends b to the peer in chunks it has credit for, waiting for
// credit, then writes them to the network (Flush). With a write deadline
// set, the frames go to the network from a goroutine, and Write returns
// os.ErrDeadlineExceeded, with the bytes it sent, once the deadline passed
// while it waited for credit or for room in the connection's write
// buffer.
func (s *Session) Write(b []byte) (int, error) {
	if debugOn {
		defer debugEnter(s, debugWrite)()
	}
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

// writeBuffers is Write of each of bufs in turn, with one flush at the
// end (and before any wait for credit): a backlog goes to the network in
// one write.
func (s *Session) writeBuffers(bufs [][]byte) (int64, error) {
	if debugOn {
		defer debugEnter(s, debugWrite)()
	}
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
		return 0, s.m.c.flush()
	}
	for p := b; len(p) > 0; {
		n := min(len(p), MaxChunk)
		if err := s.acquire(n); err != nil {
			return len(b) - len(p), err
		}
		flushErr, err := s.m.c.sendData(s.id, p[:n], &s.wd, &s.halfClosed, flush && n == len(p))
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) || err == io.ErrClosedPipe {
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
	var ws writeStep // spec/TraceSessionWrite.tla's writer
	if traceOn {
		ws = s.writeStepUp()
	}
	for {
		stop := s.writeWait(ws) // before the check: a deadline passing after it closes stop
		if s.closed.Load() || s.halfClosed.Load() {
			return s.closedPipe()
		}
		if s.writePast(ws) {
			return os.ErrDeadlineExceeded
		}
		ok, stopped := s.m.c.credit.acquire(s.id, n, s.flush, stop, true, ws)
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
	return s.m.c.flush()
}

// writeWait is closed once the write deadline passes or changes. It is
// made here when none was set yet, so that the first SetWriteDeadline
// wakes a Write already waiting for credit (spec/SessionWrite.tla,
// TestSessionWriteWaitingForCreditSeesFirstDeadline).
func (s *Session) writeWait(ws writeStep) <-chan struct{} {
	s.wdMu.Lock()
	defer s.wdMu.Unlock()
	if s.wdCh == nil {
		s.wdCh = make(chan struct{})
		if traceOn {
			s.writeNewCh()
		}
	}
	if traceOn {
		s.traceTake(ws)
	}
	return s.wdCh
}

// CloseWrite ends the session's write side: the peer reads
// io.EOF after what was written; this side still reads the peer's bytes.
// The session ends once both sides closed their write side. It returns
// io.ErrClosedPipe wrapping the connection's end when its CLOSE could not
// be written (the connection's writes failed or it closed), as Write does
// for its bytes: the peer then never reads this side's io.EOF. nil says
// the CLOSE was buffered, not that it arrived (Sync); a session that
// already ended, or a second CloseWrite, returns nil.
func (s *Session) CloseWrite() error {
	_, err := s.closeWrite(false)
	return err
}

// closeWriteSync is CloseWrite, then Sync, with Sync's ping buffered with
// the CLOSE under one write lock, so the Sync is pending before the peer
// can read the CLOSE: a draining connection closes once its sessions
// ended (Mux.leave), but not before that pong was read, whereas a ping
// buffered after the close would never be sent, failing a Sync whose
// CLOSE the peer read (spec/Join.tla DrainNeverResets, lane LINKDRAIN).
// Once CloseWrite was called (or the session ended) it is Sync.
func (s *Session) closeWriteSync() error {
	if s.m.c.endAcks() { // the far end's ENDACK, not the next hop's pong
		if _, err := s.closeWrite(false); err != nil {
			return err
		}
		if w := s.ack.Load(); w != nil {
			return s.waitAck(w)
		}
		return s.sync()
	}
	synced, err := s.closeWrite(true)
	switch {
	case err != nil:
		return err
	case synced == nil:
		return s.sync()
	}
	return waitSync(s.m.c, synced)
}

// closeWrite is CloseWrite; with sync, the returned channel is closed
// once the pong of a ping buffered right after the CLOSE was read (nil
// when nothing was sent).
func (s *Session) closeWrite(sync bool) (<-chan struct{}, error) {
	if debugOn {
		debugReleased(s)
	}
	s.autoAccept()
	if s.closed.Load() || s.halfClosed.Swap(true) {
		return nil, nil
	}
	if s.m.c.endAcks() { // before the CLOSE: its ENDACK is never missed
		s.ack.Store(s.m.expectAck(s.id))
		if s.peerClosed.Load() { // the peer closed whole already: no ENDACK comes
			s.m.ackDone(s.id, nil)
		} else if e := s.err.Load(); e != nil { // failed (a Reset racing this one: its ackDone ran before this wait existed): none comes
			s.m.ackDone(s.id, *e)
		}
	}
	var synced <-chan struct{}
	var err error
	if sync { // the ping with the CLOSE: pending before the peer can answer it
		if synced, err = s.m.c.sendCloseSync(s.id); err == nil {
			err = s.m.c.flush()
		}
	} else {
		err = s.m.sendClose(s.id, true)
	}
	if h := testHookCloseWriteSent.Load(); h != nil {
		(*h)()
	}
	// Dekker with the receive of the peer's FIN (peerHalf, then finSent):
	// one of the two sees both and drops it, after the CLOSE is buffered.
	s.finSent.Store(true)
	if s.peerHalf.Load() {
		s.peerClosed.Store(true) // both halves closed: nothing more to send
		s.m.drop(s)
	}
	if err != nil { // review F9 (lane LINKF7): the code before returned nil
		return nil, fmt.Errorf("%w: %w", io.ErrClosedPipe, s.m.connErr())
	}
	return synced, nil
}

// testHookCloseWriteSent, set by a test, runs in closeWrite once its CLOSE
// is buffered, before the session can be dropped.
var testHookCloseWriteSent atomic.Pointer[func()]

// sync waits until the peer's Mux read every frame this side buffered
// on the session's connection before the call (its bytes, its CloseWrite):
// a ping buffered after them was answered (WIRE.md section 8). It returns
// the connection's error once the connection ended first. The connection's
// liveness bounds the wait. peerlink.Join syncs after a CloseWrite before
// it ends the other side cleanly (spec/Join.tla).
func (s *Session) sync() error {
	c := s.m.c
	select {
	case <-c.Done():
		return syncEnded(c)
	default:
	}
	return waitSync(c, c.sync())
}

// waitSync waits for synced (Conn.sync): nil once its pong was read, even
// when the connection ended since; else the connection's error.
func waitSync(c *Conn, synced <-chan struct{}) error {
	select {
	case <-synced:
		return nil
	case <-c.Done():
		select {
		case <-synced: // the pong came before the end
			return nil
		default:
		}
		return syncEnded(c)
	}
}

// syncEnded is a Sync's error once the connection ended first.
func syncEnded(c *Conn) error {
	if err := c.endErr(); err != nil {
		return err
	}
	return ErrConnClosed
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
	if debugOn {
		debugReleased(s)
	}
	s.autoAccept()
	s.once.Do(func() {
		if s.peerHalf.Load() { // before the close: the peer reads the ENDACK first
			s.ackAtClose()
		}
		if !s.peerClosed.Load() {
			_ = s.m.sendClose(s.id, false) // before drop: a leaving connection closes after it
		} else if s.m.c.hop {
			// both ends' halves passed, so no CLOSE goes: a hop's clean
			// close of a session whose peer's end its join never passed on
			// (the onward peer closed whole) still ends the peer's wait,
			// as a whole close would (closeAckErr; relay-core
			// TestConnIsANetConn hung on the agent's Close without)
			s.AckEnd()
		}
		s.m.ackDone(s.id, io.ErrClosedPipe)
		s.m.drop(s)
	})
	return nil
}

// Reset ends the session as failed: the peer reads ErrReset (a peer of
// link v0.3.0 or later, which reads CLOSE code 2; an older one reads a
// close).
// Reads and writes here end with an *Abort (errors.Is ErrReset), never a
// clean end.
func (s *Session) Reset() error {
	if debugOn {
		debugReleased(s)
	}
	if s.nack() { // ended here, its end not acknowledged: the peer's wait ends
		s.m.ackDone(s.id, ErrReset) // and this side's own (spec/RelayedClose.tla AResetWaiting)
		return nil
	}
	s.once.Do(func() {
		s.endHere(nil)
		defer s.m.drop(s) // after the reset: a leaving connection closes after it
		if s.peerClosed.Load() {
			return
		}
		if s.m.c.sendResetFrame(s.id) == nil {
			_ = s.m.c.flush()
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
	s.setDLPast(!t.IsZero() && d <= 0)
	if s.dlPast.Load() {
		s.q.Wake()
	}
	if !t.IsZero() && d > 0 && !s.closed.Load() { // none for an ended session
		gen := s.dlGen
		s.dlTimer = time.AfterFunc(d, func() {
			s.dlMu.Lock()
			if s.dlGen == gen {
				s.setDLPast(true)
			}
			s.dlMu.Unlock()
			s.q.Wake()
		})
	}
	return nil
}

// SetWriteDeadline makes a pending or later Write (writeBuffers) return
// os.ErrDeadlineExceeded once t passed; zero t clears it. While one is
// set, writes leave the network write to a goroutine (see Write).
func (s *Session) SetWriteDeadline(t time.Time) error {
	s.wdMu.Lock()
	s.wdGen++
	if s.wdTimer != nil {
		s.wdTimer.Stop()
		s.wdTimer = nil
	}
	d := time.Until(t)
	past := !t.IsZero() && d <= 0
	armed := !t.IsZero() && d > 0 && !s.closed.Load() // none for an ended session
	// The trace's Set before the close: a woken Write's Wake comes after it.
	if traceOn {
		s.writeSet(past, armed)
	}
	if s.wdCh != nil {
		close(s.wdCh) // a pending wait looks at the new deadline
	}
	s.wdCh = make(chan struct{})
	if traceOn {
		s.writeNewCh()
	}
	s.wd.set.Store(!t.IsZero())
	s.wd.past.Store(past)
	if armed {
		gen := s.wdGen
		s.wdTimer = time.AfterFunc(d, func() {
			s.wdMu.Lock()
			if s.wdGen == gen {
				if traceOn { // before the close, as Set's
					s.writeFire()
				}
				s.wd.past.Store(true)
				close(s.wdCh)
				s.wdCh = make(chan struct{})
				if traceOn {
					s.writeNewCh()
				}
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
