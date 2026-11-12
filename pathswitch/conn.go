// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Stream is one path stream (an epoch): a link session through the relay,
// or a QUIC stream on a direct connection. Its bytes arrive in order or
// not at all; the path layer needs nothing else of it.
type Stream interface {
	io.Reader
	io.Writer
	// CloseWrite ends the stream's send side after what was written (FIN).
	CloseWrite() error
	// Reset ends the stream both ways at once; what is in flight is lost.
	Reset() error
}

// Config is a Conn's configuration. The zero value is usable.
type Config struct {
	// Window is W: the bytes a sender keeps until its peer acknowledged
	// them, per direction (spec BufferBound). 0: 1 MiB, link's session
	// window.
	Window int
	// Detach bounds how long an end waits for a new stream once its
	// stream ended before it resets the session (ErrDetached); an
	// aborter (Abort) and a Close wait as long for the peer. 0: 30 s.
	Detach time.Duration
	// OnStreamEnd, a client's only: the current stream ended (its path
	// died); the caller opens one on a path that works and calls Move
	// (spec Fallback). Called on its own goroutine, no lock held.
	OnStreamEnd func(epoch uint64, err error)
}

var (
	// ErrReset: the session was reset (Abort, the peer's ABORT, a Close
	// of the peer's direction before its end).
	ErrReset = errors.New("pathswitch: session reset")
	// ErrDetached: no stream came back within Config.Detach.
	ErrDetached = errors.New("pathswitch: no stream within the detach bound")
	// ErrProtocol: a frame the protocol does not allow (a relay
	// rewriting the relayed stream, a bug): the session is reset.
	ErrProtocol = errors.New("pathswitch: protocol error")
	// ErrStaleEpoch: Attach of an epoch not newer than the session's (a
	// replayed OPEN): refused, the session untouched.
	ErrStaleEpoch = errors.New("pathswitch: stale epoch")
	// ErrNotClient: Move on a server's Conn, or Attach on a client's.
	ErrNotClient = errors.New("pathswitch: wrong side")
)

const (
	abNone  = iota
	abLocal // this end reset the session; it lingers until the peer has it
	abPeer  // the peer's ABORT taken
)

// Conn is one end of a session's byte stream over a sequence of path
// streams (doc.go). Read, Write, CloseWrite, Close and Abort are safe for
// concurrent use; Move (client) and Attach (server) are the switch.
type Conn struct {
	cfg    Config
	w      uint64 // the window
	client bool

	mu   sync.Mutex
	cond *sync.Cond
	cur  *pstream
	old  []*pstream // the client's streams it left, reset once both RESUMEs are in
	ep   uint64     // the newest epoch

	// send (spec S): buf holds the positions [acked, hi); END is position hi once closed.
	buf            fifo
	acked, nxt, hi uint64
	closed, sWait  bool
	// receive (spec R)
	rbuf       fifo
	rcvd, rAck uint64
	rEnd       bool

	ab     int
	err    error         // set once: why Read and Write fail
	gone   bool          // the session's streams are released
	done   chan struct{} // closed with gone
	detach *time.Timer
	tr     traceState
}

type pstream struct {
	s     Stream
	r     *bufio.Reader
	epoch uint64
	path  string
	ctrl  [][]byte // frames this stream sends first: OPEN, RESUME, ABORT
	dead  bool     // its frames are ignored from now on (spec Kill)
	ended bool     // released (streamEnded ran)
	fin   bool     // the peer ended it with FIN after its clean end (the client's: it sends its own after)
}

// Client starts the dialling end of a session on its first stream
// (epoch 1, relayed), framed from its first byte.
func Client(first Stream, path string, cfg Config) *Conn { return start(first, path, cfg, true) }

// Server starts the answering end of a session on its first stream.
func Server(first Stream, path string, cfg Config) *Conn { return start(first, path, cfg, false) }

func start(first Stream, path string, cfg Config, client bool) *Conn {
	if cfg.Window <= 0 {
		cfg.Window = 1 << 20
	}
	if cfg.Detach <= 0 {
		cfg.Detach = 30 * time.Second
	}
	c := &Conn{cfg: cfg, w: uint64(cfg.Window), client: client, ep: 1, done: make(chan struct{})} // #nosec G115 -- Window > 0 above
	c.cond = sync.NewCond(&c.mu)
	ps := &pstream{s: first, r: bufio.NewReaderSize(first, 32<<10), epoch: 1, path: path}
	c.cur = ps
	c.run(ps)
	return c
}

func (c *Conn) run(ps *pstream) {
	go c.readLoop(ps)
	go c.writeLoop(ps)
}

func (c *Conn) end(client bool) string {
	if client {
		return "client"
	}
	return "server"
}

// sendable: the positions S may send, its bytes and the END once closed.
func (c *Conn) sendable() uint64 {
	if c.closed {
		return c.hi + 1
	}
	return c.hi
}

// rpos: R's count, its bytes and the END once taken.
func (c *Conn) rpos() uint64 {
	if c.rEnd {
		return c.rcvd + 1
	}
	return c.rcvd
}

func (c *Conn) clean() bool { return c.closed && c.acked == c.hi+1 && c.rEnd }

// left: this end's Close ended the session cleanly (shutdown(true)).
func (c *Conn) left() bool { return c.gone && c.err == io.ErrClosedPipe }

// Epoch returns the session's newest epoch.
func (c *Conn) Epoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ep
}

// Done is closed once the session is over: both directions ended and
// acknowledged, reset (either end), or detached past Config.Detach. After
// a clean end it waits for this end's last ACK and FIN to be written (at
// most Config.Detach), so that a caller dropping the stream on Done does
// not leave the peer waiting for that ACK (guard TestDoneAfterTheLastAck).
// The client's waits for the server's FIN first, which says the server
// has everything (it closed too): the client alone can open a stream, so
// it ends last, opening another (OnStreamEnd) if its stream ends before
// that FIN (spec LeaveLast, CloseEnds; guard
// TestCloseAfterReattachEndsBeforeTheBound).
func (c *Conn) Done() <-chan struct{} { return c.done }

// Path returns the path of the session's current stream.
func (c *Conn) Path() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur.path
}

// Write hands bytes to the session; it blocks while the window holds W
// bytes the peer has not acknowledged (spec Write).
func (c *Conn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for n < len(p) {
		for c.err == nil && !c.closed && c.hi-c.acked >= c.w {
			c.cond.Wait()
		}
		if c.err != nil {
			return n, c.err
		}
		if c.closed {
			return n, fmt.Errorf("pathswitch: write after CloseWrite")
		}
		k := min(uint64(len(p)-n), c.w-(c.hi-c.acked))
		c.buf.push(p[n : n+int(k)]) // #nosec G115 -- k <= len(p)-n
		c.hi += k
		n += int(k) // #nosec G115 -- k <= len(p)-n
		c.traceN("Write", c.end(c.client), 0, k)
		c.cond.Broadcast()
	}
	return n, nil
}

// Written is the count of bytes handed to Write so far: the position
// WaitAcked waits for to have the peer hold every one of them.
func (c *Conn) Written() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hi
}

// WaitAcked waits until the peer acknowledged every byte below n (ACK,
// RESUME: it holds them), at most d; false when d passed or the session
// failed first. A client whose peer takes a Move only once it holds some
// of the session's first bytes (a TLS handshake, whose key makes the
// token) moves after WaitAcked of them, never before: a Move leaves the
// bytes not yet sent to the new stream (spec DirectPath.tla Proof, mutant
// move-before-proof; TestWaitAckedThenMove).
func (c *Conn) WaitAcked(n uint64, d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waitFor(d, func() bool { return c.acked >= n || c.err != nil })
	return c.acked >= n
}

// Read reads the peer's bytes; io.EOF after its END.
func (c *Conn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.rbuf.len() == 0 && !c.rEnd && c.err == nil {
		c.cond.Wait()
	}
	if c.err != nil {
		return 0, c.err
	}
	if c.rbuf.len() == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.rbuf.bytes())
	c.rbuf.drop(n)
	c.cond.Broadcast() // an ACK may be due
	return n, nil
}

// CloseWrite ends this end's direction: END after its last byte,
// retained and resent like data.
func (c *Conn) CloseWrite() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.closed = true
	c.cond.Broadcast()
	return nil
}

// Close ends the session: CloseWrite, then it waits (at most
// Config.Detach) until the peer acknowledged every byte and the END. If
// the peer's direction had not ended, or the wait ran out, the session is
// reset (an ABORT, as TCP's RST); otherwise its streams end cleanly. After
// this end's Abort it returns at once and the reset lingers as Abort's.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gone {
		return nil
	}
	if c.err == nil {
		c.closed = true
		c.cond.Broadcast()
		c.waitFor(c.cfg.Detach, func() bool { return c.err != nil || c.acked == c.hi+1 })
	}
	switch {
	case c.ab == abLocal:
		// this end reset the session: it lingers, answering the peer's
		// streams with the ABORT, until the peer has it or Config.Detach
		// (abort's timer; spec AbortBy, SAnswerAbort). A shutdown here
		// dropped an ABORT still in flight with its stream, and the peer
		// re-attached until its detach bound (TestAbortThenCloseLingers).
	case c.err != nil:
		c.shutdown(false)
	case c.clean():
		c.err = io.ErrClosedPipe
		c.cond.Broadcast()
		c.shutdown(true)
	default:
		c.abort(ErrReset)
		c.waitFor(c.cfg.Detach, func() bool { return c.cur.ended || len(c.cur.ctrl) == 0 })
		c.shutdown(false)
	}
	return nil
}

// waitFor waits under c.mu until ok or d passed.
func (c *Conn) waitFor(d time.Duration, ok func() bool) {
	if ok() {
		return
	}
	t := time.AfterFunc(d, func() { c.mu.Lock(); c.cond.Broadcast(); c.mu.Unlock() })
	defer t.Stop()
	until := time.Now().Add(d)
	for !ok() && time.Now().Before(until) {
		c.cond.Wait()
	}
}

// Abort resets the session (spec AbortBy): Read and Write fail with
// ErrReset at both ends. This end keeps answering every stream the
// session opens with ABORT until the peer answered its ABORT or
// Config.Detach passed.
func (c *Conn) Abort() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.abort(ErrReset)
}

func (c *Conn) abort(err error) {
	if c.gone || c.ab != abNone {
		return
	}
	c.ab = abLocal
	c.fail(err)
	c.trace("Abort", c.end(c.client), 0, 0, "")
	if !c.cur.dead {
		c.cur.ctrl = append(c.cur.ctrl, []byte{fAbort})
	}
	time.AfterFunc(c.cfg.Detach, func() { c.mu.Lock(); c.shutdown(false); c.mu.Unlock() })
	c.cond.Broadcast()
}

func (c *Conn) fail(err error) {
	if c.err == nil {
		c.err = err
	}
	c.cond.Broadcast()
}

// shutdown releases the session's streams: cleanly (CloseWrite after the
// last frame) or reset.
func (c *Conn) shutdown(clean bool) {
	if c.gone {
		return
	}
	c.gone = true
	if clean && (c.client || !c.cur.dead) { // the client may open another stream for the server's FIN
		time.AfterFunc(c.cfg.Detach, func() { c.mu.Lock(); c.closeDone(); c.mu.Unlock() })
	} else {
		c.closeDone()
	}
	c.fail(ErrReset)
	if c.detach != nil {
		c.detach.Stop()
	}
	for _, ps := range append(c.old, c.cur) {
		if clean && ps == c.cur && !ps.dead {
			continue // its writer sends FIN after its last frame
		}
		c.release(ps)
	}
	c.old = nil
	c.cond.Broadcast()
}

func (c *Conn) isDone() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// closeDone closes done once (under c.mu).
func (c *Conn) closeDone() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

func (c *Conn) release(ps *pstream) {
	if !ps.ended {
		ps.ended, ps.dead = true, true
		_ = ps.s.Reset()
	}
}

// Move opens the session on a new stream (client only; spec Open:
// Fallback once the current stream ended, a Move while it lives): OPEN
// with the next epoch, this end's count of the peer's bytes and
// token(epoch), the peer's proof that the stream is this session's (the
// caller's: an exporter of the session's TLS bound to the epoch). The
// client reads only the new stream from now on and sends its data on it
// once the peer's RESUME came, then resets the streams it left.
func (c *Conn) Move(s Stream, path string, token func(epoch uint64) []byte) (uint64, error) {
	if !c.client {
		return 0, ErrNotClient
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gone && !(c.left() && c.cur.ended && !c.isDone()) || c.ab == abPeer {
		_ = s.Reset()
		return 0, c.err
	}
	e := c.ep + 1
	tok := token(e)
	if len(tok) > maxToken {
		_ = s.Reset()
		return 0, fmt.Errorf("pathswitch: token of %d bytes", len(tok))
	}
	ps := &pstream{s: s, r: bufio.NewReaderSize(s, 32<<10), epoch: e, path: path}
	ps.ctrl = [][]byte{appendOpen(nil, e, c.rpos(), c.ab == abLocal, tok)}
	c.ep = e
	if c.ab == abNone {
		c.rAck = c.rpos()
		c.sWait = true
	}
	if !c.cur.ended {
		c.old = append(c.old, c.cur)
	}
	c.cur = ps
	c.stopDetach()
	c.trace("Open", "client", e, 0, path)
	c.run(ps)
	c.cond.Broadcast()
	return e, nil
}

// Open is the first frame of a stream the client opened (ReadOpen).
type Open struct {
	Epoch uint64
	Token []byte
	Abort bool // the client reset the session
	rcvd  uint64
	s     Stream
	r     *bufio.Reader
}

// ReadOpen reads the OPEN a new stream starts with, so that the caller
// finds its session by the token and calls Attach (or Refuse).
func ReadOpen(s Stream) (Open, error) {
	r := bufio.NewReaderSize(s, 32<<10)
	var buf []byte
	f, err := readFrame(r, &buf)
	if err != nil {
		return Open{}, err
	}
	if f.t != fOpen {
		return Open{}, fmt.Errorf("%w: a stream starting with %q", ErrProtocol, f.t)
	}
	return Open{Epoch: f.epoch, Token: f.token, Abort: f.abort, rcvd: f.n, s: s, r: r}, nil
}

// Refuse answers an OPEN for no session the caller knows with ABORT: the
// client's session is reset (its peer lost it) and an aborting client
// stops lingering.
func Refuse(o Open) {
	_, _ = o.s.Write([]byte{fAbort})
	_ = o.s.Reset()
}

// Attach takes the client's new stream (server only; spec TakeOpen, and
// Resume of the other direction): it answers RESUME with this end's count
// and sends its own bytes from the client's, reading only this stream
// from now on. An epoch not newer than the session's is refused
// (ErrStaleEpoch: a replayed OPEN), the stream left to the caller.
func (c *Conn) Attach(o Open, path string) error {
	if c.client {
		return ErrNotClient
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if o.Epoch <= c.ep {
		return ErrStaleEpoch
	}
	if c.left() && !o.Abort {
		// this end left after its clean end: its count and FIN, so that a
		// client whose stream died before them ends cleanly too (spec
		// RLeftAnswer, SLeftAnswer; guard TestCloseLeftAnswersAReattach)
		c.ep = o.Epoch
		c.trace("Attach", "server", o.Epoch, o.rcvd, "left")
		go answerLeft(o, appendCount(nil, fResume, c.rpos()), c.cfg.Detach)
		return nil
	}
	if c.gone || c.ab == abPeer {
		Refuse(o)
		return c.err
	}
	if c.ab == abNone && !o.Abort && (o.rcvd < c.acked || o.rcvd > c.sendable()) {
		c.abort(fmt.Errorf("%w: OPEN resumes at %d, outside [%d, %d]", ErrProtocol, o.rcvd, c.acked, c.sendable()))
		Refuse(o)
		return c.err
	}
	ps := &pstream{s: o.s, r: o.r, epoch: o.Epoch, path: path}
	c.ep = o.Epoch
	// the stream this end leaves: nothing more is read from it or sent
	// on it (the peer reads only the new one)
	c.release(c.cur)
	c.cur = ps
	c.stopDetach()
	ab := 0
	if o.Abort {
		ab = 1
	}
	c.trace("Attach", "server", o.Epoch, o.rcvd, strAb(ab))
	switch {
	case o.Abort:
		c.takeAbort(ps)
	case c.ab == abLocal:
		ps.ctrl = [][]byte{{fAbort}}
	default:
		ps.ctrl = [][]byte{appendCount(nil, fResume, c.rpos())}
		c.rAck = c.rpos()
		c.nxt = o.rcvd
		c.free(o.rcvd)
	}
	c.run(ps)
	c.cond.Broadcast()
	return nil
}

// answerLeft writes a left end's answer and FIN on o's stream, then reads
// it to the client's FIN (sent after it took this one's, so the reset
// after it drops nothing), at most d.
func answerLeft(o Open, resume []byte, d time.Duration) {
	t := time.AfterFunc(d, func() { _ = o.s.Reset() })
	defer t.Stop()
	if _, err := o.s.Write(resume); err == nil {
		_ = o.s.CloseWrite()
		_, _ = io.Copy(io.Discard, o.r)
	}
	_ = o.s.Reset()
}

func strAb(ab int) string {
	if ab == 1 {
		return "abort"
	}
	return ""
}

// free drops the positions below n from the send buffer (spec: acked).
func (c *Conn) free(n uint64) {
	if n <= c.acked {
		return
	}
	k := min(n, c.hi) - c.acked
	c.buf.drop(int(k)) // #nosec G115 -- k <= hi-acked <= W, an int
	c.acked = n
	c.cond.Broadcast()
}

func (c *Conn) stopDetach() {
	if c.detach != nil {
		c.detach.Stop()
		c.detach = nil
	}
}

// streamEnded: ps failed (read or write error, a reset by the peer or
// the path).
func (c *Conn) streamEnded(ps *pstream, err error) {
	if ps.ended {
		return
	}
	cur := ps == c.cur
	if cur && c.client && c.clean() && c.ab == abNone && !c.isDone() {
		if errors.Is(err, io.EOF) { // the server's FIN: this end sends its own and is done
			ps.fin = true
			c.trace("TakeFin", "client", ps.epoch, 0, "")
			c.cond.Broadcast()
			return
		}
		c.release(ps)
		if c.cfg.OnStreamEnd != nil { // before the server's FIN: another stream gets it
			go c.cfg.OnStreamEnd(ps.epoch, err)
		}
		return
	}
	c.release(ps)
	if !cur || c.gone || c.clean() {
		return // a stream left, or the session's end (the peer's FIN)
	}
	if c.ab == abPeer {
		c.shutdown(false) // its answer's stream died first: nothing lingers here
		return
	}
	if c.ab == abNone || c.ab == abLocal {
		d := c.cfg.Detach
		c.detach = time.AfterFunc(d, func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.cur == ps {
				c.fail(ErrDetached)
				c.shutdown(false)
			}
		})
		if c.client && c.cfg.OnStreamEnd != nil {
			go c.cfg.OnStreamEnd(ps.epoch, err)
		}
	}
}

func (c *Conn) readLoop(ps *pstream) {
	var buf []byte
	for {
		f, err := readFrame(ps.r, &buf)
		c.mu.Lock()
		if err != nil {
			if errors.Is(err, ErrProtocol) && ps == c.cur && !ps.dead {
				c.abort(err)
			}
			c.streamEnded(ps, err)
			c.mu.Unlock()
			return
		}
		if ps == c.cur && !ps.dead && c.client && c.left() {
			switch f.t {
			case fResume: // a stream it opened after it left: the server's count
				c.trace("Resume", "client", ps.epoch, f.n, "")
			case fAbort: // refused (a server that no longer holds the session): its end
				c.streamEnded(ps, io.EOF)
			}
			c.mu.Unlock()
			continue
		}
		if ps != c.cur || ps.dead || c.gone {
			c.mu.Unlock()
			continue // a stream left: read to its end, ignored
		}
		if err := c.take(ps, f); err != nil {
			c.abort(err)
		}
		c.mu.Unlock()
	}
}

// take applies one frame of the current stream (spec Recv, TakeAck,
// Resume, STakeAbort).
func (c *Conn) take(ps *pstream, f frame) error {
	if c.ab == abLocal {
		if f.t == fAbort { // the peer has the reset
			c.shutdown(false)
		}
		return nil
	}
	if c.ab == abPeer {
		return nil
	}
	switch f.t {
	case fData:
		if c.rEnd {
			return fmt.Errorf("%w: DATA after END", ErrProtocol)
		}
		c.rbuf.push(f.data)
		c.rcvd += uint64(len(f.data))
		c.traceN("Recv", c.end(c.client), ps.epoch, uint64(len(f.data)))
	case fEnd:
		if c.rEnd || f.n != c.rcvd {
			return fmt.Errorf("%w: END(%d) after %d bytes", ErrProtocol, f.n, c.rcvd)
		}
		c.rEnd = true
		c.trace("RecvEnd", c.end(c.client), ps.epoch, 0, "")
	case fAck:
		if c.sWait || f.n > c.nxt {
			return fmt.Errorf("%w: ACK(%d), sent %d", ErrProtocol, f.n, c.nxt)
		}
		c.trace("TakeAck", c.end(c.client), ps.epoch, f.n, "")
		c.free(f.n)
	case fResume:
		if !c.client || !c.sWait || f.n < c.acked || f.n > c.sendable() {
			return fmt.Errorf("%w: RESUME(%d) outside [%d, %d]", ErrProtocol, f.n, c.acked, c.sendable())
		}
		c.trace("Resume", "client", ps.epoch, f.n, "")
		c.nxt = f.n
		c.free(f.n)
		c.sWait = false
		for _, old := range c.old { // both RESUMEs are in: the old streams go
			c.release(old)
		}
		c.old = nil
	case fAbort:
		c.takeAbort(ps)
	default:
		return fmt.Errorf("%w: %q on an open stream", ErrProtocol, f.t)
	}
	c.cond.Broadcast()
	return nil
}

// takeAbort: the peer reset the session. This end answers ABORT (the
// aborter stops lingering) and releases the session once it went out.
func (c *Conn) takeAbort(ps *pstream) {
	c.ab = abPeer
	c.fail(ErrReset)
	c.trace("TakeAbort", c.end(c.client), ps.epoch, 0, "")
	ps.ctrl = append(ps.ctrl, []byte{fAbort})
}

func (c *Conn) ackDue() bool {
	rp := c.rpos()
	if rp <= c.rAck || c.ab != abNone {
		return false
	}
	if c.rEnd {
		return true
	}
	held := uint64(c.rbuf.len()) // #nosec G115 -- a length, >= 0
	return held <= c.w/2 && (rp-c.rAck >= c.w/4 || held == 0)
}

// next picks ps's next frame (under c.mu), waiting until there is one;
// nil once ps sends nothing more.
func (c *Conn) next(ps *pstream, wbuf []byte) ([]byte, bool) {
	for {
		if ps.ended || ps.dead {
			return nil, false
		}
		if len(ps.ctrl) > 0 {
			f := ps.ctrl[0]
			ps.ctrl = ps.ctrl[1:]
			return append(wbuf[:0], f...), true
		}
		if c.gone || c.ab == abPeer {
			if c.ab == abPeer && !c.gone && ps == c.cur {
				// The answer to the peer's ABORT went out (ctrl first): a FIN
				// after it, never a reset, which could drop it unread and
				// leave the aborter lingering to open another stream
				// (spec-traces, TestResetReachesFromClient, lane DIRECTPATH3).
				c.shutdown(true)
				return nil, true
			}
			if ps == c.cur && c.left() {
				if c.rpos() > c.rAck { // the peer's END acknowledged before the FIN
					c.rAck = c.rpos()
					c.trace("Ack", c.end(c.client), ps.epoch, c.rAck, "")
					return appendCount(wbuf[:0], fAck, c.rAck), true
				}
				if c.client && !ps.fin { // the client's after the server's
					c.cond.Wait()
					continue
				}
				c.trace("Fin", c.end(c.client), ps.epoch, 0, "")
				return nil, true // a clean end: FIN
			}
			if c.ab == abPeer {
				c.shutdown(false)
			}
			return nil, false
		}
		if ps != c.cur {
			return nil, false
		}
		if c.ab == abNone {
			if f, ok := c.frame(ps, wbuf[:0]); ok {
				return f, true
			}
		}
		c.cond.Wait()
	}
}

// writeBatch: the most bytes of frames a writer sends with one Write (and
// a frame more), and at most a quarter window, so that an ACK waits for
// no longer write than that.
const writeBatch = 256 << 10

// batches: the writers' batch buffers (more), each held only while its
// batch is written, so that an idle stream or one just opened holds none.
var batches = sync.Pool{New: func() any { b := make([]byte, 0, writeBatch+maxFrame+16); return &b }}

// more appends to b the frames ps may send now without waiting (under
// c.mu), so that its writer sends them with one Write: one wake of the
// writer and one stream Write per batch instead of per 16 KiB frame
// (docs/performance.md in this repository "pathswitch: a writer sends
// what it may in one Write"; guard TestWriterBatchesFrames).
func (c *Conn) more(ps *pstream, b []byte) []byte {
	lim := int(min(writeBatch, c.w/4)) // #nosec G115 -- at most writeBatch
	for len(b) < lim && ps == c.cur && !ps.ended && !ps.dead && len(ps.ctrl) == 0 && !c.gone && c.ab == abNone {
		f, ok := c.frame(ps, b)
		if !ok {
			break
		}
		b = f
	}
	return b
}

// frame appends to b the ACK, DATA or END ps may send now, if any (spec
// Ack, Send, SendEnd).
func (c *Conn) frame(ps *pstream, b []byte) ([]byte, bool) {
	if c.ackDue() {
		c.rAck = c.rpos()
		c.trace("Ack", c.end(c.client), ps.epoch, c.rAck, "")
		return appendCount(b, fAck, c.rAck), true
	}
	if c.sWait || c.nxt >= c.sendable() {
		return b, false
	}
	if c.nxt == c.hi {
		c.nxt++
		c.trace("SendEnd", c.end(c.client), ps.epoch, 0, "")
		return appendCount(b, fEnd, c.hi), true
	}
	k := min(c.hi-c.nxt, maxFrame)
	off := c.nxt - c.acked
	b = appendData(b, c.buf.bytes()[off:off+k])
	c.nxt += k
	c.traceN("Send", c.end(c.client), ps.epoch, k)
	return b, true
}

func (c *Conn) writeLoop(ps *pstream) {
	wbuf := make([]byte, 0, maxFrame+16)
	c.mu.Lock()
	for {
		f, ok := c.next(ps, wbuf)
		if !ok {
			if c.gone && ps == c.cur {
				c.closeDone()
			}
			c.mu.Unlock()
			return
		}
		var bp *[]byte
		if f != nil {
			bp = batches.Get().(*[]byte)
			f = c.more(ps, append((*bp)[:0], f...))
		}
		c.mu.Unlock()
		var err error
		if f == nil {
			err = ps.s.CloseWrite()
		} else {
			_, err = ps.s.Write(f)
			*bp = f[:0]
			batches.Put(bp)
		}
		c.mu.Lock()
		if f == nil {
			if err != nil {
				c.release(ps)
			}
			c.closeDone() // the last ACK and FIN are written
			if ps.fin {
				c.release(ps) // both FINs: the client's goes last
			} else {
				// the peer's FIN or a reset ends the reader; a peer that
				// never sends it is reset after the detach bound
				time.AfterFunc(c.cfg.Detach, func() { c.mu.Lock(); c.release(ps); c.mu.Unlock() })
			}
			c.mu.Unlock()
			return
		}
		if err != nil {
			c.streamEnded(ps, err)
		}
	}
}

// fifo is a byte queue that reuses its array: a drop moves its head, a
// push moves what is left to the front before it would grow.
type fifo struct {
	b   []byte
	off int
}

func (q *fifo) len() int      { return len(q.b) - q.off }
func (q *fifo) bytes() []byte { return q.b[q.off:] }

func (q *fifo) push(p []byte) {
	if q.off > 0 && len(q.b)+len(p) > cap(q.b) {
		q.b = q.b[:copy(q.b, q.b[q.off:])]
		q.off = 0
	}
	q.b = append(q.b, p...)
}

func (q *fifo) drop(n int) {
	q.off += n
	if q.off == len(q.b) {
		q.b, q.off = q.b[:0], 0
	}
}
