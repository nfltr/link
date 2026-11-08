// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/link"
)

// ChainSession is a Session on a Link that answers opens: a Node's over a
// connection that negotiated link.FeatureChain, a quiclink session over a
// connection that negotiated chaining (quiclink v0.4.0). Assert it; a
// Session without it is spliced like a net.Conn. Answers says whether
// this session's connection carries answers; the methods are
// *link.Session's (see there): Accept answers an open yes, Refuse no with
// a code and a message, Abort resets with them, Answered waits for the
// peer's answer, and the *Path / *With forms carry the path's hop counts
// (Forward).
type ChainSession interface {
	Session
	Answers() bool
	Accept() error
	AcceptPath(pathLen uint8) error
	Refuse(code uint32, msg string) error
	Abort(code uint32, msg string) error
	AbortWith(a *link.Abort) error
	Answered(ctx context.Context) error
	Chain() link.Chain
	PathLen() uint8
}

var (
	_ ChainSession = (*link.Session)(nil)
	_ ChainSession = (*session)(nil)
)

// Join splices a and b until both directions ended and closes both;
// it returns nil after a clean end, else the *link.Abort it sent on (or,
// toward a conn without answers, the error that ended the other side).
// Two *link.Session (a Node's sessions among them) go through link.Join.
// Otherwise it copies both ways, from a *link.Session with Pump (no
// goroutine while idle) and into one with WriteBuffers; it answers a
// ChainSession the peer opened (Accept) first. A clean end of one side's
// writes becomes CloseWrite on the other (Close when it has none, or
// when a *link.Session's peer closed it whole); an error on one side
// aborts the other: AbortWith on a ChainSession (an *link.Abort received
// goes on with Hops+1, a local cause with Hops 0), Reset on a Session,
// an RST on a *net.TCPConn (SetLinger(0)), else Close.
func Join(a, b net.Conn) error {
	if la, lb := native(a), native(b); la != nil && lb != nil {
		err := link.Join(la, lb)
		_ = a.Close() // a Node's wrapper: Drain stops waiting for it
		_ = b.Close()
		return err
	}
	for _, c := range []net.Conn{a, b} {
		if cs, ok := c.(ChainSession); ok {
			_ = cs.Accept()
		}
	}
	ch := make(chan error, 1)
	j := &joint{c: [2]net.Conn{a, b}, left: 2, done: func(err error) { ch <- err }}
	if traceOn {
		traceStart(j)
	}
	j.dir(0)
	j.dir(1)
	return <-ch
}

// native is c's *link.Session, unwrapped from a Node's, or nil.
func native(c net.Conn) *link.Session {
	switch v := c.(type) {
	case *link.Session:
		return v
	case *session:
		return v.Session
	}
	return nil
}

// Route opens the session in continues on (on any Link, or a plain conn
// wrapped as a Session), or returns why not: a *link.Abort (its Code and
// Msg answer in), ErrDraining or link.ErrGoAway (CodeDraining), or any
// other error (CodeNoRoute). ctx carries in's link.Chain with Hops+1 (a
// Node's Open reads it: link.ChainFromContext) and ends when in ends (a
// session with Done), when AnswerTimeout passes, or once the onward
// answer came.
type Route func(ctx context.Context, peer string, header []byte, in Session) (Session, error)

// Forward is link.Forward for any Link: an accept function for Serve
// (node.Serve(peerlink.Forward(route, cfg))) that refuses an open past
// cfg.MaxHops, calls route, forwards the origin's bytes at once, answers
// in with the onward answer (when both are ChainSessions that answer;
// toward an onward session without answers it accepts in once route
// returned), then joins them (Join's rules) and returns. Between two
// *link.Session nothing holds a goroutine while idle; a side of another
// kind takes one per direction. cfg.Observer, when set, gets the Hop
// events.
func Forward(route Route, cfg link.ForwardConfig) func(peer string, header []byte, s Session) {
	maxHops := cfg.MaxHops
	if maxHops == 0 {
		maxHops = link.DefaultMaxHops
	}
	return func(peer string, header []byte, in Session) {
		cin, _ := in.(ChainSession)
		var c link.Chain
		if cin != nil {
			c = cin.Chain()
		}
		hop := func(e link.HopEvent) {
			if cfg.Observer != nil {
				e.Chain = c
				cfg.Observer.Hop(e)
			}
		}
		hop(link.HopEvent{Kind: link.HopOpened})
		t0 := time.Now()
		refuse := func(a *link.Abort) {
			r := *a
			r.Refused = true
			if cin != nil {
				_ = cin.AbortWith(&r)
			} else {
				_ = in.Reset()
			}
			hop(link.HopEvent{Kind: link.HopAnswered, Latency: time.Since(t0), Err: &r})
		}
		if c.Hops >= maxHops {
			refuse(&link.Abort{Code: link.CodeHopLimit, Msg: fmt.Sprintf("the path crossed %d hops (MaxHops %d)", c.Hops, maxHops)})
			return
		}
		ctx, cancel := context.WithCancel(link.ContextWithChain(context.Background(), link.Chain{Trace: c.Trace, Hops: c.Hops + 1}))
		defer cancel()
		if cfg.AnswerTimeout > 0 {
			var stop context.CancelFunc
			ctx, stop = context.WithTimeout(ctx, cfg.AnswerTimeout)
			defer stop()
		}
		inDone := doneOf(in)
		if inDone != nil {
			go func() {
				select {
				case <-inDone:
					cancel()
				case <-ctx.Done():
				}
			}()
		}
		ended := func() bool { return inDone != nil && isClosed(inDone) }
		out, err := route(ctx, peer, header, in)
		if err != nil {
			var ra *link.Abort
			switch {
			case ended():
				_ = in.Close() // its opener gave up: nothing to answer, but release it here
			case errors.As(err, &ra):
				refuse(&link.Abort{Code: ra.Code, Msg: ra.Msg})
			case errors.Is(err, ErrDraining) || errors.Is(err, link.ErrGoAway):
				refuse(&link.Abort{Code: link.CodeDraining, Msg: err.Error()})
			case errors.Is(ctx.Err(), context.DeadlineExceeded):
				refuse(&link.Abort{Code: link.CodeTimeout, Msg: "no route within the answer timeout"})
			default:
				refuse(&link.Abort{Code: link.CodeNoRoute, Msg: err.Error()})
			}
			return
		}
		j := &joint{c: [2]net.Conn{in, out}, left: 2, obs: cfg.Observer, chain: c, pending: true}
		if traceOn {
			traceStart(j)
		}
		j.dir(0)
		var pathLen uint8
		if cout, ok := out.(ChainSession); ok && cout.Answers() {
			err = cout.Answered(ctx)
			pathLen = cout.PathLen()
		}
		if err == nil {
			if cin != nil {
				_ = cin.AcceptPath(inc(pathLen))
			}
			hop(link.HopEvent{Kind: link.HopAnswered, Latency: time.Since(t0), Moved: j.movedBytes})
			j.answered(true)
			return
		}
		var a *link.Abort
		switch {
		case ended():
			_ = out.Close()
		case errors.As(err, &a):
			a = passed(a)
			refuse(a)
		case errors.Is(err, context.DeadlineExceeded):
			a = &link.Abort{Code: link.CodeTimeout, Msg: fmt.Sprintf("no answer within %v", cfg.AnswerTimeout)}
			j.abort(out, &link.Abort{Code: link.CodeTimeout, Msg: a.Msg})
			refuse(a)
		default:
			a = abortOf(err)
			j.abort(out, a)
			refuse(a)
		}
		if a != nil {
			j.fail(a)
		}
		j.answered(false)
	}
}

// doneOf is c's Done channel when it has one (link and quiclink sessions).
func doneOf(c net.Conn) <-chan struct{} {
	if d, ok := c.(interface{ Done() <-chan struct{} }); ok {
		return d.Done()
	}
	return nil
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func inc(h uint8) uint8 {
	if h < 255 {
		h++
	}
	return h
}

// passed is a one hop further.
func passed(a *link.Abort) *link.Abort {
	p := *a
	p.Hops = inc(p.Hops)
	return &p
}

// abortOf is the Abort a join sends on after err ended one side (as
// link.Join's): an *link.Abort one hop further, a plain reset as
// CodeUnspecified one hop further, a drain's deadline here as
// CodeDrainDeadline, a deadline as CodeTimeout, anything else as
// CodeConnLost.
func abortOf(err error) *link.Abort {
	var a *link.Abort
	switch {
	case errors.As(err, &a):
		return passed(a)
	case errors.Is(err, link.ErrReset):
		return &link.Abort{Code: link.CodeUnspecified, Hops: 1}
	case errors.Is(err, link.ErrDrainDeadline):
		return &link.Abort{Code: link.CodeDrainDeadline, Msg: "drain deadline"}
	case !errors.Is(err, link.ErrConnClosed) && errors.Is(err, os.ErrDeadlineExceeded):
		return &link.Abort{Code: link.CodeTimeout, Msg: "write deadline"}
	default:
		return &link.Abort{Code: link.CodeConnLost, Msg: err.Error()}
	}
}

// joint is two conns joined: [0] toward the origin, [1] onward.
type joint struct {
	c     [2]net.Conn
	obs   link.Observer
	chain link.Chain
	done  func(error)

	mu      sync.Mutex
	left    int
	n       [2]int64
	first   time.Time // the first byte [1]->[0] written (HopEvent.FirstOut)
	err     error
	pending bool // Forward waits for the onward answer: direction 0's failure waits (held)
	held    func()
	moved   [2]atomic.Int64 // bytes written [0]->[1] and [1]->[0] (HopEvent.Moved)
	// syncing: a direction's CloseWrite onto a link session waits for its
	// Sync; the other direction's clean end waits meanwhile (fin).
	syncing bool
	fin     func()
	tr      traceState // linktrace builds only (trace_on.go)
}

// movedBytes is link.HopEvent.Moved.
func (j *joint) movedBytes() (in, out int64) { return j.moved[0].Load(), j.moved[1].Load() }

// connWriter writes into a joined conn and keeps its write error; a
// *link.Session takes a batch with one flush. Toward the origin it notes
// the first byte written (j: nil after it, and in the other direction).
type connWriter struct {
	c     net.Conn
	err   error
	j     *joint
	moved *atomic.Int64
	tj    *joint // linktrace: the client's bytes onto the session, each write a Copy
}

func (w *connWriter) Write(p []byte) (int, error) {
	n, err := w.c.Write(p)
	if err != nil {
		w.err = err
	}
	w.wrote(int64(n))
	return n, err
}

func (w *connWriter) wrote(n int64) {
	if n > 0 {
		w.moved.Add(n)
		if traceOn && w.tj != nil {
			traceJoin(w.tj, "Copy", 0)
		}
	}
	if w.j != nil && n > 0 {
		w.j.mu.Lock()
		w.j.first = time.Now()
		w.j.mu.Unlock()
		w.j = nil
	}
}

func (w *connWriter) WriteBuffers(b [][]byte) (int64, error) {
	var n int64
	var err error
	if s := native(w.c); s != nil {
		n, err = s.WriteBuffers(b)
	} else {
		for _, p := range b {
			var k int
			k, err = w.c.Write(p)
			n += int64(k)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		w.err = err
	}
	w.wrote(n)
	return n, err
}

// dir starts direction i: c[i]'s bytes to c[1-i].
func (j *joint) dir(i int) {
	src := j.c[i]
	w := &connWriter{c: j.c[1-i], moved: &j.moved[i]}
	if i == 1 {
		w.j = j
	}
	if traceOn && traceClient(j, src) {
		w.tj = j
	}
	end := func(n int64, err error) { j.ended(i, n, err, w.err) }
	if s := native(src); s != nil {
		s.Pump(w, end)
		return
	}
	if ds := native(w.c); ds != nil {
		go j.watch(src, ds)
	}
	go func() {
		buf := copyBufs.Get().(*[]byte)
		n, err := io.CopyBuffer(w, struct{ io.Reader }{src}, *buf) // no WriterTo: write errors stay w's
		copyBufs.Put(buf)
		end(n, err)
	}()
}

// copyBufs are the copy buffers of conns spliced without Pump: a link
// data chunk each (link.MaxChunk), so a read fills one data frame;
// io.Copy's 32 KiB took twice the frames, and its buffer per direction
// was garbage at each session's end.
var copyBufs = sync.Pool{New: func() any { b := make([]byte, link.MaxChunk); return &b }}

func (j *joint) answered(ok bool) {
	j.mu.Lock()
	j.pending = false
	held := j.held
	j.held = nil
	j.mu.Unlock()
	if ok {
		j.dir(1)
	} else {
		j.finish()
	}
	if held != nil {
		held()
	}
}

func (j *joint) fail(a *link.Abort) {
	j.mu.Lock()
	if j.err == nil {
		j.err = a
	}
	j.mu.Unlock()
}

func (j *joint) failed() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.err != nil
}

// abort ends c with a: AbortWith on a ChainSession, Reset on a Session,
// an RST on a TCP conn, else Close.
func (j *joint) abort(c net.Conn, a *link.Abort) {
	j.fail(a)
	if traceOn && traceClient(j, c) {
		traceJoin(j, "Lost", 0)
	}
	switch v := c.(type) {
	case ChainSession:
		_ = v.AbortWith(a)
	case Session:
		_ = v.Reset()
	case *net.TCPConn:
		_ = v.SetLinger(0)
		_ = v.Close()
	default:
		_ = c.Close()
	}
}

// cleanClose reports whether werr says only that dst was closed (by its
// peer whole, or here): no failure to pass on. net.ErrClosed is never
// one: quic-go's connection errors (idle timeout, a peer's or a drain's
// close, a stateless reset) are net.ErrClosed, and a cut taken for a
// close ends the session upstream cleanly with bytes missing.
func cleanClose(werr error) bool {
	return errors.Is(werr, io.ErrClosedPipe) && !errors.Is(werr, net.ErrClosed) &&
		!errors.Is(werr, link.ErrReset) && !errors.Is(werr, link.ErrConnClosed)
}

func (j *joint) ended(i int, n int64, err, werr error) {
	j.mu.Lock()
	j.n[i] = n
	if i == 0 && j.pending && (err != nil || werr != nil) {
		j.held = func() { j.ended(i, n, err, werr) }
		j.mu.Unlock()
		return
	}
	j.mu.Unlock()
	src, dst := j.c[i], j.c[1-i]
	switch {
	case werr != nil:
		switch {
		case cleanClose(werr):
			if !j.failed() {
				_ = src.Close()
			}
		case !errors.Is(werr, link.ErrConnClosed) && errors.Is(werr, os.ErrDeadlineExceeded):
			a := &link.Abort{Code: link.CodeTimeout, Msg: "write deadline"}
			j.abort(src, a)
			j.abort(dst, a)
		default:
			j.abort(src, abortOf(werr))
		}
	case err != nil:
		j.abort(dst, abortOf(err))
	case j.failed():
	default:
		if s := native(src); s != nil && isClosed(s.Done()) {
			// the peer closed it whole, or its FIN ended it after our
			// CloseWrite: held while that one syncs (review r2 finding 1)
			if j.hold(func() { _ = dst.Close() }) {
				return
			}
			_ = dst.Close()
		} else if ds := native(dst); ds != nil {
			j.sync(src, ds)
			return
		} else if j.hold(func() { j.closeWrite(dst) }) {
			return
		} else {
			j.closeWrite(dst)
		}
	}
	j.finish()
}

// watch aborts src once the connection of the session its bytes go to
// ended: src's copy may wait on src for ever, an idle client that already
// read the peer's FIN (spec/Join.tla Lost, LostEnds; review r2 Join 2).
// The peer's own end of the session (an answer, a refusal, a reset, its
// close) is not watch's: the direction reading the session ends src for
// it, or Forward answers it, and a second abort would reset what that
// sent (relay-core TestChainInbandEdges: an in-band refusal, then EOF).
func (j *joint) watch(src net.Conn, ds *link.Session) {
	<-ds.Done()
	if err := ds.Err(); errors.Is(err, link.ErrConnClosed) {
		j.abort(src, abortOf(err))
	}
}

// closeWrite ends dst's writes: CloseWrite, else Close.
func (j *joint) closeWrite(dst net.Conn) {
	if cw, ok := dst.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	} else {
		_ = dst.Close()
	}
}

// sync closes ds's writes and ends the direction src -> ds once that
// CloseWrite reached the peer (Session.CloseWriteSync): a clean end at src only
// then; a connection that ends first aborts src (spec/Join.tla, relay-core finding 23). The other
// direction's clean end waits meanwhile (hold), so src reads no EOF while
// its own tail may still be lost.
func (j *joint) sync(src net.Conn, ds *link.Session) {
	j.mu.Lock()
	j.syncing = true // before the CloseWrite: the peer's FIN after it is held
	if traceOn {
		traceJoin(j, "CloseWrite", 0)
	}
	j.mu.Unlock()
	go func() {
		// the ping with the CLOSE: a drained connection closes once the
		// session ended, and a ping after that close never goes (spec/Join.tla
		// DrainNeverResets, lane LINKDRAIN)
		err := ds.CloseWriteSync()
		if err != nil {
			j.abort(src, abortOf(err))
		}
		j.mu.Lock()
		j.syncing = false
		if traceOn && err == nil {
			traceJoin(j, "Synced", 0)
		}
		fin := j.fin
		j.fin = nil
		j.mu.Unlock()
		if fin != nil {
			if err == nil {
				fin()
			}
			j.finish()
		}
		j.finish()
	}()
}

// hold keeps a clean end (end) while the other direction syncs: false
// when none does.
func (j *joint) hold(end func()) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	if traceOn {
		n := 0
		if j.syncing {
			n = 1
		}
		traceJoin(j, "Fin", n)
	}
	if !j.syncing {
		return false
	}
	j.fin = end
	return true
}

func (j *joint) finish() {
	j.mu.Lock()
	j.left--
	last, err, n, first := j.left == 0, j.err, j.n, j.first
	if traceOn && last && err == nil {
		traceJoin(j, "Finish", 0)
	}
	j.mu.Unlock()
	if !last {
		return
	}
	_ = j.c[0].Close() // both directions ended: release them (a Node's Drain waits for its sessions' Close)
	_ = j.c[1].Close()
	if j.obs != nil {
		j.obs.Hop(link.HopEvent{Kind: link.HopEnded, Chain: j.chain, Err: err, In: n[0], Out: n[1], FirstOut: first})
	}
	if j.done != nil {
		j.done(err)
	}
}
