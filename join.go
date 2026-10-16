// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// Join splices a and b, sessions of the same Mux or of two, until both
// directions ended, and returns nil after a clean end, else the *Abort it
// sent on (the first). It answers either one the peer opened and that is
// not answered yet (Accept).
//
// Each direction carries the bytes written, in order, intact. A clean end
// of one side's writes (CloseWrite) becomes CloseWrite on the other once
// every byte before it was written there, and a full close becomes Close;
// each direction ends on its own. An error on either side (an Abort or
// Reset, its connection's end, a write deadline set on a session that
// passed) aborts the other: an *Abort received goes on with Hops+1 (a
// plain reset as CodeUnspecified, Hops 1), a local cause as an Abort with
// Hops 0 and CodeConnLost, CodeTimeout or CodeDrainDeadline.
//
// Join takes a batch (at most 256 KiB) from one side's receive queue only
// when it writes it to the other, and the queue returns credit as bytes
// leave it: what a joined session holds is its two windows plus two
// batches. It holds no goroutine while the sessions are idle (Pump):
// only the caller's waits in Join. Join sets no deadline; idle limits are
// the caller's, who may Abort either session. An Observer of a's
// connection (else b's) gets a HopEnded event.
func Join(a, b *Session) error {
	_ = a.answer(0, true)
	_ = b.answer(0, true)
	ch := make(chan error, 1)
	j := newJoint(a, b, a.observer(b), func(err error) { ch <- err })
	j.dir(0)
	j.dir(1)
	return <-ch
}

// Route opens the session in continues on (on a Mux, a Pool or anything
// else that gives a *Session), or returns why not: an *Abort (its Code and
// Msg answer in), ErrGoAway (CodeDraining) or any other error
// (CodeNoRoute, with its text). ctx
// carries in's Chain with Hops+1 (Pool.Open and Mux.OpenChain read it:
// ChainFromContext) and ends when in ends, when ForwardConfig's
// AnswerTimeout passes, or once the onward answer came. A Route may wait
// for the onward answer itself (Answered) to try another next hop.
type Route func(ctx context.Context, in *Session) (*Session, error)

// ForwardConfig is a forwarding hop's settings.
type ForwardConfig struct {
	MaxHops       uint8         // refuse an open that crossed this many joins (CodeHopLimit); 0: 8
	AnswerTimeout time.Duration // refuse when no onward answer came by then (CodeTimeout); 0: wait while in is open
	// Observer gets the Hop events; nil: link.Forward uses in's
	// connection's Observer, peerlink.Forward emits none.
	Observer Observer
}

// DefaultMaxHops is ForwardConfig's MaxHops when it names none.
const DefaultMaxHops = 8

// Forward returns an accept function for Mux.Serve (or a Pool's Accept)
// that makes this side a hop of a path: it refuses an open whose Chain
// already crossed MaxHops joins (a routing loop), calls route, forwards
// the bytes the origin sent meanwhile, waits for the onward answer and
// answers in with it (an ACCEPT with the path's length, or the refusal
// one hop further), then joins the two sessions (Join) and returns: a
// forwarded session holds no goroutine while idle. Toward an onward
// session without answers (Answers false) it accepts in once route
// returned; a Route that wants answers end to end refuses such a session
// itself. Observer gets HopOpened, HopAnswered and HopEnded.
func Forward(route Route, cfg ForwardConfig) func(header []byte, s *Session) {
	maxHops := cfg.MaxHops
	if maxHops == 0 {
		maxHops = DefaultMaxHops
	}
	return func(_ []byte, in *Session) {
		obs := cfg.Observer
		if obs == nil {
			obs = in.observer(nil)
		}
		c := in.Chain()
		hop := func(e HopEvent) {
			if obs != nil {
				e.Chain = c
				obs.Hop(e)
			}
		}
		hop(HopEvent{Kind: HopOpened})
		t0 := time.Now()
		refuse := func(a *Abort) {
			r := *a
			r.Refused = true
			_ = in.AbortWith(&r)
			hop(HopEvent{Kind: HopAnswered, Latency: time.Since(t0), Err: &r})
		}
		if c.Hops >= maxHops {
			refuse(&Abort{Code: CodeHopLimit, Msg: fmt.Sprintf("the path crossed %d hops (MaxHops %d)", c.Hops, maxHops)})
			return
		}
		ctx, cancel := context.WithCancel(ContextWithChain(context.Background(), Chain{Trace: c.Trace, Hops: c.Hops + 1}))
		defer cancel()
		if cfg.AnswerTimeout > 0 {
			var stop context.CancelFunc
			ctx, stop = context.WithTimeout(ctx, cfg.AnswerTimeout)
			defer stop()
		}
		go func() { // ctx ends with in: until the answer (this function's return)
			select {
			case <-in.Done():
				cancel()
			case <-ctx.Done():
			}
		}()
		out, err := route(ctx, in)
		if err != nil {
			var ra *Abort
			switch {
			case closed(in.Done()):
			case errors.As(err, &ra):
				refuse(&Abort{Code: ra.Code, Msg: ra.Msg})
			case errors.Is(err, ErrGoAway):
				refuse(&Abort{Code: CodeDraining, Msg: err.Error()})
			case errors.Is(ctx.Err(), context.DeadlineExceeded):
				refuse(&Abort{Code: CodeTimeout, Msg: "no route within the answer timeout"})
			default:
				refuse(&Abort{Code: CodeNoRoute, Msg: err.Error()})
			}
			return
		}
		j := newJoint(in, out, obs, nil)
		j.pending = true
		j.dir(0) // the origin's bytes go on before the answer
		if out.Answers() {
			err = out.Answered(ctx)
		}
		if err == nil {
			_ = in.AcceptPath(inc(out.PathLen()))
			hop(HopEvent{Kind: HopAnswered, Latency: time.Since(t0)})
			j.answered(true)
			return
		}
		var a *Abort
		switch {
		case closed(in.Done()): // the origin's side ended first: pass its end on
			if e := in.err.Load(); e != nil {
				j.abort(out, abortFor(*e))
			} else {
				_ = out.Close()
			}
		case errors.As(err, &a):
			a = a.passed()
			a.Refused = true
			refuse(a)
		case errors.Is(err, context.DeadlineExceeded):
			a = &Abort{Code: CodeTimeout, Msg: fmt.Sprintf("no answer within %v", cfg.AnswerTimeout)}
			j.abort(out, &Abort{Code: CodeTimeout, Msg: a.Msg})
			refuse(a)
		default:
			a = abortFor(err)
			j.abort(out, a)
			refuse(a)
		}
		if a != nil {
			j.fail(a)
		}
		j.answered(false)
	}
}

// inc is h+1, at most 255.
func inc(h uint8) uint8 {
	if h < 255 {
		h++
	}
	return h
}

// observer is the Observer of s's connection, else of o's (nil: none).
func (s *Session) observer(o *Session) Observer {
	if obs := s.m.c.credit.obs; obs != nil || o == nil {
		return obs
	}
	return o.m.c.credit.obs
}

// joint is two sessions joined: [0] toward the origin, [1] onward.
type joint struct {
	s     [2]*Session
	obs   Observer
	chain Chain
	done  func(error) // nil: none

	mu      sync.Mutex
	left    int      // directions not yet ended
	n       [2]int64 // bytes [0]->[1] and [1]->[0]
	err     error    // the Abort that ended it; nil: a clean end
	pending bool     // Forward waits for the onward answer: direction 0's failure waits (held)
	held    func()
}

func newJoint(in, out *Session, obs Observer, done func(error)) *joint {
	return &joint{s: [2]*Session{in, out}, obs: obs, chain: in.chain, done: done, left: 2}
}

// joinWriter writes into a joined session and keeps its write error.
type joinWriter struct {
	s   *Session
	err error
}

func (w *joinWriter) Write(p []byte) (int, error) {
	n, err := w.s.Write(p)
	if err != nil {
		w.err = err
	}
	return n, err
}

func (w *joinWriter) WriteBuffers(b [][]byte) (int64, error) {
	n, err := w.s.WriteBuffers(b)
	if err != nil {
		w.err = err
	}
	return n, err
}

// dir starts direction i: s[i]'s bytes to s[1-i].
func (j *joint) dir(i int) {
	w := &joinWriter{s: j.s[1-i]}
	j.s[i].Pump(w, func(n int64, err error) { j.ended(i, n, err, w.err) })
}

// answered ends the wait for the onward answer: accepted, direction 1
// starts; else it never does.
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

// fail records a as the joint's end unless one is.
func (j *joint) fail(a *Abort) {
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

// abort ends s with a, recorded as the joint's end unless one is.
func (j *joint) abort(s *Session, a *Abort) {
	j.fail(a)
	_ = s.AbortWith(a)
}

// ended is direction i's end: src's bytes ended (err: why, nil when
// cleanly) or writing them to dst failed (werr).
func (j *joint) ended(i int, n int64, err, werr error) {
	j.mu.Lock()
	j.n[i] = n
	if i == 0 && j.pending && (err != nil || werr != nil) {
		j.held = func() { j.ended(i, n, err, werr) }
		j.mu.Unlock()
		return
	}
	j.mu.Unlock()
	src, dst := j.s[i], j.s[1-i]
	switch {
	case werr != nil:
		switch {
		case errors.Is(werr, ErrReset) || errors.Is(werr, ErrConnClosed): // dst was cut (Session.writeErr)
			j.abort(src, abortFor(werr))
		case errors.Is(werr, os.ErrDeadlineExceeded):
			a := &Abort{Code: CodeTimeout, Msg: "write deadline"}
			j.abort(src, a)
			_ = dst.AbortWith(a)
		case !j.failed():
			_ = src.Close() // dst's peer closed it: a full close
		}
	case err != nil:
		j.abort(dst, abortFor(err))
	case j.failed(): // ended by the other direction's abort
	case src.peerHalf.Load():
		_ = dst.CloseWrite()
	default:
		_ = dst.Close()
	}
	j.finish()
}

// finish counts a direction's end; after the last, HopEnded and done.
func (j *joint) finish() {
	j.mu.Lock()
	j.left--
	last, err, n := j.left == 0, j.err, j.n
	j.mu.Unlock()
	if !last {
		return
	}
	if j.obs != nil {
		j.obs.Hop(HopEvent{Kind: HopEnded, Chain: j.chain, Err: err, In: n[0], Out: n[1]})
	}
	if j.done != nil {
		j.done(err)
	}
}
