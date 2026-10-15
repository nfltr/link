// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The in-process N-hop rig: origin -> hop 1 -> ... -> hop n-1 -> far end,
// link i joining hop i to hop i+1 (the origin is hop 0, the far end hop
// n). Hops 1..n-1 run Forward with a Route that opens on their next
// link; the far end serves the test's accept.

type rigLink struct {
	dm, lm *Mux // the dialer's (hop i) and the listener's (hop i+1)
	tls    bool
	chain  bool // both sides announce FeatureChain
}

type chainRig struct {
	t         testing.TB
	links     []*rigLink
	obs       []*hopObserver // per hop 1..n-1 (index i-1)
	refuseAt  map[int]*Abort // hop -> the Route's refusal
	routeHook func(hop int, in *Session)
}

// rigOpts shapes a rig: n links, each over TLS where tlsAt(i), each
// announcing FeatureChain unless noChainAt(i).
type rigOpts struct {
	n         int
	tlsAt     func(i int) bool
	noChainAt func(i int) bool
	window    int
	live      Liveness
	cfg       ForwardConfig
	wrap      func(i int, nc net.Conn) net.Conn // the dialer's side of link i
}

func newChainRig(t testing.TB, o rigOpts, far func([]byte, *Session)) *chainRig {
	t.Helper()
	if o.window == 0 {
		o.window = testWindow
	}
	r := &chainRig{t: t, refuseAt: map[int]*Abort{}}
	var stls, ctls *tls.Config
	for i := range o.n {
		l := &rigLink{tls: o.tlsAt != nil && o.tlsAt(i), chain: o.noChainAt == nil || !o.noChainAt(i)}
		a, b := tcpPair(t)
		if o.wrap != nil {
			a = o.wrap(i, a)
		}
		if l.tls {
			if stls == nil {
				stls, ctls = selfSigned(t)
			}
			a, b = tls.Client(a, ctls), tls.Server(b, stls)
		}
		dcfg := Config{Window: o.window, Auth: testToken, Liveness: o.live, noChain: !l.chain}
		lcfg := Config{Window: o.window, Auth: testToken, noChain: !l.chain}
		if i < o.n-1 { // hop i+1 forwards what link i brings
			obs := &hopObserver{}
			r.obs = append(r.obs, obs)
			lcfg.Observer = obs
		}
		type res struct {
			c   *Conn
			err error
		}
		sc := make(chan res, 1)
		go func() {
			c, _, err := Server(context.Background(), b, lcfg)
			sc <- res{c, err}
		}()
		cc, err := Client(context.Background(), a, dcfg)
		if err != nil {
			t.Fatalf("link %d: Client: %v", i, err)
		}
		s := <-sc
		if s.err != nil {
			t.Fatalf("link %d: Server: %v", i, s.err)
		}
		l.dm, l.lm = NewMux(cc), NewMux(s.c)
		r.links = append(r.links, l)
		t.Cleanup(func() { _ = l.dm.Close(); _ = l.lm.Close() })
	}
	for i, l := range r.links {
		go func() { _ = l.dm.Serve(nil) }()
		accept := far
		if i < o.n-1 {
			accept = Forward(r.route(i+1), o.cfg)
		}
		go func() { _ = l.lm.Serve(accept) }()
	}
	return r
}

// route is hop h's Route: on link h.
func (r *chainRig) route(h int) Route {
	return func(ctx context.Context, in *Session) (*Session, error) {
		if r.routeHook != nil {
			r.routeHook(h, in)
		}
		if a := r.refuseAt[h]; a != nil {
			return nil, a
		}
		ch, _ := ChainFromContext(ctx)
		return r.links[h].dm.OpenChain(in.Header(), ch)
	}
}

func (r *chainRig) open(header string) *Session {
	r.t.Helper()
	s, err := r.links[0].dm.Open([]byte(header))
	if err != nil {
		r.t.Fatal(err)
	}
	return s
}

// idle waits for every Mux of the rig to carry no session.
func (r *chainRig) idle() {
	r.t.Helper()
	waitUntil(r.t, func() bool {
		for _, l := range r.links {
			if l.dm.Sessions()+l.lm.Sessions() != 0 {
				return false
			}
		}
		return true
	})
}

// hopObserver records a hop's Hop events.
type hopObserver struct {
	NopObserver
	mu     sync.Mutex
	events []HopEvent
}

func (o *hopObserver) Hop(e HopEvent) {
	o.mu.Lock()
	o.events = append(o.events, e)
	o.mu.Unlock()
}

func (o *hopObserver) kinds() []HopKind {
	o.mu.Lock()
	defer o.mu.Unlock()
	var k []HopKind
	for _, e := range o.events {
		k = append(k, e.Kind)
	}
	return k
}

// farEcho: the far end accepts and echoes, half closing after the
// origin did (Pump: no goroutine while idle).
func farEcho(_ []byte, s *Session) {
	_ = s.Accept()
	s.Pump(s, func(_ int64, err error) {
		if err != nil {
			_ = s.Reset()
			return
		}
		_ = s.CloseWrite()
	})
}

// roundTrip writes n random bytes on s while reading what comes back and
// checks both by SHA-256 against want(sent).
func roundTrip(t *testing.T, s *Session, n int) {
	t.Helper()
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(rand.Uint32())
	}
	errc := make(chan error, 1)
	go func() {
		_, err := s.Write(data)
		if err == nil {
			err = s.CloseWrite()
		}
		errc <- err
	}()
	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("write: %v", err)
	}
	if sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatalf("echo of %d bytes: got %d bytes, different", n, len(got))
	}
}

func transports() map[string]func(int) bool {
	return map[string]func(int) bool{
		"tcp":   func(int) bool { return false },
		"tls":   func(int) bool { return true },
		"mixed": func(i int) bool { return i%2 == 1 },
	}
}

// TestChainEcho: bytes intact and in order both ways over 1 to 5 links
// of TCP, TLS and both; the origin's Answered says accepted with the
// path's length; every hop reports its three events with the trace.
func TestChainEcho(t *testing.T) {
	for name, tlsAt := range transports() {
		for n := 1; n <= 5; n++ {
			t.Run(fmt.Sprintf("%s/links=%d", name, n), func(t *testing.T) {
				r := newChainRig(t, rigOpts{n: n, tlsAt: tlsAt}, farEcho)
				s := r.open("echo")
				if err := s.Answered(context.Background()); err != nil {
					t.Fatal(err)
				}
				if got := s.PathLen(); int(got) != n-1 {
					t.Fatalf("PathLen %d, want %d", got, n-1)
				}
				roundTrip(t, s, 3<<20+rand.IntN(1<<16))
				_ = s.Close()
				r.idle()
				for h, o := range r.obs {
					waitUntil(t, func() bool { return len(o.kinds()) == 3 })
					o.mu.Lock()
					for _, e := range o.events {
						if e.Chain.Trace != s.Chain().Trace || int(e.Chain.Hops) != h {
							t.Errorf("hop %d: event %+v, trace %x", h+1, e, s.Chain().Trace)
						}
						if e.Kind == HopEnded && (e.Err != nil || e.In == 0 || e.In != e.Out) {
							t.Errorf("hop %d ended: %+v", h+1, e)
						}
					}
					o.mu.Unlock()
				}
			})
		}
	}
}

// errAbort returns err's *Abort or fails.
func errAbort(t *testing.T, err error) *Abort {
	t.Helper()
	var a *Abort
	if !errors.As(err, &a) {
		t.Fatalf("error %v (%T), want an *Abort", err, err)
	}
	return a
}

// TestChainRefuse: a refusal at hop k (the Route's *Abort) reaches the
// origin as a refusal with the Route's code and Hops k-1; bytes written
// before it are discarded; nothing leaks.
func TestChainRefuse(t *testing.T) {
	for n := 2; n <= 5; n++ {
		for k := 1; k < n; k++ {
			t.Run(fmt.Sprintf("links=%d/hop=%d", n, k), func(t *testing.T) {
				r := newChainRig(t, rigOpts{n: n}, farEcho)
				r.refuseAt[k] = &Abort{Code: CodeApp + 7, Msg: "not here"}
				s := r.open("echo")
				_, _ = s.Write(make([]byte, 100<<10)) // before the answer: discarded
				a := errAbort(t, s.Answered(context.Background()))
				if !a.Refused || a.Code != CodeApp+7 || int(a.Hops) != k-1 || a.Msg != "not here" {
					t.Fatalf("abort %+v", a)
				}
				if !errors.Is(a, ErrOpenRefused) || !errors.Is(a, ErrReset) {
					t.Fatal("errors.Is")
				}
				if _, err := s.Read(make([]byte, 1)); !errors.Is(err, ErrOpenRefused) {
					t.Fatalf("read %v", err)
				}
				r.idle()
			})
		}
	}
	t.Run("far end", func(t *testing.T) {
		r := newChainRig(t, rigOpts{n: 3}, func(_ []byte, s *Session) { _ = s.Refuse(CodeApp+1, "no target") })
		a := errAbort(t, r.open("x").Answered(context.Background()))
		if !a.Refused || a.Code != CodeApp+1 || a.Hops != 2 {
			t.Fatalf("abort %+v", a)
		}
		r.idle()
	})
	t.Run("no route", func(t *testing.T) {
		r := newChainRig(t, rigOpts{n: 3}, farEcho)
		r.routeHook = func(h int, _ *Session) {
			if h == 2 {
				_ = r.links[2].dm.Close()
			}
		}
		a := errAbort(t, r.open("x").Answered(context.Background()))
		if !a.Refused || (a.Code != CodeNoRoute && a.Code != CodeConnLost) || a.Hops != 1 {
			t.Fatalf("abort %+v", a)
		}
	})
	t.Run("mux without accept", func(t *testing.T) {
		r := newChainRig(t, rigOpts{n: 2}, nil)
		a := errAbort(t, r.open("x").Answered(context.Background()))
		if !a.Refused || a.Code != CodeRefused || a.Hops != 1 {
			t.Fatalf("abort %+v", a)
		}
	})
}

// TestChainAbort: a reset with a code at the far end, or at the origin,
// reaches the other end with its code and the hop it came from.
func TestChainAbort(t *testing.T) {
	for n := 1; n <= 4; n++ {
		t.Run(fmt.Sprintf("far/links=%d", n), func(t *testing.T) {
			r := newChainRig(t, rigOpts{n: n}, func(_ []byte, s *Session) {
				_, _ = s.Write([]byte("hi"))
				_ = s.Abort(CodeApp+3, "bye")
			})
			s := r.open("x")
			b, err := io.ReadAll(s)
			a := errAbort(t, err)
			if string(b) != "hi" || a.Refused || a.Code != CodeApp+3 || int(a.Hops) != n-1 || a.Msg != "bye" {
				t.Fatalf("read %q, abort %+v", b, a)
			}
			r.idle()
		})
		t.Run(fmt.Sprintf("origin/links=%d", n), func(t *testing.T) {
			got := make(chan error, 1)
			r := newChainRig(t, rigOpts{n: n}, func(_ []byte, s *Session) {
				_ = s.Accept()
				_, err := io.ReadAll(s)
				got <- err
			})
			s := r.open("x")
			if err := s.Answered(context.Background()); err != nil {
				t.Fatal(err)
			}
			_ = s.Abort(CodeApp+4, "origin")
			a := errAbort(t, <-got)
			if a.Code != CodeApp+4 || int(a.Hops) != n-1 || a.Msg != "origin" {
				t.Fatalf("abort %+v", a)
			}
			r.idle()
		})
	}
}

// TestChainHalfClose: each direction ends on its own through the hops:
// the far end keeps sending after the origin half-closed, and the other
// way round.
func TestChainHalfClose(t *testing.T) {
	t.Run("origin first", func(t *testing.T) {
		r := newChainRig(t, rigOpts{n: 3}, func(_ []byte, s *Session) {
			b, _ := io.ReadAll(s) // to the origin's CloseWrite
			_, _ = s.Write(append([]byte("after:"), b...))
			_ = s.CloseWrite()
		})
		s := r.open("x")
		_, _ = s.Write([]byte("ping"))
		_ = s.CloseWrite()
		b, err := io.ReadAll(s)
		if err != nil || string(b) != "after:ping" {
			t.Fatalf("%q %v", b, err)
		}
		r.idle()
	})
	t.Run("far first", func(t *testing.T) {
		r := newChainRig(t, rigOpts{n: 3}, func(_ []byte, s *Session) {
			_, _ = s.Write([]byte("hello"))
			_ = s.CloseWrite()
			b, _ := io.ReadAll(s)
			if string(b) != "late" {
				_ = s.Abort(CodeApp, "lost")
			}
		})
		s := r.open("x")
		b, err := io.ReadAll(s)
		if err != nil || string(b) != "hello" {
			t.Fatalf("%q %v", b, err)
		}
		_, _ = s.Write([]byte("late"))
		_ = s.CloseWrite()
		r.idle()
	})
}

// TestChainKillMiddleHop: hop k's process dies (its connections close):
// the origin reads CodeConnLost from hop k-1 (Hops k-2); its connection
// and other sessions are unaffected.
func TestChainKillMiddleHop(t *testing.T) {
	for n := 2; n <= 5; n++ {
		for k := 2; k <= n; k++ {
			t.Run(fmt.Sprintf("links=%d/hop=%d", n, k), func(t *testing.T) {
				r := newChainRig(t, rigOpts{n: n}, farEcho)
				s := r.open("x")
				if err := s.Answered(context.Background()); err != nil {
					t.Fatal(err)
				}
				_ = r.links[k-1].lm.Close()
				if k < n {
					_ = r.links[k].dm.Close()
				}
				_, err := io.ReadAll(s)
				a := errAbort(t, err)
				if a.Code != CodeConnLost || int(a.Hops) != k-2 {
					t.Fatalf("abort %+v", a)
				}
				waitUntil(t, func() bool { return r.links[0].dm.Sessions() == 0 })
				_ = r.open("y").Close() // the origin's link still carries sessions
			})
		}
	}
}

// blackHole is a conn whose reads and writes stop silently once cut,
// until it is closed.
type blackHole struct {
	net.Conn
	cut    chan struct{}
	closed chan struct{}
	once   sync.Once
}

func (b *blackHole) Read(p []byte) (int, error) {
	n, err := b.Conn.Read(p)
	select {
	case <-b.cut:
		<-b.closed
		return 0, net.ErrClosed
	default:
	}
	return n, err
}

func (b *blackHole) Close() error {
	b.once.Do(func() { close(b.closed) })
	return b.Conn.Close()
}

func (b *blackHole) Write(p []byte) (int, error) {
	select {
	case <-b.cut:
		return len(p), nil
	default:
		return b.Conn.Write(p)
	}
}

// TestChainBlackHole: link k goes silent (no close): the hops beside it
// see it dead within the liveness bound and abort their other side; the
// origin reads CodeConnLost.
func TestChainBlackHole(t *testing.T) {
	if testing.Short() {
		t.Skip("waits a liveness bound")
	}
	cut := make(chan struct{})
	r := newChainRig(t, rigOpts{n: 3, live: LivenessFast, wrap: func(i int, nc net.Conn) net.Conn {
		if i == 2 {
			return &blackHole{Conn: nc, cut: cut, closed: make(chan struct{})}
		}
		return nc
	}}, farEcho)
	s := r.open("x")
	if err := s.Answered(context.Background()); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	close(cut)
	_, err := io.ReadAll(s)
	a := errAbort(t, err)
	bound := LivenessFast.Interval + LivenessFast.Timeout + time.Second
	if a.Code != CodeConnLost || a.Hops != 1 || time.Since(t0) > bound {
		t.Fatalf("abort %+v after %v (bound %v)", a, time.Since(t0), bound)
	}
}

// TestChainStalledFarReader: a far end that reads nothing stalls only
// its session: the origin's writes stop within sum(windows) + (n-1) x
// 256 KiB, and another session through the same hops keeps moving.
func TestChainStalledFarReader(t *testing.T) {
	for n := 1; n <= 4; n++ {
		t.Run(fmt.Sprintf("links=%d", n), func(t *testing.T) {
			stuck := make(chan struct{})
			defer close(stuck)
			r := newChainRig(t, rigOpts{n: n}, func(h []byte, s *Session) {
				if string(h) == "stuck" {
					_ = s.Accept()
					<-stuck
					_ = s.Close()
					return
				}
				farEcho(h, s)
			})
			s := r.open("stuck")
			var sent atomic.Int64
			go func() {
				buf := make([]byte, 32<<10)
				for {
					n, err := s.Write(buf)
					sent.Add(int64(n))
					if err != nil {
						return
					}
				}
			}()
			last := int64(-1)
			for sent.Load() != last {
				last = sent.Load()
				time.Sleep(300 * time.Millisecond)
			}
			bound := int64(n*testWindow + (n-1)*writeBuf)
			t.Logf("in flight: %d bytes (bound %d)", last, bound)
			if last > bound || last < int64(n*testWindow)/2 {
				t.Fatalf("in flight %d, bound %d", last, bound)
			}
			e := r.open("echo")
			roundTrip(t, e, 1<<20)
			_ = s.Close()
		})
	}
}

// TestChainMaxHops: two hops routing to each other (a loop) cost at most
// MaxHops joins, and the origin reads CodeHopLimit from the hop that
// stopped it.
func TestChainMaxHops(t *testing.T) {
	// A <-> B: link ab (A dials B) and link ba (B dials A); the origin dials A.
	mk := func() (*Mux, *Mux) {
		c, s := v2pair(t, Config{Window: testWindow, Auth: testToken}, Config{Window: testWindow, Auth: testToken})
		return NewMux(c), NewMux(s)
	}
	oa, ao := mk() // origin -> A
	ab, ba := mk() // A -> B (ba: B's side)
	bA, aB := mk() // B -> A (aB: A's side)
	cfg := ForwardConfig{MaxHops: 6}
	to := func(m *Mux) Route {
		return func(ctx context.Context, in *Session) (*Session, error) {
			ch, _ := ChainFromContext(ctx)
			return m.OpenChain(in.Header(), ch)
		}
	}
	for _, m := range []*Mux{oa, ab, bA} {
		go func() { _ = m.Serve(nil) }()
	}
	go func() { _ = ao.Serve(Forward(to(ab), cfg)) }()
	go func() { _ = aB.Serve(Forward(to(ab), cfg)) }()
	go func() { _ = ba.Serve(Forward(to(bA), cfg)) }()
	s, err := oa.Open([]byte("loop"))
	if err != nil {
		t.Fatal(err)
	}
	a := errAbort(t, s.Answered(context.Background()))
	if !a.Refused || a.Code != CodeHopLimit || a.Hops != 6 {
		t.Fatalf("abort %+v", a)
	}
	waitUntil(t, func() bool { return ab.Sessions()+bA.Sessions()+oa.Sessions() == 0 })
}

// TestChainDrainMiddleHop: hop k drains its inbound connection: sessions
// joined through it finish intact, new opens through it are refused
// CodeDraining, and at the drain's deadline the rest end with
// CodeDrainDeadline from hop k (Hops k-1).
func TestChainDrainMiddleHop(t *testing.T) {
	for _, k := range []int{2, 3} {
		t.Run(fmt.Sprintf("hop=%d", k), func(t *testing.T) {
			r := newChainRig(t, rigOpts{n: 4}, farEcho)
			running := r.open("running")
			if err := running.Answered(context.Background()); err != nil {
				t.Fatal(err)
			}
			held := r.open("held")
			if err := held.Answered(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			drained := make(chan error, 1)
			go func() { drained <- r.links[k-1].lm.Drain(ctx) }()
			waitUntil(t, func() bool { return r.links[k-1].dm.Conn().GoingAway() })
			a := errAbort(t, r.open("new").Answered(context.Background()))
			if !a.Refused || a.Code != CodeDraining || int(a.Hops) != k-2 {
				t.Fatalf("new open: %+v", a)
			}
			roundTrip(t, running, 1<<20) // finishes intact
			_, err := io.ReadAll(held)
			a = errAbort(t, err)
			if a.Code != CodeDrainDeadline || int(a.Hops) != k-1 {
				t.Fatalf("held: %+v", a)
			}
			<-drained
		})
	}
}

// TestChainMixedLegs: a leg without FeatureChain (a v0.9.4 peer) keeps
// today's semantics on it: Answers false there, and the hop before it
// accepts once its Route returned.
func TestChainMixedLegs(t *testing.T) {
	t.Run("origin leg", func(t *testing.T) {
		r := newChainRig(t, rigOpts{n: 3, noChainAt: func(i int) bool { return i == 0 }}, farEcho)
		s := r.open("x")
		if s.Answers() || !errors.Is(s.Answered(context.Background()), ErrNoAnswers) {
			t.Fatal("answers on a leg without FeatureChain")
		}
		roundTrip(t, s, 1<<20)
		r.idle()
	})
	t.Run("last leg", func(t *testing.T) {
		r := newChainRig(t, rigOpts{n: 3, noChainAt: func(i int) bool { return i == 2 }}, farEcho)
		s := r.open("x")
		if err := s.Answered(context.Background()); err != nil || s.PathLen() != 2 {
			t.Fatalf("%v, PathLen %d", err, s.PathLen())
		}
		roundTrip(t, s, 1<<20)
		r.idle()
	})
	t.Run("refused on the old leg", func(t *testing.T) {
		r := newChainRig(t, rigOpts{n: 2, noChainAt: func(i int) bool { return i == 1 }}, nil)
		s := r.open("x")
		_ = s.Answered(context.Background()) // accepted at hop 1, then reset
		_, err := io.ReadAll(s)
		if a := errAbort(t, err); a.Code != CodeUnspecified || a.Hops != 1 {
			t.Fatalf("abort %+v", a)
		}
	})
}

// TestChainAnswerTimeout: no onward answer within AnswerTimeout: the
// origin reads a refusal CodeTimeout from that hop.
func TestChainAnswerTimeout(t *testing.T) {
	r := newChainRig(t, rigOpts{n: 3, cfg: ForwardConfig{AnswerTimeout: 200 * time.Millisecond}}, func(_ []byte, s *Session) {
		time.Sleep(time.Second)
		_ = s.Close()
	})
	a := errAbort(t, r.open("x").Answered(context.Background()))
	if !a.Refused || a.Code != CodeTimeout || a.Hops > 1 { // hop 1's timer, or hop 2's
		t.Fatalf("abort %+v", a)
	}
}

// TestChainProperty: random paths of 1 to 5 links, random transports,
// one random fault each; bytes intact, the origin's typed outcome, no
// leak (sessions, goroutines).
func TestChainProperty(t *testing.T) {
	runs := 40
	if testing.Short() {
		runs = 10
	}
	base := runtime.NumGoroutine()
	for run := range runs {
		n := 1 + rand.IntN(5)
		fault := rand.IntN(5)
		t.Run(fmt.Sprintf("%d/links=%d/fault=%d", run, n, fault), func(t *testing.T) {
			tlsAt := func(int) bool { return rand.IntN(2) == 0 }
			r := newChainRig(t, rigOpts{n: n, tlsAt: tlsAt}, func(h []byte, s *Session) {
				switch string(h) {
				case "refuse":
					_ = s.Refuse(CodeApp+9, "no")
				case "abort":
					_ = s.Accept()
					_, _ = io.CopyN(io.Discard, s, 1000)
					_ = s.Abort(CodeApp+2, "mid")
				default:
					farEcho(h, s)
				}
			})
			switch fault {
			case 0: // none
				roundTrip(t, r.open("echo"), rand.IntN(4<<20))
			case 1: // refusal at the far end
				a := errAbort(t, r.open("refuse").Answered(context.Background()))
				if !a.Refused || a.Code != CodeApp+9 || int(a.Hops) != n-1 {
					t.Fatalf("%+v", a)
				}
			case 2: // abort mid-session
				s := r.open("abort")
				go func() { _, _ = s.Write(make([]byte, 64<<10)) }()
				_, err := io.ReadAll(s)
				if a := errAbort(t, err); a.Code != CodeApp+2 || int(a.Hops) != n-1 {
					t.Fatalf("%+v", a)
				}
			case 3: // refusal at a random hop
				if n == 1 {
					return
				}
				k := 1 + rand.IntN(n-1)
				r.refuseAt[k] = &Abort{Code: CodeApp + 5}
				a := errAbort(t, r.open("echo").Answered(context.Background()))
				if !a.Refused || a.Code != CodeApp+5 || int(a.Hops) != k-1 {
					t.Fatalf("%+v", a)
				}
			case 4: // kill a hop past the first
				if n == 1 {
					return
				}
				k := 2 + rand.IntN(n-1)
				s := r.open("echo")
				if err := s.Answered(context.Background()); err != nil {
					t.Fatal(err)
				}
				_ = r.links[k-1].lm.Close()
				if k < n {
					_ = r.links[k].dm.Close()
				}
				_, err := io.ReadAll(s)
				if a := errAbort(t, err); a.Code != CodeConnLost || int(a.Hops) != k-2 {
					t.Fatalf("%+v", a)
				}
				return
			}
			r.idle()
		})
	}
	waitUntil(t, func() bool { return runtime.NumGoroutine() <= base+8 })
}

// TestChainIdleCost: joined sessions hold no goroutine while idle; heap
// per joined session through one Forward hop is logged.
func TestChainIdleCost(t *testing.T) {
	r := newChainRig(t, rigOpts{n: 2}, farEcho)
	const k = 200
	warm := r.open("warm")
	roundTrip(t, warm, 1000)
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	g0 := runtime.NumGoroutine()
	ss := make([]*Session, k)
	for i := range ss {
		ss[i] = r.open("idle")
		if err := ss[i].Answered(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, _ = ss[i].Write([]byte("x")) // through and back: both directions ran once
		if _, err := io.ReadFull(ss[i], make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	runtime.ReadMemStats(&m1)
	g1 := runtime.NumGoroutine()
	t.Logf("per idle joined session (origin, one Forward hop, far end): %d goroutines, %d heap bytes",
		(g1-g0)/k, (int64(m1.HeapAlloc)-int64(m0.HeapAlloc))/k)
	if g1-g0 > k/20 {
		t.Fatalf("%d goroutines for %d idle sessions", g1-g0, k)
	}
	for _, s := range ss {
		_ = s.Close()
	}
	r.idle()
}

// TestAbortMsgClipped: Refuse and Abort cut a message to MaxAbortMsg bytes
// at a UTF-8 boundary, and invalid UTF-8 becomes U+FFFD.
func TestAbortMsgClipped(t *testing.T) {
	long := string(bytes.Repeat([]byte("é"), 600)) // 1200 bytes
	if got := clipMsg(long); len(got) != 1024 || got != long[:1024] {
		t.Fatalf("len %d", len(got))
	}
	if got := clipMsg("a\xffb"); got != "a�b" {
		t.Fatalf("%q", got)
	}
	b := appendAbort(nil, &Abort{Code: 1, Msg: long + "x"})
	a, err := parseAbort(b)
	if err != nil || len(a.Msg) != 1024 {
		t.Fatalf("%v %d", err, len(a.Msg))
	}
}
