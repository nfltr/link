// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// brokenWrites fails its writes once broken, its reads unchanged: a
// connection the peer reset, before its reader saw the end.
type brokenWrites struct {
	net.Conn
	broken *atomic.Bool
}

func (c brokenWrites) Write(b []byte) (int, error) {
	if c.broken.Load() {
		return 0, errors.New("write: connection reset by peer")
	}
	return c.Conn.Write(b)
}

// TestPoolOpenPassesABrokenConnection: Open on a Pool whose only
// connection broke (its writes fail, its reader has not seen the end yet)
// dials a new connection and opens there, instead of failing with the
// broken one's write error; Max (1) does not count the broken one.
func TestPoolOpenPassesABrokenConnection(t *testing.T) {
	addr, _, accepted := echoListener(t)
	var first atomic.Bool
	var dials atomic.Int32
	p := mustPool(t, PoolConfig{
		Link: v2cfg,
		Dial: func(ctx context.Context) (net.Conn, error) {
			nc, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			if dials.Add(1) == 1 {
				return brokenWrites{nc, &first}, nil
			}
			return nc, nil
		},
	})
	defer p.Close() //nolint:errcheck
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := p.Open(ctx, []byte("x")) // the first connection is up
	if err != nil {
		t.Fatal(err)
	}
	echoOK(t, s)
	_ = s.Close()
	first.Store(true) // its writes fail from now on; its reader runs on
	s, err = p.Open(ctx, []byte("x"))
	if err != nil {
		t.Fatalf("Open with the pool's connection broken: %v", err)
	}
	echoOK(t, s)
	_ = s.Close()
	if n := accepted.Load(); n != 2 {
		t.Fatalf("%d connections, want a second one", n)
	}
}

// pipeDial is a Pool's Dial to an in-process peer whose sessions close at
// once; each dial waits for release, and dials counts them.
func pipeDial(release <-chan struct{}, dials *atomic.Int32) func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) {
		dials.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		a, b := net.Pipe()
		go func() {
			c, _, err := Server(context.Background(), b, v2cfg)
			if err != nil {
				_ = b.Close()
				return
			}
			_ = NewMux(c).Serve(func(_ []byte, s *Session) { _ = s.Close() })
		}()
		return a, nil
	}
}

// TestPoolMaxHoldsUnderConcurrentOpens: Opens on an empty Pool of Max 1,
// all past pick before any dial ends, make one dial and wait for it. Each
// once dialled when its pick ran before the first dial counted itself
// (pick now reserves the dial's slot); rounds, as the window is narrow.
func TestPoolMaxHoldsUnderConcurrentOpens(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for round := range 300 {
			release, start := make(chan struct{}), make(chan struct{})
			var dials atomic.Int32
			p := mustPool(t, PoolConfig{Link: v2cfg, Max: 1, Dial: pipeDial(release, &dials)})
			var wg sync.WaitGroup
			for range 64 {
				wg.Go(func() {
					<-start
					s, err := p.Open(context.Background(), []byte("x"))
					if err != nil {
						t.Error(err)
						return
					}
					_ = s.Close()
				})
			}
			synctest.Wait()
			close(start)
			synctest.Wait() // every Open in a dial or waiting for one
			n := dials.Load()
			close(release)
			wg.Wait()
			st := p.Stats()
			_ = p.Close()
			if n != 1 || st.Conns > 1 {
				t.Fatalf("round %d: Pool Max 1 dialled %d connections at once and holds %d", round, n, st.Conns)
			}
		}
	})
}

// TestPoolMinReplacesAConnectionGoingAway: the peer sends GOAWAY on the
// Pool's only connection while a session runs on it. The Pool dials Min's
// replacement at once, not once that session ends, which may be never
// (spec/Pool.tla MinHeld; the mutant no-wake-on-goaway is the code before).
func TestPoolMinReplacesAConnectionGoingAway(t *testing.T) {
	addr, muxes, accepted := echoListener(t)
	p := mustPool(t, PoolConfig{
		Link: v2cfg,
		Dial: func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "tcp", addr) },
		Min:  1,
	})
	defer p.Close() //nolint:errcheck
	s, err := p.Open(context.Background(), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	echoOK(t, s)
	for _, m := range muxes() {
		_ = m.goAway(time.Now().Add(time.Minute))
	}
	waitUntil(t, func() bool { return accepted.Load() == 2 && p.Stats().Conns == 1 })
	echoOK(t, s) // the old session runs on
	_ = s.Close()
}

// TestPoolDrainStaysWithinMax: Drain replaces only connections that take
// sessions, one replacement each at most. Here the only connection is
// already going away and an Open's dial holds Max's room: Drain dials
// nothing, and the Pool never has more than Max connections taking
// sessions (spec/Pool.tla MaxHolds; the mutant drain-past-max is the code
// before, which dialled a replacement past Max).
func TestPoolDrainStaysWithinMax(t *testing.T) {
	addr, muxes, _ := echoListener(t)
	var dials atomic.Int32
	hold := make(chan struct{})
	p := mustPool(t, PoolConfig{
		Link: v2cfg,
		Dial: func(ctx context.Context) (net.Conn, error) {
			if dials.Add(1) == 2 {
				<-hold
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		},
		Max: 1,
	})
	defer p.Close() //nolint:errcheck
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s1, err := p.Open(ctx, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	echoOK(t, s1) // the listener's Mux serves it: muxes() has it
	for _, m := range muxes() {
		_ = m.goAway(time.Now().Add(time.Minute))
	}
	waitUntil(t, func() bool { return p.Stats().Conns == 0 })
	opened := make(chan *Session, 1)
	go func() {
		s, err := p.Open(ctx, []byte("x"))
		if err != nil {
			t.Error(err)
		}
		opened <- s
	}()
	waitUntil(t, func() bool { return dials.Load() == 2 })
	drained := make(chan error, 1)
	go func() { drained <- p.Drain(ctx) }()
	_ = s1.Close()
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	close(hold)
	s2 := <-opened
	if s2 == nil {
		t.FailNow()
	}
	defer s2.Close() //nolint:errcheck
	echoOK(t, s2)
	if n, st := dials.Load(), p.Stats(); n != 2 || st.Conns != 1 {
		t.Fatalf("%d dials, %d connections taking sessions; want 2 and Max (1)", n, st.Conns)
	}
}

// scriptDial is a Pool's Dial whose n-th call (from 1) does script(n): nil
// dials the in-process peer of pipeDial, an error fails the dial.
func scriptDial(dials *atomic.Int32, script func(n int32) error) func(context.Context) (net.Conn, error) {
	pipe := pipeDial(closedChan(), new(atomic.Int32))
	return func(ctx context.Context) (net.Conn, error) {
		if err := script(dials.Add(1)); err != nil {
			return nil, err
		}
		return pipe(ctx)
	}
}

func closedChan() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

var errDialRefused = errors.New("dial: connection refused")

// TestPoolOpenWaitsOnTheChangeItsPickSaw (finding H1, spec/Pool.tla
// mutant wait-takes-late): pick decides to wait while a dial holds Max's
// room; that dial ends before Open waits. Open waits on the p.changed pick
// read under p.mu, which the dial closed, not on the next one, which
// nothing may ever close.
func TestPoolOpenWaitsOnTheChangeItsPickSaw(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var dials atomic.Int32
		p := mustPool(t, PoolConfig{Link: v2cfg, Max: 1, Dial: scriptDial(&dials, func(int32) error { return nil })})
		defer p.Close() //nolint:errcheck
		p.reserve()     // a dial in flight holds Max's room
		m, grow, _, ch, err := p.pick()
		if m != nil || grow || err != nil {
			t.Fatalf("pick: %v, %v, %v; want a wait", m, grow, err)
		}
		if _, err := p.dial(context.Background()); err != nil { // it ends before Open waits
			t.Fatal(err)
		}
		select {
		case <-ch:
		default:
			t.Fatal("the dial that ended after pick closed no channel Open waits on")
		}
	})
}

// TestPoolOpenFailsWithTheDialErrorWhileMaintainDials (finding H2, mutant
// no-wake-on-dial-fail): Backoff nil, the peer unreachable, and Open waits
// for maintain's dial, which holds Max's room. That dial fails: Open looks
// again, dials itself and fails with the dial's error, not at its ctx.
func TestPoolOpenFailsWithTheDialErrorWhileMaintainDials(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		var dials atomic.Int32
		p := mustPool(t, PoolConfig{Link: v2cfg, Min: 1, Max: 1, Dial: scriptDial(&dials, func(n int32) error {
			if n == 1 {
				<-gate
			}
			return errDialRefused
		})})
		defer p.Close() //nolint:errcheck
		synctest.Wait() // maintain's dial in flight
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		opened := make(chan error, 1)
		go func() { _, err := p.Open(ctx, []byte("x")); opened <- err }()
		synctest.Wait() // Open waits for it
		close(gate)
		if err := <-opened; !errors.Is(err, errDialRefused) {
			t.Fatalf("Open: %v; want the dial's error", err)
		}
	})
}

// TestPoolMinRedialsAfterAnOpensDialFails (finding H2, mutant
// no-wake-on-dial-fail): maintain waits because an Open's dial counts
// toward Min; that dial fails and the Open returns its error (Backoff nil).
// maintain looks again and dials: the Pool gets back to Min.
func TestPoolMinRedialsAfterAnOpensDialFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		var dials atomic.Int32
		p := mustPool(t, PoolConfig{Link: v2cfg, Min: 1, Max: 1, Dial: scriptDial(&dials, func(n int32) error {
			switch n {
			case 1: // maintain's: it sleeps a second
				return errDialRefused
			case 2: // the Open's
				<-gate
				return errDialRefused
			}
			return nil
		})})
		defer p.Close() //nolint:errcheck
		synctest.Wait()
		opened := make(chan error, 1)
		go func() { _, err := p.Open(context.Background(), []byte("x")); opened <- err }()
		synctest.Wait()
		time.Sleep(2 * time.Second) // maintain wakes, sees the Open's dial, waits
		synctest.Wait()
		close(gate)
		if err := <-opened; !errors.Is(err, errDialRefused) {
			t.Fatalf("Open: %v; want the dial's error", err)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if st := p.Stats(); st.Conns != 1 {
			t.Fatalf("%d connections after %d dials; want Min (1)", st.Conns, dials.Load())
		}
	})
}
