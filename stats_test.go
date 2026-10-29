// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStatsCountTheWire: a connection's BytesOut and BytesIn are exactly
// the bytes written to and read from its net.Conn after the handshake
// (BenchmarkThroughput's workload, both ways), and SessionsOpened counts
// the sessions on each side.
func TestStatsCountTheWire(t *testing.T) {
	r := newPerfRig(t, "tcp", perfSink)
	msg := make([]byte, 64<<10)
	var wg sync.WaitGroup
	for _, m := range []*Mux{r.a, r.b} {
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s, err := m.Open([]byte("sink"))
				if err != nil {
					t.Error(err)
					return
				}
				for range 16 {
					if _, err := s.Write(msg); err != nil {
						t.Error(err)
						return
					}
				}
				_ = s.CloseWrite()
			}()
		}
	}
	wg.Wait()
	payload := uint64(2 * 8 * 16 * len(msg))
	waitUntil(t, func() bool {
		a, b := r.a.c.Stats(), r.b.c.Stats()
		return a.BytesOut == uint64(r.la.wbytes.Load()) && b.BytesIn == uint64(r.lb.rbytes.Load()) &&
			b.BytesOut == uint64(r.lb.wbytes.Load()) && a.BytesIn == uint64(r.la.rbytes.Load()) &&
			a.BytesOut == b.BytesIn && b.BytesOut == a.BytesIn && a.BytesOut+b.BytesOut > payload
	})
	for _, c := range []*Conn{r.a.c, r.b.c} {
		if st := c.Stats(); st.SessionsOpened != 16 {
			t.Errorf("SessionsOpened %d, want 16 (8 each way)", st.SessionsOpened)
		}
	}
}

// TestStatsCreditAndQueue: a session whose reader takes nothing holds its
// window (QueuedBytes) and its sender waits for credit (CreditWaits,
// CreditWait, the Observer's CreditWait); once read, nothing is queued.
func TestStatsCreditAndQueue(t *testing.T) {
	read := make(chan struct{})
	got := make(chan int, 1)
	accept := func(_ []byte, s *Session) {
		defer s.Close() //nolint:errcheck
		<-read
		n, _ := io.Copy(io.Discard, s)
		got <- int(n)
	}
	obs := &statsObserver{}
	dcfg := v2cfg
	dcfg.Observer = obs
	dm, lm := v2muxPair(t, accept, dcfg, v2cfg)
	s, err := dm.Open([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	const n = 3 * testWindow
	go func() {
		_, _ = s.Write(make([]byte, n))
		_ = s.CloseWrite()
	}()
	waitUntil(t, func() bool { return lm.c.Stats().QueuedBytes == testWindow })
	time.Sleep(20 * time.Millisecond) // the sender waits meanwhile
	close(read)
	if g := <-got; g != n {
		t.Fatalf("read %d, want %d", g, n)
	}
	waitUntil(t, func() bool { return lm.c.Stats().QueuedBytes == 0 })
	st := dm.c.Stats()
	if st.CreditWaits == 0 || st.CreditWait < 20*time.Millisecond {
		t.Errorf("CreditWaits %d, CreditWait %v: want a wait of at least 20ms", st.CreditWaits, st.CreditWait)
	}
	if w := obs.waits.Load(); uint64(w) != st.CreditWaits {
		t.Errorf("Observer saw %d credit waits, Stats %d", w, st.CreditWaits)
	}
}

// TestSessionAbortObserved: an ABORT, sent or received, is the Observer's
// SessionAbort with its code (and who sent it) and counts in
// SessionsAborted on both ends.
func TestSessionAbortObserved(t *testing.T) {
	accepted := make(chan *Session, 1)
	dobs, lobs := &statsObserver{}, &statsObserver{}
	dcfg, lcfg := v2cfg, v2cfg
	dcfg.Observer, lcfg.Observer = dobs, lobs
	dm, lm := v2muxPair(t, func(_ []byte, s *Session) { accepted <- s; <-s.done }, dcfg, lcfg)
	s, err := dm.Open([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	<-accepted
	if err := s.Abort(CodeApp+7, "why"); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return lm.c.Stats().SessionsAborted == 1 })
	if a := dm.c.Stats().SessionsAborted; a != 1 {
		t.Errorf("sender SessionsAborted %d, want 1", a)
	}
	for _, c := range []struct {
		o    *statsObserver
		peer bool
	}{{dobs, false}, {lobs, true}} {
		if c.o.abortCode.Load() != CodeApp+7 || c.o.abortPeer.Load() != c.peer {
			t.Errorf("SessionAbort code %d byPeer %v, want %d %v", c.o.abortCode.Load(), c.o.abortPeer.Load(), CodeApp+7, c.peer)
		}
	}
}

// TestCreditObserver: a Credit made with NewCredit (a caller's own
// transport) reports a sender's wait to its observer.
func TestCreditObserver(t *testing.T) {
	c := NewCredit(MaxChunk, MaxChunk, func(string, int) {})
	obs := &statsObserver{}
	c.SetObserver(obs)
	c.Open("s")
	if !c.Acquire("s", MaxChunk) {
		t.Fatal("Acquire")
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = c.Grant("s", MaxChunk)
	}()
	if !c.Acquire("s", 1) {
		t.Fatal("Acquire after the grant")
	}
	if obs.waits.Load() != 1 {
		t.Errorf("CreditWait events %d, want 1", obs.waits.Load())
	}
}

// TestPoolConnStats: a Pool's ConnStats sums its connections, ended ones
// included, so its counters never go back.
func TestPoolConnStats(t *testing.T) {
	addr, _, _ := echoListener(t)
	p := mustPool(t, PoolConfig{Link: v2cfg, Dial: func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}})
	s, err := p.Open(context.Background(), []byte("echo"))
	if err != nil {
		t.Fatal(err)
	}
	echoOK(t, s)
	before := p.ConnStats()
	if before.BytesOut == 0 || before.BytesIn == 0 || before.SessionsOpened != 1 {
		t.Fatalf("ConnStats %+v: want bytes both ways and one session", before)
	}
	p.mu.Lock()
	m := p.conns[0]
	p.mu.Unlock()
	_ = m.Close()
	waitUntil(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.conns) == 0 })
	if after := p.ConnStats(); after.BytesOut < before.BytesOut || after.SessionsOpened != 1 {
		t.Errorf("after the connection ended: %+v, before %+v", after, before)
	}
}

// TestStatsNoAllocs: reading the counters allocates nothing.
func TestStatsNoAllocs(t *testing.T) {
	c, _ := v2pair(t, v2cfg, v2cfg)
	if n := testing.AllocsPerRun(100, func() { _ = c.Stats() }); n != 0 {
		t.Errorf("Stats: %v allocations, want 0", n)
	}
}

type statsObserver struct {
	NopObserver
	waits     atomic.Int32
	abortCode atomic.Uint32
	abortPeer atomic.Bool
}

func (o *statsObserver) CreditWait(time.Duration) { o.waits.Add(1) }
func (o *statsObserver) SessionAbort(a Abort, byPeer bool) {
	o.abortCode.Store(a.Code)
	o.abortPeer.Store(byPeer)
}
