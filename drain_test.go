// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pattern is n bytes of a session's stream: byte i is i mod 251 + seed.
func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i%251) + seed
	}
	return b
}

// TestGoAwayDrainKeepsTails: sessions open on a connection that goes
// away finish intact, the last one too: the connection closes only after
// their tails and CLOSE frames went out.
func TestGoAwayDrainKeepsTails(t *testing.T) {
	const n, sessions = 2 << 20, 8
	release := make(chan struct{})
	accept := func(h []byte, s *Session) {
		<-release
		_, _ = s.Write(pattern(n, h[0]))
		_ = s.Close()
	}
	dm, lm := v2muxPair(t, accept, Config{Auth: testToken}, Config{Auth: testToken})
	var ss []*Session
	for i := range sessions {
		s, err := openV2(dm, string(rune('a'+i)))
		if err != nil {
			t.Fatal(err)
		}
		ss = append(ss, s)
	}
	waitUntil(t, func() bool { return lm.Sessions() == sessions })
	if err := lm.GoAway(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	close(release)
	var wg sync.WaitGroup
	var intact atomic.Int32
	for i, s := range ss {
		wg.Go(func() {
			b, err := io.ReadAll(s)
			if err == nil && bytes.Equal(b, pattern(n, byte('a'+i))) {
				intact.Add(1)
			} else {
				t.Logf("session %d: %d bytes, %v", i, len(b), err)
			}
		})
	}
	wg.Wait()
	if intact.Load() != sessions {
		t.Fatalf("%d of %d sessions intact", intact.Load(), sessions)
	}
	select {
	case <-lm.c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the leaving connection did not close")
	}
}

// TestCrossedOpenRunsOnTheLeavingConnection: an OPEN the peer sent before
// it read this side's GOAWAY is a session like the others (never refused):
// it runs, and the connection closes once it ended.
func TestCrossedOpenRunsOnTheLeavingConnection(t *testing.T) {
	c, s := v2pair(t, v2cfg, v2cfg)
	dm, lm := NewMux(c, nil), NewMux(s, nil)
	go func() { _ = dm.Serve(nil) }()
	sess, err := openV2(dm, "x") // waits unread at the listener
	if err != nil {
		t.Fatal(err)
	}
	if err := lm.GoAway(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	go func() { _ = lm.Serve(v2echo) }()
	if _, err := sess.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 2)
	if _, err := io.ReadFull(sess, b); err != nil || string(b) != "hi" {
		t.Fatalf("crossed session: %q %v", b, err)
	}
	if _, err := openV2(dm, "y"); err != ErrGoAway {
		t.Fatalf("open after GOAWAY: %v", err)
	}
	_ = sess.Close()
	select {
	case <-lm.c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the leaving connection did not close after its last session")
	}
}

// echoListener serves v2echo on a TCP listener and records each Mux.
func echoListener(t *testing.T) (addr string, muxes func() []*Mux, accepted *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var mu sync.Mutex
	var ms []*Mux
	accepted = new(atomic.Int32)
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				c, _, err := Server(context.Background(), nc, v2cfg)
				if err != nil {
					_ = nc.Close()
					return
				}
				accepted.Add(1)
				m := NewMux(c, nil)
				mu.Lock()
				ms = append(ms, m)
				mu.Unlock()
				t.Cleanup(func() { _ = m.Close() })
				_ = m.Serve(v2echo)
			}()
		}
	}()
	return ln.Addr().String(), func() []*Mux {
		mu.Lock()
		defer mu.Unlock()
		return append([]*Mux(nil), ms...)
	}, accepted
}

func echoOK(t *testing.T, s *Session) {
	t.Helper()
	if _, err := s.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 4)
	if _, err := io.ReadFull(s, b); err != nil || string(b) != "ping" {
		t.Fatalf("echo %q %v", b, err)
	}
}

// TestPoolReplacesConnectionsGoingAway: a Pool at Max whose every
// connection is going away opens new sessions on new connections (going
// away ones do not count against Max), while the old sessions run on.
func TestPoolReplacesConnectionsGoingAway(t *testing.T) {
	addr, muxes, accepted := echoListener(t)
	p := NewPool(PoolConfig{
		Link: v2cfg,
		Dial: func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "tcp", addr) },
		Max:  2,
		Grow: func(st PoolStats) bool { return st.Sessions >= 2*st.Conns },
	})
	defer p.Close() //nolint:errcheck
	ctx := context.Background()
	var old []*Session
	for range 4 {
		s, err := p.Open(ctx, []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		echoOK(t, s)
		old = append(old, s)
	}
	waitUntil(t, func() bool { return accepted.Load() == 2 })
	for _, m := range muxes() {
		_ = m.GoAway(time.Now().Add(30 * time.Second))
	}
	waitUntil(t, func() bool { return p.Stats().Conns == 0 })
	for range 4 {
		s, err := p.Open(ctx, []byte("x"))
		if err != nil {
			t.Fatalf("open with every connection going away: %v", err)
		}
		echoOK(t, s)
		defer s.Close() //nolint:errcheck
	}
	for _, s := range old {
		echoOK(t, s)
		_ = s.Close()
	}
	if accepted.Load() != 4 {
		t.Fatalf("%d connections, want 2 new ones", accepted.Load())
	}
}

// TestPoolGrowsInTheBackground: a growth dial holds up the Open that
// started it for GrowWait at most, which then takes a connection that is
// up; the new connection takes the next sessions once up.
func TestPoolGrowsInTheBackground(t *testing.T) {
	addr, _, accepted := echoListener(t)
	var dials atomic.Int32
	hold := make(chan struct{})
	p := NewPool(PoolConfig{
		Link: v2cfg,
		Dial: func(ctx context.Context) (net.Conn, error) {
			if dials.Add(1) == 2 {
				select {
				case <-hold:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		},
		Max:      2,
		Grow:     func(st PoolStats) bool { return st.Sessions >= st.Conns },
		GrowWait: 200 * time.Millisecond,
	})
	defer p.Close() //nolint:errcheck
	s1, err := p.Open(context.Background(), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	echoOK(t, s1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	s2, err := p.Open(ctx, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Open waited %v for a growth dial", d)
	}
	echoOK(t, s2)
	if dials.Load() != 2 {
		t.Fatalf("%d dials, want the growth dial started", dials.Load())
	}
	close(hold)
	waitUntil(t, func() bool { return accepted.Load() == 2 && p.Stats().Conns == 2 })
	s3, err := p.Open(context.Background(), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	echoOK(t, s3)
	if st := p.Stats(); st.Conns != 2 || st.Sessions != 3 {
		t.Fatalf("stats %+v", st)
	}
}
