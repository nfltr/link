// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// starvedConn's reads wait while hold is set and its channel open: the
// receive loop of a process that does not run.
type starvedConn struct {
	net.Conn
	hold atomic.Pointer[chan struct{}]
}

func (g *starvedConn) Read(b []byte) (int, error) {
	if h := g.hold.Load(); h != nil {
		<-*h
	}
	return g.Conn.Read(b)
}

// TestStarvedProcessKeepsALivePeer: a process that did not run for longer
// than Timeout after its ping (spec/README.md "Starvation", finding S1):
// the peer's pong waits in the socket, the timer fires late and before the
// receive loop reads it. The connection must live on: the late fire is a
// look, the receive loop reads, the peer answered. Before the fix that
// fire ended the connection with ErrDeadPeer.
func TestStarvedProcessKeepsALivePeer(t *testing.T) {
	l := Liveness{Interval: 2 * time.Second, Timeout: 4 * time.Second}
	a0, b := tcpPair(t)
	a := &starvedConn{Conn: a0}
	sc := make(chan *Conn, 1)
	go func() {
		s, _, err := Server(context.Background(), b, Config{Auth: testToken})
		if err != nil {
			t.Error(err)
		}
		sc <- s
	}()
	c, err := Client(context.Background(), a, Config{Liveness: l, Auth: testToken})
	if err != nil {
		t.Fatal(err)
	}
	s := <-sc
	if s == nil {
		t.FailNow()
	}
	t.Cleanup(func() { _ = c.Close(); _ = s.Close() })
	for _, x := range []*Conn{c, s} {
		go func() { _ = NewMux(x).Serve(nil) }()
	}
	shut := make(chan struct{})
	a.hold.Store(&shut) // the next Read waits (the one in flight reads on)
	released := make(chan struct{})
	h := func(fc *Conn, pingAt time.Duration) {
		if fc != c || pingAt < 0 {
			return
		}
		select {
		case <-released:
			return
		default:
		}
		// The process does not run until well past the ping's Timeout;
		// its receive loop runs again just after this fire.
		time.Sleep(time.Until(c.t0.Add(pingAt + l.Timeout + 300*time.Millisecond)))
		close(released)
		go func() { time.Sleep(50 * time.Millisecond); close(shut) }()
	}
	testHookLiveFire.Store(&h)
	t.Cleanup(func() { testHookLiveFire.Store(nil) })
	select {
	case <-released:
	case <-c.Done():
		t.Fatalf("ended before the starvation: %v", c.Err())
	case <-time.After(3 * (l.Interval + l.Timeout)):
		t.Fatal("no fire with a ping waiting")
	}
	select {
	case <-c.Done():
		t.Fatalf("a live peer taken for dead after the process ran again: %v", c.Err())
	case <-time.After(l.Interval + l.Timeout/2):
	}
}
