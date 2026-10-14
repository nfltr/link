// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink/peerlinktest"
	"github.com/nfltr/link/peerlink/quiclink"
	"github.com/quic-go/quic-go"
)

// stallConn is a PacketConn whose writes wait while stalled, as a send
// queue stuck behind a full socket buffer does.
type stallConn struct {
	net.PacketConn
	mu      sync.Mutex
	stalled chan struct{} // non-nil: closed when writes may go on
}

func (c *stallConn) stall() {
	c.mu.Lock()
	c.stalled = make(chan struct{})
	c.mu.Unlock()
}

func (c *stallConn) resume() {
	c.mu.Lock()
	close(c.stalled)
	c.stalled = nil
	c.mu.Unlock()
}

func (c *stallConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	c.mu.Lock()
	ch := c.stalled
	c.mu.Unlock()
	if ch != nil {
		<-ch
	}
	return c.PacketConn.WriteTo(b, addr)
}

// TestDeadPeerSendStalled: the path goes silent while the dialer cannot
// send (its writes stalled from just before, for 2.4 s). QUIC's idle timer
// runs from the first packet sent after the last one received, so the
// dialer, which sent none, would time out 3 s after its writes resumed
// (5.4 s); every session must still end within DeadAfter.
func TestDeadPeerSendStalled(t *testing.T) {
	t.Setenv("QUIC_GO_DISABLE_RECEIVE_BUFFER_WARNING", "true") // stallConn is not a *net.UDPConn
	pc := listenUDP(t)
	px := peerlinktest.NewUDPProxy(t, pc.LocalAddr().String())
	stall := &stallConn{PacketConn: listenUDP(t)}
	tr := &quic.Transport{Conn: stall}
	t.Cleanup(func() { _ = tr.Close() })
	raddr, err := net.ResolveUDPAddr("udp", px.Addr())
	if err != nil {
		t.Fatal(err)
	}
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: link.LivenessFast})
	a := mustNew(t, quiclink.Config{Liveness: link.LivenessFast,
		Dial: func(ctx context.Context, _ string, qc *quic.Config) (*quic.Conn, error) {
			return tr.Dial(ctx, raddr, clientTLS("a", "b"), qc)
		}})
	go func() { _ = b.Serve(func(_ string, _ []byte, s net.Conn) { _, _ = io.Copy(s, s); _ = s.Close() }) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = a.Drain(ctx)
		_ = b.Drain(ctx)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var ended []chan time.Time
	for i := range 3 {
		s, err := a.Open(ctx, "b", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if i == 0 { // busy both ways
			go func() { _, _ = io.Copy(s, rand.Reader) }()
		}
		ch := make(chan time.Time, 1)
		go func() { _, _ = io.Copy(io.Discard, s); ch <- time.Now() }()
		ended = append(ended, ch)
	}
	time.Sleep(time.Second)
	stall.stall()
	time.Sleep(100 * time.Millisecond)
	holed := time.Now()
	px.BlackHole()
	time.AfterFunc(2400*time.Millisecond, stall.resume)
	bound := a.DeadAfter() + 300*time.Millisecond // a timer's slack
	for i, ch := range ended {
		select {
		case at := <-ch:
			d := at.Sub(holed)
			t.Logf("session %d ended %v after the path went silent", i, d.Round(time.Millisecond))
			if d > bound {
				t.Errorf("session %d ended %v after the path went silent, want within %v", i, d.Round(time.Millisecond), bound)
			}
		case <-time.After(time.Until(holed.Add(2 * bound))):
			t.Fatalf("session %d still open %v after the path went silent", i, 2*bound)
		}
	}
}
