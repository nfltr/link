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
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/peerlinktest"
	"github.com/nfltr/link/peerlink/quiclink"
	quic "github.com/nfltr/quic-go"
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
// (5.4 s); every session must still end within DeadAfter of the later of
// the cut and the moment the process last ran again after a stop
// (spec/QuicWatch.tla DetectBound: silence counts only while the process
// runs, link's spec/README.md "Starvation"; stopProbe).
func TestDeadPeerSendStalled(t *testing.T) {
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
	go func() {
		_ = b.Serve(func(_ string, _ []byte, s peerlink.Session) { _, _ = io.Copy(s, s); _ = s.Close() })
	}()
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
	stops := startStopProbe(t)
	holed := time.Now()
	px.BlackHole()
	time.AfterFunc(2400*time.Millisecond, stall.resume)
	bound := a.DeadAfter() + 300*time.Millisecond // a timer's slack
	for i, ch := range ended {
		select {
		case at := <-ch:
			from, n, late := stops.since(holed, at)
			d, most := at.Sub(from), bound+2*late // the two fires that decide it (DetectBound's 2 x Late)
			t.Logf("session %d ended %v after the path went silent, %v after the process last ran again (%d stops over %v, timers up to %v late)",
				i, at.Sub(holed).Round(time.Millisecond), d.Round(time.Millisecond), n, stopAfter, late)
			if d > most {
				t.Errorf("session %d ended %v after the later of the cut and the process's last stop, want within %v", i, d.Round(time.Millisecond), most)
			}
		case <-time.After(time.Minute): // a hang: a starved process's bound moves with its stops
			t.Fatalf("session %d still open a minute after the path went silent", i)
		}
	}
}

// stopAfter: a 10 ms sleep that came back later than this was a stop of
// the process (spec/QuicWatch.tla Sleep); less is a running process's
// lateness (Late).
const stopAfter = 100 * time.Millisecond

// stopProbe records when this process ran again after each stop, and the
// worst lateness short of one: a goroutine sleeps 10 ms at a time and
// notes every wake later than stopAfter. On a starved machine (load,
// background priority) the liveness check stops with it.
type stopProbe struct {
	mu   sync.Mutex
	ends []time.Time
	late time.Duration
}

func startStopProbe(t *testing.T) *stopProbe {
	p := &stopProbe{}
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		const step = 10 * time.Millisecond
		for {
			t0 := time.Now()
			select {
			case <-stop:
				return
			case <-time.After(step):
			}
			now := time.Now()
			late := now.Sub(t0) - step
			p.mu.Lock()
			if late > stopAfter {
				p.ends = append(p.ends, now)
			} else {
				p.late = max(p.late, late)
			}
			p.mu.Unlock()
		}
	}()
	return p
}

// since is the later of from and the last stop's end before at (QuicWatch
// DetectBound's Max(rx, since)), how many stops ended in between, and the
// worst lateness short of a stop.
func (p *stopProbe) since(from, at time.Time) (time.Time, int, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, e := range p.ends {
		if e.After(from) && e.Before(at) {
			from, n = e, n+1
		}
	}
	return from, n, p.late
}
