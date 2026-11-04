// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
	quic "github.com/nfltr/quic-go"
)

// delayConn sends every packet d late: a path with a round trip of d.
type delayConn struct {
	net.PacketConn
	d time.Duration
}

func (c *delayConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	p := append([]byte(nil), b...)
	time.AfterFunc(c.d, func() { _, _ = c.PacketConn.WriteTo(p, addr) })
	return len(b), nil
}

// TestSlowPathKeepsALiveIdlePeer: an idle connection on a path with a
// round trip of 0.8 s, under LivenessFast's Timeout/2 (1.5 s). The first
// RTT sample sets rttvar to half of it, so the probe timeout is about 2 s
// and quic-go sends its keep-alive 1.5 PTO (3 s) after the last packet,
// not Interval (finding QW1, spec/QuicWatch.tla, QuicWatchPTO.cfg). The
// watch must wait for the answer. Before the fix it closed the live peer
// (CodeDeadPeer) 3 s in.
func TestSlowPathKeepsALiveIdlePeer(t *testing.T) {
	t.Setenv("QUIC_GO_DISABLE_RECEIVE_BUFFER_WARNING", "true") // delayConn is not a *net.UDPConn
	l := link.LivenessFast
	pc := listenUDP(t)
	tr := &quic.Transport{Conn: &delayConn{PacketConn: listenUDP(t), d: 800 * time.Millisecond}}
	t.Cleanup(func() { _ = tr.Close() })
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: l})
	a := mustNew(t, quiclink.Config{Liveness: l,
		Dial: func(ctx context.Context, _ string, qc *quic.Config) (*quic.Conn, error) {
			return tr.Dial(ctx, pc.LocalAddr(), clientTLS("a", "b"), qc)
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
	s, err := a.Open(ctx, "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(s, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	ended := make(chan error, 1)
	go func() { _, err := s.Read(make([]byte, 1)); ended <- err }()
	select {
	case err := <-ended:
		t.Fatalf("a live idle peer taken for dead: %v", err)
	case <-time.After(l.Interval + l.Timeout):
	}
}
