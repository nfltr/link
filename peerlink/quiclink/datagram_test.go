// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
	quic "github.com/nfltr/quic-go"
)

// dgPair is a session's two ends between Nodes a (listening) and b
// (dialling), with Datagrams as given; b opened them.
func dgPair(t testing.TB, aDG, bDG bool, n int) (a, b *quiclink.Node, opened, accepted []*quiclink.Session) {
	t.Helper()
	pc := listenUDP(t)
	a = mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("a"), Liveness: link.LivenessFast, Datagrams: aDG})
	b = mustNew(t, quiclink.Config{Dial: dialer("b", "a", pc.LocalAddr().String()), Liveness: link.LivenessFast, Datagrams: bDG})
	acc := make(chan *quiclink.Session, n)
	go func() { _ = a.Serve(func(_ string, _ []byte, s peerlink.Session) { acc <- s.(*quiclink.Session) }) }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for range n {
		s, err := b.Open(ctx, "a", nil)
		if err != nil {
			t.Fatal(err)
		}
		opened = append(opened, s.(*quiclink.Session))
		select {
		case s := <-acc:
			accepted = append(accepted, s)
		case <-ctx.Done():
			t.Fatal("no session accepted")
		}
	}
	t.Cleanup(func() {
		for _, s := range append(opened, accepted...) {
			_ = s.Reset()
		}
		_ = b.Drain(context.Background())
		_ = a.Drain(context.Background())
	})
	return a, b, opened, accepted
}

func recv(t *testing.T, s *quiclink.Session) chan []byte {
	ch := make(chan []byte, 16)
	s.OnDatagram(func(p []byte) {
		select {
		case ch <- append([]byte(nil), p...):
		default:
			t.Error("receiver full")
		}
	})
	return ch
}

func want(t *testing.T, ch chan []byte, w string) {
	t.Helper()
	select {
	case p := <-ch:
		if string(p) != w {
			t.Fatalf("got %q, want %q", p, w)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no datagram %q", w)
	}
}

// TestSessionDatagrams: each end's datagrams reach the peer's session of
// the same stream, never another session of the connection; an ended
// session's tag is dropped and counted; a payload too large for a packet
// says how much fits, its tag counted.
func TestSessionDatagrams(t *testing.T) {
	a, _, opened, accepted := dgPair(t, true, true, 2)
	if quiclink.StreamID(opened[0]) != quiclink.StreamID(accepted[0]) {
		t.Fatalf("stream ids %d, %d: the tag is the stream id at both ends", quiclink.StreamID(opened[0]), quiclink.StreamID(accepted[0]))
	}
	in0, in1, back0 := recv(t, accepted[0]), recv(t, accepted[1]), recv(t, opened[0])
	for _, w := range []string{"zero", "one"} {
		s := opened[0]
		if w == "one" {
			s = opened[1]
		}
		if err := s.SendDatagram([]byte(w)); err != nil {
			t.Fatal(err)
		}
	}
	want(t, in0, "zero")
	want(t, in1, "one")
	if err := accepted[0].SendDatagram([]byte("back")); err != nil {
		t.Fatal(err)
	}
	want(t, back0, "back")

	err := opened[0].SendDatagram(make([]byte, 64<<10))
	tl, ok := errors.AsType[*quic.DatagramTooLargeError](err)
	if !ok || tl.MaxDatagramPayloadSize <= 0 {
		t.Fatalf("a 64 KiB datagram: %v, want *quic.DatagramTooLargeError", err)
	}
	if err := opened[0].SendDatagram(make([]byte, tl.MaxDatagramPayloadSize)); err != nil {
		t.Fatalf("a datagram of the size the error names (%d): %v", tl.MaxDatagramPayloadSize, err)
	}
	<-in0

	_ = accepted[1].Reset() // its tag is gone at a
	before := quiclink.DatagramsDroppedForTest(a)
	if err := opened[1].SendDatagram([]byte("late")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for quiclink.DatagramsDroppedForTest(a) == before {
		if time.Now().After(deadline) {
			t.Fatal("a datagram for an ended session was not counted dropped")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case p := <-in1:
		t.Fatalf("an ended session got %q", p)
	default:
	}
	if err := accepted[1].SendDatagram([]byte("x")); !errors.Is(err, quiclink.ErrNoDatagramsForTest) {
		t.Fatalf("SendDatagram on a reset session: %v, want ErrNoDatagrams", err)
	}
}

// TestSessionDatagramsNeedBothEnds: a peer that does not offer datagrams
// (an older build) gets none: SendDatagram fails at once (the caller uses
// the stream), at either end.
func TestSessionDatagramsNeedBothEnds(t *testing.T) {
	for _, c := range []struct{ a, b bool }{{true, false}, {false, true}} {
		_, _, opened, accepted := dgPair(t, c.a, c.b, 1)
		for _, s := range []*quiclink.Session{opened[0], accepted[0]} {
			if err := s.SendDatagram([]byte("x")); !errors.Is(err, quiclink.ErrNoDatagramsForTest) {
				t.Fatalf("a=%v b=%v: SendDatagram %v, want ErrNoDatagrams", c.a, c.b, err)
			}
		}
	}
}

// BenchmarkSessionDatagrams: one session's datagrams of 1200 bytes, one
// way over loopback, as fast as the sender goes; the op is one datagram
// sent, delivered% the share that arrived (datagrams are lossy: a
// receiver that falls behind loses them, never the sender waits).
func BenchmarkSessionDatagrams(b *testing.B) {
	_, _, opened, accepted := dgPair(b, true, true, 1)
	var got atomic.Int64
	accepted[0].OnDatagram(func([]byte) { got.Add(1) })
	msg := make([]byte, 1200)
	b.SetBytes(int64(len(msg)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := opened[0].SendDatagram(msg); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	time.Sleep(100 * time.Millisecond) // the last ones in flight
	b.ReportMetric(100*float64(got.Load())/float64(b.N), "delivered%")
}
