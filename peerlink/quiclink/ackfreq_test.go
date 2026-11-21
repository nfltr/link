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
	"github.com/nfltr/quic-go"
)

// TestNodeAsksForAckFrequency: a Node's connections ask their peer for
// quiclink.DefaultAckPolicy (an ACK every 10 packets, none for reordering
// within 10: spec/AckFrequency.tla) or Config.Acks.
func TestNodeAsksForAckFrequency(t *testing.T) {
	dial := func(context.Context, string, *quic.Config) (*quic.Conn, error) { return nil, nil }
	n := mustNew(t, quiclink.Config{Dial: dial})
	if got, want := quiclink.AckFrequency(n), (quic.AckFrequency{AckElicitingThreshold: 9, ReorderingThreshold: 10}); got == nil || *got != want {
		t.Fatalf("AckFrequency = %+v, want %+v", got, want)
	}
	acks := quiclink.AckPolicy{AckElicitingThreshold: 1, ReorderingThreshold: 3, MaxAckDelay: 2 * time.Millisecond}
	n = mustNew(t, quiclink.Config{Dial: dial, Acks: acks})
	if got, want := quiclink.AckFrequency(n), (quic.AckFrequency{AckElicitingThreshold: 1, ReorderingThreshold: 3, MaxAckDelay: 2 * time.Millisecond}); got == nil || *got != want {
		t.Fatalf("AckFrequency = %+v, want %+v", got, want)
	}
	for _, bad := range []quiclink.AckPolicy{
		{AckElicitingThreshold: 10, ReorderingThreshold: 10},
		{AckElicitingThreshold: -1, ReorderingThreshold: 3},
		{AckElicitingThreshold: 1, ReorderingThreshold: 3, MaxAckDelay: time.Second},
	} {
		if _, err := quiclink.New(quiclink.Config{Dial: dial, Acks: bad}); err == nil {
			t.Errorf("New accepted Acks %+v", bad)
		}
	}
}

// TestRefusesPeerWithoutAckFrequency: a peer that does not advertise
// min_ack_delay (quiclink before lane SPEED-ACK) is refused both ways:
// a hard cut, the peer reads CodeNoAckFrequency. ServedPeer still sees
// the connection first: it may answer another protocol on the socket
// (relay-core's QAD, whose dialler has no ACK frequency).
func TestRefusesPeerWithoutAckFrequency(t *testing.T) {
	pc := listenUDP(t)
	var served atomic.Int32
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: link.LivenessFast,
		ServedPeer: func(*quic.Conn) (string, error) { served.Add(1); return "a", nil }})
	go func() { _ = b.Serve(func(_ string, _ []byte, s peerlink.Session) { _ = s.Close() }) }()
	t.Cleanup(func() { _ = b.Drain(context.Background()) })
	old := func(ctx context.Context, _ string, qc *quic.Config) (*quic.Conn, error) {
		qc.AckFrequency = nil
		return quic.DialAddr(ctx, pc.LocalAddr().String(), clientTLS("a", "b"), qc)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The old dialler sees the listener's min_ack_delay, but the listener
	// refuses it: its sessions fail with the code.
	q, err := old(ctx, "b", b.QUICConfig())
	if err != nil {
		t.Fatal(err)
	}
	<-q.Context().Done()
	var ae *quic.ApplicationError
	if err := context.Cause(q.Context()); !errors.As(err, &ae) || ae.ErrorCode != quiclink.CodeNoAckFrequency {
		t.Fatalf("listener's close: %v, want CodeNoAckFrequency", err)
	}
	if served.Load() != 1 {
		t.Fatalf("ServedPeer ran %d times before the refusal, want 1", served.Load())
	}
	// A Node dialling a peer without it refuses the connection itself.
	pc2 := listenUDP(t)
	qc := b.QUICConfig()
	qc.AckFrequency = nil
	tc := serverTLS("b")
	tc.NextProtos = []string{quiclink.ALPNEndAck}
	ln, err := quic.Listen(pc2, tc, qc)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			if _, err := ln.Accept(context.Background()); err != nil {
				return
			}
		}
	}()
	c := mustNew(t, quiclink.Config{Dial: dialer("a", "b", pc2.LocalAddr().String()), Liveness: link.LivenessFast})
	t.Cleanup(func() { _ = c.Drain(context.Background()) })
	if _, err := c.Open(ctx, "b", nil); !errors.Is(err, quiclink.ErrNoAckFrequency) {
		t.Fatalf("Open toward a peer without min_ack_delay: %v, want ErrNoAckFrequency", err)
	}
}

// TestAckFrequencyOnReorderingPath: 4 MiB in order over a 20 ms path whose
// every 20th datagram is 2 ms late, so ACKs go for the count, the alarm,
// a deep hole (the sender's skipped packet numbers) and packets late below
// the last ACK. make spec-traces runs it with -tags quicacktrace: the
// fork's receivers trace each step, checked by spec/TraceAckFrequency.tla.
func TestAckFrequencyOnReorderingPath(t *testing.T) {
	if testing.Short() {
		t.Skip("moves data for seconds")
	}
	pc := listenUDP(t)
	addr := reorderingPath(t, pc.LocalAddr().String(), 10*time.Millisecond, 20, 2*time.Millisecond, 0)
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b")})
	a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", addr), Connect: []string{"b"}})
	const size = 4 << 20
	done := make(chan error, 1)
	go func() {
		_ = a.Serve(func(_ string, _ []byte, s peerlink.Session) {
			buf := make([]byte, 64<<10)
			var n int
			for {
				k, err := s.Read(buf)
				for i := range k {
					if buf[i] != byte(n+i) {
						done <- errors.New("a byte out of order")
						return
					}
				}
				n += k
				if err != nil {
					if n != size {
						err = errors.New("short read")
					} else {
						err = nil
					}
					done <- err
					return
				}
			}
		})
	}()
	go func() { _ = b.Serve(func(string, []byte, peerlink.Session) {}) }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := b.Open(ctx, "a", []byte("acks"))
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(i)
	}
	if _, err := s.Write(buf); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_ = s.Close()
	dctx, dcancel := context.WithCancel(context.Background())
	dcancel()
	_ = a.Drain(dctx)
	_ = b.Drain(dctx)
}
