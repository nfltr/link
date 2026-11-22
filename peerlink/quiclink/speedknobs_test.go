// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
	"github.com/nfltr/quic-go"
)

// TestSpeedKnobsValidation: New refuses a speed knob that cannot work,
// naming the field, and takes every valid one (link 1.0 gate item 8c).
func TestSpeedKnobsValidation(t *testing.T) {
	dial := func(context.Context, string, *quic.Config) (*quic.Conn, error) { return nil, nil }
	pc := listenUDP(t)
	for _, tc := range []struct {
		name    string
		cfg     quiclink.Config
		wantErr string // "": valid
	}{
		{"zero", quiclink.Config{Dial: dial}, ""},
		{"reno", quiclink.Config{Dial: dial, Congestion: quiclink.CongestionReno}, ""},
		{"congestion unknown", quiclink.Config{Dial: dial, Congestion: quiclink.CongestionReno + 1}, "Config.Congestion"},
		{"no pacing", quiclink.Config{Dial: dial, NoPacing: true}, ""},
		{"packet 1200", quiclink.Config{Dial: dial, MaxPacketSize: 1200}, ""},
		{"packet 1452", quiclink.Config{Dial: dial, MaxPacketSize: 1452}, ""},
		{"packet 1199", quiclink.Config{Dial: dial, MaxPacketSize: 1199}, "Config.MaxPacketSize"},
		{"packet 1453", quiclink.Config{Dial: dial, MaxPacketSize: 1453}, ""},
		{"packet 8952", quiclink.Config{Dial: dial, MaxPacketSize: 8952}, ""},
		{"packet 8953", quiclink.Config{Dial: dial, MaxPacketSize: 8953}, "Config.MaxPacketSize"},
		{"packet 9000", quiclink.Config{Dial: dial, MaxPacketSize: 9000}, "Config.MaxPacketSize"},
		{"packet negative", quiclink.Config{Dial: dial, MaxPacketSize: -1}, "Config.MaxPacketSize"},
		{"socket", quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Socket: quiclink.Socket{ReceiveBuffer: 1 << 20, SendBuffer: 1 << 20, NoGSO: true, NoGRO: true, NoECN: true}}, ""},
		{"socket negative receive", quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Socket: quiclink.Socket{ReceiveBuffer: -1}}, "Config.Socket"},
		{"socket negative send", quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Socket: quiclink.Socket{SendBuffer: -1}}, "Config.Socket"},
		{"socket without PacketConn", quiclink.Config{Dial: dial, Socket: quiclink.Socket{NoGRO: true}}, "Config.Socket needs Config.PacketConn"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := quiclink.New(tc.cfg)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("New: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("New: %v, want an error naming %s", err, tc.wantErr)
			}
		})
	}
}

// TestSpeedKnobsReachQUIC: each knob is the fork's setting on the Node's
// connections, and the zero values are today's (BBR, paced, 1452).
func TestSpeedKnobsReachQUIC(t *testing.T) {
	dial := func(context.Context, string, *quic.Config) (*quic.Conn, error) { return nil, nil }
	for _, tc := range []struct {
		cfg  quiclink.Config
		want quic.Config
	}{
		{quiclink.Config{Dial: dial}, quic.Config{CongestionControl: quic.CongestionControlBBR}},
		{quiclink.Config{Dial: dial, Congestion: quiclink.CongestionReno}, quic.Config{CongestionControl: quic.CongestionControlReno}},
		{quiclink.Config{Dial: dial, NoPacing: true}, quic.Config{CongestionControl: quic.CongestionControlBBR, DisablePacing: true}},
		{quiclink.Config{Dial: dial, MaxPacketSize: 1300}, quic.Config{CongestionControl: quic.CongestionControlBBR, MaxPacketSize: 1300}},
	} {
		qc := mustNew(t, tc.cfg).QUICConfig()
		if qc.CongestionControl != tc.want.CongestionControl || qc.DisablePacing != tc.want.DisablePacing || qc.MaxPacketSize != tc.want.MaxPacketSize {
			t.Errorf("%+v: QUIC config congestion %d, no pacing %t, max packet %d; want %d, %t, %d", tc.cfg, qc.CongestionControl, qc.DisablePacing,
				qc.MaxPacketSize, tc.want.CongestionControl, tc.want.DisablePacing, tc.want.MaxPacketSize)
		}
	}
}

// TestSocketBuffersReported: a buffer the host refuses (MaxInt32: past
// rmem_max and kern.ipc.maxsockbuf, and Linux clamps even a forced size to
// 2*(INT_MAX/2)) is reported as what it gave, not silently smaller; a Node
// without a socket of its own says so.
func TestSocketBuffersReported(t *testing.T) {
	pc := listenUDP(t)
	const huge = math.MaxInt32
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Socket: quiclink.Socket{ReceiveBuffer: huge, SendBuffer: huge}})
	a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", pc.LocalAddr().String())})
	go func() {
		_ = b.Serve(func(_ string, _ []byte, s peerlink.Session) { _, _ = io.Copy(io.Discard, s); _ = s.Close() })
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = a.Drain(ctx)
		_ = b.Drain(ctx)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, err := a.Open(ctx, "b", nil) // Serve has set up its socket
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, w, err := b.SocketBuffers()
	if err != nil {
		t.Fatal(err)
	}
	if r <= 0 || r >= huge || w <= 0 || w >= huge {
		t.Fatalf("SocketBuffers = %d, %d: want what the host gave, less than the %d asked", r, w, huge)
	}
	if _, _, err := a.SocketBuffers(); err == nil {
		t.Fatal("SocketBuffers of a Node without PacketConn: no error")
	}
}

// TestJumboPackets: on loopback (a large MTU) a Node pair with
// MaxPacketSize 8952 reaches jumbo packets (the datagram room grows past
// 1452) and carries a session's bytes intact; against a peer at the zero
// value the room stays under 1452.
func TestJumboPackets(t *testing.T) {
	for _, tc := range []struct {
		name          string
		serve         int
		wantAtLeast   int64
		wantBelowSize int64
	}{
		{"both 8952", 8952, 8000, 8952},
		{"server default", 0, 1300, 1452},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pc := listenUDP(t)
			b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Datagrams: true, MaxPacketSize: tc.serve})
			// quic.DialAddr's Transport reads its Config's MaxPacketSize.
			a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", pc.LocalAddr().String()), Datagrams: true, MaxPacketSize: 8952})
			got := make(chan []byte, 1)
			go func() {
				_ = b.Serve(func(_ string, _ []byte, s peerlink.Session) { p, _ := io.ReadAll(s); got <- p; _ = s.Close() })
			}()
			t.Cleanup(func() {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_ = a.Drain(ctx)
				_ = b.Drain(ctx)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			ps, err := a.Open(ctx, "b", nil)
			if err != nil {
				t.Fatal(err)
			}
			s := ps.(*quiclink.Session)
			room := func() int64 {
				var e *quic.DatagramTooLargeError
				if err := s.SendDatagram(make([]byte, 16<<10)); !errors.As(err, &e) {
					t.Fatalf("SendDatagram 16 KiB: %v, want DatagramTooLargeError", err)
				}
				return e.MaxDatagramPayloadSize
			}
			// Path MTU discovery probes every 5 RTTs: write until the room
			// reaches what the ends allow, or the deadline.
			chunk := bytes.Repeat([]byte("jumbo..."), 128<<10)
			var sent []byte
			for room() < tc.wantAtLeast && ctx.Err() == nil {
				if _, err := s.Write(chunk); err != nil {
					t.Fatal(err)
				}
				sent = append(sent, chunk...)
			}
			if _, err := s.Write(chunk); err != nil {
				t.Fatal(err)
			}
			sent = append(sent, chunk...)
			if err := s.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			if r := room(); r < tc.wantAtLeast || r >= tc.wantBelowSize {
				t.Fatalf("datagram room %d: want [%d, %d)", r, tc.wantAtLeast, tc.wantBelowSize)
			}
			if p := <-got; !bytes.Equal(p, sent) {
				t.Fatalf("got %d bytes, sent %d: differ", len(p), len(sent))
			}
		})
	}
}
