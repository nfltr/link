// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
)

// TestNodeStats: a Node's Stats counts the bytes of its connections (as
// QUIC sent and received them) and the sessions on them, both ways; the
// counters of a connection that ended stay in.
func TestNodeStats(t *testing.T) {
	pc := listenUDP(t)
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: link.LivenessFast})
	a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", pc.LocalAddr().String()), Liveness: link.LivenessFast})
	go func() {
		_ = b.Serve(func(_ string, _ []byte, s peerlink.Session) { defer s.Close(); _, _ = io.Copy(s, s) })
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := a.Open(ctx, "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	msg := make([]byte, 256<<10)
	go func() { _, _ = s.Write(msg); _ = s.CloseWrite() }()
	if n, err := io.Copy(io.Discard, s); err != nil || n != int64(len(msg)) {
		t.Fatalf("echo: %d %v", n, err)
	}
	_ = s.Close()
	sa, sb := a.Stats(), b.Stats()
	if sa.BytesOut < uint64(len(msg)) || sa.BytesIn < uint64(len(msg)) || sb.BytesIn < uint64(len(msg)) {
		t.Errorf("Stats a %+v b %+v: want at least the %d bytes each way", sa, sb, len(msg))
	}
	if sa.SessionsOpened != 1 || sb.SessionsOpened != 1 {
		t.Errorf("SessionsOpened a %d b %d, want 1 each", sa.SessionsOpened, sb.SessionsOpened)
	}
	if err := a.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if after := a.Stats(); after.BytesOut < sa.BytesOut || after.BytesIn < sa.BytesIn || after.SessionsOpened != 1 {
		t.Errorf("after Drain: %+v, before %+v: the counters went back", after, sa)
	}
}
