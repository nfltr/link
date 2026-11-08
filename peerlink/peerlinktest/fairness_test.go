// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlinktest

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

// TestFairnessCatchesAWait is Fairness's mutant: a Link whose first
// session reads nothing from half a second into the run to its end (one of
// 8 sessions waiting while the others take the path, as under link's
// scheduler once one waited 2.4 s) fails the verdict on its wait.
func TestFairnessCatchesAWait(t *testing.T) {
	f := measureFairness(t, func(t *testing.T) Pair {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		px := NewProxy(t, ln.Addr().String())
		b, err := peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("b")}})
		if err != nil {
			t.Fatal(err)
		}
		a, err := peerlink.New(peerlink.Config{
			Link: link.Config{Auth: link.NameAuth("a")},
			Dial: func(ctx context.Context, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "tcp", px.Addr())
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return Pair{A: &holdFirst{Link: a}, B: b, AName: "a", BName: "b",
			DeadAfter: a.DeadAfter(), BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle}
	})
	t.Logf("%v", f)
	if f.worst <= f.total/2 || f.verdict() == nil {
		t.Fatalf("the verdict passes a session that waited: %v", f)
	}
}

// holdFirst holds its first opened session's reads for 4 s, from 2.5 s
// after its first read (Fairness's run is from 2 s to 6 s).
type holdFirst struct {
	peerlink.Link
	n atomic.Int32
}

func (h *holdFirst) Open(ctx context.Context, peer string, header []byte) (peerlink.Session, error) {
	s, err := h.Link.Open(ctx, peer, header)
	if err == nil && h.n.Add(1) == 1 {
		s = &held{Session: s}
	}
	return s, err
}

type held struct {
	peerlink.Session
	first time.Time
	once  bool
}

func (h *held) Read(b []byte) (int, error) {
	if h.first.IsZero() {
		h.first = time.Now()
	}
	if !h.once && time.Since(h.first) > 2500*time.Millisecond {
		h.once = true
		time.Sleep(4 * time.Second)
	}
	return h.Session.Read(b)
}
