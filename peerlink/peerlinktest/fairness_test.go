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
		p := linkPair(t, 0, 0)
		p.A = &holdFirst{Link: p.A}
		return p
	})
	t.Logf("%v", f)
	if f.worst <= f.total/2 || f.verdict() == nil {
		t.Fatalf("the verdict passes a session that waited: %v", f)
	}
}

// TestFairnessOnADeepPath: on a path that buffers more than the 8
// sessions' windows (8 MiB with link's 1 MiB DefaultWindow) in front of
// the throttle, as a loaded Linux host's autotuned socket buffers did on
// the CI runner, the sessions wait on credit: each moves a grant (a
// quarter window, 256 KiB) at a time, in turn, so one waits while the 7
// others move theirs, about 2 MiB. Fairness's verdict holds there: the run
// moves enough that this round of grants is not half of it (at 1 MiB/s
// over 4 s it was, and Fairness failed a fair Link: link fbc982501,
// 5f322678d; at 2 MiB/s, under load, now and then: lane LINKSTABLE3).
// Half the run is past the wait the others' credit allows (verdict). The
// windows stay fixed (WindowMax = Window): that bound is the others' first
// windows, and growth makes it their grown ones (spec/CreditGrow.tla
// TurnBound; TestFairnessOnADeepPathGrowing).
func TestFairnessOnADeepPath(t *testing.T) {
	if credit := int64(fairSessions-1) * link.DefaultWindow; int64(fairRate*fairRun.Seconds())/2 <= credit {
		t.Fatalf("half of Fairness's run (%d B/s for %v) is within the %d B the other sessions' credit lets one wait", fairRate, fairRun, credit)
	}
	f := measureFairness(t, func(t *testing.T) Pair { return linkPair(t, 16<<20, link.DefaultWindow) })
	t.Logf("%v", f)
	if err := f.verdict(); err != nil {
		t.Fatal(err)
	}
}

// TestFairnessOnADeepPathGrowing: the deep path with windows that grow
// (FeatureGrow, link's default). A wait is then bounded by the others'
// grown windows, not by the run: each window's growth leaves with one
// update, so the round in which the sessions grow puts a growth-sized run
// of each on the path, and a session waited 7.2-9.2 MiB, past half the
// run (10 of 50 runs failed the verdict with -race, lane FAIRNESS; fixed
// windows 2.0-3.9 MiB, none). Every session still gets its share.
func TestFairnessOnADeepPathGrowing(t *testing.T) {
	f := measureFairness(t, func(t *testing.T) Pair { return linkPair(t, 16<<20, 0) })
	t.Logf("%v", f)
	if err := f.share(); err != nil {
		t.Fatal(err)
	}
}

// linkPair is a Pair of link Nodes through a Proxy whose path buffers
// deep bytes (Proxy.deep; 0: the kernel's), their windows growing to
// windowMax (link.Config.WindowMax; 0: link's default).
func linkPair(t *testing.T, deep, windowMax int) Pair {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	px := NewProxy(t, ln.Addr().String())
	px.deep.Store(int64(deep))
	b, err := peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("b"), WindowMax: windowMax}})
	if err != nil {
		t.Fatal(err)
	}
	a, err := peerlink.New(peerlink.Config{
		Link: link.Config{Auth: link.NameAuth("a"), WindowMax: windowMax},
		Dial: func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", px.Addr())
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return Pair{A: a, B: b, AName: "a", BName: "b",
		DeadAfter: a.DeadAfter(), BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle}
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
