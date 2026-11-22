// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"io"
	"net"
	"testing"
	"time"
)

// growCredit is a receiver's Credit of one session on a FeatureGrow
// connection: window w, at most max, budget b.
func growCredit(w, max, b int) *Credit {
	c := NewCredit(w, w, func(string, int) {})
	c.grows, c.winMax, c.budget = true, max, b
	c.Open("s")
	return c
}

func (c *Credit) win(sid string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessions[sid].win
}

// The growth rule (spec/CreditGrow.tla RecvAB): a BLOCKED doubles the
// window only once a whole window arrived since it last grew and less
// than half of it is unread; never past WindowMax or the budget; an end
// gives the growth back.
func TestWindowGrowthRule(t *testing.T) {
	const w = 4 * MaxChunk
	c := growCredit(w, 4*w, 3*w+w/2)
	c.peerBlocked("s") // before any data: a peer's early BLOCKED (mutant no-epoch)
	if c.win("s") != w {
		t.Fatalf("grew on a BLOCKED before a window arrived: %d", c.win("s"))
	}
	if err := c.Received("s", w); err != nil {
		t.Fatal(err)
	}
	c.peerBlocked("s") // nothing read: the reader, not the path (mutant no-keepup)
	if c.win("s") != w {
		t.Fatalf("grew with the window unread: %d", c.win("s"))
	}
	var granted []int
	c.grant = func(_ string, n int) { granted = append(granted, n) }
	c.Consumed("s", w) // the reader took it: one update of w
	c.peerBlocked("s")
	if c.win("s") != 2*w {
		t.Fatalf("window %d after a kept-up BLOCKED, want %d", c.win("s"), 2*w)
	}
	c.peerBlocked("s") // again, no new window received (mutant no-epoch)
	if c.win("s") != 2*w {
		t.Fatalf("grew twice on one window: %d", c.win("s"))
	}
	if err := c.Received("s", 2*w); err != nil { // the grown window: no ErrCredit
		t.Fatal(err)
	}
	c.Consumed("s", 1) // the growth goes with the next update
	if len(granted) != 2 || granted[0] != w || granted[1] != 1+w {
		t.Fatalf("updates %v, want [%d %d]", granted, w, 1+w)
	}
	if err := c.Received("s", 2); err != ErrCredit { // held 2w-1 of 2w
		t.Fatalf("data past the grown window: %v, want ErrCredit", err)
	}
	c.Consumed("s", 2*w-1) // an ErrCredit ends the connection; the test goes on with the Credit
	c.peerBlocked("s")
	if c.win("s") != 4*w { // the cap (mutant no-cap)
		t.Fatalf("window %d, want the cap %d", c.win("s"), 4*w)
	}
	c.Open("t") // a second session: half a window of budget left (mutant no-budget)
	if err := c.Received("t", w); err != nil {
		t.Fatal(err)
	}
	c.Consumed("t", w)
	c.peerBlocked("t")
	if c.win("t") != w+w/2 || c.grown != 3*w+w/2 {
		t.Fatalf("second session %d, grown %d, want %d and the budget %d", c.win("t"), c.grown, w+w/2, 3*w+w/2)
	}
	c.Forget("s")
	c.Forget("t")
	if c.grown != 0 { // mutant drop-keeps-growth
		t.Fatalf("grown %d after both ended, want 0", c.grown)
	}
}

// A sender's credit past the peer's hello window: a FeatureGrow peer's
// growth is taken up to MaxWindow, a fixed one's is ErrCredit (mutant
// fixed-cap-sender is the other way round).
func TestGrantPastTheHelloWindow(t *testing.T) {
	const w = 4 * MaxChunk
	for _, grows := range []bool{false, true} {
		c := NewCredit(w, w, func(string, int) {})
		c.grows = grows
		c.Open("s")
		err := c.Grant("s", w)
		if grows && err != nil || !grows && err != ErrCredit {
			t.Errorf("grows %v: Grant past the hello window: %v", grows, err)
		}
	}
}

// End to end over a 20 ms round trip: the window grows, the sender's
// BLOCKEDs go out, the growth comes back to the budget when the session
// ends; toward a peer without FeatureGrow (v0.13) it stays and the sender
// sends no BLOCKED.
func TestWindowGrowsOverALongPath(t *testing.T) {
	for _, old := range []bool{false, true} {
		r := newChainRig(t, rigOpts{n: 1, window: DefaultWindow, noGrow: old, wrap: func(_ int, nc net.Conn) net.Conn {
			return newDelayConn(nc, 10*time.Millisecond)
		}}, farSink)
		s := r.open("sink")
		buf := make([]byte, 256<<10)
		for sent := 0; sent < 32<<20; sent += len(buf) {
			if _, err := s.Write(buf); err != nil {
				t.Fatal(err)
			}
		}
		lm := r.links[0].lm.c
		st := lm.Stats()
		_ = s.CloseWrite()
		_, _ = io.Copy(io.Discard, s)
		if old {
			if st.WindowGrowths != 0 {
				t.Errorf("v0.13 dialer: %d growths, want 0", st.WindowGrowths)
			}
			continue
		}
		if st.WindowGrowths == 0 || st.WindowGrown == 0 || st.WindowGrown > DefaultWindowMax-DefaultWindow {
			t.Errorf("growths %d, grown %d: want some, at most %d", st.WindowGrowths, st.WindowGrown, DefaultWindowMax-DefaultWindow)
		}
		r.idle()
		if g := lm.Stats().WindowGrown; g != 0 {
			t.Errorf("grown %d after the session ended, want 0", g)
		}
	}
}

// A reader that never reads leaves its window as it was (spec/CreditGrow.tla
// NeverReadNoGrowth): the sender stalls at the hello's window.
func TestStoppedReaderGrowsNothing(t *testing.T) {
	stuck := make(chan struct{})
	defer close(stuck)
	r := newChainRig(t, rigOpts{n: 1, window: testWindow}, func(_ []byte, s *Session) {
		_ = s.Accept()
		<-stuck
	})
	s := r.open("stuck")
	go func() {
		buf := make([]byte, 32<<10)
		for {
			if _, err := s.Write(buf); err != nil {
				return
			}
		}
	}()
	lm := r.links[0].lm.c
	waitUntil(t, func() bool { return lm.Stats().QueuedBytes == testWindow })
	time.Sleep(50 * time.Millisecond) // BLOCKEDs on the way
	if st := lm.Stats(); st.WindowGrowths != 0 || st.QueuedBytes != testWindow {
		t.Fatalf("stopped reader: %d growths, %d queued, want 0 and %d", st.WindowGrowths, st.QueuedBytes, testWindow)
	}
}
