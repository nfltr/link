// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// Lane DIRECTCLOSE: a Close wakes a Read waiting in it, a Close whose
// peer is gone ends within Config.Linger, and a reset never takes bytes
// an end already took from its application (spec/DirectPath.tla
// CloseWakes, CloseBounded, TakenRead).

// gone: the peer's process vanished: every byte on the session's path is
// held from now on (nothing reaches it, nothing comes back), as a killed
// agent on a direct QUIC path until its idle timeout.
func (h *harness) gone() { h.stall("relay") }

// A Read waiting when its end closes returns net.ErrClosed at once, while
// the Close itself still waits for its peer (here: gone, so until its
// bound).
func TestCloseWakesABlockedRead(t *testing.T) {
	for _, how := range []string{"Close", "CloseRead"} {
		t.Run(how, func(t *testing.T) {
			h := newHarness(t, 64<<10, false)
			errc := make(chan error, 1)
			go func() { _, err := h.c.Read(make([]byte, 1)); errc <- err }()
			time.Sleep(20 * time.Millisecond) // the Read waits by now; if not, it must still return net.ErrClosed
			h.gone()
			start := time.Now()
			if how == "Close" {
				go h.c.Close()
			} else {
				h.c.CloseRead()
			}
			select {
			case err := <-errc:
				if !errors.Is(err, net.ErrClosed) {
					t.Fatalf("Read after %s: %v, want net.ErrClosed", how, err)
				}
				if d := time.Since(start); d > time.Second {
					t.Fatalf("Read returned %v after %s", d, how)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("Read still waits 2 s after %s", how)
			}
			if _, err := h.c.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("a later Read: %v, want net.ErrClosed", err)
			}
		})
	}
}

// After CloseRead the peer's bytes are taken, acknowledged and dropped
// (its writes never stall on the window), but its END is never
// acknowledged: this end's program did not read the stream (spec
// DirectPath.tla EndTruth, mutant close-took-end; lane ENDACK). Its Close
// then resets the session (RCloseReset), as a TCP close with data unread.
func TestCloseReadDropsAndResets(t *testing.T) {
	h := newHarness(t, 16, false)
	h.c.CloseRead()
	if _, err := h.s.Write(payload(64, 3)); err != nil { // 4 windows; small: the test is traced
		t.Fatal(err)
	}
	_ = h.s.CloseWrite()
	if !h.s.WaitAcked(h.s.Written(), 5*time.Second) {
		t.Fatal("the peer's bytes were not acknowledged")
	}
	if n := func() int { h.c.mu.Lock(); defer h.c.mu.Unlock(); return h.c.rbuf.len() }(); n != 0 {
		t.Fatalf("the closed end holds %d bytes", n)
	}
	if h.s.WaitAcked(h.s.Written()+1, 200*time.Millisecond) {
		t.Fatal("the END of a stream dropped unread was acknowledged")
	}
	go h.s.Close()
	h.c.Close()
	h.s.mu.Lock()
	err := h.s.err
	h.s.mu.Unlock()
	if !errors.Is(err, ErrReset) {
		t.Fatalf("the peer's session: %v, want a reset", err)
	}
}

// A Close whose peer vanished (nothing heard from it) resets and returns
// within Config.Linger, not the detach bound (5 s here).
func TestCloseGonePeerWithinLinger(t *testing.T) {
	h := newHarness(t, 64<<10, false)
	h.c.cfg.Linger = 200 * time.Millisecond
	if _, err := h.c.Write(payload(1000, 4)); err != nil {
		t.Fatal(err)
	}
	h.gone()
	start := time.Now()
	h.c.Close()
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Close took %v with its peer gone; Linger is 200 ms", d)
	}
	select {
	case <-h.c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("not Done 2 s after its Close")
	}
}

// WaitAcked gives up once nothing was heard from the peer for
// Config.Linger, whatever its own bound.
func TestWaitAckedGonePeerWithinLinger(t *testing.T) {
	h := newHarness(t, 64<<10, false)
	h.c.cfg.Linger = 200 * time.Millisecond
	h.gone()
	if _, err := h.c.Write(payload(1000, 5)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if h.c.WaitAcked(h.c.Written(), 30*time.Second) {
		t.Fatal("acknowledged by a gone peer")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("WaitAcked took %v with its peer gone; Linger is 200 ms", d)
	}
}

// A Close waits for a live peer that keeps answering, past Linger: the
// bound is on silence, not on the close's length.
func TestCloseWaitsForALivePeerPastLinger(t *testing.T) {
	h := newHarness(t, 64<<10, false)
	h.c.cfg.Linger = 300 * time.Millisecond
	h.s.cfg.Linger = 300 * time.Millisecond
	rs := readAll(h.s)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 10 { // the peer's direction ends 1 s later, a frame every 100 ms
			time.Sleep(100 * time.Millisecond)
			if _, err := h.s.Write([]byte{1}); err != nil {
				return
			}
		}
		_ = h.s.CloseWrite()
	}()
	h.c.Close()
	select {
	case <-done:
	default:
		t.Fatal("Close returned before the live peer's END: its bound was on the close's length")
	}
	if err := <-rs; err != nil {
		t.Fatalf("the peer's read: %v (reset by the Close's bound)", err)
	}
	// The bytes that came after the Close were dropped unread: a reset,
	// not the clean end (lane ENDACK: spec RCloseReset).
	h.c.mu.Lock()
	err := h.c.err
	h.c.mu.Unlock()
	if !errors.Is(err, ErrReset) {
		t.Fatalf("Close ended with %v, want a reset (bytes dropped)", err)
	}
}

// PIPEMATRIX defect 1, both directions done: the server's Abort right
// after its clean end (its stop after Close) is the clean end, never a
// reset, and the client reads every byte it acknowledged (spec AbortBy:
// no reset once the direction is clean).
func TestAbortAfterCleanKeepsTheTail(t *testing.T) {
	h := newHarness(t, 64<<10, false)
	want := payload(2<<10, 6) // small: the test is traced
	_ = h.c.CloseWrite()
	if _, err := io.ReadAll(h.s); err != nil {
		t.Fatal(err)
	}
	type res struct {
		b   []byte
		err error
	}
	rc := make(chan res, 1)
	go func() { b, err := io.ReadAll(h.c); rc <- res{b, err} }() // the END is acknowledged once read (lane ENDACK)
	if _, err := h.s.Write(want); err != nil {
		t.Fatal(err)
	}
	_ = h.s.CloseWrite()
	if !h.s.WaitAcked(h.s.Written()+1, 5*time.Second) {
		t.Fatal("not acknowledged")
	}
	h.s.Abort()
	r := <-rc
	if r.err != nil || !bytes.Equal(r.b, want) {
		t.Fatalf("client read %d of %d bytes, err %v", len(r.b), len(want), r.err)
	}
	h.c.mu.Lock()
	ab := h.c.ab
	h.c.mu.Unlock()
	if ab != abNone {
		t.Fatal("the client took an ABORT: a session clean at the server was reset")
	}
}

// PIPEMATRIX defect 1, the client's direction still open: the server's
// Abort after its own direction ended resets the client's direction, but
// the client still reads every byte of the server's it took, then EOF.
func TestResetKeepsACleanDirection(t *testing.T) {
	h := newHarness(t, 1<<20, false)
	want := payload(256<<10, 7)
	if _, err := h.s.Write(want); err != nil {
		t.Fatal(err)
	}
	_ = h.s.CloseWrite()
	if !h.s.WaitAcked(h.s.Written(), 5*time.Second) { // every byte taken; the END too, unread (lane ENDACK)
		t.Fatal("not acknowledged")
	}
	waitFor(t, func() bool { h.c.mu.Lock(); defer h.c.mu.Unlock(); return h.c.rEnd })
	h.s.Abort()
	waitFor(t, func() bool { _, err := h.c.Write([]byte{1}); return errors.Is(err, ErrReset) })
	got, err := io.ReadAll(h.c)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("client read %d of %d bytes, err %v", len(got), len(want), err)
	}
}

// A WaitAcked bound holds when nothing else happens on the session: the
// timer's wake ends the wait (link 9a31a62's make test-race hung 10 min in
// TestWaitAckedThenMove: the wait's own deadline, read after its timer
// started, was still ahead when the timer's one wake came).
func TestWaitAckedBoundHoldsOnAnIdleConn(t *testing.T) {
	h := newHarness(t, 64<<10, false)
	if _, err := h.c.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	h.gone() // nothing acknowledged, nothing heard
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 2000 {
			if h.c.WaitAcked(1, time.Duration(i%50)*time.Microsecond) {
				t.Error("acknowledged by a gone peer")
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("2000 WaitAcked of at most 50 µs each still wait after 10 s")
	}
}

// WaitAcked gives up once nothing was acknowledged for Config.Detach,
// even while the peer keeps answering (its own bytes): frames heard are
// no progress (link spec/RelayedClose.tla, mutant
// liveness-counts-as-progress). Before, a peer that wrote but took
// nothing held it to its own bound (relay TestNF1DirectModelBindings:
// 600 s in a Close, lane V1READY).
func TestWaitAckedLivePeerWithoutProgress(t *testing.T) {
	h := newHarness(t, 64<<10, false)
	h.c.cfg.Detach = 300 * time.Millisecond
	go func() { _, _ = h.c.Write(payload(256<<10, 6)) }() // past the peer's window: it takes no more
	stop := make(chan struct{})
	defer close(stop)
	go func() { // the peer keeps answering, a frame every 100 ms
		for {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
				if _, err := h.s.Write([]byte{1}); err != nil {
					return
				}
			}
		}
	}()
	time.Sleep(200 * time.Millisecond) // the window fills
	start := time.Now()
	if h.c.WaitAcked(256<<10, 30*time.Second) {
		t.Fatal("acknowledged past the peer's window: it read nothing")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("WaitAcked took %v while nothing was acknowledged; Detach is 300 ms", d)
	}
}

// A Close waits for a slow, live tail past Config.Detach: its bound is
// Detach with nothing acknowledged, never Detach from the Close
// (spec/DirectPath.tla CloseKeepsLive, mutant total-bound; link
// spec/EndAckLeg.tla IdleTruth). Before, the Close reset a tail still
// being acknowledged once Detach passed (lane ENDACKCODE).
func TestCloseSlowTailNotReset(t *testing.T) {
	h := newHarness(t, 256<<10, false)
	h.c.cfg.Detach = 300 * time.Millisecond
	h.str[0].sv.r.set(func(p *pipe) { p.slow = 5 * time.Millisecond }) // 256 KiB take about 1.3 s, an ACK every 16 KiB frame (80 ms)
	if err := h.s.CloseWrite(); err != nil {                           // the peer sends nothing
		t.Fatal(err)
	}
	tail := payload(256<<10, 7)
	if _, err := h.c.Write(tail); err != nil {
		t.Fatal(err)
	}
	rs := make(chan []byte, 1)
	go func() { b, _ := io.ReadAll(h.s); rs <- b }()
	start := time.Now()
	h.c.Close()
	h.c.mu.Lock()
	err := h.c.err
	h.c.mu.Unlock()
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Close after %v ended with %v, want the clean end (Detach is 300 ms, the tail was acknowledged throughout)", time.Since(start), err)
	}
	if b := <-rs; !bytes.Equal(b, tail) {
		t.Fatalf("the peer read %d of %d bytes", len(b), len(tail))
	}
}

// A Close gives up once nothing was acknowledged for Config.Detach, while
// the peer keeps answering: frames heard are no progress (spec
// DirectPath.tla, link spec/EndAckLeg.tla WaitEnds, mutant
// liveness-counts-as-progress).
func TestCloseLivePeerWithoutProgress(t *testing.T) {
	h := newHarness(t, 64<<10, false)
	h.c.cfg.Detach = 300 * time.Millisecond
	if _, err := h.c.Write(payload(64<<10, 8)); err != nil { // the window: the peer reads none, acknowledges none
		t.Fatal(err)
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() { // the peer keeps answering, a frame every 100 ms
		for {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
				if _, err := h.s.Write([]byte{1}); err != nil {
					return
				}
			}
		}
	}()
	start := time.Now()
	h.c.Close()
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("Close took %v while nothing was acknowledged; Detach is 300 ms", d)
	}
	h.c.mu.Lock()
	err := h.c.err
	h.c.mu.Unlock()
	if errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("Close ended clean: the peer acknowledged nothing")
	}
}
