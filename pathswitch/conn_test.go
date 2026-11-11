// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"bytes"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// Each test is named for the invariant of spec/DirectPath.tla it guards;
// each runs under make spec-traces too (TRACE_PATHSWITCH_TESTS), its
// trace checked against the model per direction.

func checkBytes(t *testing.T, side string, got, want []byte, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s read: %v", side, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s read %d bytes, want %d (equal prefix: %d)", side, len(got), len(want), prefix(got, want))
	}
}

func prefix(a, b []byte) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

// InOrder: bytes arrive once each, in order, relayed only.
func TestInOrderRelayed(t *testing.T) {
	h := newHarness(t, 64, true)
	a, b := payload(600, 1), payload(500, 2)
	gs, gc, es, ec := h.transfer(a, b, 50, nil)
	checkBytes(t, "server", gs, a, es)
	checkBytes(t, "client", gc, b, ec)
	if err := h.c.Close(); err != nil {
		t.Fatal(err)
	}
}

// InOrder across moves both ways: a punch that fails, then relayed,
// direct, relayed, direct.
func TestInOrderAcrossMoves(t *testing.T) {
	h := newHarness(t, 64, true)
	a, b := payload(800, 3), payload(800, 4)
	gs, gc, es, ec := h.transfer(a, b, 20, func(i int) {
		switch i {
		case 2:
			h.punchFails()
		case 5:
			h.directUp()
			h.move("direct")
		case 15:
			h.move("relay")
		case 30:
			h.move("direct")
		}
	})
	checkBytes(t, "server", gs, a, es)
	checkBytes(t, "client", gc, b, ec)
	if h.c.Path() != "direct" {
		t.Fatalf("path %q", h.c.Path())
	}
}

// NoLoss: the direct path dies with a window in flight (stalled, then
// cut): the client falls back to the relay and resends from the RESUME.
func TestNoLossDirectDies(t *testing.T) {
	h := newHarness(t, 64, true)
	a, b := payload(600, 5), payload(600, 6)
	gs, gc, es, ec := h.transfer(a, b, 20, func(i int) {
		switch i {
		case 3:
			h.directUp()
			h.move("direct")
		case 10:
			// a window in flight on the stalled path, then the cut
			h.stall("direct")
			go func() { time.Sleep(20 * time.Millisecond); h.die("direct") }()
		}
	})
	checkBytes(t, "server", gs, a, es)
	checkBytes(t, "client", gc, b, ec)
	if h.c.Path() != "relay" {
		t.Fatalf("path %q", h.c.Path())
	}
}

// AllDelivered: the relay dies while the session is direct (the session
// stays); later the relayed path is back and the direct one dies.
func TestAllDeliveredRelayRestart(t *testing.T) {
	h := newHarness(t, 64, true)
	a, b := payload(600, 7), payload(600, 8)
	gs, gc, es, ec := h.transfer(a, b, 20, func(i int) {
		switch i {
		case 2:
			h.directUp()
			h.move("direct")
		case 8:
			h.die("relay")
		case 20:
			h.mu.Lock()
			h.up["relay"] = true
			traceEnv(h.c, "Revive", "")
			h.mu.Unlock()
			h.die("direct")
		}
	})
	checkBytes(t, "server", gs, a, es)
	checkBytes(t, "client", gc, b, ec)
}

// BufferBound: a writer whose peer reads nothing keeps at most W bytes;
// its Write blocks.
func TestBufferBound(t *testing.T) {
	const w = 64
	h := newHarness(t, w, true)
	done := make(chan error, 1)
	go func() { _, err := h.c.Write(payload(10*w, 9)); done <- err }()
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if r := retained(h.c); r > w {
			t.Fatalf("retained %d > W %d", r, w)
		}
		select {
		case err := <-done:
			t.Fatalf("Write returned (%v) with the peer reading nothing", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	got := make([]byte, 10*w)
	if _, err := io.ReadFull(h.s, got); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// EndOnce, EndAfterData: the END is lost with its path (stalled, then
// cut) and resent on the next stream: the peer reads every byte and one
// EOF.
func TestEndOnceResentAcrossSwitch(t *testing.T) {
	h := newHarness(t, 512, true)
	a := payload(300, 10)
	h.stall("relay")
	if _, err := h.c.Write(a); err != nil {
		t.Fatal(err)
	}
	if err := h.c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // the END goes out onto the stalled stream
	h.directUp()
	h.die("relay")
	got, err := io.ReadAll(h.s)
	checkBytes(t, "server", got, a, err)
	if n, err := h.s.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("after EOF: %d, %v", n, err)
	}
	_ = h.s.CloseWrite()
	if _, err := io.ReadAll(h.c); err != nil {
		t.Fatal(err)
	}
}

// ResetReaches: the client's ABORT is lost with its path; it falls back
// lingering and the server learns the reset on the new stream.
func TestResetReachesFromClient(t *testing.T) {
	h := newHarness(t, 64, true)
	if _, err := h.c.Write(payload(30, 11)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(h.s, make([]byte, 30)); err != nil {
		t.Fatal(err)
	}
	h.stall("relay")
	h.c.Abort()
	time.Sleep(20 * time.Millisecond)
	h.directUp()
	h.die("relay")
	if _, err := io.ReadAll(h.s); !errors.Is(err, ErrReset) {
		t.Fatalf("server read: %v, want ErrReset", err)
	}
	if _, err := h.c.Write([]byte("x")); !errors.Is(err, ErrReset) {
		t.Fatalf("client write: %v", err)
	}
}

// ResetReaches: the server's ABORT is lost with its path; it answers the
// client's next stream with it.
func TestResetReachesFromServer(t *testing.T) {
	h := newHarness(t, 64, true)
	h.stall("relay")
	h.s.Abort()
	time.Sleep(20 * time.Millisecond)
	h.directUp()
	h.die("relay")
	if _, err := io.ReadAll(h.c); !errors.Is(err, ErrReset) {
		t.Fatalf("client read: %v, want ErrReset", err)
	}
	waitDone(t, h.c)
}

// OnlyPeer is the binding check's (relay client/agent, the token); here:
// an OPEN replayed with an epoch not newer than the session's is refused
// and the session goes on.
func TestStaleEpochRefused(t *testing.T) {
	h := newHarness(t, 64, false)
	h.directUp()
	h.move("direct")
	cl, sv := memPair()
	go func() { _, _ = cl.Write(appendOpen(nil, 1, 0, false, token(1))) }()
	o, err := ReadOpen(sv)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.s.Attach(o, "relay"); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("Attach of epoch 1: %v", err)
	}
	a, b := payload(300, 12), payload(300, 13)
	gs, gc, es, ec := h.transfer(a, b, 100, nil)
	checkBytes(t, "server", gs, a, es)
	checkBytes(t, "client", gc, b, ec)
}

// A server that lost the session refuses the client's stream: the client
// is reset, not stuck.
func TestRefusedOpenResetsTheClient(t *testing.T) {
	h := newHarness(t, 64, true)
	h.refuse = true
	h.die("relay")
	if _, err := io.ReadAll(h.c); !errors.Is(err, ErrReset) {
		t.Fatalf("client read: %v, want ErrReset", err)
	}
}

// No stream within the detach bound: the session ends (ErrDetached).
func TestDetachBound(t *testing.T) {
	cl, sv := memPair()
	s := Server(sv, "relay", Config{Detach: 50 * time.Millisecond})
	c := Client(cl, "relay", Config{Detach: 50 * time.Millisecond})
	cl.kill()
	for _, e := range []*Conn{c, s} {
		if _, err := e.Read(make([]byte, 1)); !errors.Is(err, ErrDetached) {
			t.Fatalf("read: %v, want ErrDetached", err)
		}
	}
}

// A frame the protocol does not allow (here a RESUME out of range, as a
// relay rewriting the relayed stream would send) resets the session.
func TestProtocolErrorResets(t *testing.T) {
	cl, sv := memPair()
	c := Client(cl, "relay", Config{})
	defer c.Close()
	cl2, sv2 := memPair()
	if _, err := c.Move(cl2, "relay", token); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadOpen(sv2); err != nil {
		t.Fatal(err)
	}
	_, _ = sv2.Write(appendCount(nil, fResume, 99))
	if _, err := c.Read(make([]byte, 1)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("read: %v, want ErrProtocol", err)
	}
	_ = sv
}

// A clean end: both directions ended, both Closes return, the streams end
// with FIN, not a reset.
func TestCleanClose(t *testing.T) {
	h := newHarness(t, 64, true)
	a, b := payload(200, 14), payload(200, 15)
	gs, gc, es, ec := h.transfer(a, b, 50, nil)
	checkBytes(t, "server", gs, a, es)
	checkBytes(t, "client", gc, b, ec)
	start := time.Now()
	errc := make(chan error, 1)
	go func() { errc <- h.s.Close() }()
	if err := h.c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Close took %v", d)
	}
	waitDone(t, h.c)
	waitDone(t, h.s)
}

// waitDone: Done is closed once the session is over.
func waitDone(t *testing.T, c *Conn) {
	t.Helper()
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed after the session ended")
	}
}

// countStream counts its Writes.
type countStream struct {
	*memStream
	writes atomic.Int64
}

func (s *countStream) Write(b []byte) (int, error) { s.writes.Add(1); return s.memStream.Write(b) }

// The writer sends what it may in one Write (more, docs/performance.md
// "pathswitch: a writer sends what it may in one Write"): 1 MiB written at
// once goes out in a few stream Writes, not one per 16 KiB frame (64
// before).
func TestWriterBatchesFrames(t *testing.T) {
	a, b := memPair()
	cs := &countStream{memStream: a}
	c, s := Client(cs, "relay", Config{}), Server(b, "relay", Config{})
	t.Cleanup(func() { c.Abort(); s.Abort() })
	p := payload(1<<20, 3)
	done := make(chan error, 1)
	go func() { _, err := c.Write(p); done <- err }()
	got := make([]byte, len(p))
	if _, err := io.ReadFull(s, got); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	checkBytes(t, "server", got, p, nil)
	if n := cs.writes.Load(); n > 16 {
		t.Fatalf("1 MiB took %d stream Writes, want at most 16 (64 frames of 16 KiB)", n)
	}
}
