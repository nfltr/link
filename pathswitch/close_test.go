// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"errors"
	"io"
	"testing"
	"time"
)

// reattached is a harness session moved to the direct path whose direct
// path then died: the client fell back to a relayed stream (epoch 3).
func reattached(t *testing.T) *harness {
	h := newHarness(t, 64, true)
	h.directUp()
	h.move("direct")
	waitFor(t, func() bool { return h.c.Path() == "direct" && h.s.Path() == "direct" })
	h.die("direct")
	waitFor(t, func() bool { return h.c.Epoch() == 3 && h.s.Epoch() == 3 })
	return h
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !ok(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
	}
}

// cur is the session's current stream (the harness's last).
func (h *harness) cur() hstream {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.str[len(h.str)-1]
}

// readAll reads c to its end in the background.
func readAll(c *Conn) chan error {
	errc := make(chan error, 1)
	go func() { _, err := io.ReadAll(c); errc <- err }()
	return errc
}

// A Close right after a re-attach ends at both ends once both closed,
// never by the detach bound, even when the client's last ACK (of the
// agent's END) is held when it closes and the client's caller drops the
// stream as soon as Done: the client is done only after the agent's FIN,
// which says the agent has everything (spec/DirectPath.tla LeaveLast,
// CloseEnds; lane PSCLOSE: relay's leakcheck found the agent's Close
// waiting out the bound).
func TestCloseAfterReattachEndsBeforeTheBound(t *testing.T) { closeAfterReattach(t, "drop") }

// TestCloseAfterReattachPathDiesBeforeTheFIN: as above, the held path
// dying instead.
func TestCloseAfterReattachPathDiesBeforeTheFIN(t *testing.T) { closeAfterReattach(t, "die") }

func closeAfterReattach(t *testing.T, then string) {
	h := reattached(t)
	rs, rc := readAll(h.s), readAll(h.c)
	if _, err := h.c.Write(payload(100, 1)); err != nil {
		t.Fatal(err)
	}
	_ = h.c.CloseWrite()
	if err := <-rs; err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return retained(h.c) == 0 && func() bool { h.c.mu.Lock(); defer h.c.mu.Unlock(); return h.c.acked == h.c.hi+1 }()
	})
	st := h.cur()
	st.cl.w.set(func(p *pipe) { p.stalled = true })      // the client's ACKs held
	if _, err := h.s.Write(payload(20, 2)); err != nil { // within the window: no ACK needed
		t.Fatal(err)
	}
	_ = h.s.CloseWrite()
	if err := <-rc; err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	go func() { _ = h.s.Close() }()
	_ = h.c.Close()
	select {
	case <-h.c.Done(): // the caller drops the stream
		st.cl.kill()
	case <-time.After(time.Second):
	}
	switch then {
	case "drop":
		st.cl.w.set(func(p *pipe) { p.stalled = false })
	case "die":
		h.die("relay")
	}
	for _, c := range []*Conn{h.s, h.c} {
		select {
		case <-c.Done():
		case <-time.After(3 * time.Second):
			t.Fatalf("the %s's Done waits out the detach bound", c.end(c.client))
		}
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Close took %v", d)
	}
	for _, c := range []*Conn{h.s, h.c} {
		c.mu.Lock()
		clean := c.err == io.ErrClosedPipe
		c.mu.Unlock()
		if !clean {
			t.Fatalf("the %s's end is not clean", c.end(c.client))
		}
	}
}

// An agent that left after its clean end answers a stream the client
// opens with its count and FIN, so a client whose stream died before that
// count arrived (the ACK of its END lost with the path) still ends
// cleanly, at once (spec RLeftAnswer, SLeftAnswer). A refusal (ABORT)
// would reset the client's cleanly ended session.
func TestCloseLeftAnswersAReattach(t *testing.T) {
	h := reattached(t)
	rs, rc := readAll(h.s), readAll(h.c)
	if _, err := h.c.Write(payload(100, 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.s.Write(payload(100, 4)); err != nil {
		t.Fatal(err)
	}
	_ = h.s.CloseWrite()
	if err := <-rc; err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return retained(h.s) == 0 && retained(h.c) == 0 })
	st := h.cur()
	st.sv.w.set(func(p *pipe) { p.stalled = true }) // the agent's last frames held
	_ = h.c.CloseWrite()
	if err := <-rs; err != nil {
		t.Fatal(err)
	}
	_ = h.s.Close()
	waitDone(t, h.s) // the agent left; its ACK of the client's END is held
	h.die("relay")   // and lost: the client opens another stream
	start := time.Now()
	_ = h.c.Close()
	waitDone(t, h.c)
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Close took %v", d)
	}
	h.c.mu.Lock()
	err := h.c.err
	h.c.mu.Unlock()
	if err != io.ErrClosedPipe {
		t.Fatalf("the client's end: %v, want a clean end", err)
	}
}

// An end that reset the session and then closed it (nfltr's revocation
// cut: tlsend's Reset, then its Close) lingers as a reset end: its ABORT
// lost with the dying path, the peer's next stream is answered with the
// ABORT and the peer's session ends at once as reset, never by the detach
// bound (spec AbortBy, SAnswerAbort, ResetReaches; lane REVOKEDIRECT: the
// Close shut the session down at once, the agent forgot it and reset the
// client's re-attaches until the bound).
func TestAbortThenCloseLingers(t *testing.T) {
	h := newHarness(t, 64, true)
	h.forget = true
	h.directUp()
	h.move("direct")
	waitFor(t, func() bool { return h.c.Path() == "direct" && h.s.Path() == "direct" })
	rc := readAll(h.c)
	h.cur().sv.w.set(func(p *pipe) { p.stalled = true }) // the agent's ABORT held
	h.s.Abort()
	_ = h.s.Close()
	h.die("direct") // and lost: the client falls back to a relayed stream
	start := time.Now()
	select {
	case err := <-rc:
		if !errors.Is(err, ErrReset) {
			t.Fatalf("the client's session ended with %v, want the reset", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the client's session did not take the reset")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("the reset took %v", d)
	}
}
