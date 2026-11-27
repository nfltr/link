// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"errors"
	"io"
	"strconv"
	"testing"
	"time"
)

// TestEndReadAfterMove (spec DirectPath.tla AckSent, mutant
// ack-end-unsent; lane ENDACKFIX): one byte and the END taken, then a
// move to the direct path before the program read them. The count that
// opened the new stream (the client's OPEN, the server's RESUME) has no
// END, so its sender sends the END again there. The reader's ACK of it
// once its program read it (or its Abort's ACK) waits for that END: before,
// it exceeded what the sender had sent on the stream, and the session
// reset ("pathswitch: protocol error: ACK(2797), sent 2796", nfltr
// TestGroupDial's e2ee dial on the direct path).
func TestEndReadAfterMove(t *testing.T) {
	for _, tc := range []struct {
		name          string
		serverReads   bool
		abortAfterEOF bool
	}{
		{"server reads to EOF", true, false},
		{"server reads to EOF, then Abort", true, true},
		{"client reads to EOF", false, false},
		{"client reads to EOF, then Abort", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.serverReads { // its RESUME and ACK held behind the stall: the failing order each time
				endReadAfterMove(t, true, tc.abortAfterEOF)
				return
			}
			for i := range 10 { // the client's ACK against the server's END sent again: a race, run often
				t.Run(strconv.Itoa(i), func(t *testing.T) { endReadAfterMove(t, false, tc.abortAfterEOF) })
			}
		})
	}
}

func endReadAfterMove(t *testing.T, serverReads, abortAfterEOF bool) {
	h := newHarness(t, 64<<10, false)
	snd, rcv := h.c, h.s
	if !serverReads {
		snd, rcv = h.s, h.c
	}
	go func() { _, _ = io.ReadAll(snd) }() // the other direction, unused
	if _, err := snd.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	_ = snd.CloseWrite()
	waitFor(t, func() bool { rcv.mu.Lock(); defer rcv.mu.Unlock(); return rcv.rEnd }) // taken, not read
	cl, sv := memPair()
	held := cl.r // the reader's frames to the sender: the server's RESUME, then its ACK
	if !serverReads {
		held = cl.w // the client's OPEN, then its ACK
	}
	held.set(func(p *pipe) { p.stalled = true })
	h.mu.Lock()
	if _, err := h.c.Move(cl, "direct", token); err != nil {
		h.mu.Unlock()
		t.Fatal(err)
	}
	h.str = append(h.str, hstream{cl, sv, "direct"})
	h.mu.Unlock()
	go h.accept(sv, "direct")
	if serverReads {
		waitFor(t, func() bool { return h.s.Epoch() == 2 }) // its RESUME without the END
	}
	if b, err := io.ReadAll(rcv); err != nil || len(b) != 1 {
		t.Fatalf("read %d bytes, %v", len(b), err)
	}
	if abortAfterEOF {
		rcv.Abort()
	}
	time.Sleep(10 * time.Millisecond) // the reader's frames queue behind the stall (the failing order); no outcome waits on it
	held.set(func(p *pipe) { p.stalled = false })
	if abortAfterEOF {
		waitFor(t, func() bool { snd.mu.Lock(); defer snd.mu.Unlock(); return snd.err != nil })
	} else if !snd.WaitAcked(snd.Written()+1, 5*time.Second) {
		t.Error("the END was not acknowledged")
	}
	snd.mu.Lock()
	err := snd.err
	snd.mu.Unlock()
	if errors.Is(err, ErrProtocol) {
		t.Fatalf("the sender: %v", err)
	}
	if abortAfterEOF && !errors.Is(err, ErrReset) {
		t.Fatalf("the sender after the reader's Abort: %v, want ErrReset", err)
	}
}
