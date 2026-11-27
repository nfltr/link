// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"errors"
	"io"
	"testing"
	"time"
)

// TestEndAckedOnlyOnceRead (spec DirectPath.tla EndTruth, mutants
// ack-on-receipt and close-took-end; lane ENDACK): R holding every byte
// and the END is no acknowledgement of the END. S's WaitAcked of it is
// true only once R's application read it (io.EOF), or closed its end
// having read every byte; R's Abort after arrival resets the session, and
// a Close that dropped bytes resets it too, so S never reads a clean end
// its peer's program did not take (pipe TestReceiverStopsAfterArrival).
func TestEndAckedOnlyOnceRead(t *testing.T) {
	want := payload(64, 9) // small: the test is traced
	for _, tc := range []struct {
		name  string
		end   func(c *Conn) error // the client's program once it holds the stream
		clean bool
	}{
		{"read to EOF", func(c *Conn) error { _, err := io.ReadAll(c); return err }, true},
		{"read every byte, then Close", func(c *Conn) error {
			if _, err := io.ReadFull(c, make([]byte, len(want))); err != nil {
				return err
			}
			return c.Close()
		}, true},
		{"Abort after arrival", func(c *Conn) error { c.Abort(); return nil }, false},
		{"read to EOF, then Abort", func(c *Conn) error { // the stop after the end: the peer's direction stays clean
			if _, err := io.ReadAll(c); err != nil {
				return err
			}
			c.Abort()
			return nil
		}, true},
		{"read a byte, then Close", func(c *Conn) error {
			if _, err := c.Read(make([]byte, 1)); err != nil {
				return err
			}
			return c.Close()
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, 64<<10, false)
			go func() { _, _ = io.ReadAll(h.s) }()
			if _, err := h.s.Write(want); err != nil {
				t.Fatal(err)
			}
			_ = h.s.CloseWrite()
			if !h.s.WaitAcked(h.s.Written(), 5*time.Second) {
				t.Fatal("the bytes were not acknowledged")
			}
			waitFor(t, func() bool { h.c.mu.Lock(); defer h.c.mu.Unlock(); return h.c.rEnd }) // the client holds the END
			if h.s.WaitAcked(h.s.Written()+1, 200*time.Millisecond) {
				t.Fatal("the END acknowledged before the client's program read it")
			}
			if err := tc.end(h.c); err != nil {
				t.Fatal(err)
			}
			if d, want := h.c.Dropped(), tc.name == "read a byte, then Close"; d != want {
				t.Fatalf("Dropped %v, want %v", d, want)
			}
			if got := h.s.WaitAcked(h.s.Written()+1, 5*time.Second); got != tc.clean {
				t.Fatalf("WaitAcked of the END: %v, want %v", got, tc.clean)
			}
			if !tc.clean {
				waitFor(t, func() bool { _, err := h.s.Write([]byte{1}); return errors.Is(err, ErrReset) })
			}
		})
	}
}
