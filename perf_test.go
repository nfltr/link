// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"fmt"
	"io"
	"testing"
)

// Guards of the per-message costs that docs/performance.md records: a
// change that brings an allocation, a frame or a read of the connection
// back onto the hot path fails here, not only in a benchmark nobody ran.

// TestSteadyStateAllocs: an echo on an established session (Write, the
// peer's receive loop, its Read and Write back, this side's receive loop
// and Read) allocates nothing per message, for short payloads (read ahead
// and packed) and long ones (pooled buffers).
func TestSteadyStateAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops items at random under -race")
	}
	for _, size := range []int{64, 1 << 10, 4 << 10, 64 << 10} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			r := newPerfRig(t, "tcp", perfEcho)
			s, err := r.a.Open([]byte("echo"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close() //nolint:errcheck
			msg, got := make([]byte, size), make([]byte, size)
			echo := func() {
				if _, err := s.Write(msg); err != nil {
					t.Fatal(err)
				}
				if _, err := io.ReadFull(s, got); err != nil {
					t.Fatal(err)
				}
			}
			for range 100 { // warm the pools and the queues' arrays
				echo()
			}
			if n := testing.AllocsPerRun(500, echo); n != 0 {
				t.Errorf("%d-byte echo: %v allocations per message, want 0", size, n)
			}
		})
	}
}

// TestFramesAndReadsPerMessage: a message of up to MaxChunk bytes is one
// data frame, a longer one a frame per MaxChunk; the sender writes at most
// once per message (a Write flushes, and concurrent frames share a write);
// the receiver reads the connection at most once per short frame (read
// ahead: header, sid and payload together) and at most about twice per
// long one (its header, then the payload straight into its buffer).
func TestFramesAndReadsPerMessage(t *testing.T) {
	for _, tc := range []struct {
		size          int
		frames        int64   // data frames per message
		writes, reads float64 // at most, per message
	}{
		{64, 1, 1, 1.5},
		{4 << 10, 1, 1, 2.5},
		{64 << 10, 1, 1, 2.5},
		{1 << 20, 16, 8, 40},
	} {
		t.Run(fmt.Sprint(tc.size), func(t *testing.T) {
			r := newPerfRig(t, "tcp", perfSink)
			s, err := r.a.Open([]byte("sink"))
			if err != nil {
				t.Fatal(err)
			}
			const n = 200
			tx0, rx0 := r.la.snap(), r.lb.snap()
			send(t, s, make([]byte, tc.size), n)
			tx, rx := r.la.snap().sub(tx0), r.lb.snap().sub(rx0)
			if tx.dataFrames != n*tc.frames {
				t.Errorf("%d data frames for %d messages of %d bytes, want %d", tx.dataFrames, n, tc.size, n*tc.frames)
			}
			// The sender's own frames beyond its data: its CLOSE, its window
			// updates for the sink's (none) and liveness (none this soon).
			if extra := tx.frames - tx.dataFrames; extra > 2 {
				t.Errorf("%d frames besides the data frames", extra)
			}
			if w := float64(tx.writes-2) / n; w > tc.writes { // -2: the CLOSE frames' own
				t.Errorf("%.2f writes of the connection per message, want at most %v", w, tc.writes)
			}
			if rd := float64(rx.reads) / n; rd > tc.reads {
				t.Errorf("%.2f reads of the connection per message, want at most %v", rd, tc.reads)
			}
		})
	}
}

// An idle connection holds no pooled read buffer: after a burst of small
// frames (read ahead into one), the receive loop gives it back at the
// first frame boundary with nothing buffered and waits on its own small
// array.
func TestIdleConnHoldsNoReadBuffer(t *testing.T) {
	ca, cb := v2pair(t, v2cfg, v2cfg)
	a, b := NewMux(ca), NewMux(cb)
	served := make(chan struct{})
	go func() { _ = a.Serve(nil) }()
	go func() { _ = b.Serve(perfEcho); close(served) }()
	s, err := a.Open([]byte("echo"))
	if err != nil {
		t.Fatal(err)
	}
	msg := make([]byte, 64)
	const n = 2000
	go func() {
		for range n {
			if _, err := s.Write(msg); err != nil {
				return
			}
		}
	}()
	if _, err := io.ReadFull(s, make([]byte, n*len(msg))); err != nil {
		t.Fatal(err)
	}
	_ = a.Close() // b's receive loop reads io.EOF where it waited, idle
	<-served
	if cb.rbig != nil {
		t.Fatal("the idle receive loop held a pooled read buffer")
	}
}
