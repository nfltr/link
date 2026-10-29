// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"fmt"
	"io"
	"sync"
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
	if debugOn {
		t.Skip("linkdebug records a stack per Read and Write")
	}
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

// Short frames of a session whose reader is behind are queued packed, in
// one buffer, not one each: memory about their bytes, and one Read takes
// them all.
func TestShortFramesQueuePacked(t *testing.T) {
	got := make(chan *Session, 2)
	dm, _ := v2muxPair(t, func(_ []byte, s *Session) { got <- s }, v2cfg, v2cfg)
	a, err := dm.Open([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := dm.Open([]byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	const n = 1000
	for range n {
		if _, err := a.Write([]byte{'x'}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Write([]byte{'y'}); err != nil {
		t.Fatal(err)
	}
	peer := map[string]*Session{}
	for range 2 {
		s := <-got
		peer[string(s.Header())] = s
	}
	// b's byte arrived after a's: one receive loop applies frames in order.
	if _, err := io.ReadFull(peer["b"], make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if k, err := peer["a"].Read(make([]byte, 64<<10)); err != nil || k != n {
		t.Fatalf("Read: %d bytes, %v; want the %d frames' bytes at once", k, err, n)
	}
}

// TestManyWritersWakeForTheirTurn: 64 sessions writing 64 KiB at once on
// one connection wake about one sender per frame, the one whose turn it
// is, not every waiting sender (docs/performance.md, P9). Goroutines
// scheduled to run per message (runtime/metrics /sched/latencies, both
// ends, the receivers included): about 70 when each write woke every
// waiting sender.
func TestManyWritersWakeForTheirTurn(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's scheduling")
	}
	r := newPerfRig(t, "tcp", perfSink)
	const sessions, per = 64, 40
	ss := make([]*Session, sessions)
	for i := range ss {
		s, err := r.a.Open([]byte("sink"))
		if err != nil {
			t.Fatal(err)
		}
		ss[i] = s
	}
	msg := make([]byte, 64<<10)
	w0, _ := schedCounts()
	var wg sync.WaitGroup
	for _, s := range ss {
		wg.Go(func() { send(t, s, msg, per) })
	}
	wg.Wait()
	w1, _ := schedCounts()
	if w := 8 * float64(w1-w0) / (sessions * per); w > 20 {
		t.Errorf("%.1f goroutines woken per 64 KiB message, want at most 20", w)
	} else {
		t.Logf("%.1f goroutines woken per 64 KiB message", w)
	}
}
