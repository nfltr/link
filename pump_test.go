// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

// pumpResult is what a Pump's done reported.
type pumpResult struct {
	n   int64
	err error
}

// lockedBuffer is a bytes.Buffer safe from the pump's goroutine.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

// Pump delivers every byte in order, then done once with the total and a
// clean end; bytes queued before Pump are delivered too.
func TestSessionPump(t *testing.T) {
	s, p := openPair(t, v2cfg, v2cfg)
	want := bytes.Repeat([]byte("0123456789abcdef"), 3*testWindow/16+5)
	if _, err := p.Write(want[:100]); err != nil { // queued before the pump
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return s.q.Ready() })
	var got lockedBuffer
	done := make(chan pumpResult, 2)
	s.Pump(&got, func(n int64, err error) { done <- pumpResult{n, err} })
	go func() {
		for i := 100; i < len(want); i += 1000 { // many small frames: the pump stops and restarts
			if _, err := p.Write(want[i:min(i+1000, len(want))]); err != nil {
				t.Errorf("Write: %v", err)
				return
			}
		}
		_ = p.CloseWrite()
	}()
	select {
	case r := <-done:
		if r.err != nil || r.n != int64(len(want)) {
			t.Fatalf("done(%d, %v); want (%d, nil)", r.n, r.err, len(want))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("done not called")
	}
	got.mu.Lock()
	defer got.mu.Unlock()
	if !bytes.Equal(got.b.Bytes(), want) {
		t.Fatalf("pumped %d bytes, want %d intact", got.b.Len(), len(want))
	}
	select {
	case r := <-done:
		t.Fatalf("done called twice: %v", r)
	case <-time.After(20 * time.Millisecond):
	}
}

// An idle pumped session has no goroutine of its own.
func TestSessionPumpIdleHoldsNoGoroutine(t *testing.T) {
	const n = 50
	got := make(chan *Session, n)
	dm, _ := v2muxPair(t, func(_ []byte, s *Session) { got <- s }, v2cfg, v2cfg)
	before := runtime.NumGoroutine()
	for range n {
		s, err := openV2(dm, "h")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		p := <-got
		p.Pump(io.Discard, func(int64, error) {})
	}
	waitUntil(t, func() bool { return runtime.NumGoroutine() <= before+n/10 })
}

// A pump whose writer fails reports the error once; a session that was
// cut reads the cut's error.
func TestSessionPumpErrors(t *testing.T) {
	s, p := openPair(t, v2cfg, v2cfg)
	done := make(chan pumpResult, 2)
	boom := errors.New("boom")
	s.Pump(failWriter{boom}, func(n int64, err error) { done <- pumpResult{n, err} })
	if _, err := p.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if r := <-done; !errors.Is(r.err, boom) {
		t.Fatalf("done(%d, %v); want boom", r.n, r.err)
	}
	if _, err := p.Write([]byte("more")); err != nil { // queued, never pumped
		t.Fatal(err)
	}
	select {
	case r := <-done:
		t.Fatalf("done called twice: %v", r)
	case <-time.After(50 * time.Millisecond):
	}

	s, p = openPair(t, v2cfg, v2cfg)
	s.Pump(io.Discard, func(n int64, err error) { done <- pumpResult{n, err} })
	_ = p.Reset()
	if r := <-done; !errors.Is(r.err, ErrReset) {
		t.Fatalf("done(%d, %v) after a reset; want ErrReset", r.n, r.err)
	}
}

// A pump whose writer blocks stalls only its session: 8 other pumped
// sessions on the connection move 1 MiB each meanwhile (the receive loop
// never waits on a pump).
func TestSessionPumpStuckWriterIsolation(t *testing.T) {
	stuck := make(chan struct{})
	defer close(stuck)
	accept := func(h []byte, s *Session) {
		var w io.Writer = io.Discard
		if string(h) == "stuck" {
			w = blockWriter(stuck)
		}
		s.Pump(w, func(int64, error) { _ = s.Close() })
	}
	dm, _ := v2muxPair(t, accept, v2cfg, v2cfg)
	st, err := openV2(dm, "stuck")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = st.Write(make([]byte, 8*testWindow)) }() // stops at the window
	time.Sleep(50 * time.Millisecond)
	errs := make(chan error, 8)
	for range 8 {
		go func() {
			s, err := openV2(dm, "sink")
			if err == nil {
				_, err = s.Write(make([]byte, 1<<20))
				_ = s.Close()
			}
			errs <- err
		}()
	}
	for range 8 {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("sessions stalled beside a stuck pump")
		}
	}
}

type blockWriter chan struct{}

func (b blockWriter) Write([]byte) (int, error) {
	<-b
	return 0, io.ErrClosedPipe
}

type failWriter struct{ err error }

func (f failWriter) Write([]byte) (int, error) { return 0, f.err }
