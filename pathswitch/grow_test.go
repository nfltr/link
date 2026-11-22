// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

// latePipe delivers each write d after it was written, in order: a path
// with a round trip of 2d and no bandwidth limit.
type latePipe struct {
	mu   sync.Mutex
	cond *sync.Cond
	q    []lateChunk
	d    time.Duration
	fin  bool
}

type lateChunk struct {
	b  []byte
	at time.Time
}

func (p *latePipe) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.q) == 0 && !p.fin {
		p.cond.Wait()
	}
	if len(p.q) == 0 {
		return 0, io.EOF
	}
	if w := time.Until(p.q[0].at); w > 0 {
		p.mu.Unlock()
		time.Sleep(w)
		p.mu.Lock()
	}
	n := copy(b, p.q[0].b)
	if p.q[0].b = p.q[0].b[n:]; len(p.q[0].b) == 0 {
		p.q = p.q[1:]
	}
	return n, nil
}

func (p *latePipe) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.q = append(p.q, lateChunk{bytes.Clone(b), time.Now().Add(p.d)})
	p.cond.Broadcast()
	return len(b), nil
}

type lateStream struct{ r, w *latePipe }

// MinRTT: a direct stream's (RoundTripper).
func (s *lateStream) MinRTT() time.Duration { return s.r.d + s.w.d }

func (s *lateStream) Read(b []byte) (int, error)  { return s.r.Read(b) }
func (s *lateStream) Write(b []byte) (int, error) { return s.w.Write(b) }
func (s *lateStream) CloseWrite() error {
	s.w.mu.Lock()
	s.w.fin = true
	s.w.cond.Broadcast()
	s.w.mu.Unlock()
	return nil
}
func (s *lateStream) Reset() error { return s.CloseWrite() }

func latePair(d time.Duration) (*lateStream, *lateStream) {
	a, b := &latePipe{d: d}, &latePipe{d: d}
	a.cond, b.cond = sync.NewCond(&a.mu), sync.NewCond(&b.mu)
	return &lateStream{r: a, w: b}, &lateStream{r: b, w: a}
}

func window(c *Conn) uint64 { c.mu.Lock(); defer c.mu.Unlock(); return c.w }

func held(c *Conn) int { c.mu.Lock(); defer c.mu.Unlock(); return c.rbuf.len() }

// growPair: a session over a 20 ms round trip, its window from w up to max.
func growPair(t *testing.T, w, max int) (cl, sv *Conn) {
	a, b := latePair(10 * time.Millisecond)
	cfg := Config{Window: w, MaxWindow: max, Detach: 5 * time.Second}
	sv, cl = Server(b, "relay", cfg), Client(a, "relay", cfg)
	t.Cleanup(func() { cl.Abort(); sv.Abort() })
	return cl, sv
}

// The window stays on a stream that knows no round trip (a relayed one).
func TestWindowFixedWithoutRoundTrip(t *testing.T) {
	a, b := latePair(10 * time.Millisecond)
	cfg := Config{Window: 64 << 10, MaxWindow: 1 << 20, Detach: 5 * time.Second}
	sv, cl := Server(noRTT{b}, "relay", cfg), Client(noRTT{a}, "relay", cfg)
	t.Cleanup(func() { cl.Abort(); sv.Abort() })
	data := payload(2<<20, 13)
	go func() { _, _ = cl.Write(data) }()
	if _, err := io.ReadFull(sv, make([]byte, len(data))); err != nil {
		t.Fatal(err)
	}
	if g := window(cl); g != 64<<10 {
		t.Fatalf("window %d on a stream with no round trip, want %d", g, 64<<10)
	}
}

// noRTT hides a lateStream's MinRTT.
type noRTT struct{ *lateStream }

func (s noRTT) MinRTT() {}

// WitnessGrown: a reader that keeps up on a path whose round trip holds
// more than the window: the window grows to MaxWindow (spec TakeAck).
func TestWindowGrowsWithTheReader(t *testing.T) {
	const w, max = 64 << 10, 1 << 20
	cl, sv := growPair(t, w, max)
	data := payload(8<<20, 11)
	go func() { _, _ = cl.Write(data) }()
	got := make([]byte, len(data))
	if _, err := io.ReadFull(sv, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, equal prefix %d", len(got), prefix(got, data))
	}
	if g := window(cl); g != max {
		t.Fatalf("window %d, want it grown to MaxWindow %d", g, max)
	}
}

// BufferBound, WindowBound traced (make spec-traces): a window of 64
// bytes growing to 256 over a 20 ms path, every byte a trace line.
func TestWindowGrowsTraced(t *testing.T) {
	cl, sv := growPair(t, 64, 256)
	tracePair(t.Name(), cl, sv)
	t.Cleanup(func() {
		var none traceState // the trace ends with the test
		for _, c := range []*Conn{cl, sv} {
			c.mu.Lock()
			c.tr = none
			c.mu.Unlock()
		}
	})
	data := payload(2<<10, 14)
	go func() { _, _ = cl.Write(data); _ = cl.CloseWrite() }()
	got, err := io.ReadAll(sv)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes (%v), equal prefix %d", len(got), err, prefix(got, data))
	}
	if g := window(cl); g != 256 {
		t.Fatalf("window %d, want it grown to 256", g)
	}
}

// AckGate, HeldBound: a slow reader's session keeps its window, its
// sender keeps at most it, its receiver holds at most Window/2 + w.
func TestSlowReaderKeepsTheWindow(t *testing.T) {
	const w, max = 64 << 10, 1 << 20
	cl, sv := growPair(t, w, max)
	data := payload(192<<10, 12)
	go func() { _, _ = cl.Write(data) }()
	got := make([]byte, 0, len(data))
	buf := make([]byte, 4<<10)
	for len(got) < len(data) {
		n, err := sv.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, buf[:n]...)
		if r, h := retained(cl), held(sv); r > w || h > w/2+w {
			t.Fatalf("sender keeps %d, receiver holds %d: want at most %d and %d", r, h, w, w/2+w)
		}
		time.Sleep(20 * time.Millisecond) // 200 KiB/s: a sixteenth of the path's
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, equal prefix %d", len(got), prefix(got, data))
	}
	if g := window(cl); g != w {
		t.Fatalf("window %d grew for a slow reader, want %d", g, w)
	}
}
