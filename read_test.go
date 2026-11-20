// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

// Two goroutines may Read one session at once (net.Conn allows it): each
// byte goes to exactly one of them, none twice, none lost. Before, both
// took the same unread tail (s.rest) and released its buffer twice
// (go test -race reports it; without -race the sum is off).
func TestSessionConcurrentReads(t *testing.T) {
	s, p := openPair(t, v2cfg, v2cfg)
	n := 1 << 20
	if traceOn { // spec-traces checks each step: a trace TLC takes in seconds
		n = 2000
	}
	go func() {
		b := make([]byte, 1000)
		for i := range b {
			b[i] = byte(i%251) + 1
		}
		for sent := 0; sent < n; sent += len(b) {
			if _, err := p.Write(b[:min(len(b), n-sent)]); err != nil {
				return
			}
		}
		_ = p.CloseWrite()
	}()
	var want int
	for i := range n {
		want += i%1000%251 + 1
	}
	var mu sync.Mutex
	var got, sum int
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			b := make([]byte, 7) // smaller than a slice: Read keeps a tail
			for {
				m, err := s.Read(b)
				mu.Lock()
				got += m
				for _, x := range b[:m] {
					sum += int(x)
				}
				mu.Unlock()
				if err != nil {
					if err != io.EOF {
						t.Errorf("Read: %v", err)
					}
					return
				}
			}
		})
	}
	wg.Wait()
	if got != n || sum != want {
		t.Fatalf("read %d bytes summing %d; want %d summing %d", got, sum, n, want)
	}
}

// A read deadline that fires while SetReadDeadline clears or moves it
// never ends a live session's Read with io.EOF: the pending Read returns
// os.ErrDeadlineExceeded, or waits on and takes the next byte. Before,
// Read decided by the deadline flag after the queue let it go, and a
// flag cleared in between read as the session's end.
func TestSessionReadDeadlineClearedNeverEOF(t *testing.T) {
	s, p := openPair(t, v2cfg, v2cfg)
	b := make([]byte, 1)
	rounds := 20000
	if traceOn {
		rounds = 400
	}
	for i := range rounds {
		_ = s.SetReadDeadline(time.Now().Add(20 * time.Microsecond))
		errc := make(chan error, 1)
		go func() { _, err := s.Read(b); errc <- err }()
		time.Sleep(time.Duration(i%40) * time.Microsecond)
		if i%2 == 0 {
			_ = s.SetReadDeadline(time.Time{})
		} else {
			_ = s.SetReadDeadline(time.Now().Add(time.Hour))
		}
		if _, err := p.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		err := <-errc
		switch {
		case err == nil:
			continue
		case errors.Is(err, os.ErrDeadlineExceeded):
			if _, err := s.Read(b); err != nil { // the byte is still there
				t.Fatalf("iteration %d: Read after the deadline: %v", i, err)
			}
		default:
			t.Fatalf("iteration %d: Read on a live session: %v", i, err)
		}
	}
}

// sumWriter counts and sums what it is given; as an OwnedWriter it keeps
// the slices (and never hands them back to the pool).
type sumWriter struct {
	mu       sync.Mutex
	got, sum int
}

func (w *sumWriter) add(b []byte) {
	w.mu.Lock()
	w.got += len(b)
	for _, x := range b {
		w.sum += int(x)
	}
	w.mu.Unlock()
}

func (w *sumWriter) Write(b []byte) (int, error) { w.add(b); return len(b), nil }

type ownedSumWriter struct{ *sumWriter }

func (w ownedSumWriter) WriteOwned(b []byte) error { w.add(b); return nil }

// A Read beside WriteTo (io.Copy(dst, sess): misuse, but nothing stopped
// it) or Pump on one session: each byte goes to one of them, none twice,
// none lost, and no buffer goes back to the pool while it is read
// (SessionRead.tla, SessionReadWriteTo.cfg; a Read after Pump returns
// errPumped, SessionReadPump.cfg). Before, WriteTo and Pump
// wrote the unread tail of a Read (s.rest) without rmu, then cut it and
// released its buffer: the bytes went twice, or a Read's new tail was cut
// (go test -race reports it at once; without -race the sum is off).
func TestSessionReadBesideWriteTo(t *testing.T) {
	got := make(chan *Session, 1)
	dm, _ := v2muxPair(t, func(_ []byte, s *Session) { got <- s }, v2cfg, v2cfg)
	msg := make([]byte, 1000)
	want := 0
	for i := range msg {
		msg[i] = byte(i%251) + 1
		want += int(msg[i])
	}
	rounds := 300
	if traceOn {
		rounds = 6
	}
	for i := range rounds {
		p, err := dm.Open(context.Background(), []byte("h"))
		if err != nil {
			t.Fatal(err)
		}
		s := <-got
		if _, err := p.Write(msg); err != nil {
			t.Fatal(err)
		}
		_ = p.CloseWrite()
		w := &sumWriter{}
		one := make([]byte, 1)
		if _, err := io.ReadFull(s, one); err != nil { // leaves a tail
			t.Fatal(err)
		}
		w.add(one)
		var wg sync.WaitGroup
		wg.Go(func() {
			b := make([]byte, 7)
			for {
				m, err := s.Read(b)
				w.add(b[:m])
				if err != nil {
					return
				}
			}
		})
		switch i % 3 {
		case 0:
			_, err = s.WriteTo(w)
		case 1:
			_, err = s.WriteTo(ownedSumWriter{w})
		default:
			done := make(chan error, 1)
			s.startPump(w, func(_ int64, err error) { done <- err })
			err = <-done
		}
		if err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if w.got != len(msg) || w.sum != want {
			t.Fatalf("round %d: handed out %d bytes summing %d; want %d summing %d", i, w.got, w.sum, len(msg), want)
		}
		_ = s.Close()
		_ = p.Close()
	}
}
