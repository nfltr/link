// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
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
	const n = 1 << 20
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
	for i := range 20000 {
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
