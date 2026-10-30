// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"io"
	"testing"
	"time"
)

// TestSessionOnGrant: a session's OnGrant callback gets each window
// update its peer sends, as the peer's reader takes what this side wrote;
// a later OnGrant replaces it, and nil removes it.
func TestSessionOnGrant(t *testing.T) {
	read := make(chan int)     // the accept side reads this many bytes
	readDone := make(chan int) // and reports it read them
	client := muxPair(t, func(_ []byte, s *Session) {
		defer s.Close() //nolint:errcheck
		for n := range read {
			if _, err := io.ReadFull(s, make([]byte, n)); err != nil {
				t.Error(err)
			}
			readDone <- n
		}
	})
	s, err := client.Open([]byte("slow"))
	if err != nil {
		t.Fatal(err)
	}
	defer close(read)
	first, second := make(chan int, 8), make(chan int, 8)
	s.OnGrant(func(n int) { first <- n })
	if _, err := s.Write(pattern(testWindow, 1)); err != nil { // the peer's whole window: no grant needed
		t.Fatal(err)
	}
	const quarter = testWindow / 4
	step := func(grants chan int) {
		t.Helper()
		read <- quarter
		<-readDone
		select {
		case n := <-grants:
			if n != quarter {
				t.Fatalf("grant of %d bytes, want %d", n, quarter)
			}
		case <-time.After(10 * time.Second): // only against a hang
			t.Fatal("no grant after the peer read a quarter window")
		}
	}
	step(first)
	step(first)
	s.OnGrant(func(n int) { second <- n })
	step(second)
	if len(first) != 0 {
		t.Fatal("the replaced callback got a grant")
	}
	s.OnGrant(nil)
	read <- quarter
	<-readDone
	if _, err := s.Write(pattern(quarter, 2)); err != nil {
		t.Fatal(err)
	}
	third := make(chan int, 8)
	s.OnGrant(func(n int) { third <- n })
	step(third)
	// Grants arrive in order on one receive loop: the one after the nil
	// came before the last, and went to no one.
	if len(first)+len(second) != 0 {
		t.Fatal("a removed callback got a grant")
	}
}
