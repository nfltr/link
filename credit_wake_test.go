// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestConcurrentAcquiresShareAGrant: three goroutines Acquire on one
// session (5, 2 and 1 bytes), its credit spent; a grant of 3 is enough
// for the 2 and the 1, and both must return, the 5 waiting on; the next
// grant returns it (spec/CreditWaiters.tla, lane LINKWAKE). The code
// before woke one waiter per grant: the other slept with credit there.
func TestConcurrentAcquiresShareAGrant(t *testing.T) {
	c := NewCredit(8, 8, func(string, int) {})
	c.Open("s")
	if !c.Acquire("s", 8) {
		t.Fatal("the window's credit")
	}
	done := make(chan int, 3)
	for _, n := range []int{5, 2, 1} {
		go func() {
			if c.Acquire("s", n) {
				done <- n
			}
		}()
	}
	waitAcquiring(t, 3)
	if err := c.Grant("s", 3); err != nil {
		t.Fatal(err)
	}
	got := 0
	for range 2 {
		select {
		case n := <-done:
			got += n
		case <-time.After(5 * time.Second): // only against a hang
			t.Fatalf("Acquires of %d bytes returned after a grant of 3; want 2 and 1 (a waiter sleeps with credit there)", got)
		}
	}
	if got != 3 {
		t.Fatalf("Acquires of %d bytes returned after a grant of 3, want the 2 and the 1", got)
	}
	if err := c.Grant("s", 5); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-done:
		if n != 5 {
			t.Fatalf("returned %d, want 5", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the Acquire of 5 did not return after a grant of 5")
	}
}

// waitAcquiring waits until n goroutines wait in Credit.acquire's select.
func waitAcquiring(t *testing.T, n int) {
	t.Helper()
	buf := make([]byte, 1<<20)
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(time.Millisecond) {
		k := 0
		for _, g := range strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
			if strings.Contains(g, "[select") && strings.Contains(g, "(*Credit).acquire") {
				k++
			}
		}
		if k >= n {
			return
		}
	}
	t.Fatalf("fewer than %d goroutines wait in Credit.acquire", n)
}
