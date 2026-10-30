// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
)

// TestSessionOnGrant: as link's Session.OnGrant, a session's callback gets
// each window update (MAX_STREAM_DATA) its peer sends as the peer's reader
// takes what this side wrote, after this side's Writes returned; a later
// OnGrant replaces it, and nil removes it.
func TestSessionOnGrant(t *testing.T) {
	const window, half, slack = 128 << 10, 64 << 10, 1 << 10 // slack: the session's preamble
	read := make(chan struct{}) // the accept side reads half a window, a quarter at a time
	readDone := make(chan struct{})
	pc := listenUDP(t)
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Window: window})
	a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", pc.LocalAddr().String()), Window: window})
	go func() {
		_ = b.Serve(func(_ string, _ []byte, s peerlink.Session) {
			defer s.Close() //nolint:errcheck
			for range read {
				for range 2 {
					if _, err := io.ReadFull(s, make([]byte, window/4)); err != nil {
						t.Error(err)
					}
				}
				readDone <- struct{}{}
			}
		})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ps, err := a.Open(ctx, "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	s := ps.(*quiclink.Session)
	defer s.Close() //nolint:errcheck
	defer close(read)

	granted := make(chan struct{}, 1)
	counter := func(sum *atomic.Int64) func(int) {
		return func(n int) {
			sum.Add(int64(n))
			select {
			case granted <- struct{}{}:
			default:
			}
		}
	}
	// step writes half a window (inside the peer's: no grant needed for
	// it), has the peer read it, and waits for want to get a grant: the
	// peer sends one once it read a quarter window past its last one.
	step := func(want *atomic.Int64) {
		t.Helper()
		before := int64(0)
		if want != nil {
			before = want.Load()
		}
		if _, err := s.Write(make([]byte, half)); err != nil {
			t.Fatal(err)
		}
		read <- struct{}{}
		<-readDone
		for want != nil && want.Load() == before {
			select {
			case <-granted:
			case <-time.After(10 * time.Second): // only against a hang
				t.Fatalf("no grant after the peer read %d bytes", half)
			}
		}
	}
	var first, second, third atomic.Int64
	s.OnGrant(counter(&first))
	step(&first)
	s.OnGrant(counter(&second))
	step(&second)
	s.OnGrant(nil)
	step(nil)
	s.OnGrant(counter(&third))
	step(&third) // grants arrive in order: the ones of the steps before came first
	// The grants add up to what the peer had read when it sent the last:
	// first's to at most the first step's bytes, first's and second's to
	// at most the second's; a grant of the step after reaches past them.
	if f, sc := first.Load(), second.Load(); f > half+slack || f+sc > 2*half+slack {
		t.Fatalf("granted %d, then %d: a replaced or removed callback got grants", f, sc)
	}
}
