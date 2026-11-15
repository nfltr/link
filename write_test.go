// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"errors"
	"os"
	"testing"
	"time"
)

// A Write already waiting for credit when the session's first write
// deadline is set (a past one) returns os.ErrDeadlineExceeded. Before, it
// waited on writeWait's nil channel, which SetWriteDeadline cannot close,
// until a grant that never came (spec/SessionWrite.tla "nil-channel";
// found by nettest's PresentTimeout through relay's client/nfnet).
func TestSessionWriteWaitingForCreditSeesFirstDeadline(t *testing.T) {
	s, _ := openPair(t, v2cfg, v2cfg) // the peer never reads: no grant
	done := make(chan error, 1)
	go func() {
		b := make([]byte, 1024)
		for {
			if _, err := s.Write(b); err != nil {
				done <- err
				return
			}
		}
	}()
	for !s.waitingForCredit() {
		select {
		case err := <-done:
			t.Fatalf("Write ended before it waited for credit: %v", err)
		case <-time.After(time.Millisecond):
		}
	}
	_ = s.SetWriteDeadline(time.Unix(1, 0))
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Write: %v, want os.ErrDeadlineExceeded", err)
		}
	case <-time.After(10 * time.Second): // a bound against the hang only
		t.Fatal("a Write waiting for credit did not see its first write deadline")
	}
}

// waitingForCredit: a Write of s waits in Credit.acquire.
func (s *Session) waitingForCredit() bool {
	c := s.m.c.credit
	c.mu.Lock()
	defer c.mu.Unlock()
	sc := c.sessions[s.id]
	return sc != nil && sc.wake != nil
}
