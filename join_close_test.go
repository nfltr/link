// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// TestJoinWholeCloseDeliversTheTail: the onward peer writes more than the
// origin's window, then closes the session whole (a TLS server's alert,
// then its close); the origin, not reading yet, writes a byte, whose write
// onward fails (the peer takes no more). The origin then reads every byte
// the peer sent, then EOF: the failed write must not close the origin's
// session before the other direction delivered (spec/JoinClose.tla
// TailDelivered, mutant close-at-write-fail; nfltr lane REFUSALFLAKE: a
// refused client read EOF without the agent's alert).
func TestJoinWholeCloseDeliversTheTail(t *testing.T) {
	tail := bytes.Repeat([]byte("alert"), 2*testWindow/5)
	agentClosed := make(chan struct{})
	agent := muxPair(t, func(_ []byte, s *Session) {
		_, _ = s.Write(tail)
		_ = s.Close()
		close(agentClosed)
	})
	onward := make(chan *Session, 1)
	relay := muxPair(t, func(_ []byte, in *Session) {
		out, err := agent.Open([]byte("agent"))
		if err != nil {
			t.Error(err)
			_ = in.Close()
			return
		}
		onward <- out
		_ = Join(in, out)
	})
	c, err := relay.Open([]byte("client"))
	if err != nil {
		t.Fatal(err)
	}
	out := <-onward
	<-agentClosed
	select { // the agent's whole close reached the joint
	case <-out.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the agent's close never reached the joint")
	}
	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // let the joint try the byte onward (the race made certain)
	got, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(got, tail) {
		t.Fatalf("read %d of the agent's %d bytes, then %v: want all, then EOF", len(got), len(tail), err)
	}
}
