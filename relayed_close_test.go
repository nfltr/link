// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// muxPairCut is muxPair whose server side's Mux it also returns: Close
// on it ends the connection there (the agent's exit, as the relay sees it).
func muxPairCut(t *testing.T, accept func([]byte, *Session)) (client, server *Mux) {
	t.Helper()
	ca, cb := v2pair(t, Config{Window: testWindow, Auth: testToken}, Config{Window: testWindow, Auth: testToken})
	client, server = NewMux(ca), NewMux(cb)
	go func() { _ = client.Serve(nil) }()
	go func() { _ = server.Serve(accept) }()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return client, server
}

// waitDone waits for s to end, failing the test after a bound against a hang.
func waitDone(t *testing.T, s *Session, what string) {
	t.Helper()
	select {
	case <-s.Done():
	case <-time.After(10 * time.Second):
		t.Fatalf("%s never ended", what)
	}
}

// TestFinThenFailureReadsEOF: a session whose peer's FIN was read before
// the session failed (the peer's reset, the connection's end) reads every
// byte, then io.EOF; Err still names the failure (spec/LinkSession.tla
// GEnds: the first end a reader learns is its end; RelayedClose.tla
// ReadsEof, mutant fail-drops-fin). Before, it read the failure: a relay
// whose agent exited after its Close passed a reset on to the client.
func TestFinThenFailureReadsEOF(t *testing.T) {
	for _, how := range []string{"reset", "cut"} {
		t.Run(how, func(t *testing.T) {
			tail := []byte("the agent's last bytes")
			sent := make(chan *Session, 1)
			c, srv := muxPairCut(t, func(_ []byte, s *Session) {
				_, _ = s.Write(tail)
				if err := s.CloseWrite(); err != nil { // the FIN, then a sync: the peer's Mux read it
					t.Error(err)
				}
				if err := s.sync(); err != nil { // not CloseWriteSync: its ENDACK waits for a reader (lane ENDACK)
					t.Error(err)
				}
				sent <- s
			})
			s, err := c.Open(context.Background(), []byte("x"))
			if err != nil {
				t.Fatal(err)
			}
			peer := <-sent // the FIN was read here (its ping answered)
			if how == "reset" {
				_ = peer.Reset()
			} else {
				_ = srv.Close()
			}
			waitDone(t, s, "the session")
			got, err := io.ReadAll(s)
			if err != nil || !bytes.Equal(got, tail) {
				t.Fatalf("read %q, %v: want every byte, then io.EOF", got, err)
			}
			if s.Err() == nil {
				t.Fatal("Err is nil: the session failed")
			}
			if _, err := s.Write([]byte("y")); err == nil {
				t.Fatal("a write after the failure succeeded")
			}
		})
	}
}

// TestJoinFailureAfterFinDeliversTheTail: the onward peer (an agent)
// writes more than the origin's window, half-closes and is then cut (its
// program exits); the origin (a client), not reading yet, writes a byte,
// whose write onward fails. The origin then reads every byte and io.EOF:
// the failed write's abort waits for the other direction's tail and FIN
// (spec/RelayedClose.tla TailDelivered, mutant abort-at-write-fail), and
// that direction reads io.EOF after the FIN (mutant fail-drops-fin).
func TestJoinFailureAfterFinDeliversTheTail(t *testing.T) {
	tail := bytes.Repeat([]byte("agent"), 2*testWindow/5)
	agentDone := make(chan struct{})
	agent, agentSrv := muxPairCut(t, func(_ []byte, s *Session) {
		_, _ = s.Write(tail)
		if err := s.CloseWrite(); err != nil {
			t.Error(err)
		}
		if err := s.sync(); err != nil { // the relay holds the tail and the FIN (CloseWriteSync waits for the client's reader)
			t.Error(err)
		}
		close(agentDone)
	})
	onward := make(chan *Session, 1)
	relay := muxPair(t, func(_ []byte, in *Session) {
		out, err := agent.Open(context.Background(), []byte("agent"))
		if err != nil {
			t.Error(err)
			_ = in.Close()
			return
		}
		onward <- out
		_ = join(in, out)
	})
	c, err := relay.Open(context.Background(), []byte("client"))
	if err != nil {
		t.Fatal(err)
	}
	out := <-onward
	<-agentDone
	_ = agentSrv.Close() // the agent exits once the relay holds its tail and FIN
	waitDone(t, out, "the agent's session at the relay")
	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // let the joint try the byte onward (the race made certain)
	got, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(got, tail) {
		t.Fatalf("read %d of the agent's %d bytes, then %v: want all, then io.EOF", len(got), len(tail), err)
	}
	waitDone(t, c, "the client's session")
	if !errors.Is(c.Err(), ErrReset) {
		t.Fatalf("client's Err %v: want the agent's failure passed on (an abort)", c.Err())
	}
}
