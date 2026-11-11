// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

// TestPeerWholeCloseDeliversTheTail (not TestJoin*: spec-traces checks
// those against TraceJoin.tla, which leaves the whole close to
// JoinClose.tla, untraced): the agent writes its tail and closes
// the session whole (a TLS server's alert, then its close); the client,
// not reading yet, writes a byte whose write onto the session fails. The
// client then reads the whole tail, then EOF: the failed write does not
// close the client before the other direction wrote the tail to it
// (spec/JoinClose.tla TailDelivered, mutant close-at-write-fail; nfltr
// lane REFUSALFLAKE: a refused client read EOF without the alert).
func TestPeerWholeCloseDeliversTheTail(t *testing.T) {
	tail := bytes.Repeat([]byte("alert"), 1000)
	ln, rn := tcpPair(t)
	ctx := context.Background()
	agentClosed := make(chan struct{})
	go func() {
		c, _, err := link.Server(ctx, ln, link.Config{Auth: link.NoAuth})
		if err != nil {
			_ = ln.Close()
			return
		}
		_ = link.NewMux(c).Serve(func(_ []byte, s *link.Session) {
			_, _ = s.Write(tail)
			_ = s.Close()
			close(agentClosed)
		})
	}()
	rc, err := link.Client(ctx, rn, link.Config{})
	if err != nil {
		t.Fatal(err)
	}
	rm := link.NewMux(rc)
	go func() { _ = rm.Serve(nil) }()
	s, err := rm.Open([]byte("h"))
	if err != nil {
		t.Fatal(err)
	}
	client, relay := net.Pipe() // the joint's write to the client waits for its read
	t.Cleanup(func() { _ = client.Close() })
	go func() { _ = peerlink.Join(relay, s) }()
	<-agentClosed
	select { // the agent's whole close reached the joint
	case <-s.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the agent's close never reached the joint")
	}
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // the joint's write onto the session fails meanwhile (makes the race certain)
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(client)
	if err != nil || !bytes.Equal(got, tail) {
		t.Fatalf("read %d of the agent's %d bytes, then %v: want all, then EOF", len(got), len(tail), err)
	}
}
