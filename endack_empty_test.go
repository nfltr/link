// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"testing"
	"time"
)

// TestEndAckEmptyStreamAfterThePeersWholeClose (spec/EndAckLeg.tla
// EmptyClosed, NoFalseReset, mutant close-resets; lane PIPERESET2): the
// peer closed the session whole, cleanly, before this side's
// CloseWriteSync on a stream that carried no byte: nothing is lost, nil
// (TestEndAckPeerClosedWholeFirstFails: with a byte written, ErrReset).
func TestEndAckEmptyStreamAfterThePeersWholeClose(t *testing.T) {
	accepted := make(chan *Session, 1)
	am, _ := v2muxPair(t, func(_ []byte, s *Session) { accepted <- s },
		Config{Window: testWindow, Auth: testToken}, Config{Window: testWindow, Auth: testToken})
	a, err := am.Open(context.Background(), []byte("p"))
	if err != nil {
		t.Fatal(err)
	}
	_ = (<-accepted).Close()
	select {
	case <-a.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the peer's close never ended the session")
	}
	if err := a.CloseWriteSync(); err != nil {
		t.Fatalf("CloseWriteSync of an empty stream after the peer's clean close: %v, want nil", err)
	}
}

// TestEndAckEmptyStreamRelayedCloseRaceIsClean (spec/RelayedClose.tla
// RelayedCloseEmpty.cfg NoFalseReset, mutant close-resets; relay-core
// TestConnIsANetConn/BasicIO, lane PIPERESET2): C ended its stream and
// closes the session whole while A, which wrote nothing, ends its own: the
// hop's close reaches A before or after A's FIN reached the hop, and A's
// CloseWriteSync is nil either way.
func TestEndAckEmptyStreamRelayedCloseRaceIsClean(t *testing.T) {
	for i := range 20 {
		r := newHopRig(t)
		go func() { _ = r.c.Close() }()
		if err := r.a.CloseWriteSync(); err != nil {
			t.Fatalf("run %d: A's CloseWriteSync of an empty stream: %v, want nil", i, err)
		}
	}
}
