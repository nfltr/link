// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// holdConn holds its reads once armed until open is closed: a connection
// that stops carrying toward this side.
type holdConn struct {
	net.Conn
	armed *atomic.Bool
	open  chan struct{}
}

func (c holdConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if c.armed.Load() { // a read already waiting when armed is held too
		<-c.open
	}
	return n, err
}

// hopRig is A -> H (a hop: Config.Hop, Join) -> C. C's connection to H
// holds what H sends once armed, until open is closed.
type hopRig struct {
	a, c       *Session
	armed      *atomic.Bool
	open       chan struct{}
	cMux, hToC *Mux
	aMux       *Mux     // A's, on its connection to the hop
	hFromA     *Mux     // the hop's, on A's connection
	jin        *Session // the hop's session from A
}

func newHopRig(t *testing.T) *hopRig { return newHopRigMute(t, false) }

// newHopRigMute is newHopRig whose C, when mute, announces FeatureEndAck
// but never acknowledges (a broken or hostile far end that answers pings).
func newHopRigMute(t *testing.T, mute bool) *hopRig { return newHopRigWith(t, mute, false) }

// newHopRigWith is newHopRigMute whose C, when cOpen, has not half-closed:
// its writes still go to A.
func newHopRigWith(t *testing.T, mute, cOpen bool) *hopRig {
	t.Helper()
	r := &hopRig{armed: new(atomic.Bool), open: make(chan struct{})}
	hc, cc := tcpPair(t)
	var idle time.Duration // a mute C: every wait's idle bound short
	if mute {
		idle = 300 * time.Millisecond
	}
	cCfg := Config{Window: testWindow, Auth: testToken, muteEndAck: mute, endAckIdle: idle}
	type res struct {
		c   *Conn
		err error
	}
	sc := make(chan res, 1)
	go func() {
		c, _, err := Server(t.Context(), holdConn{cc, r.armed, r.open}, cCfg)
		sc <- res{c, err}
	}()
	h2c, err := Client(t.Context(), hc, Config{Window: testWindow, Auth: testToken, Hop: true, endAckIdle: idle})
	if err != nil {
		t.Fatal(err)
	}
	cs := <-sc
	if cs.err != nil {
		t.Fatal(cs.err)
	}
	accepted := make(chan *Session, 1)
	joined := make(chan [2]*Session, 1)
	r.hToC, r.cMux = NewMux(h2c), NewMux(cs.c)
	go func() { _ = r.hToC.Serve(nil) }()
	go func() { _ = r.cMux.Serve(func(_ []byte, s *Session) { accepted <- s }) }()
	t.Cleanup(func() { _ = r.hToC.Close(); _ = r.cMux.Close() })
	var am *Mux
	am, r.hFromA = v2muxPair(t, func(_ []byte, in *Session) {
		out, err := r.hToC.Open(context.Background(), []byte("c"))
		if err != nil {
			_ = in.Reset()
			return
		}
		joined <- [2]*Session{in, out}
		_ = join(in, out)
	}, Config{Window: testWindow, Auth: testToken, endAckIdle: idle}, Config{Window: testWindow, Auth: testToken, Hop: true, endAckIdle: idle})
	r.aMux = am
	if r.a, err = am.Open(context.Background(), []byte("h")); err != nil {
		t.Fatal(err)
	}
	r.c = <-accepted
	if !cOpen {
		if err := r.c.CloseWrite(); err != nil { // C sends nothing (pipe's dialler, empty stdin)
			t.Fatal(err)
		}
		if b, err := io.ReadAll(r.a); err != nil || len(b) != 0 {
			t.Fatalf("A read %q, %v", b, err)
		}
	}
	j := <-joined
	r.jin = j[0]
	traceRelayed(t.Name(), r.a, j[0], j[1], r.c) // -tags linktrace: spec/TraceRelayedClose.tla
	if mute {
		traceRCEnv("CMute")
	}
	t.Cleanup(traceRelayedOff) // before the rig's cleanups end its connections
	return r
}

// TestEndAckWaitsForTheFarEnd (spec/RelayedClose.tla CloseTruth, mutant
// relay-read-ack): through a hop, A's CloseWriteSync returns once C's Mux
// took the end, not once the hop read it (pipe
// TestRelayedEndDropAfterLastByte: the hop's pong let the agent exit 0
// while the client's link was not carrying).
func TestEndAckWaitsForTheFarEnd(t *testing.T) {
	r := newHopRig(t)
	r.armed.Store(true)
	tail := bytes.Repeat([]byte("a"), 1000)
	if _, err := r.a.Write(tail); err != nil {
		t.Fatal(err)
	}
	ret := make(chan error, 1)
	go func() { ret <- r.a.CloseWriteSync() }()
	select {
	case err := <-ret:
		t.Fatalf("CloseWriteSync returned %v while C's connection carried nothing", err)
	case <-time.After(300 * time.Millisecond): // the hop read the end long since
	}
	close(r.open)
	select {
	case err := <-ret:
		if err != nil {
			t.Fatalf("CloseWriteSync: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CloseWriteSync never returned once C took the end")
	}
	if b, err := io.ReadAll(r.c); err != nil || !bytes.Equal(b, tail) {
		t.Fatalf("C read %d bytes, %v", len(b), err)
	}
}

// TestEndAckFailsWhenTheFarEndIsCut (CloseEnds, SyncEnds): C's
// connection ends before it took the end: A's CloseWriteSync fails.
func TestEndAckFailsWhenTheFarEndIsCut(t *testing.T) {
	r := newHopRig(t)
	r.armed.Store(true)
	if _, err := r.a.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	ret := make(chan error, 1)
	go func() { ret <- r.a.CloseWriteSync() }()
	time.Sleep(100 * time.Millisecond) // the hop holds the end
	traceRCEnv("CCut")
	_ = r.hToC.Close() // C's connection ends at the hop
	select {
	case err := <-ret:
		if err == nil {
			t.Fatal("CloseWriteSync returned nil: C never took the end")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CloseWriteSync never returned after C's connection ended")
	}
}

// TestEndAckFailsWhenItsConnectionIsCut (spec/RelayedClose.tla ADown,
// CloseEnds): A's own connection to the hop ends while its Close waits for
// the far end's ENDACK (C takes nothing): the Close fails at once, never
// waiting for an ENDACK that connection can no longer carry.
func TestEndAckFailsWhenItsConnectionIsCut(t *testing.T) {
	r := newHopRig(t)
	r.armed.Store(true)
	if _, err := r.a.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	ret := make(chan error, 1)
	go func() { ret <- r.a.CloseWriteSync() }()
	time.Sleep(100 * time.Millisecond) // the hop holds the end
	traceRCEnv("Cut")
	_ = r.aMux.c.nc.Close() // the network cuts A's connection
	select {
	case err := <-ret:
		if err == nil {
			t.Fatal("CloseWriteSync returned nil: C never took the end")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CloseWriteSync never returned after its connection ended")
	}
	close(r.open)
}

// TestEndAckGivesUpWithoutProgress (spec/RelayedClose.tla CloseEnds,
// SyncEnds, mutant liveness-counts-as-progress): a far end that answers
// pings but never acknowledges the end holds neither the hop's onward
// wait nor A's Close past the idle bound: they give up once nothing of
// the session was acknowledged for that long, whatever the pings say.
func TestEndAckGivesUpWithoutProgress(t *testing.T) {
	r := newHopRigMute(t, true)
	if _, err := r.a.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	ret := make(chan error, 1)
	go func() { ret <- r.a.CloseWriteSync() }()
	select {
	case err := <-ret:
		if err == nil {
			t.Fatal("CloseWriteSync returned nil: the far end acknowledged nothing")
		}
	case <-time.After(10 * time.Second): // the connections' pings keep them up
		t.Fatal("CloseWriteSync waited past the idle bound for a mute far end")
	}
	if b, err := io.ReadAll(r.c); !bytes.Equal(b, []byte("tail")) {
		t.Fatalf("C read %q, %v: the bytes still went", b, err)
	}
}

// TestEndAckResetEndsTheWait: a session reset here after its half close
// (a local side that took the EOF, then failed: nfltr connect's Pump)
// waits for no ENDACK: a CloseWriteSync after the Reset (nfnet Conn.Close)
// returns at once with ErrReset, not after the idle bound (30 s).
func TestEndAckResetEndsTheWait(t *testing.T) {
	r := newHopRig(t)
	r.armed.Store(true) // C takes nothing: no ENDACK can come
	if err := r.a.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_ = r.a.Reset()
	ret := make(chan error, 1)
	go func() { ret <- r.a.CloseWriteSync() }()
	select {
	case err := <-ret:
		if !errors.Is(err, ErrReset) {
			t.Fatalf("CloseWriteSync after Reset: %v, want ErrReset", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CloseWriteSync after Reset waited for an ENDACK")
	}
	close(r.open)
}

// TestEndAckCloseWriteRacingResetEndsTheWait: a CloseWrite racing a Reset
// (nfltr connect's Pump: one direction's EOF, the other's local failure)
// that registers its ENDACK wait after the Reset ended the session here,
// but before the Reset dropped it, waits for no ENDACK: the
// CloseWriteSync after them returns at once (box: TestConnectMappings
// drain_at_shutdown waited the 30 s idle bound).
func TestEndAckCloseWriteRacingResetEndsTheWait(t *testing.T) {
	r := newHopRig(t)
	r.armed.Store(true) // C takes nothing: no ENDACK can come
	r.a.endHere(nil)    // Reset's first step; its drop has not run yet
	if err := r.a.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	ret := make(chan error, 1)
	go func() { ret <- r.a.CloseWriteSync() }()
	select {
	case err := <-ret:
		if !errors.Is(err, ErrReset) {
			t.Fatalf("CloseWriteSync: %v, want ErrReset", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CloseWriteSync waited for an ENDACK of a session reset here")
	}
	close(r.open)
}

// TestEndAckExitThenTheFarEndWrites (spec/RelayedClose.tla Exit, JDown,
// D0Fail, TailDelivered): A's Close returns clean (C took the end), then
// A's program exits; the hop finds A's connection ended, and C's write
// toward A fails there: C reads A's bytes and io.EOF, its session then
// aborted (the failure passed on after the FIN).
func TestEndAckExitThenTheFarEndWrites(t *testing.T) {
	r := newHopRigWith(t, false, true)
	if _, err := r.a.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := r.a.CloseWriteSync(); err != nil {
		t.Fatalf("CloseWriteSync: %v", err)
	}
	traceRCEnv("Exit")
	_ = r.aMux.Close() // A's program exits: its connection ends
	waitDone(t, r.jin, "the hop's session from A")
	if _, err := r.c.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if b, err := io.ReadAll(r.c); err != nil || string(b) != "tail" {
		t.Fatalf("C read %q, %v: want A's bytes, then io.EOF", b, err)
	}
	waitDone(t, r.c, "C's session")
	if r.c.Err() == nil {
		t.Fatal("C's session ended clean: A's failure was not passed on")
	}
}

// TestEndAckCutThenTheFarEndWrites (spec/RelayedClose.tla JDown, D0Fail
// held, TailDelivered): A's connection is cut while its Close waits for
// C, which takes nothing yet, then writes toward A: the write fails at the
// hop, and its abort of C waits for A's tail and FIN, which C then reads,
// then io.EOF.
func TestEndAckCutThenTheFarEndWrites(t *testing.T) {
	r := newHopRigWith(t, false, true)
	r.armed.Store(true)
	if _, err := r.a.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	ret := make(chan error, 1)
	go func() { ret <- r.a.CloseWriteSync() }()
	time.Sleep(100 * time.Millisecond) // the hop holds the end
	traceRCEnv("Cut")
	_ = r.aMux.c.nc.Close() // the network cuts A's connection
	if err := <-ret; err == nil {
		t.Fatal("CloseWriteSync returned nil: C never took the end")
	}
	waitDone(t, r.jin, "the hop's session from A")
	if _, err := r.c.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // the hop reads the byte and fails to write it on
	close(r.open)
	if b, err := io.ReadAll(r.c); err != nil || string(b) != "tail" {
		t.Fatalf("C read %q, %v: want A's bytes, then io.EOF", b, err)
	}
}

// TestEndAckFailsWhenTheRelayIsGone (spec/RelayedClose.tla RelayGone,
// CloseEnds): the relay's process ends while A's Close waits for C: both
// of its connections end, and A's Close fails.
func TestEndAckFailsWhenTheRelayIsGone(t *testing.T) {
	r := newHopRig(t)
	r.armed.Store(true)
	if _, err := r.a.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	ret := make(chan error, 1)
	go func() { ret <- r.a.CloseWriteSync() }()
	time.Sleep(100 * time.Millisecond) // the hop holds the end
	traceRCEnv("RelayGone")
	// the relay's process ends: both its connections, A's first, so the
	// hop cannot answer A once C's ended (a crash does neither)
	_ = r.hFromA.Close()
	_ = r.hToC.Close()
	select {
	case err := <-ret:
		if err == nil {
			t.Fatal("CloseWriteSync returned nil: C never took the end")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CloseWriteSync never returned after the relay ended")
	}
	traceRelayedOff() // C may still read what its socket holds: the model loses it (RelayGone)
	close(r.open)
}

// TestEndAckFarEndCloseBeforeFinFails (spec/RelayedClose.tla CClose,
// CloseTruth, mutant close-acks): C closes the session whole before A's
// FIN reached it (C's Mux read neither the tail nor the FIN): the hop's
// onward wait and so A's CloseWriteSync fail, the end not known
// delivered, never nil (closeAckErr read the clean close as the ENDACK).
func TestEndAckFarEndCloseBeforeFinFails(t *testing.T) {
	r := newHopRig(t)
	r.armed.Store(true)
	if _, err := r.a.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	ret := make(chan error, 1)
	go func() { ret <- r.a.CloseWriteSync() }()
	time.Sleep(100 * time.Millisecond) // the hop holds the end: its onward wait runs
	traceRCEnv("CClose")
	_ = r.c.Close()
	select {
	case err := <-ret:
		if err == nil {
			t.Fatal("CloseWriteSync returned nil: C closed the session before it took the end")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CloseWriteSync never returned after C closed the session")
	}
	close(r.open)
}

// TestEndAckFarEndClosedFirstFails (spec/EndAckLeg.tla RClose, AckTruth,
// mutant close-acks): through a hop, C closed the session whole before A
// wrote its tail: the hop's write onto C fails and its close of A's
// session (both halves passed there) must not acknowledge the end it
// never passed on; A's CloseWriteSync fails, whichever of the hop's close
// or A's own half close comes first.
func TestEndAckFarEndClosedFirstFails(t *testing.T) {
	r := newHopRig(t)
	_ = r.c.Close()
	time.Sleep(100 * time.Millisecond) // the hop read C's close
	if _, err := r.a.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	ret := make(chan error, 1)
	go func() { ret <- r.a.CloseWriteSync() }()
	select {
	case err := <-ret:
		if err == nil {
			t.Fatal("CloseWriteSync returned nil: C closed the session before the tail")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CloseWriteSync never returned after C closed the session")
	}
}

// TestEndAckPeerClosedWholeFirstFails (spec/EndAckLeg.tla RClose): the
// peer closed the session whole, unread bytes and all, before this side's
// CloseWriteSync: it fails (link.ErrReset), never a Sync's nil.
func TestEndAckPeerClosedWholeFirstFails(t *testing.T) {
	accepted := make(chan *Session, 1)
	am, _ := v2muxPair(t, func(_ []byte, s *Session) { accepted <- s },
		Config{Window: testWindow, Auth: testToken}, Config{Window: testWindow, Auth: testToken})
	a, err := am.Open(context.Background(), []byte("p"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Write([]byte("unread")); err != nil {
		t.Fatal(err)
	}
	_ = (<-accepted).Close()
	select {
	case <-a.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the peer's close never ended the session")
	}
	if err := a.CloseWriteSync(); !errors.Is(err, ErrReset) {
		t.Fatalf("CloseWriteSync after the peer's whole close: %v, want ErrReset", err)
	}
}

// TestEndAckCutWhileTheTailCopiesHoldsTheAbort (spec/RelayedClose.tla
// D0Fail held, mutant abort-at-write-fail): A's connection is cut while
// the hop still copies A's bytes toward C, which takes nothing yet; C's
// write toward A fails at the hop, and that abort of C waits for the copy:
// C reads every byte the hop holds before its session fails. With the
// abort sent at once C still reads its window (in flight before it): the
// trace check catches that one (fixture relayedclose-abort-at-write-fail,
// recorded from this test with cutLater's hold taken out).
func TestEndAckCutWhileTheTailCopiesHoldsTheAbort(t *testing.T) {
	r := newHopRigWith(t, false, true)
	r.armed.Store(true)
	tail := bytes.Repeat([]byte("a"), 2*testWindow)
	go func() { _, _ = r.a.Write(tail) }() // more than C's window: the hop's copy waits
	time.Sleep(100 * time.Millisecond)
	traceRCEnv("Cut")
	_ = r.aMux.c.nc.Close() // the network cuts A's connection
	waitDone(t, r.jin, "the hop's session from A")
	if _, err := r.c.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // the hop reads the byte and fails to write it on
	close(r.open)
	b, err := io.ReadAll(r.c)
	if err == nil {
		t.Fatal("C read io.EOF: A's FIN never came")
	}
	if len(b) < testWindow || !bytes.Equal(b, tail[:len(b)]) {
		t.Fatalf("C read %d bytes, then %v: want at least its window of A's bytes, then the failure", len(b), err)
	}
}
