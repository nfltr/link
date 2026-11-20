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
}

func newHopRig(t *testing.T) *hopRig { return newHopRigMute(t, false) }

// newHopRigMute is newHopRig whose C, when mute, announces FeatureEndAck
// but never acknowledges (a broken or hostile far end that answers pings).
func newHopRigMute(t *testing.T, mute bool) *hopRig {
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
	r.hToC, r.cMux = NewMux(h2c), NewMux(cs.c)
	go func() { _ = r.hToC.Serve(nil) }()
	go func() { _ = r.cMux.Serve(func(_ []byte, s *Session) { accepted <- s }) }()
	t.Cleanup(func() { _ = r.hToC.Close(); _ = r.cMux.Close() })
	am, _ := v2muxPair(t, func(_ []byte, in *Session) {
		out, err := r.hToC.Open(context.Background(), []byte("c"))
		if err != nil {
			_ = in.Reset()
			return
		}
		_ = join(in, out)
	}, Config{Window: testWindow, Auth: testToken, endAckIdle: idle}, Config{Window: testWindow, Auth: testToken, Hop: true, endAckIdle: idle})
	if r.a, err = am.Open(context.Background(), []byte("h")); err != nil {
		t.Fatal(err)
	}
	r.c = <-accepted
	if err := r.c.CloseWrite(); err != nil { // C sends nothing (pipe's dialler, empty stdin)
		t.Fatal(err)
	}
	if b, err := io.ReadAll(r.a); err != nil || len(b) != 0 {
		t.Fatalf("A read %q, %v", b, err)
	}
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
	_ = r.hToC.Close()                 // C's connection ends at the hop
	select {
	case err := <-ret:
		if err == nil {
			t.Fatal("CloseWriteSync returned nil: C never took the end")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CloseWriteSync never returned after C's connection ended")
	}
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
