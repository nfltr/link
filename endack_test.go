// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
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

func newHopRig(t *testing.T) *hopRig {
	t.Helper()
	r := &hopRig{armed: new(atomic.Bool), open: make(chan struct{})}
	hc, cc := tcpPair(t)
	cCfg := Config{Window: testWindow, Auth: testToken}
	type res struct {
		c   *Conn
		err error
	}
	sc := make(chan res, 1)
	go func() {
		c, _, err := Server(t.Context(), holdConn{cc, r.armed, r.open}, cCfg)
		sc <- res{c, err}
	}()
	h2c, err := Client(t.Context(), hc, Config{Window: testWindow, Auth: testToken, Hop: true})
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
		out, err := r.hToC.Open([]byte("c"))
		if err != nil {
			_ = in.Reset()
			return
		}
		_ = Join(in, out)
	}, Config{Window: testWindow, Auth: testToken}, Config{Window: testWindow, Auth: testToken, Hop: true})
	if r.a, err = am.Open([]byte("h")); err != nil {
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
