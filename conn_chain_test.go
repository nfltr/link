// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"errors"
	"testing"
)

// connPairChain is two Conns over TCP that both announce FeatureChain.
func connPairChain(t *testing.T) (opener, accepter *Conn) {
	t.Helper()
	a, b := tcpPair(t)
	done := make(chan *Conn, 1)
	go func() {
		c, _, err := Server(context.Background(), b, v2cfg)
		if err != nil {
			t.Error(err)
		}
		done <- c
	}()
	o, err := Client(context.Background(), a, v2cfg)
	if err != nil {
		t.Fatal(err)
	}
	acc := <-done
	if acc == nil || !o.chain() || !acc.chain() {
		t.Fatal("no FeatureChain connection")
	}
	t.Cleanup(func() { _ = o.Close(); _ = acc.Close() })
	return o, acc
}

// A caller of Conn (not Mux) on a FeatureChain connection sends no answers
// of its own: the first data or close it sends on a session the peer
// opened is preceded by an ACCEPT, so the opener reads the session, never
// ErrFrame for data before an answer. An explicit answer is never repeated.
func TestConnSendsAnswerPeerOpens(t *testing.T) {
	o, acc := connPairChain(t)
	recv := func(c *Conn) rxFrame {
		t.Helper()
		f, err := c.recvFrame()
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	for _, sid := range []string{"data", "close", "reset"} {
		if err := o.sendOpenFrame(sid, []byte("h")); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.flush(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if f := recv(acc); !f.Open {
			t.Fatalf("accepter read %+v, want an open", f)
		}
	}
	if !acc.credit.Acquire("data", 3) || acc.sendDataFrame("data", []byte("hi")) != nil || acc.sendDataFrame("data", []byte("!")) != nil ||
		acc.sendCloseFrame("close", true) != nil || acc.sendResetFrame("reset") != nil || acc.flush() != nil {
		t.Fatal("accepter sends failed")
	}
	want := []string{"data accept", "data data", "data data", "close accept", "close close", "reset accept", "reset close"}
	for _, w := range want {
		f := recv(o)
		got := f.SID + " "
		switch {
		case f.Accept:
			got += "accept"
		case f.Close:
			got += "close"
		case f.Data != nil:
			got += "data"
		}
		if got != w {
			t.Fatalf("opener read %q (%+v), want %q", got, f, w)
		}
	}
	// An explicit answer is the only one.
	if err := o.sendOpenFrame("explicit", nil); err != nil || o.flush() != nil {
		t.Fatal(err)
	}
	recv(acc)
	if err := acc.sendAccept("explicit", 3); err != nil || acc.sendDataFrame("explicit", []byte("x")) != nil || acc.flush() != nil {
		t.Fatal(err)
	}
	if f := recv(o); !f.Accept || f.Chain.Hops != 3 {
		t.Fatalf("opener read %+v, want the explicit accept", f)
	}
	if f, err := o.recvFrame(); err != nil || string(f.Data) != "x" {
		t.Fatalf("opener read %+v, %v; want the data, no second accept", f, err)
	}
	// Sessions this side opened are never answered by it.
	if err := acc.sendOpenFrame("mine", nil); err != nil || acc.sendDataFrame("mine", []byte("y")) != nil || acc.flush() != nil {
		t.Fatal(err)
	}
	recv(o)
	if f := recv(o); f.Accept || string(f.Data) != "y" {
		t.Fatalf("peer read %+v for a session it did not open, want the data", f)
	}
	if errors.Is(o.endErr(), ErrFrame) || errors.Is(acc.endErr(), ErrFrame) {
		t.Fatal("a connection ended with ErrFrame")
	}
}
