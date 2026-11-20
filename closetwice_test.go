// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"errors"
	"net"
	"testing"
)

// TestCloseTwice: every Close of the API may be called again, and an end
// called after another is a no-op, as the docs say (lane LINKAPI round 2,
// "safe to misuse"): a Session's ends return nil once it ended, a Pool's
// second Close nil, a Credit's no panic, a Mux's (and its Conn's) the
// connection's close error.
func TestCloseTwice(t *testing.T) {
	client := muxPair(t, echoAccept)
	s, err := client.Open(context.Background(), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	for name, end := range map[string]func() error{
		"Close": s.Close, "Close again": s.Close, "CloseWrite": s.CloseWrite, "Reset": s.Reset,
		"AbortWith": func() error { return s.AbortWith(&Abort{Code: CodeApp}) },
	} {
		if err := end(); err != nil {
			t.Errorf("session %s after Close: %v, want nil", name, err)
		}
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Mux.Close: %v", err)
	}
	if err := client.Close(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("second Mux.Close: %v, want net.ErrClosed", err)
	}
	cr := NewCredit(DefaultWindow, DefaultWindow, func(string, int) {})
	cr.Close()
	cr.Close()
	p, err := NewPool(PoolConfig{Dial: func(context.Context) (net.Conn, error) { return nil, net.ErrClosed }})
	if err != nil {
		t.Fatal(err)
	}
	if err1, err2 := p.Close(), p.Close(); err1 != nil || err2 != nil {
		t.Errorf("Pool.Close twice: %v, %v, want nil", err1, err2)
	}
	if _, err := p.Open(context.Background(), nil); err == nil {
		t.Error("Pool.Open after Close succeeded")
	}
}
