// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/nfltr/link"
)

// TestNodeSessionPassesTheEndOn (lane ENDTRACE): a Node's session, the one
// net.Conn wrapper of link over a link session, has every end method of
// its *link.Session (a joint's sync, its receipt and its reset find them
// through it: syncerOf, receiptSyncer, EndAcker, HoldEndAck) and its own
// Reset still resets: the peer reads link.ErrReset, not a clean end.
// relay's wrappers: client/agent TestSessionWrappersPassTheEndOn.
func TestNodeSessionPassesTheEndOn(t *testing.T) {
	ctx := context.Background()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	peer := make(chan *link.Session, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		lc, _, err := link.Server(ctx, c, link.Config{Auth: link.NoAuth})
		if err != nil {
			_ = c.Close()
			return
		}
		_ = link.NewMux(lc).Serve(func(_ []byte, s *link.Session) { peer <- s })
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	lc, err := link.Client(ctx, nc, link.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lc.Close() }()
	m := link.NewMux(lc)
	go func() { _ = m.Serve(nil) }()
	s, err := m.Open(ctx, []byte("h"))
	if err != nil {
		t.Fatal(err)
	}
	var w net.Conn = &session{Session: s, done: func() {}}
	for name, ok := range map[string]bool{
		"CloseWrite":             func() bool { _, ok := w.(interface{ CloseWrite() error }); return ok }(),
		"CloseWriteSync":         syncerOf(w) != nil,
		"CloseWriteSyncReceived": func() bool { _, ok := w.(receiptSyncer); return ok }(),
		"AckEnd":                 func() bool { _, ok := w.(EndAcker); return ok }(),
		"HoldEndAck":             func() bool { _, ok := w.(interface{ HoldEndAck() }); return ok }(),
		"Reset":                  func() bool { _, ok := w.(interface{ Reset() error }); return ok }(),
	} {
		if !ok {
			t.Errorf("a Node's session hides its session's %s", name)
		}
	}
	p := <-peer
	if err := w.(interface{ Reset() error }).Reset(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(p); !errors.Is(err, link.ErrReset) {
		t.Fatalf("the peer read %v after the wrapper's Reset, want link.ErrReset", err)
	}
}
