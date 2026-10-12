// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/peerlinktest"
)

// nameAuth sends a name and takes the one it is sent: an Authenticator for
// tests only (it proves nothing).
type nameAuth string

func (a nameAuth) Hello(context.Context, tls.ConnectionState) (byte, []byte, error) {
	return 128, []byte(a), nil
}

func (nameAuth) Verify(_ context.Context, _ tls.ConnectionState, method byte, payload []byte) (link.Identity, error) {
	if method != 128 {
		return link.Identity{}, fmt.Errorf("method %d", method)
	}
	return link.Identity{Method: "name", Subject: string(payload)}, nil
}

// nodes returns a listening Node b and a Node a that dials "b" at addr
// (through the caller's proxy, say) with Liveness l.
func nodes(t *testing.T, l link.Liveness, addr func(net.Listener) string, obs func(string) link.Observer) (a, b *peerlink.Node) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	to := addr(ln)
	b = peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Liveness: l, Auth: nameAuth("b")}, Observer: obs})
	a = peerlink.New(peerlink.Config{
		Link: link.Config{Liveness: l, Auth: nameAuth("a")},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			if peer != "b" {
				return nil, fmt.Errorf("no peer %q", peer)
			}
			var d net.Dialer
			return d.DialContext(ctx, "tcp", to)
		},
		Observer: obs,
	})
	return a, b
}

func TestConformance(t *testing.T) {
	peerlinktest.Run(t, func(t *testing.T) peerlinktest.Pair {
		var px *peerlinktest.Proxy
		a, b := nodes(t, link.LivenessFast, func(ln net.Listener) string {
			px = peerlinktest.NewProxy(t, ln.Addr().String())
			return px.Addr()
		}, nil)
		return peerlinktest.Pair{
			A: a, B: b, AName: "a", BName: "b",
			DeadAfter: a.DeadAfter(),
			BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle,
		}
	})
}

// upObserver records the peers its connections came up with.
type upObserver struct {
	link.NopObserver
	peer string
	up   chan string
}

func (o upObserver) ConnUp(link.Identity) { o.up <- o.peer }

// TestObserverPerPeer: dialled and accepted connections report to the
// observer of their peer: the name Open got, the authenticated Subject.
func TestObserverPerPeer(t *testing.T) {
	up := make(chan string, 2)
	a, b := nodes(t, link.LivenessFast, func(ln net.Listener) string { return ln.Addr().String() },
		func(peer string) link.Observer { return upObserver{peer: peer, up: up} })
	go func() { _ = b.Serve(func(_ string, _ []byte, s net.Conn) { _, _ = io.Copy(s, s); _ = s.Close() }) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = a.Drain(ctx)
		_ = b.Drain(ctx)
	})
	s, err := a.Open(context.Background(), "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got := map[string]bool{<-up: true, <-up: true}
	if !got["a"] || !got["b"] {
		t.Errorf("connections came up for peers %v, want a (accepted) and b (dialled)", got)
	}
}
