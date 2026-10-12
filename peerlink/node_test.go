// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

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

// reverse returns a listening Node a without Dial, and a Node b that keeps
// pool connections to "a" at addr (Config.Connect) once it serves: a opens
// its sessions on b's connections.
func reverse(t *testing.T, l link.Liveness, pool link.PoolConfig, addr func(net.Listener) string, dial func(net.Conn), obs func(string) link.Observer) (a, b *peerlink.Node) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	to := addr(ln)
	a = peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Liveness: l, Auth: nameAuth("a")}, Observer: obs})
	b = peerlink.New(peerlink.Config{
		Link:    link.Config{Liveness: l, Auth: nameAuth("b")},
		Pool:    pool,
		Connect: []string{"a"},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			if peer != "a" {
				return nil, fmt.Errorf("no peer %q", peer)
			}
			var d net.Dialer
			nc, err := d.DialContext(ctx, "tcp", to)
			if err == nil && dial != nil {
				dial(nc)
			}
			return nc, err
		},
	})
	return a, b
}

// TestConformanceReverse: the suite with A opening its sessions on the
// connections B dialled.
func TestConformanceReverse(t *testing.T) {
	peerlinktest.Run(t, func(t *testing.T) peerlinktest.Pair {
		var px *peerlinktest.Proxy
		a, b := reverse(t, link.LivenessFast, link.PoolConfig{}, func(ln net.Listener) string {
			px = peerlinktest.NewProxy(t, ln.Addr().String())
			return px.Addr()
		}, nil, nil)
		return peerlinktest.Pair{
			A: a, B: b, AName: "a", BName: "b", Reverse: true,
			DeadAfter: a.DeadAfter(),
			BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle,
		}
	})
}

func serveBoth(t *testing.T, a, b *peerlink.Node, accept func(string, []byte, net.Conn)) {
	t.Helper()
	go func() { _ = a.Serve(func(_ string, _ []byte, s net.Conn) { _ = s.Close() }) }()
	go func() { _ = b.Serve(accept) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = a.Drain(ctx)
		_ = b.Drain(ctx)
	})
}

func echoOnce(t *testing.T, s net.Conn) {
	t.Helper()
	defer s.Close()
	if _, err := io.WriteString(s, "hi"); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 2)
	if _, err := io.ReadFull(s, b); err != nil || string(b) != "hi" {
		t.Fatalf("echo %q %v", b, err)
	}
}

func echoAccept(_ string, _ []byte, s net.Conn) { _, _ = io.Copy(s, s); _ = s.Close() }

// TestOpenWaitsForPeer: an Open toward a peer that has not dialled in yet
// waits, and opens on its connection once it is up.
func TestOpenWaitsForPeer(t *testing.T) {
	a, b := reverse(t, link.LivenessFast, link.PoolConfig{}, func(ln net.Listener) string { return ln.Addr().String() }, nil, nil)
	opened := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), peerlinktest.Within)
		defer cancel()
		s, err := a.Open(ctx, "b", nil)
		if err == nil {
			echoOnce(t, s)
		}
		opened <- err
	}()
	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-opened:
		t.Fatalf("Open returned before the peer connected: %v", err)
	default:
	}
	serveBoth(t, a, b, echoAccept)
	if err := <-opened; err != nil {
		t.Fatal(err)
	}
}

// downObserver reports its connections going down.
type downObserver struct {
	link.NopObserver
	down chan struct{}
}

func (o downObserver) ConnDown(link.Identity, error) { o.down <- struct{}{} }

// TestSeveralConnections: sessions spread over the connections a peer
// dialled in; when one goes away, new opens go on the others.
func TestSeveralConnections(t *testing.T) {
	var mu sync.Mutex
	var dialled []net.Conn
	down := make(chan struct{}, 8)
	a, b := reverse(t, link.LivenessFast, link.PoolConfig{Min: 2, Max: 2},
		func(ln net.Listener) string { return ln.Addr().String() },
		func(nc net.Conn) { mu.Lock(); dialled = append(dialled, nc); mu.Unlock() },
		func(string) link.Observer { return downObserver{down: down} })
	serveBoth(t, a, b, echoAccept)
	from := map[string]bool{}
	for deadline := time.Now().Add(peerlinktest.Within); len(from) < 2; {
		if time.Now().After(deadline) {
			t.Fatalf("sessions came on %d connection(s), want 2", len(from))
		}
		s, err := a.Open(context.Background(), "b", nil)
		if err != nil {
			t.Fatal(err)
		}
		from[s.RemoteAddr().String()] = true
		defer s.Close() // held: the next goes on the connection with fewer
	}
	mu.Lock()
	_ = dialled[0].Close()
	mu.Unlock()
	<-down // a saw it go
	for range 4 {
		s, err := a.Open(context.Background(), "b", nil)
		if err != nil {
			t.Fatal(err)
		}
		echoOnce(t, s)
	}
}

// TestServeNeedsDialForConnect: Config.Connect without Dial is refused.
func TestServeNeedsDialForConnect(t *testing.T) {
	n := peerlink.New(peerlink.Config{Connect: []string{"x"}})
	if err := n.Serve(echoAccept); err == nil {
		t.Fatalf("Serve: %v, want an error", err)
	}
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

// TestServedPeer: Config.ServedPeer names an accepted connection's peer,
// for accept and the Observer, from the connection and its Identity (here
// with its remote address: peers sharing one credential stay apart).
func TestServedPeer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	up := make(chan string, 1)
	b := peerlink.New(peerlink.Config{
		Listener:   ln,
		Link:       link.Config{Auth: nameAuth("b")},
		ServedPeer: func(nc net.Conn, id link.Identity) string { return id.Subject + "@" + nc.RemoteAddr().String() },
		Observer:   func(peer string) link.Observer { return upObserver{peer: peer, up: up} },
	})
	from := make(chan string, 1)
	a := peerlink.New(peerlink.Config{
		Link: link.Config{Auth: nameAuth("a")},
		Dial: func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			nc, err := d.DialContext(ctx, "tcp", ln.Addr().String())
			if err == nil {
				from <- nc.LocalAddr().String()
			}
			return nc, err
		},
	})
	accepted := make(chan string, 1)
	go func() { _ = b.Serve(func(peer string, _ []byte, s net.Conn) { accepted <- peer; _ = s.Close() }) }()
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
	want := "a@" + <-from
	if got := <-accepted; got != want {
		t.Errorf("accept got peer %q, want %q", got, want)
	}
	if got := <-up; got != want {
		t.Errorf("observer of peer %q, want %q", got, want)
	}
}
