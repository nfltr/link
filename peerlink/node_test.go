// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/peerlinktest"
)

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
	b = mustNew(t, peerlink.Config{Listener: ln, Link: link.Config{Liveness: l, Auth: link.NameAuth("b")}, Observer: obs})
	a = mustNew(t, peerlink.Config{
		Link: link.Config{Liveness: l, Auth: link.NameAuth("a")},
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
	a = mustNew(t, peerlink.Config{Listener: ln, Link: link.Config{Liveness: l, Auth: link.NameAuth("a")}, Observer: obs})
	b = mustNew(t, peerlink.Config{
		Link:    link.Config{Liveness: l, Auth: link.NameAuth("b")},
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

func serveBoth(t *testing.T, a, b *peerlink.Node, accept func(string, []byte, peerlink.Session)) {
	t.Helper()
	go func() { _ = a.Serve(func(_ string, _ []byte, s peerlink.Session) { _ = s.Close() }) }()
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

func echoAccept(_ string, _ []byte, s peerlink.Session) { _, _ = io.Copy(s, s); _ = s.Close() }

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

func mustNew(t testing.TB, cfg peerlink.Config) *peerlink.Node {
	t.Helper()
	n, err := peerlink.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestNewRefuses: New refuses a Config that cannot work, naming the field.
func TestNewRefuses(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	dial := func(context.Context, string) (net.Conn, error) { return nil, errors.New("no") }
	for _, c := range []struct {
		cfg  peerlink.Config
		want string
	}{
		{peerlink.Config{}, "Config.Dial and Config.Listener"},
		{peerlink.Config{Listener: ln}, "Config.Link.Auth"},
		{peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NoAuth}, Connect: []string{"x"}}, "Config.Connect"},
		{peerlink.Config{Dial: dial, Link: link.Config{Window: 1}}, "Config.Window"},
		{peerlink.Config{Dial: dial, Link: link.Config{Liveness: link.Liveness{Interval: time.Hour, Timeout: time.Hour}}}, "Config.Liveness"},
		{peerlink.Config{Dial: dial, Pool: link.PoolConfig{Min: 3, Max: 2}}, "PoolConfig.Min"},
	} {
		if _, err := peerlink.New(c.cfg); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("New(%+v): %v, want an error naming %s", c.cfg, err, c.want)
		}
	}
	if _, err := peerlink.New(peerlink.Config{Dial: dial}); err != nil {
		t.Errorf("New with only Dial: %v", err)
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
	go func() {
		_ = b.Serve(func(_ string, _ []byte, s peerlink.Session) { _, _ = io.Copy(s, s); _ = s.Close() })
	}()
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
	b := mustNew(t, peerlink.Config{
		Listener:   ln,
		Link:       link.Config{Auth: link.NameAuth("b")},
		ServedPeer: func(nc net.Conn, id link.Identity) string { return id.Subject + "@" + nc.RemoteAddr().String() },
		Observer:   func(peer string) link.Observer { return upObserver{peer: peer, up: up} },
	})
	from := make(chan string, 1)
	a := mustNew(t, peerlink.Config{
		Link: link.Config{Auth: link.NameAuth("a")},
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
	go func() {
		_ = b.Serve(func(peer string, _ []byte, s peerlink.Session) { accepted <- peer; _ = s.Close() })
	}()
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

// TestHandshakeErrorHook: a connection whose handshake fails reaches
// Config.HandshakeError with why.
func TestHandshakeErrorHook(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan error, 1)
	n := mustNew(t, peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("b")},
		HandshakeError: func(_ net.Conn, err error) { got <- err }})
	go func() { _ = n.Serve(echoAccept) }()
	defer drainNow(n)
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = io.WriteString(c, "GET / HTTP/1.1\r\n\r\n")
	select {
	case err := <-got:
		if !errors.Is(err, link.ErrHandshake) {
			t.Fatalf("HandshakeError got %v, want ErrHandshake", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("HandshakeError not called")
	}
}

// TestUnreachableNamesConnected: Open toward a peer with no connection
// names the peers that are connected (a typo shows).
func TestUnreachableNamesConnected(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := mustNew(t, peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("b")}})
	go func() { _ = b.Serve(echoAccept) }()
	defer drainNow(b)
	a := mustNew(t, peerlink.Config{Link: link.Config{Auth: link.NameAuth("client-1")}, Connect: []string{"b"},
		Dial: func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", ln.Addr().String())
		}})
	go func() { _ = a.Serve(echoAccept) }()
	defer drainNow(a)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if s, err := b.Open(ctx, "client-1", nil); err != nil { // up
		t.Fatal(err)
	} else {
		_ = s.Close()
	}
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = b.Open(ctx, "client-l", nil)
	if !errors.Is(err, peerlink.ErrPeerUnreachable) || !strings.Contains(err.Error(), `"client-1"`) {
		t.Fatalf("Open toward a misnamed peer: %v, want ErrPeerUnreachable naming client-1", err)
	}
}

func drainNow(n *peerlink.Node) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = n.Drain(ctx)
}

// brokenWrites fails its writes once broken, its reads unchanged: a
// connection the peer reset, before its reader saw the end.
type brokenWrites struct {
	net.Conn
	broken *atomic.Bool
}

func (c brokenWrites) Write(b []byte) (int, error) {
	if c.broken.Load() {
		return 0, errors.New("write: connection reset by peer")
	}
	return c.Conn.Write(b)
}

// brokenListener's first connection is the one broken can break.
type brokenListener struct {
	net.Listener
	broken *atomic.Bool
	n      *atomic.Int32
}

func (l brokenListener) Accept() (net.Conn, error) {
	nc, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if l.n.Add(1) > 1 {
		return brokenWrites{nc, new(atomic.Bool)}, nil
	}
	return brokenWrites{nc, l.broken}, nil
}

// TestOpenServedPassesABrokenConnection: Open toward a peer whose only
// connection broke (its writes fail, its reader has not seen the end yet)
// waits for the peer's next connection and opens there, instead of
// failing with the broken one's error (a peer that closed its connection
// and dials again: the agent link's lazy redial).
func TestOpenServedPassesABrokenConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var first atomic.Bool
	a := mustNew(t, peerlink.Config{Listener: brokenListener{ln, &first, new(atomic.Int32)}, Link: link.Config{Auth: link.NameAuth("a")}})
	go func() { _ = a.Serve(func(_ string, _ []byte, s peerlink.Session) { _ = s.Close() }) }()
	t.Cleanup(func() { _ = a.Drain(context.Background()) })
	dialB := func() *link.Mux {
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		c, err := link.Client(context.Background(), nc, link.Config{Auth: link.NameAuth("b")})
		if err != nil {
			t.Fatal(err)
		}
		m := link.NewMux(c)
		go func() {
			_ = m.Serve(func(_ []byte, s *link.Session) { defer s.Close(); _, _ = io.Copy(s, s) })
		}()
		t.Cleanup(func() { _ = m.Close() })
		return m
	}
	dialB()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := a.Open(ctx, "b", nil) // the first connection is up
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	first.Store(true) // its writes fail from now on; it stays served
	opened := make(chan error, 1)
	go func() {
		s, err := a.Open(ctx, "b", []byte("x"))
		if err == nil {
			err = s.Close()
		}
		opened <- err
	}()
	dialB() // the peer's next connection
	if err := <-opened; err != nil {
		t.Fatalf("Open with the peer's first connection broken and a second up: %v", err)
	}
}
