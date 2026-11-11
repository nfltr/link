// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"bytes"
	"context"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/nfltr/quic-go"

	"github.com/nfltr/link"
	"github.com/nfltr/link/pathswitch"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
)

// linkStream is a relayed path stream: a link session (through the
// relay, here a pipe).
type linkStream struct{ *link.Session }

func (s linkStream) Reset() error { return s.Abort(0, "reset") }

func addrPort(t *testing.T, pc net.PacketConn) netip.AddrPort {
	t.Helper()
	return pc.LocalAddr().(*net.UDPAddr).AddrPort()
}

// directPair is two machines, a (the client) and b (the agent): a
// relayed path (a link connection standing for the relay) and b's Node
// on b's shared Transport, which a's Node reaches by Punch.
type directPair struct {
	relayA, relayB *link.Mux
	a, b           *quiclink.Node
	punches        atomic.Int32
	mu             sync.Mutex
	conns          []*quic.Conn              // the direct connections a punched
	servers        map[byte]*pathswitch.Conn // b's sessions, by token
	ready          chan *pathswitch.Conn
}

func newDirectPair(t *testing.T) *directPair {
	p := &directPair{servers: map[byte]*pathswitch.Conn{}, ready: make(chan *pathswitch.Conn, 1)}
	pa, pb := listenUDP(t), listenUDP(t)
	trA, trB := &quic.Transport{Conn: pa}, &quic.Transport{Conn: pb}
	var err error
	p.b, err = quiclink.New(quiclink.Config{Transport: trB, TLS: serverTLS("b"), Liveness: link.LivenessFast})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = p.b.Serve(func(_ string, _ []byte, s peerlink.Session) { p.attach(s, "direct") }) }()
	p.a, err = quiclink.New(quiclink.Config{Liveness: link.LivenessFast,
		Dial: func(ctx context.Context, peer string, qc *quic.Config) (*quic.Conn, error) {
			p.punches.Add(1)
			if err := quiclink.Probe(trB, []netip.AddrPort{addrPort(t, pa)}); err != nil { // b's half
				return nil, err
			}
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			q, err := quiclink.Punch(ctx, trA, []netip.AddrPort{addrPort(t, pb)}, clientTLS("a", "b"), qc)
			if err == nil {
				p.mu.Lock()
				p.conns = append(p.conns, q)
				p.mu.Unlock()
			}
			return q, err
		}})
	if err != nil {
		t.Fatal(err)
	}
	dc, lc := bindLink(t)
	p.relayA, p.relayB = link.NewMux(dc), link.NewMux(lc)
	go p.relayA.Serve(nil)
	go p.relayB.Serve(func(h []byte, s *link.Session) {
		if len(h) == 2 && h[0] == 'F' { // a session's first stream: b's end starts
			srv := pathswitch.Server(linkStream{s}, "relay", pathswitch.Config{})
			p.mu.Lock()
			p.servers[h[1]] = srv
			p.mu.Unlock()
			p.ready <- srv
			return
		}
		p.attach(linkStream{s}, "relay")
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = p.a.Drain(ctx)
		_ = p.b.Drain(ctx)
		_ = p.relayA.Close()
		_ = p.relayB.Close()
	})
	return p
}

func bindLink(t *testing.T) (*link.Conn, *link.Conn) {
	a, b := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch := make(chan *link.Conn, 1)
	go func() {
		c, _, err := link.Server(ctx, b, link.Config{Auth: link.NoAuth})
		if err != nil {
			t.Error(err)
		}
		ch <- c
	}()
	c, err := link.Client(ctx, a, link.Config{Auth: link.NoAuth})
	if err != nil {
		t.Fatal(err)
	}
	return c, <-ch
}

// attach: b takes a stream a opened (relayed or direct): the OPEN's
// token names the session (the relay client's check, here an index).
func (p *directPair) attach(s pathswitch.Stream, path string) {
	o, err := pathswitch.ReadOpen(s)
	if err != nil {
		_ = s.Reset()
		return
	}
	p.mu.Lock()
	srv := p.servers[o.Token[0]]
	p.mu.Unlock()
	if srv == nil {
		pathswitch.Refuse(o)
		return
	}
	if err := srv.Attach(o, path); err != nil {
		_ = s.Reset()
	}
}

// session opens session id relayed (one at a time); the client falls
// back to the relay when its stream ends.
func (p *directPair) session(t *testing.T, id byte) (c, s *pathswitch.Conn) {
	first, err := p.relayA.Open([]byte{'F', id})
	if err != nil {
		t.Fatal(err)
	}
	tok := func(e uint64) []byte { return []byte{id, byte(e)} }
	var mu sync.Mutex
	mu.Lock()
	c = pathswitch.Client(linkStream{first}, "relay", pathswitch.Config{OnStreamEnd: func(uint64, error) {
		mu.Lock()
		defer mu.Unlock()
		if rs, err := p.relayA.Open([]byte("attach")); err == nil {
			_, _ = c.Move(linkStream{rs}, "relay", tok)
		}
	}})
	mu.Unlock()
	return c, <-p.ready
}

// direct moves c onto a direct stream (the pair's one connection).
func (p *directPair) direct(t *testing.T, c *pathswitch.Conn, id byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := p.a.Open(ctx, "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Move(s, "direct", func(e uint64) []byte { return []byte{id, byte(e)} }); err != nil {
		t.Fatal(err)
	}
}

func randBytes(n int, seed uint64) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(seed, 1))
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// exchange writes a to c and b to s, a direction's end after each, and
// checks what each end reads; mid runs after the first half of a went.
func exchange(t *testing.T, c, s *pathswitch.Conn, a, b []byte, mid func()) {
	t.Helper()
	var wg sync.WaitGroup
	var gotS, gotC []byte
	var errS, errC error
	wg.Add(3)
	go func() { defer wg.Done(); gotS, errS = io.ReadAll(s) }()
	go func() { defer wg.Done(); gotC, errC = io.ReadAll(c) }()
	go func() { defer wg.Done(); _, _ = s.Write(b); _ = s.CloseWrite() }()
	if _, err := c.Write(a[:len(a)/2]); err != nil {
		t.Error(err)
	}
	mid()
	if _, err := c.Write(a[len(a)/2:]); err != nil {
		t.Error(err)
	}
	_ = c.CloseWrite()
	wg.Wait()
	if errS != nil || !bytes.Equal(gotS, a) {
		t.Fatalf("b read %d of %d bytes: %v", len(gotS), len(a), errS)
	}
	if errC != nil || !bytes.Equal(gotC, b) {
		t.Fatalf("a read %d of %d bytes: %v", len(gotC), len(b), errC)
	}
}

// TestDirectPairSwitching: two sessions of one machine pair move from the
// relayed path to the direct one mid-transfer (on one direct QUIC
// connection, punched on b's shared Transport), then the direct
// connection dies and both fall back to the relay: every byte arrives,
// once, in order.
func TestDirectPairSwitching(t *testing.T) {
	p := newDirectPair(t)
	const n = 4 << 20
	type sess struct{ c, s *pathswitch.Conn }
	var ss []sess
	for id := range byte(2) {
		c, s := p.session(t, id)
		ss = append(ss, sess{c, s})
	}
	var wg sync.WaitGroup
	for id, x := range ss {
		wg.Add(1)
		go func() {
			defer wg.Done()
			exchange(t, x.c, x.s, randBytes(n, uint64(id)), randBytes(n, uint64(id)+10), func() {
				p.direct(t, x.c, byte(id))
			})
		}()
	}
	wg.Wait()
	if got := p.punches.Load(); got != 1 {
		t.Fatalf("%d punches for one machine pair, want 1 (one direct connection)", got)
	}
	for _, x := range ss {
		if x.c.Path() != "direct" {
			t.Fatalf("path %q, want direct", x.c.Path())
		}
	}

	// the direct path dies: new sessions' bytes fall back to the relay
	c, s := p.session(t, 7)
	exchange(t, c, s, randBytes(n, 70), randBytes(n, 71), func() {
		p.direct(t, c, 7)
		time.Sleep(10 * time.Millisecond)
		p.mu.Lock()
		for _, q := range p.conns {
			_ = q.CloseWithError(0, "the direct path dies")
		}
		p.mu.Unlock()
	})
	if c.Path() != "relay" {
		t.Fatalf("path %q after the direct path died, want relay", c.Path())
	}
}
