// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
)

// A path over QUIC and TCP links: link i is a pair (A dials B) of
// quiclink Nodes where quic(i), else of peerlink Nodes over TCP; hop h
// serves peerlink.Forward on links[h-1].B and opens on links[h].A.

type pathLink struct {
	a, b peerlink.Link
}

func quicPair(t *testing.T) pathLink {
	pc := listenUDP(t)
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: link.LivenessFast})
	a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", pc.LocalAddr().String()), Liveness: link.LivenessFast})
	return pathLink{a: a, b: b}
}

func tcpPair(t *testing.T) pathLink {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	b, err := peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Liveness: link.LivenessFast, Auth: link.NameAuth("b")}})
	if err != nil {
		t.Fatal(err)
	}
	a, err := peerlink.New(peerlink.Config{Link: link.Config{Liveness: link.LivenessFast, Auth: link.NameAuth("a")},
		Dial: func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", ln.Addr().String())
		}})
	if err != nil {
		t.Fatal(err)
	}
	return pathLink{a: a, b: b}
}

type path struct {
	t     *testing.T
	links []pathLink
}

// newPath builds n links (quic(i) picks the transport); far serves the
// far end; refuseAt[h] makes hop h's Route refuse.
func newPath(t *testing.T, n int, quic func(int) bool, far func(string, []byte, peerlink.Session), refuseAt map[int]*link.Abort) *path {
	p := &path{t: t}
	for i := range n {
		l := tcpPair(t)
		if quic(i) {
			l = quicPair(t)
		}
		p.links = append(p.links, l)
	}
	for i, l := range p.links {
		accept := far
		if i < n-1 {
			next := p.links[i+1]
			h := i + 1
			accept = peerlink.Forward(func(ctx context.Context, _ string, header []byte, _ peerlink.Session) (peerlink.Session, error) {
				if a := refuseAt[h]; a != nil {
					return nil, a
				}
				return next.a.Open(ctx, "b", header)
			}, link.ForwardConfig{})
		}
		go func() { _ = l.b.Serve(accept) }()
		go func() { _ = l.a.Serve(func(_ string, _ []byte, s peerlink.Session) { _ = s.Reset() }) }()
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for _, l := range p.links {
			_ = l.a.Drain(ctx)
			_ = l.b.Drain(ctx)
		}
	})
	return p
}

func (p *path) open(header string) peerlink.ChainSession {
	p.t.Helper()
	s, err := p.links[0].a.Open(context.Background(), "b", []byte(header))
	if err != nil {
		p.t.Fatal(err)
	}
	cs, ok := s.(peerlink.ChainSession)
	if !ok || !cs.Answers() {
		p.t.Fatalf("%T does not answer", s)
	}
	return cs
}

// kill ends hop k's Links at once (its process died).
func (p *path) kill(k int) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = p.links[k-1].b.Drain(ctx)
	if k < len(p.links) {
		_ = p.links[k].a.Drain(ctx)
	}
}

func farEcho(_ string, _ []byte, s peerlink.Session) {
	_ = s.(peerlink.ChainSession).Accept()
	_, _ = io.Copy(s, s)
	_ = s.CloseWrite()
}

func pathTransports() map[string]func(int) bool {
	return map[string]func(int) bool{
		"quic":  func(int) bool { return true },
		"mixed": func(i int) bool { return i%2 == 0 },
	}
}

// TestChainQUIC: paths of 2 to 5 links over QUIC, and over QUIC and TCP
// in turn: bytes intact both ways, the far end's refusal and a hop's,
// and a middle hop's death, each reaching the origin typed.
func TestChainQUIC(t *testing.T) {
	for name, quic := range pathTransports() {
		for n := 2; n <= 5; n++ {
			t.Run(fmt.Sprintf("%s/links=%d", name, n), func(t *testing.T) {
				p := newPath(t, n, quic, func(peer string, h []byte, s peerlink.Session) {
					if string(h) == "refuse" {
						_ = s.(peerlink.ChainSession).Refuse(link.CodeApp+1, "no")
						return
					}
					farEcho(peer, h, s)
				}, nil)
				s := p.open("echo")
				if err := s.Answered(context.Background()); err != nil || int(s.PathLen()) != n-1 {
					t.Fatalf("Answered %v, PathLen %d", err, s.PathLen())
				}
				data := make([]byte, 2<<20)
				_, _ = rand.Read(data)
				go func() { _, _ = s.Write(data); _ = s.CloseWrite() }()
				got, err := io.ReadAll(s)
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("echo: %d bytes, %v", len(got), err)
				}
				_ = s.Close()

				var a *link.Abort
				if err := p.open("refuse").Answered(context.Background()); !errors.As(err, &a) || !a.Refused || a.Code != link.CodeApp+1 || int(a.Hops) != n-1 {
					t.Fatalf("far refusal: %v", err)
				}

				k := 2 + (n-2)/2 // a middle hop (or the far end at n=2)
				s = p.open("echo")
				if err := s.Answered(context.Background()); err != nil {
					t.Fatal(err)
				}
				p.kill(k)
				_, err = io.ReadAll(s)
				if !errors.As(err, &a) || a.Code != link.CodeConnLost || int(a.Hops) != k-2 {
					t.Fatalf("hop %d killed: %v", k, err)
				}
			})
		}
	}
}

// TestChainQUICRefuseAtHop: a refusal at hop k reaches the origin with
// the Route's code and Hops k-1.
func TestChainQUICRefuseAtHop(t *testing.T) {
	for k := 1; k <= 3; k++ {
		t.Run(fmt.Sprintf("hop=%d", k), func(t *testing.T) {
			p := newPath(t, 4, func(i int) bool { return i != 1 }, farEcho, map[int]*link.Abort{k: {Code: link.CodeApp + 4, Msg: "no"}})
			var a *link.Abort
			if err := p.open("x").Answered(context.Background()); !errors.As(err, &a) || !a.Refused || a.Code != link.CodeApp+4 || int(a.Hops) != k-1 {
				t.Fatalf("%v", err)
			}
		})
	}
}

// TestChainQUICAbortCarriesHops: an abort over QUIC carries its code and
// hops in the stream error, and the bytes written before it arrive
// (RESET_STREAM_AT).
func TestChainQUICAbortCarriesHops(t *testing.T) {
	p := newPath(t, 3, func(int) bool { return true }, func(_ string, _ []byte, s peerlink.Session) {
		_, _ = s.Write(bytes.Repeat([]byte("x"), 100000))
		_ = s.(peerlink.ChainSession).Abort(link.CodeApp+6, "")
	}, nil)
	s := p.open("x")
	got, err := io.ReadAll(s)
	var a *link.Abort
	if len(got) != 100000 || !errors.As(err, &a) || a.Code != link.CodeApp+6 || a.Hops != 2 {
		t.Fatalf("%d bytes, %v", len(got), err)
	}
}

// TestChainQUICOldPeer: a dialler offering only ALPN (quiclink v0.3)
// gets sessions that do not answer; Answered says so.
func TestChainQUICOldPeer(t *testing.T) {
	pc := listenUDP(t)
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: link.LivenessFast})
	a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", pc.LocalAddr().String(), quiclink.ALPN), Liveness: link.LivenessFast})
	go func() { _ = b.Serve(farEcho) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = a.Drain(ctx)
		_ = b.Drain(ctx)
	})
	s, err := a.Open(context.Background(), "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	cs := s.(peerlink.ChainSession)
	if cs.Answers() || !errors.Is(cs.Answered(context.Background()), link.ErrNoAnswers) {
		t.Fatal("answers without ALPNChain")
	}
	_, _ = s.Write([]byte("hi"))
	_ = s.CloseWrite()
	if b, err := io.ReadAll(s); err != nil || string(b) != "hi" {
		t.Fatalf("%q %v", b, err)
	}
}
