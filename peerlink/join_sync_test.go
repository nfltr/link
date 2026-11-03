// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

// gateConn, once held, takes writes and never delivers them (blocked
// signals the first): bytes in the kernel's send buffer of a connection
// about to die.
type gateConn struct {
	net.Conn
	mu      sync.Mutex
	held    bool
	blocked chan struct{}
	once    sync.Once
	gated   []byte // what it took once held
}

func (g *gateConn) Write(p []byte) (int, error) {
	g.mu.Lock()
	held := g.held
	g.mu.Unlock()
	if held {
		g.mu.Lock()
		g.gated = append(g.gated, p...)
		g.mu.Unlock()
		g.once.Do(func() { close(g.blocked) })
		return len(p), nil
	}
	return g.Conn.Write(p)
}

func (g *gateConn) hold() { g.mu.Lock(); g.held = true; g.mu.Unlock() }

// closeThenPing reports whether the gate took a close frame (WIRE.md
// type 4) and a ping (5) after it: a CloseWrite and its Sync.
func (g *gateConn) closeThenPing() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	closed := false
	for b := g.gated; len(b) >= 5; {
		n := int(binary.BigEndian.Uint32(b))
		switch b[4] {
		case 4:
			closed = true
		case 5:
			if closed {
				return true
			}
		}
		if len(b) < 5+n {
			break
		}
		b = b[5+n:]
	}
	return false
}

// tcpPair is both ends of a loopback TCP connection.
func tcpPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	acc := make(chan net.Conn, 1)
	go func() { c, _ := ln.Accept(); acc <- c }()
	d, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	a := <-acc
	if a == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { _ = d.Close(); _ = a.Close() })
	return d.(*net.TCPConn), a.(*net.TCPConn)
}

// TestJoinCleanEndOnlyOnceThePeerReadTheFIN (relay-core finding 23,
// spec/Join.tla): the peer half-closes first; the client then writes its
// tail and half-closes; the link connection dies with them still in its
// write buffer. The joint must not end the client cleanly: Join returns
// an abort (the client's connection is reset), never nil.
func TestJoinCleanEndOnlyOnceThePeerReadTheFIN(t *testing.T) {
	ln, rn := tcpPair(t)
	g := &gateConn{Conn: rn, blocked: make(chan struct{})}
	ctx := context.Background()
	agentGot := make(chan []byte, 1)
	srv := make(chan *link.Conn, 1)
	go func() {
		c, _, err := link.Server(ctx, ln, link.Config{Auth: link.NoAuth})
		if err != nil {
			_ = ln.Close()
			srv <- nil
			return
		}
		srv <- c
		_ = link.NewMux(c).Serve(func(_ []byte, s *link.Session) {
			_ = s.CloseWrite() // the agent's target half-closes first
			b, _ := io.ReadAll(s)
			agentGot <- b
		})
	}()
	rc, err := link.Client(ctx, g, link.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if c := <-srv; c == nil {
		t.Fatal("server handshake failed")
	}
	rm := link.NewMux(rc)
	go func() { _ = rm.Serve(nil) }()
	s, err := rm.Open([]byte("h"))
	if err != nil {
		t.Fatal(err)
	}
	client, relay := tcpPair(t)
	joinErr := make(chan error, 1)
	go func() { joinErr <- peerlink.Join(relay, s) }()

	// The agent's FIN reaches the client.
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
	if b, err := io.ReadAll(client); err != nil || len(b) != 0 {
		t.Fatalf("client read %q, %v; want EOF", b, err)
	}
	g.hold()
	if _, err := client.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	_ = client.CloseWrite()
	select {
	case err := <-joinErr:
		t.Fatalf("Join ended (%v) before the peer read the client's FIN", err)
	case <-g.blocked: // the tail and FIN never reach the peer
	case <-time.After(10 * time.Second):
		t.Fatal("the relay never wrote the client's tail")
	}
	_ = rn.Close() // the connection dies
	select {
	case err := <-joinErr:
		if err == nil {
			t.Fatal("Join ended the client cleanly while its tail was lost")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Join did not end after the connection died")
	}
	if b := <-agentGot; len(b) != 0 {
		t.Fatalf("the agent read %q: the tail was not lost", b)
	}
}

// TestJoinPeerFinDuringSyncIsHeld (review r2 of Join.tla, finding 1): the
// peer's FIN arrives while the joint's Sync waits. The session then ends at
// the joint (both halves closed), which must not hand the client a clean
// end before the sync: the link connection then dies with the client's
// tail in its write buffer, and the client must not have read a clean EOF.
func TestJoinPeerFinDuringSyncIsHeld(t *testing.T) {
	ln, rn := tcpPair(t)
	g := &gateConn{Conn: rn, blocked: make(chan struct{})}
	ctx := context.Background()
	finNow := make(chan struct{})
	srv := make(chan *link.Conn, 1)
	go func() {
		c, _, err := link.Server(ctx, ln, link.Config{Auth: link.NoAuth})
		if err != nil {
			srv <- nil
			return
		}
		srv <- c
		_ = link.NewMux(c).Serve(func(_ []byte, s *link.Session) {
			_ = s.Accept()
			<-finNow
			_ = s.CloseWrite()
			_, _ = io.ReadAll(s)
		})
	}()
	rc, err := link.Client(ctx, g, link.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if c := <-srv; c == nil {
		t.Fatal("server handshake failed")
	}
	rm := link.NewMux(rc)
	go func() { _ = rm.Serve(nil) }()
	s, err := rm.Open([]byte("h"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Answered(ctx); err != nil {
		t.Fatal(err)
	}
	client, relay := tcpPair(t)
	joinErr := make(chan error, 1)
	go func() { joinErr <- peerlink.Join(relay, s) }()
	g.hold()
	_, _ = client.Write([]byte("tail"))
	_ = client.CloseWrite()
	for !g.closeThenPing() { // the joint's CloseWrite and its Sync's ping are in the gate
		time.Sleep(time.Millisecond)
	}
	close(finNow) // the peer's FIN arrives while the sync waits
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if b, err := io.ReadAll(client); err == nil {
		t.Fatalf("client read a clean EOF (%q) while its tail is unacknowledged", b)
	}
	_ = rn.Close()
	<-joinErr
}
