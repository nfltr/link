// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

// tcpTarget listens on loopback and hands each conn to serve.
func tcpTarget(t *testing.T, serve func(c *net.TCPConn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go serve(c.(*net.TCPConn))
		}
	}()
	return ln.Addr().String()
}

// farJoin is a far end that dials target and joins the session to it
// (peerlink.Join over a TCP conn), refusing when the dial fails.
func farJoin(target string) func(string, []byte, peerlink.Session) {
	return func(_ string, _ []byte, s peerlink.Session) {
		cs := s.(peerlink.ChainSession)
		c, err := net.Dial("tcp", target)
		if err != nil {
			_ = cs.Refuse(link.CodeApp+1, err.Error())
			return
		}
		_ = cs.Accept()
		_ = peerlink.Join(s, c)
	}
}

// TestJoinTCP: a session joined to a TCP conn at the far end carries the
// bytes both ways and each direction's half close.
func TestJoinTCP(t *testing.T) {
	target := tcpTarget(t, func(c *net.TCPConn) {
		b, _ := io.ReadAll(c) // to the origin's CloseWrite
		_, _ = c.Write(append([]byte("echo:"), b...))
		_ = c.CloseWrite()
		_ = c.Close()
	})
	a, b := nodes(t, link.LivenessFast, func(ln net.Listener) string { return ln.Addr().String() }, nil)
	serveBoth(t, a, b, farJoin(target))
	s, err := a.Open(context.Background(), "b", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(peerlink.ChainSession).Answered(context.Background()); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("0123456789"), 300000)
	go func() { _, _ = s.Write(data); _ = s.CloseWrite() }()
	got, err := io.ReadAll(s)
	if err != nil || !bytes.Equal(got, append([]byte("echo:"), data...)) {
		t.Fatalf("%d bytes, %v", len(got), err)
	}
}

// TestJoinTCPAborts: a target that resets its conn aborts the session
// (CodeConnLost from the far end, Hops 0); a target refused at dial is
// the far end's refusal; the origin's abort resets the target's conn.
func TestJoinTCPAborts(t *testing.T) {
	a, b := nodes(t, link.LivenessFast, func(ln net.Listener) string { return ln.Addr().String() }, nil)
	reset := tcpTarget(t, func(c *net.TCPConn) {
		_, _ = io.ReadFull(c, make([]byte, 2)) // an RST may drop "partial" in flight: only the abort is checked
		_, _ = c.Write([]byte("partial"))
		_ = c.SetLinger(0)
		_ = c.Close()
	})
	targetErr := make(chan error, 1)
	reads := tcpTarget(t, func(c *net.TCPConn) { _, err := io.ReadAll(c); targetErr <- err })
	dead := func() string { // a port nothing listens on
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		defer ln.Close()
		return ln.Addr().String()
	}()
	serveBoth(t, a, b, func(peer string, h []byte, s peerlink.Session) { farJoin(string(h))(peer, h, s) })
	open := func(target string) peerlink.ChainSession {
		s, err := a.Open(context.Background(), "b", []byte(target))
		if err != nil {
			t.Fatal(err)
		}
		return s.(peerlink.ChainSession)
	}

	s := open(reset)
	if err := s.Answered(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _ = s.Write([]byte("go"))
	got, err := io.ReadAll(s)
	var ab *link.Abort
	if (len(got) > 0 && string(got) != "partial") || !errors.As(err, &ab) || ab.Code != link.CodeConnLost || ab.Hops != 0 {
		t.Fatalf("target reset: %q, %v", got, err)
	}

	s = open(dead)
	if err := s.Answered(context.Background()); !errors.As(err, &ab) || !ab.Refused || ab.Code != link.CodeApp+1 {
		t.Fatalf("target refused: %v", err)
	}

	s = open(reads)
	if err := s.Answered(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _ = s.Write([]byte("x"))
	_ = s.Abort(link.CodeApp+2, "give up")
	if err := <-targetErr; err == nil {
		t.Fatal("the target read a clean end after the origin's abort")
	}
}

// sizeConn records the size of each Write.
type sizeConn struct {
	net.Conn
	sizes chan int
}

func (c sizeConn) Write(p []byte) (int, error) {
	c.sizes <- len(p)
	return c.Conn.Write(p)
}

// Join copies a conn without Pump in link data chunks (link.MaxChunk): a
// read fills one data frame. io.Copy's 32 KiB took twice the frames and
// cost relay-server's replica link a tenth of its upload throughput.
func TestJoinCopiesInChunks(t *testing.T) {
	src, srcPeer := net.Pipe()
	dst, dstPeer := net.Pipe()
	sizes := make(chan int, 64)
	go func() { _ = peerlink.Join(src, sizeConn{dst, sizes}) }()
	go func() { _, _ = io.Copy(io.Discard, dstPeer) }()
	go func() { _, _ = srcPeer.Write(make([]byte, 4*link.MaxChunk)); _ = srcPeer.Close() }()
	biggest := 0
	for range 4 {
		biggest = max(biggest, <-sizes)
	}
	if biggest != link.MaxChunk {
		t.Fatalf("largest write %d bytes, want link.MaxChunk (%d)", biggest, link.MaxChunk)
	}
	_ = dstPeer.Close()
}
