// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

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
// cost nfltr-relay's replica link a tenth of its upload throughput.
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

// lostConn is a conn whose connection was lost: its writes fail with an
// error that is net.ErrClosed (as quic-go's IdleTimeoutError,
// ApplicationError and StatelessResetError are); its reads wait for
// Close.
type lostConn struct {
	net.Conn
	closed chan struct{}
}

type connLost struct{}

func (connLost) Error() string { return "timeout: no recent network activity" }
func (connLost) Unwrap() error { return net.ErrClosed }

func (lostConn) Write([]byte) (int, error) { return 0, connLost{} }

func (c lostConn) Read([]byte) (int, error) {
	<-c.closed
	return 0, connLost{}
}

func (c lostConn) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

// TestJoinWriteToLostConnAborts: a write that failed because the conn's
// connection was lost (an error that is net.ErrClosed) aborts the other
// side (an RST on a TCP conn), never closes it cleanly: quiclink's
// writes fail so when its QUIC connection dies (idle timeout, dead
// peer, drain deadline), and a clean close upstream truncated the
// session (TestChainMiddleHopFaultsMixed in quiclink).
func TestJoinWriteToLostConnAborts(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	peer, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close() //nolint:errcheck
	a, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	b := lostConn{closed: make(chan struct{})}
	defer b.Close() //nolint:errcheck
	go func() { _ = peerlink.Join(a, b) }()
	if _, err := peer.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil || errors.Is(err, io.EOF) || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read %v, want a reset", err)
	}
}

// drainCutConn is the onward side of a hop whose drain's deadline cut it
// (link.Mux: link.ErrConnClosed wrapping link.ErrDrainDeadline).
type drainCutConn struct{ lostConn }

func (drainCutConn) Write([]byte) (int, error) {
	return 0, fmt.Errorf("%w: %w", link.ErrConnClosed, link.ErrDrainDeadline)
}

func (c drainCutConn) Read([]byte) (int, error) {
	<-c.closed
	return 0, fmt.Errorf("%w: %w", link.ErrConnClosed, link.ErrDrainDeadline)
}

// TestJoinSendsDrainDeadlineOn: a hop whose onward session its own
// drain's deadline cut aborts the origin's side CodeDrainDeadline, as
// link.Join does, not CodeConnLost: a relay that drains both its
// replica link and its agents' links at one deadline gave the origin
// either code, by which cut came first.
func TestJoinSendsDrainDeadlineOn(t *testing.T) {
	a, b := nodes(t, link.LivenessFast, func(ln net.Listener) string { return ln.Addr().String() }, nil)
	serveBoth(t, a, b, func(_ string, _ []byte, s peerlink.Session) {
		_ = peerlink.Join(s, drainCutConn{lostConn{closed: make(chan struct{})}})
	})
	s, err := a.Open(context.Background(), "b", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Write([]byte("go"))
	_ = s.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = io.ReadAll(s)
	var ab *link.Abort
	if !errors.As(err, &ab) || ab.Code != link.CodeDrainDeadline || ab.Hops != 0 {
		t.Fatalf("read %v, want aborted CodeDrainDeadline from hop 0", err)
	}
}

// endedSession is a Session its peer already ended: Done is closed, and
// it records whether this side released it (Close or Reset).
type endedSession struct {
	net.Conn
	released chan struct{}
}

func (s *endedSession) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (s *endedSession) Close() error      { close(s.released); return s.Conn.Close() }
func (s *endedSession) Reset() error      { return s.Close() }
func (s *endedSession) CloseWrite() error { return nil }

// A session that ends while its route runs is still released here when
// the route fails: link's debug mode reported it garbage-collected and
// never closed (nfltr-relay's replica link, a replica's accepted
// session whose opener gave up while the agent was looked up).
func TestForwardReleasesAnEndedSessionWhenItsRouteFails(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	in := &endedSession{Conn: a, released: make(chan struct{})}
	peerlink.Forward(func(context.Context, string, []byte, peerlink.Session) (peerlink.Session, error) {
		return nil, errors.New("no agent")
	}, link.ForwardConfig{})("", nil, in)
	select {
	case <-in.released:
	default:
		t.Fatal("Forward returned without closing a session that ended while its route ran")
	}
}

// pipeSession is a net.Pipe end as a Session without answers.
type pipeSession struct{ net.Conn }

func (s pipeSession) CloseWrite() error { return s.Close() }
func (s pipeSession) Reset() error      { return s.Close() }

// hopEnds sends each HopEnded event on its channel.
type hopEnds struct {
	link.NopObserver
	ch chan link.HopEvent
}

func (o hopEnds) Hop(e link.HopEvent) {
	if e.Kind == link.HopEnded {
		o.ch <- e
	}
}

// TestForwardReportsFirstOut: HopEnded carries when the far end's first
// byte was written toward the origin, which a caller of a join (whose
// bytes it never sees) reports as the session's first byte; zero when
// none crossed.
func TestForwardReportsFirstOut(t *testing.T) {
	for _, reply := range []bool{true, false} {
		in, origin := net.Pipe()
		out, far := net.Pipe()
		obs := hopEnds{ch: make(chan link.HopEvent, 1)}
		before := time.Now()
		peerlink.Forward(func(context.Context, string, []byte, peerlink.Session) (peerlink.Session, error) {
			return pipeSession{out}, nil
		}, link.ForwardConfig{Observer: obs})("", nil, pipeSession{in})
		if reply {
			go func() { _, _ = far.Write([]byte("hi")) }()
			if _, err := io.ReadFull(origin, make([]byte, 2)); err != nil {
				t.Fatal(err)
			}
		}
		_ = far.Close()
		_ = origin.Close()
		e := <-obs.ch
		read := time.Now()
		switch {
		case reply && (e.FirstOut.Before(before) || e.FirstOut.After(read)):
			t.Fatalf("FirstOut %v, want between %v and its HopEnded %v", e.FirstOut, before, read)
		case !reply && !e.FirstOut.IsZero():
			t.Fatalf("FirstOut %v with no byte from the far end", e.FirstOut)
		}
	}
}
