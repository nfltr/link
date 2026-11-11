// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/peerlinktest"
)

// Failure classes of a relay's accept path (iroh-relay issues 4630, 4603,
// 4636): a dialer that stalls its handshake holds a connection, one stuck
// peer stalls sends to the others, Accept spins on EMFILE.

// TestServeClosesAStalledHandshake: a dialer that connects and then sends
// nothing, or only part of its hello, is closed within the Node's
// dead-peer bound, not held.
func TestServeClosesAStalledHandshake(t *testing.T) {
	for name, sent := range map[string]string{"nothing": "", "part of a hello": "NFL"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			failed := make(chan error, 1)
			n := mustNew(t, peerlink.Config{Listener: ln, Link: link.Config{Liveness: link.LivenessFast, Auth: link.NameAuth("b")},
				HandshakeError: func(_ net.Conn, err error) { failed <- err }})
			go func() { _ = n.Serve(echoAccept) }()
			defer drainNow(n)
			c, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_, _ = io.WriteString(c, sent)
			start := time.Now()
			bound := n.DeadAfter() + 3*time.Second
			_ = c.SetReadDeadline(start.Add(bound))
			if _, err := io.Copy(io.Discard, c); errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("a stalled handshake held past %v", bound)
			}
			select {
			case <-failed:
			case <-time.After(time.Second):
				t.Fatal("HandshakeError not called")
			}
			t.Logf("closed after %v (DeadAfter %v)", time.Since(start).Round(time.Millisecond), n.DeadAfter())
		})
	}
}

// emfileForever fails every Accept with EMFILE until Close.
type emfileForever struct {
	calls  atomic.Int64
	closed chan struct{}
}

func (l *emfileForever) Accept() (net.Conn, error) {
	select {
	case <-l.closed:
		return nil, net.ErrClosed
	default:
	}
	l.calls.Add(1)
	return nil, &net.OpError{Op: "accept", Net: "tcp", Err: os.NewSyscallError("accept", syscall.EMFILE)}
}

func (l *emfileForever) Close() error   { close(l.closed); return nil }
func (l *emfileForever) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

// TestServeBacksOffPersistentAcceptErrors: a process at its descriptor
// limit fails every Accept; Serve backs off (5 ms doubling to 1 s), it
// does not call Accept in a loop.
func TestServeBacksOffPersistentAcceptErrors(t *testing.T) {
	ln := &emfileForever{closed: make(chan struct{})}
	n := mustNew(t, peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("b")}})
	go func() { _ = n.Serve(echoAccept) }()
	time.Sleep(400 * time.Millisecond)
	drainNow(n)
	// 5+10+20+40+80+160 ms: 7 calls in 400 ms; a spin makes millions.
	if got := ln.calls.Load(); got > 12 {
		t.Fatalf("%d Accept calls in 400 ms under EMFILE: Serve spins", got)
	}
}

// TestStuckPeerDoesNotStallOthers: a peer that stops reading (its process
// stopped: every byte toward it black-holed) holds the writes toward it,
// not the Node's sessions to another peer.
func TestStuckPeerDoesNotStallOthers(t *testing.T) {
	listen := func(name string) (*peerlink.Node, string) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		n := mustNew(t, peerlink.Config{Listener: ln, Link: link.Config{Liveness: link.LivenessFast, Auth: link.NameAuth(name)}})
		return n, ln.Addr().String()
	}
	b, bAddr := listen("b")
	c, cAddr := listen("c")
	px := peerlinktest.NewProxy(t, cAddr)
	addrs := map[string]string{"b": bAddr, "c": px.Addr()}
	a := mustNew(t, peerlink.Config{Link: link.Config{Liveness: link.LivenessFast, Auth: link.NameAuth("a")},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			addr, ok := addrs[peer]
			if !ok {
				return nil, fmt.Errorf("no peer %q", peer)
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		}})
	hold := make(chan struct{})
	go func() { _ = b.Serve(echoAccept) }()
	go func() {
		_ = c.Serve(func(_ string, _ []byte, s peerlink.Session) { <-hold; _ = s.Close() }) // never reads
	}()
	go func() { _ = a.Serve(func(_ string, _ []byte, s peerlink.Session) { _ = s.Close() }) }()
	t.Cleanup(func() { close(hold); drainNow(a); drainNow(b); drainNow(c) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Writers toward c, enough to fill every window and the socket
	// buffers once the proxy holds every byte.
	blocked := make(chan error, 8)
	var toC []peerlink.Session
	for range 8 {
		s, err := a.Open(ctx, "c", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Reset() //nolint:errcheck
		toC = append(toC, s)
	}
	px.BlackHole()
	for _, s := range toC {
		go func() {
			buf := make([]byte, 1<<20)
			for {
				if _, err := s.Write(buf); err != nil {
					blocked <- err
					return
				}
			}
		}()
	}
	time.Sleep(500 * time.Millisecond) // the writers toward c are stuck by now
	select {
	case err := <-blocked:
		t.Fatalf("a write toward the stuck peer ended: %v; want it held", err)
	default:
	}

	start := time.Now()
	octx, ocancel := context.WithTimeout(ctx, 2*time.Second)
	defer ocancel()
	s, err := a.Open(octx, "b", nil)
	if err != nil {
		t.Fatalf("Open to b while c is stuck: %v", err)
	}
	_ = s.SetDeadline(time.Now().Add(2 * time.Second))
	echoOnce(t, s)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("echo to b took %v while c was stuck", d)
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("test ran out of time")
	}
}
