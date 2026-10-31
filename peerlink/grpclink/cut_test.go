// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpclink_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/grpclink"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// pair is a dialer a and a listener b serving accept.
func pair(t *testing.T, accept func(string, []byte, peerlink.Session)) *grpclink.Node {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	b := mustNew(t, grpclink.Config{Listener: ln, Identify: identify})
	go func() { _ = b.Serve(accept) }()
	a := mustNew(t, grpclink.Config{
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			if peer != "b" {
				return nil, fmt.Errorf("no peer %q", peer)
			}
			var d net.Dialer
			return d.DialContext(ctx, "tcp", ln.Addr().String())
		},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(name("a"))},
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = a.Drain(ctx)
		_ = b.Drain(ctx)
	})
	return a
}

// TestWriteAfterCutIsNoClose: a write on a session the peer reset fails
// with an error, never io.ErrClosedPipe alone (peerlink.Join takes that
// for the peer's Close and closes the other side cleanly: bytes go
// missing without an error).
func TestWriteAfterCutIsNoClose(t *testing.T) {
	t.Run("accepter", func(t *testing.T) {
		werr := make(chan error, 1)
		a := pair(t, func(_ string, _ []byte, s peerlink.Session) {
			_, _ = io.Copy(io.Discard, s) // the opener's reset ends it
			time.Sleep(200 * time.Millisecond)
			_, err := s.Write([]byte("x"))
			werr <- err
		})
		s, err := a.Open(context.Background(), "b", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		_ = s.Reset()
		if err := <-werr; err == nil || errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("accepter's write after the opener's reset: %v", err)
		}
	})
	t.Run("opener", func(t *testing.T) {
		a := pair(t, func(_ string, _ []byte, s peerlink.Session) { _ = s.Reset() })
		s, err := a.Open(context.Background(), "b", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, s); err == nil {
			t.Fatal("read the accepter's reset as a clean end")
		}
		if _, err := s.Write([]byte("x")); err == nil || errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("opener's write after the accepter's reset: %v", err)
		}
	})
}

// TestNodeDropsIdlePeers: a Node keeps no grpc.ClientConn (its goroutines,
// its redials) for a peer it reaches no more and has no session with: a
// peer named by an address that changes otherwise left one per address
// until Drain.
func TestNodeDropsIdlePeers(t *testing.T) {
	waitNone := func(t *testing.T, n *grpclink.Node) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); grpclink.Peers(n) != 0; time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%d peers kept with no session and no connection", grpclink.Peers(n))
			}
		}
	}
	t.Run("dial fails", func(t *testing.T) {
		n := mustNew(t, grpclink.Config{
			Dial:        func(context.Context, string) (net.Conn, error) { return nil, errors.New("unreachable") },
			DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		})
		defer func() { _ = n.Drain(context.Background()) }()
		for i := range 100 {
			s, err := n.Open(context.Background(), fmt.Sprint("10.0.0.", i), nil)
			if err == nil {
				if _, err = s.Read(make([]byte, 1)); err == nil {
					t.Fatal("a session to an unreachable peer read")
				}
				_ = s.Close()
			}
		}
		waitNone(t, n)
	})
	t.Run("peer gone", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		b := mustNew(t, grpclink.Config{Listener: ln, Identify: identify})
		go func() {
			_ = b.Serve(func(_ string, _ []byte, s peerlink.Session) { defer s.Close(); _, _ = io.Copy(s, s) })
		}()
		a := mustNew(t, grpclink.Config{
			Dial: func(ctx context.Context, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
			},
			DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(name("a"))},
		})
		defer func() { _ = a.Drain(context.Background()) }()
		s, err := a.Open(context.Background(), "b", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(s, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
		if got := grpclink.Peers(a); got != 1 {
			t.Fatalf("%d peers with a connection up, want 1", got)
		}
		if err := b.Drain(context.Background()); err != nil { // the peer is gone
			t.Fatal(err)
		}
		waitNone(t, a)
	})
}
