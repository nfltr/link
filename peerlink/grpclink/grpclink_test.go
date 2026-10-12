// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpclink_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/grpclink"
	"github.com/nfltr/link/peerlink/peerlinktest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// name is per-RPC credentials that say who the opener is: for tests only
// (they prove nothing).
type name string

func (n name) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"x-peer": string(n)}, nil
}

func (name) RequireTransportSecurity() bool { return false }

func identify(ctx context.Context) (string, error) {
	v := metadata.ValueFromIncomingContext(ctx, "x-peer")
	if len(v) != 1 {
		return "", errors.New("no x-peer")
	}
	return v[0], nil
}

// TestConformance runs the peerlink suite with LivenessFast: the listener
// keeps its 1.5 s + 3 s; the dialer's keepalive is held to gRPC's 10 s
// floor, plus up to 5 s trying to send GOAWAY, so DeadAfter is 18 s.
func TestConformance(t *testing.T) {
	peerlinktest.Run(t, func(t *testing.T) peerlinktest.Pair {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		px := peerlinktest.NewProxy(t, ln.Addr().String())
		b := grpclink.New(grpclink.Config{Listener: ln, Liveness: link.LivenessFast, Identify: identify})
		a := grpclink.New(grpclink.Config{
			Liveness: link.LivenessFast,
			Dial: func(ctx context.Context, peer string) (net.Conn, error) {
				if peer != "b" {
					return nil, fmt.Errorf("no peer %q", peer)
				}
				var d net.Dialer
				return d.DialContext(ctx, "tcp", px.Addr())
			},
			DialOptions: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithPerRPCCredentials(name("a")),
			},
		})
		return peerlinktest.Pair{
			A: a, B: b, AName: "a", BName: "b",
			DeadAfter: a.DeadAfter(),
			BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle,
		}
	})
}

// TestReverseUnsupported: a Node opens no sessions on connections its
// peers dialled; without Config.Dial, Open fails at once (it does not wait
// for the peer as peerlink.Node does).
func TestReverseUnsupported(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	n := grpclink.New(grpclink.Config{Listener: ln, Identify: identify})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	start := time.Now()
	s, err := n.Open(ctx, "a", nil)
	if err == nil {
		_ = s.Close()
		t.Fatal("Open without Config.Dial succeeded")
	}
	if errors.Is(err, peerlink.ErrPeerUnreachable) || time.Since(start) > time.Second {
		t.Fatalf("Open without Config.Dial: %v after %v, want an immediate refusal", err, time.Since(start))
	}
}
