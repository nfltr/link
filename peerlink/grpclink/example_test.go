// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpclink_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/grpclink"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// upperService is application code: it knows only peerlink.Link and the
// net.Conn sessions it carries, never gRPC.
func upperService(l peerlink.Link) error {
	return l.Serve(func(peer string, header []byte, s net.Conn) {
		defer s.Close()
		body, err := io.ReadAll(s) // until the opener's CloseWrite
		if err != nil {
			return
		}
		fmt.Fprintf(s, "%s: %s", header, bytes.ToUpper(body))
	})
}

// callUpper is application code too: one session per request.
func callUpper(ctx context.Context, l peerlink.Link, peer, msg string) (string, error) {
	s, err := l.Open(ctx, peer, []byte("upper"))
	if err != nil {
		return "", err
	}
	defer s.Close()
	if _, err := io.WriteString(s, msg); err != nil {
		return "", err
	}
	if err := s.(peerlink.Session).CloseWrite(); err != nil { // the peer reads io.EOF after msg
		return "", err
	}
	reply, err := io.ReadAll(s)
	return string(reply), err
}

// Example wires the application above to gRPC: only this function knows the
// transport, so swapping in peerlink.Node (link) changes these lines alone.
// Plaintext keeps the example short; in production pass TLS or mTLS
// transport credentials in DialOptions and ServerOptions.
func Example() {
	ctx := context.Background()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	server, err := grpclink.New(grpclink.Config{Listener: ln, Liveness: link.LivenessDefault})
	if err != nil {
		panic(err)
	}
	go upperService(server) //nolint:errcheck // ends with Drain

	client, err := grpclink.New(grpclink.Config{
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", peer)
		},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Liveness:    link.LivenessDefault,
	})
	if err != nil {
		panic(err)
	}

	for _, msg := range []string{"hello", "world"} {
		reply, err := callUpper(ctx, client, ln.Addr().String(), msg)
		if err != nil {
			panic(err)
		}
		fmt.Println(reply)
	}

	drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = client.Drain(drainCtx)
	_ = server.Drain(drainCtx)
	// Output:
	// upper: HELLO
	// upper: WORLD
}

// Example_grpcOverLink is the other way round from this package: a
// grpc.Server serving the sessions peers open on a link (peerlink.Node,
// through a peerlink.Listener), and a grpc.ClientConn whose connections
// are sessions (grpc.WithContextDialer over Open). gRPC then runs over the
// link's connections, beside any other sessions they carry.
func Example_grpcOverLink() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	server, err := peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("server")}})
	if err != nil {
		panic(err)
	}
	sessions := peerlink.NewListener()
	go server.Serve(sessions.Handle) //nolint:errcheck // ends with Drain
	gs := grpc.NewServer()
	healthpb.RegisterHealthServer(gs, health.NewServer()) // any gRPC service
	go gs.Serve(sessions)                                 //nolint:errcheck // ends with Stop

	client, err := peerlink.New(peerlink.Config{
		Link: link.Config{Auth: link.NameAuth("client")},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", ln.Addr().String())
		},
	})
	if err != nil {
		panic(err)
	}
	cc, err := grpc.NewClient("passthrough:///server", // the target names the peer
		grpc.WithContextDialer(func(ctx context.Context, peer string) (net.Conn, error) {
			return client.Open(ctx, peer, []byte("grpc"))
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()), // the link's connection is secured, or not, below
	)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.GetStatus())

	_ = cc.Close()
	gs.Stop()
	_ = client.Drain(ctx)
	_ = server.Drain(ctx)
	// Output: SERVING
}
