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
	if cw, ok := s.(interface{ CloseWrite() error }); ok {
		if err := cw.CloseWrite(); err != nil { // the peer reads io.EOF after msg
			return "", err
		}
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

	server := grpclink.New(grpclink.Config{Listener: ln, Liveness: link.LivenessDefault})
	go upperService(server) //nolint:errcheck // ends with Drain

	client := grpclink.New(grpclink.Config{
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", peer)
		},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Liveness:    link.LivenessDefault,
	})

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
