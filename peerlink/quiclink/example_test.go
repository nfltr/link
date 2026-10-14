// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/nfltr/link/peerlink/quiclink"
	"github.com/quic-go/quic-go"
)

// Example: node "b" listens on a UDP socket with a certificate and
// requires the dialer's (mutual TLS: peers are named by their
// certificates' CommonName); node "a" dials it with its own and opens a
// session. serverTLS and clientTLS (quiclink_test.go) build the
// tls.Configs from a throwaway CA.
func Example() {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	b := quiclink.New(quiclink.Config{PacketConn: pc, TLS: serverTLS("b")})
	go b.Serve(func(peer string, header []byte, s net.Conn) {
		defer s.Close()
		msg, _ := io.ReadAll(s)
		fmt.Fprintf(s, "%s %s %s", peer, header, msg)
	})

	a := quiclink.New(quiclink.Config{
		Dial: func(ctx context.Context, peer string, qc *quic.Config) (*quic.Conn, error) {
			return quic.DialAddr(ctx, pc.LocalAddr().String(), clientTLS("a", peer), qc)
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := a.Open(ctx, "b", []byte("echo"))
	if err != nil {
		panic(err)
	}
	_, _ = io.WriteString(s, "hello")
	_ = s.(*quiclink.Session).CloseWrite()
	reply, _ := io.ReadAll(s)
	_ = s.Close()
	fmt.Println(string(reply))

	_ = a.Drain(ctx)
	_ = b.Drain(ctx)
	// Output: a echo hello
}
