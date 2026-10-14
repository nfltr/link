// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Command quickstart runs two peers, a and b, in one process: a dials b
// over plaintext TCP, then each opens a session to the other on that one
// connection.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

func main() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}

	// b listens. NameAuth: each peer sends its name and is known by it
	// (a trusted network only; see docs/recipes.md for mutual TLS).
	b, err := peerlink.New(peerlink.Config{
		Listener: ln,
		Link:     link.Config{Auth: link.NameAuth("b")},
	})
	if err != nil {
		log.Fatal(err)
	}
	go b.Serve(reply("b"))

	// a dials b, and keeps a connection to it (Connect) so that b can
	// open sessions to a as well.
	a, err := peerlink.New(peerlink.Config{
		Link:    link.Config{Auth: link.NameAuth("a")},
		Connect: []string{"b"},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", ln.Addr().String()) // peer is "b"
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	go a.Serve(reply("a"))

	fmt.Println(ask(a, "b")) // a opens a session to b
	fmt.Println(ask(b, "a")) // b opens one to a, on the connection a dialled

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = a.Drain(ctx)
	_ = b.Drain(ctx)
}

// reply serves each session a peer opens: it reads the request and
// answers it.
func reply(me string) func(peer string, header []byte, s net.Conn) {
	return func(peer string, header []byte, s net.Conn) {
		defer s.Close()
		req, _ := io.ReadAll(s) // until the opener's CloseWrite
		fmt.Fprintf(s, "%s got %q (%s) from %s", me, req, header, peer)
	}
}

// ask opens a session to peer, sends a request and reads the answer.
func ask(l peerlink.Link, peer string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := l.Open(ctx, peer, []byte("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	if _, err := io.WriteString(s, "hello"); err != nil {
		log.Fatal(err)
	}
	// The peer reads io.EOF after "hello"; this side can still read.
	if err := s.(peerlink.Session).CloseWrite(); err != nil {
		log.Fatal(err)
	}
	answer, err := io.ReadAll(s)
	if err != nil {
		log.Fatal(err)
	}
	return string(answer)
}
