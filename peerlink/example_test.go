// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

// echo serves a session: it writes back what it reads, prefixed with who
// opened it and the header.
func echo(peer string, header []byte, s net.Conn) {
	defer s.Close()
	b, _ := io.ReadAll(s)
	fmt.Fprintf(s, "%s %s %s", peer, header, b)
}

// call opens a session to peer, writes msg and returns the reply.
func call(l peerlink.Link, peer, msg string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := l.Open(ctx, peer, []byte("echo"))
	if err != nil {
		panic(err)
	}
	defer s.Close()
	_, _ = io.WriteString(s, msg)
	_ = s.(interface{ CloseWrite() error }).CloseWrite()
	b, _ := io.ReadAll(s)
	return string(b)
}

func drain(l peerlink.Link) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = l.Drain(ctx)
}

// ExampleNode: a Node that listens serves the sessions a Node that dials
// opens to it. Peers are named by what Link.Auth verified (here a name
// each sends, which proves nothing: use TLS or a real Authenticator).
func ExampleNode() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	server := peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: nameAuth("server")}})
	go server.Serve(echo)
	defer drain(server)

	client := peerlink.New(peerlink.Config{
		Link: link.Config{Auth: nameAuth("client")},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", ln.Addr().String()) // peer "server"
		},
	})
	defer drain(client)

	fmt.Println(call(client, "server", "hello"))
	// Output: client echo hello
}

// ExampleNode_reverse: the server opens sessions to a client it cannot
// dial (behind NAT, say). The client dials it (Config.Connect) and serves
// the sessions the server opens on that connection; the server, with no
// Config.Dial, opens on the connections its peers dialled in.
func ExampleNode_reverse() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	server := peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: nameAuth("server")}})
	go server.Serve(echo)
	defer drain(server)

	client := peerlink.New(peerlink.Config{
		Link:    link.Config{Auth: nameAuth("client")},
		Connect: []string{"server"},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", ln.Addr().String())
		},
	})
	go client.Serve(echo)
	defer drain(client)

	// Open waits (up to its ctx) for the client's connection to come up.
	fmt.Println(call(server, "client", "hello"))
	// Output: server echo hello
}
