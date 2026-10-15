// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Command twoproc is the quickstart in two processes: b listens, a dials
// it, and each opens a session to the other on that one connection.
//
//	go run github.com/nfltr/link/examples/twoproc@latest -role b &
//	go run github.com/nfltr/link/examples/twoproc@latest -role a
//
// a exits once both sessions ran; b serves until interrupted.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

func main() {
	role := flag.String("role", "", "a (dials) or b (listens)")
	addr := flag.String("addr", "127.0.0.1:7000", "b's address: where b listens and a dials")
	flag.Parse()
	switch *role {
	case "a":
		runA(*addr)
	case "b":
		runB(*addr)
	default:
		log.Fatal("-role a or -role b")
	}
}

// runB listens on addr, serves the sessions a opens, opens one to a once
// a is connected, and runs until interrupted.
func runB(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	b, err := peerlink.New(peerlink.Config{
		Listener: ln,
		Link:     link.Config{Auth: link.NameAuth("b")},
	})
	if err != nil {
		log.Fatal(err)
	}
	go b.Serve(reply("b", nil))
	fmt.Println(ask(b, "a", time.Minute)) // waits up to a minute for a to connect

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()
	drain(b)
}

// runA dials b at addr. Connect keeps that connection up and Serve takes
// the sessions b opens on it: without both, b cannot reach a.
func runA(addr string) {
	served := make(chan struct{}, 1)
	a, err := peerlink.New(peerlink.Config{
		Link:    link.Config{Auth: link.NameAuth("a")},
		Connect: []string{"b"},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr) // peer is "b"
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	go a.Serve(reply("a", served))
	fmt.Println(ask(a, "b", 5*time.Second))
	select {
	case <-served: // b's session to a ran
	case <-time.After(time.Minute):
		log.Fatal("b opened no session to a")
	}
	drain(a)
}

// reply serves each session a peer opens: it reads the request, answers
// it, and then signals served (when not nil).
func reply(me string, served chan<- struct{}) func(peer string, header []byte, s peerlink.Session) {
	return func(peer string, header []byte, s peerlink.Session) {
		defer s.Close()
		req, _ := io.ReadAll(s) // until the opener's CloseWrite
		fmt.Fprintf(s, "%s got %q from %s", me, req, peer)
		if served != nil {
			served <- struct{}{}
		}
	}
}

// ask opens a session to peer, sends a request and reads the answer.
func ask(l peerlink.Link, peer string, wait time.Duration) string {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	s, err := l.Open(ctx, peer, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	if _, err := io.WriteString(s, "hello"); err != nil {
		log.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
		log.Fatal(err)
	}
	answer, err := io.ReadAll(s)
	if err != nil {
		log.Fatal(err)
	}
	return string(answer)
}

func drain(l peerlink.Link) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = l.Drain(ctx)
}
