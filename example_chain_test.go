// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link_test

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/nfltr/link"
)

// ExampleForward joins two links end to end: the origin opens through a
// forwarding hop to a far end, which answers each open; the origin reads
// the answer (accepted, with the path's length, or a typed refusal
// saying which hop and why) before it trusts the session.
func ExampleForward() {
	o, h1 := bind(link.NoAuth) // origin -> hop
	h2, f := bind(link.NoAuth) // hop -> far end

	// The far end: an echo, or a refusal with the application's code.
	go link.NewMux(f).Serve(func(header []byte, s *link.Session) {
		if string(header) != "echo" {
			_ = s.Refuse(link.CodeApp+1, "no such service")
			return
		}
		_ = s.Accept()
		_, _ = io.Copy(s, s)
		_ = s.CloseWrite()
	})

	// The hop: every session goes on to the far end (the Route decides
	// where; Forward answers, joins and holds no goroutine while idle).
	onward := link.NewMux(h2)
	go onward.Serve(nil)
	go link.NewMux(h1).Serve(link.Forward(func(ctx context.Context, in *link.Session) (*link.Session, error) {
		c, _ := link.ChainFromContext(ctx)
		return onward.OpenChain(in.Header(), c)
	}, link.ForwardConfig{}))

	// The origin.
	m := link.NewMux(o)
	go m.Serve(nil)
	defer m.Close()
	s, _ := m.Open([]byte("echo"))
	_, _ = io.WriteString(s, "hello") // before the answer: no round trip per hop
	_ = s.CloseWrite()
	if err := s.Answered(context.Background()); err != nil {
		panic(err)
	}
	reply, _ := io.ReadAll(s)
	fmt.Printf("%s, %d hop(s) beyond the peer\n", reply, s.PathLen())

	r, _ := m.Open([]byte("ftp"))
	var a *link.Abort
	if errors.As(r.Answered(context.Background()), &a) {
		fmt.Printf("refused %d hop(s) beyond the peer: code %d, %s\n", a.Hops, a.Code-link.CodeApp, a.Msg)
	}
	// Output:
	// hello, 1 hop(s) beyond the peer
	// refused 1 hop(s) beyond the peer: code 1, no such service
}
