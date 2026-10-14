// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/nfltr/link"
)

// bind binds both ends of one connection (here an in-memory pipe; in
// practice a TLS connection the caller dialled and accepted).
func bind(auth link.Authenticator) (dialer, listener *link.Conn) {
	a, b := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type bound struct {
		c   *link.Conn
		err error
	}
	ch := make(chan bound, 1)
	go func() {
		c, _, err := link.Server(ctx, b, link.Config{Auth: auth})
		ch <- bound{c, err}
	}()
	dialer, err := link.Client(ctx, a, link.Config{Auth: auth})
	l := <-ch
	if err != nil || l.err != nil {
		panic(fmt.Sprint(err, l.err))
	}
	return dialer, l.c
}

// ExampleMux carries sessions over one connection without a Pool: each
// side wraps its Conn in a Mux, the listener serves the sessions the
// dialer opens, and each session is a net.Conn.
func ExampleMux() {
	dc, lc := bind(tokenAuth("s3cret"))

	go link.NewMux(lc).Serve(func(header []byte, s *link.Session) {
		defer s.Close()
		n, _ := io.Copy(io.Discard, s) // until the opener's CloseWrite
		fmt.Fprintf(s, "%s: %d bytes", header, n)
	})

	m := link.NewMux(dc)
	go m.Serve(nil) // the dialer takes no sessions
	defer m.Close()
	s, err := m.Open(func(sid string) ([]byte, error) { return []byte("count"), nil })
	if err != nil {
		panic(err)
	}
	_, _ = io.WriteString(s, "hello, world")
	_ = s.CloseWrite()
	reply, _ := io.ReadAll(s)
	_ = s.Close()
	fmt.Println(string(reply))
	// Output: count: 12 bytes
}

// ExampleConn drives a Conn without a Mux: message frames, the caller's
// own bytes, one receive loop.
func ExampleConn() {
	dc, lc := bind(link.NoAuth)
	defer dc.Close()
	defer lc.Close()

	go func() {
		for _, msg := range []string{"one", "two"} {
			_ = dc.SendMsg([]byte(msg))
		}
		_ = dc.Flush() // a send buffers; Flush writes
	}()
	for range 2 {
		f, err := lc.Recv()
		if err != nil {
			panic(err)
		}
		fmt.Println(string(f.Msg))
	}
	// Output:
	// one
	// two
}
