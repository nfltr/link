// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/nfltr/link"
)

// chain3 builds origin -> hop1 -> hop2 -> far end, two Forward hops, and
// returns the origin's Mux; far serves the far end's sessions. wrap, when
// set, wraps both ends of the middle link (hop1 <-> hop2); live is the
// middle link's liveness (zero: the default).
func chain3(far func([]byte, *link.Session), wrap func(net.Conn) net.Conn, live link.Liveness) *link.Mux {
	o, h1 := bind(link.NoAuth)
	h1out, h2in := bindVia(wrap, live)
	h2out, f := bind(link.NoAuth)
	go link.NewMux(f).Serve(far)
	forward := func(in, out *link.Conn) {
		onward := link.NewMux(out)
		go onward.Serve(nil)
		go link.NewMux(in).Serve(link.Forward(func(ctx context.Context, s *link.Session) (*link.Session, error) {
			c, _ := link.ChainFromContext(ctx)
			return onward.OpenChain(s.Header(), c)
		}, link.ForwardConfig{}))
	}
	forward(h1, h1out)
	forward(h2in, h2out)
	m := link.NewMux(o)
	go m.Serve(nil)
	return m
}

// bindVia is bind with both ends of the pipe wrapped and the dialer's
// liveness set (the listener adopts it).
func bindVia(wrap func(net.Conn) net.Conn, live link.Liveness) (dialer, listener *link.Conn) {
	a, b := net.Pipe()
	if wrap != nil {
		a, b = wrap(a), wrap(b)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch := make(chan *link.Conn, 1)
	go func() {
		c, _, err := link.Server(ctx, b, link.Config{Auth: link.NoAuth})
		if err != nil {
			panic(err)
		}
		ch <- c
	}()
	dialer, err := link.Client(ctx, a, link.Config{Auth: link.NoAuth, Liveness: live})
	if err != nil {
		panic(err)
	}
	return dialer, <-ch
}

// silent is a path that can go silent: once off, every write is
// dropped and reported written (no FIN, no reset), as a black-holed
// network does.
type silent struct {
	net.Conn
	off *atomic.Bool
}

func (c silent) Write(b []byte) (int, error) {
	if c.off.Load() {
		return len(b), nil
	}
	return c.Conn.Write(b)
}

// Example_deadlines: link carries no deadline value from hop to hop.
// The origin bounds the open across every hop with Answered's context,
// sets its own read deadline (local, as on a net.Conn), sends its budget
// in the header where the far end needs it, and propagates its giving up
// with Abort, which reaches the far end through the hop as a typed
// *link.Abort.
func Example_deadlines() {
	gaveUp := make(chan error, 1)
	m := chain3(func(header []byte, s *link.Session) {
		budget, _ := time.ParseDuration(string(header)) // the origin's budget, if the far end needs it
		_ = s.Accept()
		_ = s.SetReadDeadline(time.Now().Add(budget)) // the far end's own deadline
		_, err := io.Copy(io.Discard, s)              // works, never answers
		gaveUp <- err
	}, nil, link.Liveness{})
	defer m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := m.Open([]byte("5s"))
	if err != nil {
		panic(err)
	}
	if err := s.Answered(ctx); err != nil { // the open, across every hop, within ctx
		panic(err)
	}
	stop := context.AfterFunc(ctx, func() { _ = s.Abort(link.CodeApp+1, "caller gave up") })
	defer stop()

	_ = s.SetReadDeadline(time.Now().Add(200 * time.Millisecond)) // this side only
	_, err = s.Read(make([]byte, 1))
	fmt.Println("origin read:", errors.Is(err, os.ErrDeadlineExceeded))
	cancel() // give up: Abort, both ways along the chain

	var a *link.Abort
	if errors.As(<-gaveUp, &a) {
		fmt.Printf("far end: code %d, %d join(s) away: %s\n", a.Code-link.CodeApp, a.Hops, a.Msg)
	}
	// Output:
	// origin read: true
	// far end: code 1, 2 join(s) away: caller gave up
}

// Example_deadMiddleHop: liveness is per link. The middle link of
// origin -> hop1 -> hop2 -> far end goes silent; the hops beside it
// declare it dead within its own bound (here interval 1 s, timeout 2 s:
// 3 s) and abort both ways, so both ends read CodeConnLost, never a clean
// end, while the links they hold themselves stay up.
func Example_deadMiddleHop() {
	off := new(atomic.Bool)
	farErr := make(chan error, 1)
	m := chain3(func(_ []byte, s *link.Session) {
		_ = s.Accept()
		_, err := io.Copy(io.Discard, s)
		farErr <- err
	}, func(c net.Conn) net.Conn { return silent{c, off} }, link.Liveness{Interval: time.Second, Timeout: 2 * time.Second})
	defer m.Close()

	s, _ := m.Open(nil)
	if err := s.Answered(context.Background()); err != nil {
		panic(err)
	}
	start := time.Now()
	off.Store(true) // the middle link goes silent
	_, originErr := io.Copy(io.Discard, s)
	took := time.Since(start)

	for _, end := range []struct {
		name string
		err  error
	}{{"origin", originErr}, {"far end", <-farErr}} {
		var a *link.Abort
		if errors.As(end.err, &a) {
			fmt.Printf("%s: CodeConnLost %v, from the hop beside the dead link: %v\n", end.name, a.Code == link.CodeConnLost, a.Hops == 0)
		}
	}
	fmt.Println("within the link's bound plus a trip:", took < 3*time.Second+time.Second)
	// Output:
	// origin: CodeConnLost true, from the hop beside the dead link: true
	// far end: CodeConnLost true, from the hop beside the dead link: true
	// within the link's bound plus a trip: true
}
