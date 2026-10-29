// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package defaults

import (
	"context"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/nfltr/link"
	"golang.ngrok.com/muxado/v2"
)

// mux is one connection's two ends as one library makes them, each with
// that library's defaults: open opens a stream on the dialer side, accept
// takes the next stream the listener side received.
type mux struct {
	open   func() (net.Conn, error)
	accept func() (net.Conn, error)
}

// lib makes a mux over a connected pair (x dials, y listens).
type lib struct {
	name string
	pair func(tb testing.TB, x, y net.Conn) mux
}

var allLibs = []lib{
	{"link", linkPair},
	{"yamux", yamuxPair},
	{"muxado", muxadoPair},
}

// libs is allLibs in the order $LIBS names (default link,yamux,muxado):
// run.sh rotates it between passes so no library always runs first.
func libs() []lib {
	order := os.Getenv("LIBS")
	if order == "" {
		return allLibs
	}
	var out []lib
	for _, n := range strings.Split(order, ",") {
		for _, l := range allLibs {
			if l.name == n {
				out = append(out, l)
			}
		}
	}
	return out
}

// linkPair: link.Config{} on the dialer; the listener needs an
// Authenticator, NoAuth (no credential, as yamux and muxado have none).
func linkPair(tb testing.TB, x, y net.Conn) mux {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sc := make(chan *link.Conn, 1)
	go func() {
		c, _, err := link.Server(ctx, y, link.Config{Auth: link.NoAuth})
		if err != nil {
			tb.Error(err)
		}
		sc <- c
	}()
	ca, err := link.Client(ctx, x, link.Config{})
	if err != nil {
		tb.Fatal(err)
	}
	cb := <-sc
	if cb == nil {
		tb.FailNow()
	}
	a, b := link.NewMux(ca), link.NewMux(cb)
	acc := make(chan net.Conn, 4096)
	go func() { _ = a.Serve(nil) }()
	go func() { _ = b.Serve(func(_ []byte, s *link.Session) { acc <- s }) }()
	tb.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return mux{
		open:   func() (net.Conn, error) { return a.Open(nil) },
		accept: chanAccept(acc, b.Conn().Done()),
	}
}

func chanAccept(acc chan net.Conn, done <-chan struct{}) func() (net.Conn, error) {
	return func() (net.Conn, error) {
		select {
		case s := <-acc:
			return s, nil
		case <-done:
			return nil, net.ErrClosed
		}
	}
}

// yamuxPair: yamux.DefaultConfig() (nil), its log discarded.
func yamuxPair(tb testing.TB, x, y net.Conn) mux {
	cfg := yamux.DefaultConfig()
	cfg.LogOutput = nil
	cfg.Logger = discardLogger{}
	a, err := yamux.Client(x, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	b, err := yamux.Server(y, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return mux{
		open:   func() (net.Conn, error) { return a.Open() },
		accept: func() (net.Conn, error) { return b.Accept() },
	}
}

type discardLogger struct{}

func (discardLogger) Print(...any)          {}
func (discardLogger) Printf(string, ...any) {}
func (discardLogger) Println(...any)        {}

// muxadoPair: muxado.Client and Server with a nil (default) Config.
func muxadoPair(tb testing.TB, x, y net.Conn) mux {
	a, b := muxado.Client(x, nil), muxado.Server(y, nil)
	tb.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return mux{open: a.Open, accept: b.Accept}
}

// closeWrite ends a stream's sending side (link and muxado: CloseWrite;
// yamux's Close is a half close).
func closeWrite(c net.Conn) error {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return c.Close()
}

// tcpPair is a TCP loopback connection's two ends; with rtt > 0 each end
// delays what it writes by rtt/2 (delayConn).
func tcpPair(tb testing.TB, rtt time.Duration) (net.Conn, net.Conn) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	ac := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		ac <- c
	}()
	x, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		tb.Fatal(err)
	}
	y := <-ac
	if y == nil {
		tb.Fatal("accept failed")
	}
	if rtt > 0 {
		x, y = newDelayConn(x, rtt/2), newDelayConn(y, rtt/2)
	}
	tb.Cleanup(func() { _ = x.Close(); _ = y.Close() })
	return x, y
}

// delayConn delivers each Write to the connection d after it was made, in
// order, without limiting the bytes in flight: a long path whose
// bandwidth is loopback's, so a session's window bounds its throughput.
type delayConn struct {
	net.Conn
	d    time.Duration
	q    chan delayed
	done chan struct{}
	once sync.Once
}

type delayed struct {
	at time.Time
	b  []byte
}

func newDelayConn(c net.Conn, d time.Duration) *delayConn {
	dc := &delayConn{Conn: c, d: d, q: make(chan delayed, 1<<16), done: make(chan struct{})}
	go dc.pump()
	return dc
}

func (c *delayConn) Write(b []byte) (int, error) {
	select {
	case c.q <- delayed{time.Now().Add(c.d), append([]byte(nil), b...)}:
		return len(b), nil
	case <-c.done:
		return 0, net.ErrClosed
	}
}

func (c *delayConn) pump() {
	for {
		select {
		case w := <-c.q:
			time.Sleep(time.Until(w.at))
			if _, err := c.Conn.Write(w.b); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *delayConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return c.Conn.Close()
}
