// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// stuckConn reads r and never completes a write: a peer that sends and
// does not read.
type stuckConn struct {
	net.Conn
	r      io.Reader
	closed chan struct{}
}

func (c *stuckConn) Read(b []byte) (int, error) { return c.r.Read(b) }

func (c *stuckConn) Write([]byte) (int, error) {
	<-c.closed
	return 0, net.ErrClosed
}

func (c *stuckConn) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

// TestPingFloodBoundsBuffer: a peer that pings without reading gets one
// pong per write, not a write buffer that grows with its pings (each ping
// is answered, the latest when several wait: SECURITY.md).
func TestPingFloodBoundsBuffer(t *testing.T) {
	const pings = 50000
	var wire bytes.Buffer
	for i := range pings {
		wire.Write(frame(pingLen, typePing, 0, 0, 0, 0, 0, 0, byte(i>>8), byte(i)))
	}
	nc := &stuckConn{r: &wire, closed: make(chan struct{})}
	c := newConnV2(nc, Config{Window: testWindow, MaxSessions: MaxSessions, Liveness: LivenessDatacenter}, testWindow, MaxSessions, false)
	c.up(nil, Identity{})
	defer c.Close() //nolint:errcheck
	for {
		if _, err := c.Recv(); err != nil {
			break // every ping read
		}
	}
	// keepAlive answers the pings it took meanwhile; then the buffer holds
	// what waits behind the stuck write.
	deadline := time.Now().Add(5 * time.Second)
	for len(c.pong) > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)
	c.wmu.Lock()
	n := 0
	if c.wbuf != nil {
		n = len(*c.wbuf)
	}
	c.wmu.Unlock()
	if n > 5+pingLen {
		t.Fatalf("%d bytes buffered behind a stuck write after %d pings, want at most one pong", n, pings)
	}
}

// TestPoolBacksOffRefusingPeer: a peer that takes the connection and then
// sends GOAWAY (or closes it) before any session went on it refused it: the
// Pool redials Min with Backoff, never in a tight loop.
func TestPoolBacksOffRefusingPeer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				c, _, err := Server(context.Background(), nc, Config{Auth: NoAuth})
				if err != nil {
					_ = nc.Close()
					return
				}
				m := NewMux(c)
				_ = m.GoAway(time.Time{}) // a listener that is going away
				_ = m.Serve(nil)
			}()
		}
	}()
	var dials atomic.Int32
	p := mustPool(t, PoolConfig{Min: 1, Backoff: func(int) time.Duration { return 100 * time.Millisecond },
		Dial: func(ctx context.Context) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
		}})
	time.Sleep(time.Second)
	_ = p.Close()
	if n := dials.Load(); n > 15 {
		t.Fatalf("%d dials in 1s to a peer that refuses every connection, Backoff 100ms", n)
	}
}
