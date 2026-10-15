// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// brokenWrites fails its writes once broken, its reads unchanged: a
// connection the peer reset, before its reader saw the end.
type brokenWrites struct {
	net.Conn
	broken *atomic.Bool
}

func (c brokenWrites) Write(b []byte) (int, error) {
	if c.broken.Load() {
		return 0, errors.New("write: connection reset by peer")
	}
	return c.Conn.Write(b)
}

// TestPoolOpenPassesABrokenConnection: Open on a Pool whose only
// connection broke (its writes fail, its reader has not seen the end yet)
// dials a new connection and opens there, instead of failing with the
// broken one's write error; Max (1) does not count the broken one.
func TestPoolOpenPassesABrokenConnection(t *testing.T) {
	addr, _, accepted := echoListener(t)
	var first atomic.Bool
	var dials atomic.Int32
	p := mustPool(t, PoolConfig{
		Link: v2cfg,
		Dial: func(ctx context.Context) (net.Conn, error) {
			nc, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			if dials.Add(1) == 1 {
				return brokenWrites{nc, &first}, nil
			}
			return nc, nil
		},
	})
	defer p.Close() //nolint:errcheck
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := p.Open(ctx, []byte("x")) // the first connection is up
	if err != nil {
		t.Fatal(err)
	}
	echoOK(t, s)
	_ = s.Close()
	first.Store(true) // its writes fail from now on; its reader runs on
	s, err = p.Open(ctx, []byte("x"))
	if err != nil {
		t.Fatalf("Open with the pool's connection broken: %v", err)
	}
	echoOK(t, s)
	_ = s.Close()
	if n := accepted.Load(); n != 2 {
		t.Fatalf("%d connections, want a second one", n)
	}
}
