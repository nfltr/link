// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/nfltr/link"
)

// linkLib is github.com/nfltr/link configured as relay-server's replica
// link configures it (peer_link.go): window 1 MiB, the liveness preset on
// both sides, Pool growing one connection per 2 sessions up to maxConns.
type linkLib struct {
	live liveness
	ev   *events

	mu   sync.Mutex
	muxs map[*link.Mux]struct{}
}

type linkObs struct {
	link.NopObserver
	ev *events
}

func (o linkObs) ConnDown(_ link.Identity, err error) {
	o.ev.add("down", err, errors.Is(err, link.ErrDeadPeer))
}

func (l *linkLib) config() link.Config {
	return link.Config{
		Window:   window,
		Liveness: link.Liveness{Interval: l.live.interval, Timeout: l.live.timeout},
		Auth:     link.NoAuth,
		Observer: linkObs{ev: l.ev},
	}
}

func (l *linkLib) client(dial func(context.Context) (net.Conn, error), maxConns int) client {
	return linkClient{must(link.NewPool(link.PoolConfig{
		Link:     l.config(),
		Dial:     dial,
		Max:      maxConns,
		Grow:     func(st link.PoolStats) bool { return st.Sessions >= 2*st.Conns },
		GrowWait: 500 * time.Millisecond,
	}))}
}

type linkClient struct{ p *link.Pool }

func (c linkClient) Open(ctx context.Context, hdr []byte) (net.Conn, error) {
	return c.p.Open(ctx, hdr)
}
func (c linkClient) Close() error { return c.p.Close() }

func (l *linkLib) serve(nc net.Conn, handle func([]byte, net.Conn)) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	c, _, err := link.Server(ctx, nc, l.config())
	cancel()
	if err != nil {
		_ = nc.Close()
		return
	}
	m := link.NewMux(c)
	l.mu.Lock()
	if l.muxs == nil {
		l.muxs = map[*link.Mux]struct{}{}
	}
	l.muxs[m] = struct{}{}
	l.mu.Unlock()
	_ = m.Serve(func(hdr []byte, s *link.Session) { handle(hdr, s) })
	l.mu.Lock()
	delete(l.muxs, m)
	l.mu.Unlock()
}

func (l *linkLib) goAwayAll(deadline time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for m := range l.muxs {
		_ = m.GoAway(deadline)
	}
}

func (l *linkLib) served() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.muxs)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
