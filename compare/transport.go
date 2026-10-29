// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package main compares github.com/nfltr/link and github.com/hashicorp/yamux
// as the relay's replica-to-replica link: many byte-stream sessions between
// two processes over one or more TLS or plaintext TCP connections.
package main

import (
	"context"
	"net"
	"sync"
	"time"
)

// lib is one multiplexing library behind the same shape: a dialer side
// (client: a pool of up to maxConns connections, sessions opened with a
// header) and a listener side (serve: one accepted connection, every
// session the peer opens handed to handle).
type lib interface {
	client(dial func(context.Context) (net.Conn, error), maxConns int) client
	serve(nc net.Conn, handle func(hdr []byte, s net.Conn))
	goAwayAll(deadline time.Time) // listener: no new sessions on served conns; each closes once its sessions end
	served() int                  // listener: connections open now
}

type client interface {
	Open(ctx context.Context, hdr []byte) (net.Conn, error)
	Close() error
}

// liveness is the one setting both libraries get from the same preset.
type liveness struct{ interval, timeout time.Duration }

var presets = map[string]liveness{
	"default": {5 * time.Second, 10 * time.Second},        // link.LivenessDefault
	"fast":    {1500 * time.Millisecond, 3 * time.Second}, // link.LivenessFast
}

const window = 1 << 20 // per-session receive window, both libraries

// events records connection ends (both sides) for the liveness and fault cases.
type events struct {
	mu       sync.Mutex
	list     []event
	deadPeer int
}

type event struct {
	T    int64  `json:"t"` // unix ns
	Kind string `json:"kind"`
	Err  string `json:"err,omitempty"`
}

func (e *events) add(kind string, err error, dead bool) {
	ev := event{T: time.Now().UnixNano(), Kind: kind}
	if err != nil {
		ev.Err = err.Error()
	}
	e.mu.Lock()
	e.list = append(e.list, ev)
	if dead {
		e.deadPeer++
	}
	e.mu.Unlock()
}

func (e *events) snapshot() ([]event, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]event(nil), e.list...), e.deadPeer
}
