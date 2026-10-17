// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
	quic "github.com/nfltr/quic-go"
)

// quicLib is github.com/nfltr/link/peerlink/quiclink: one QUIC connection
// per peer (maxConns does not apply), window 1 MiB per session, the
// liveness preset. It carries itself over UDP (udpLib): mutual TLS 1.3
// always (no plaintext).
type quicLib struct {
	live  liveness
	certs string // tlsConfig's directory
}

// udpLib is a library that dials and listens on UDP itself.
type udpLib interface {
	dialUDP(addr string) client
	listenUDP(addr string, handle func([]byte, net.Conn))
}

func (q *quicLib) liveness() link.Liveness {
	return link.Liveness{Interval: q.live.interval, Timeout: q.live.timeout}
}

func (q *quicLib) dialUDP(addr string) client {
	tc := tlsConfig(q.certs)
	tc.NextProtos = []string{quiclink.ALPN}
	n := must(quiclink.New(quiclink.Config{
		Liveness: q.liveness(), Window: window,
		Dial: func(ctx context.Context, _ string, qc *quic.Config) (*quic.Conn, error) {
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			return quic.DialAddr(ctx, addr, tc, qc)
		},
	}))
	return quicClient{n}
}

type quicClient struct{ n *quiclink.Node }

func (c quicClient) Open(ctx context.Context, hdr []byte) (net.Conn, error) {
	return c.n.Open(ctx, "b", hdr)
}

func (c quicClient) Close() error {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = c.n.Drain(ctx)
	return nil
}

func (q *quicLib) listenUDP(addr string, handle func([]byte, net.Conn)) {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	n := must(quiclink.New(quiclink.Config{
		PacketConn: pc, TLS: tlsConfig(q.certs), Liveness: q.liveness(), Window: window,
		ServedPeer: func(*quic.Conn) (string, error) { return "a", nil },
	}))
	go func() { log.Fatal(n.Serve(func(_ string, h []byte, s peerlink.Session) { handle(h, s) })) }()
}

// The TCP side of lib does not apply.
func (q *quicLib) client(func(context.Context) (net.Conn, error), int) client { panic("quic: dialUDP") }
func (q *quicLib) serve(net.Conn, func([]byte, net.Conn))                     { panic("quic: listenUDP") }
func (q *quicLib) goAwayAll(time.Time)                                        {}
func (q *quicLib) served() int                                                { return 0 }
