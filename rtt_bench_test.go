// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// delayConn delays every byte d each way: its Writes are queued and
// written d later, its Reads return bytes d after they arrived (a long fat
// pipe of round trip 2d, unbounded in bytes).
type delayConn struct {
	net.Conn
	d      time.Duration
	wq, rq chan stamped
	rbuf   []byte
	rerr   error
	once   sync.Once
}

type stamped struct {
	at time.Time
	b  []byte
}

func newDelayConn(nc net.Conn, d time.Duration) *delayConn {
	c := &delayConn{Conn: nc, d: d, wq: make(chan stamped, 1<<14), rq: make(chan stamped, 1<<14)}
	go func() {
		for p := range c.wq {
			time.Sleep(time.Until(p.at))
			if _, err := nc.Write(p.b); err != nil {
				return
			}
		}
	}()
	go func() {
		defer close(c.rq)
		for {
			b := make([]byte, 64<<10)
			n, err := nc.Read(b)
			if n > 0 {
				c.rq <- stamped{time.Now().Add(d), b[:n]}
			}
			if err != nil {
				return
			}
		}
	}()
	return c
}

func (c *delayConn) Write(b []byte) (int, error) {
	c.wq <- stamped{time.Now().Add(c.d), append([]byte(nil), b...)}
	return len(b), nil
}

func (c *delayConn) Read(b []byte) (int, error) {
	if len(c.rbuf) == 0 {
		p, ok := <-c.rq
		if !ok {
			return 0, io.EOF
		}
		time.Sleep(time.Until(p.at))
		c.rbuf = p.b
	}
	n := copy(b, c.rbuf)
	c.rbuf = c.rbuf[n:]
	return n, nil
}

func (c *delayConn) Close() error {
	c.once.Do(func() { close(c.wq) })
	return c.Conn.Close()
}

// BenchmarkRTTThroughput: MB/s of one session origin -> far end over a
// first link of round trip rtt (and a joined second one at hops=1).
func BenchmarkRTTThroughput(b *testing.B) {
	for _, rtt := range []time.Duration{20 * time.Millisecond} {
		for _, hops := range []int{0, 1} {
			for _, w := range []int{DefaultWindow, 4 << 20} {
				b.Run(fmt.Sprintf("rtt=%s/hops=%d/window=%dMiB", rtt, hops, w>>20), func(b *testing.B) {
					r := newChainRig(b, rigOpts{n: hops + 1, window: w, wrap: func(i int, nc net.Conn) net.Conn {
						if i == 0 {
							return newDelayConn(nc, rtt/2)
						}
						return nc
					}}, farSink)
					const per = 64 << 20
					buf := make([]byte, 256<<10)
					b.SetBytes(per)
					b.ResetTimer()
					for range b.N {
						s := r.open("sink")
						for sent := 0; sent < per; sent += len(buf) {
							if _, err := s.Write(buf); err != nil {
								b.Fatal(err)
							}
						}
						_ = s.CloseWrite()
						_, _ = io.Copy(io.Discard, s)
					}
				})
			}
		}
	}
}
