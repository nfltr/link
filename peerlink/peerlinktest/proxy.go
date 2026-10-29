// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlinktest

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Proxy relays TCP connections to a target and injects the faults a Pair
// needs: an implementation's test points its dialer at Addr and sets
// Pair.BlackHole, Cut and Throttle to the Proxy's methods.
type Proxy struct {
	ln     net.Listener
	target string
	hole   chan struct{} // closed by BlackHole
	stop   chan struct{} // closed by the test's cleanup
	once   sync.Once
	rate   atomic.Int64 // bytes per second each way; 0: unthrottled

	mu    sync.Mutex
	conns []*net.TCPConn
}

// NewProxy listens on loopback and relays each connection to target; the
// test's cleanup closes it and every relayed connection.
func NewProxy(t testing.TB, target string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{ln: ln, target: target, hole: make(chan struct{}), stop: make(chan struct{})}
	go p.accept()
	t.Cleanup(func() {
		_ = ln.Close()
		close(p.stop)
		p.Cut()
	})
	return p
}

// Addr is where to dial the target through the Proxy.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// BlackHole silently partitions every relayed connection, now and later:
// nothing more crosses either way and nothing is closed (no FIN, no RST),
// so writers block once the kernel's buffers fill, as on a path a NAT or a
// firewall dropped.
func (p *Proxy) BlackHole() { p.once.Do(func() { close(p.hole) }) }

// Cut resets every relayed connection, both sides (RST).
func (p *Proxy) Cut() {
	p.mu.Lock()
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.SetLinger(0)
		_ = c.Close()
	}
}

// Throttle relays at most bytesPerSecond each way on every connection,
// with small send buffers, so a backlog waits in the endpoints rather
// than in the kernel. (Receive buffers keep their size: on Linux, one
// shrunk under an established connection stalls it on zero-window
// probes, which relayed a seventh of the rate.)
func (p *Proxy) Throttle(bytesPerSecond int) {
	p.rate.Store(int64(bytesPerSecond))
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		smallBuffers(c)
	}
}

func smallBuffers(c *net.TCPConn) {
	_ = c.SetWriteBuffer(16 << 10)
}

func (p *Proxy) accept() {
	for {
		nc, err := p.ln.Accept()
		if err != nil {
			return
		}
		tc, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = nc.Close()
			continue
		}
		a, b := nc.(*net.TCPConn), tc.(*net.TCPConn)
		if p.rate.Load() > 0 {
			smallBuffers(a)
			smallBuffers(b)
		}
		p.mu.Lock()
		p.conns = append(p.conns, a, b)
		p.mu.Unlock()
		var ended atomic.Int32 // both directions ended: close both
		go p.pipe(b, a, &ended)
		go p.pipe(a, b, &ended)
	}
}

// pipe relays src to dst. Once holed it parks: it neither reads nor
// closes anything.
func (p *Proxy) pipe(dst, src *net.TCPConn, ended *atomic.Int32) {
	buf := make([]byte, 16<<10)
	var due time.Time // when the bytes relayed so far are due at the rate
	for {
		n, err := src.Read(buf)
		select {
		case <-p.hole:
			<-p.stop
			return
		default:
		}
		if n > 0 {
			if r := p.rate.Load(); r > 0 {
				// Paced by schedule, not by a sleep per read: Linux hands
				// a small read per segment and each sleep overshoots, so
				// per-read sleeps relayed a fraction of the rate. A
				// relay that fell behind catches up, by 50 ms at most.
				now := time.Now()
				if floor := now.Add(-time.Second / 20); due.Before(floor) {
					due = floor
				}
				due = due.Add(time.Duration(n) * time.Second / time.Duration(r))
				time.Sleep(due.Sub(now))
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				err = werr
			}
		}
		if err == io.EOF {
			_ = dst.CloseWrite()
		} else if err != nil {
			_ = src.Close()
			_ = dst.Close()
		}
		if err != nil {
			if ended.Add(1) == 2 {
				_ = src.Close()
				_ = dst.Close()
			}
			return
		}
	}
}
