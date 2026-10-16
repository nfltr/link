// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Benchmarks of one connection's raw speed: throughput by message size
// and sessions, small-message latency, and the per-message cost (writes on
// the connection, frames, allocations). Results and how to run the A/B
// comparison: docs/performance.md.

var (
	perfSizes      = []int{64, 1 << 10, 4 << 10, 64 << 10, 1 << 20}
	perfTransports = []string{"tcp", "tls"}
)

// perfConn counts the Write and Read calls on a net.Conn and, once on,
// the frames written through it (the stream after the handshake).
type perfConn struct {
	net.Conn
	writes, reads atomic.Int64
	on            atomic.Bool

	mu                 sync.Mutex
	hdr                [5]byte
	nh, rest           int
	frames, dataFrames int64
}

func (c *perfConn) Read(b []byte) (int, error) {
	c.reads.Add(1)
	return c.Conn.Read(b)
}

func (c *perfConn) Write(b []byte) (int, error) {
	c.writes.Add(1)
	if c.on.Load() {
		c.mu.Lock()
		c.feed(b)
		c.mu.Unlock()
	}
	return c.Conn.Write(b)
}

func (c *perfConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return c.Conn.Close()
}

// feed counts the frames that begin in p.
func (c *perfConn) feed(p []byte) {
	for len(p) > 0 {
		if c.rest > 0 {
			k := min(c.rest, len(p))
			c.rest -= k
			p = p[k:]
			continue
		}
		k := copy(c.hdr[c.nh:], p)
		c.nh += k
		p = p[k:]
		if c.nh == len(c.hdr) {
			c.rest = int(binary.BigEndian.Uint32(c.hdr[:4]))
			c.frames++
			if c.hdr[4] == typeData {
				c.dataFrames++
			}
			c.nh = 0
		}
	}
}

// counts is a snapshot of a perfConn.
type counts struct{ writes, reads, frames, dataFrames int64 }

func (c *perfConn) snap() counts {
	c.mu.Lock()
	defer c.mu.Unlock()
	return counts{c.writes.Load(), c.reads.Load(), c.frames, c.dataFrames}
}

func (a counts) sub(b counts) counts {
	return counts{a.writes - b.writes, a.reads - b.reads, a.frames - b.frames, a.dataFrames - b.dataFrames}
}

// perfRig is two Muxes on one connection: a dialed, b listened; la and lb
// are the connections the Conns hold (the tls.Conn over TLS), na and nb
// the sockets below.
type perfRig struct {
	a, b           *Mux
	la, lb, na, nb *perfConn
}

func newPerfRig(tb testing.TB, transport string, accept func([]byte, *Session)) *perfRig {
	tb.Helper()
	var x, y net.Conn
	if transport == "pipe" {
		x, y = net.Pipe()
		tb.Cleanup(func() { _ = x.Close(); _ = y.Close() })
	} else {
		x, y = tcpPair(tb)
	}
	r := &perfRig{na: &perfConn{Conn: x}, nb: &perfConn{Conn: y}}
	x, y = r.na, r.nb
	if transport == "tls" {
		stls, ctls := selfSigned(tb)
		x, y = tls.Client(x, ctls), tls.Server(y, stls)
	}
	r.la, r.lb = &perfConn{Conn: x}, &perfConn{Conn: y}
	if transport != "tls" {
		r.la, r.lb = r.na, r.nb
	}
	cfg := Config{Auth: testToken}
	sc := make(chan *Conn, 1)
	go func() {
		c, _, err := Server(context.Background(), r.lb, cfg)
		if err != nil {
			tb.Error(err)
		}
		sc <- c
	}()
	ca, err := Client(context.Background(), r.la, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	cb := <-sc
	if cb == nil {
		tb.FailNow()
	}
	r.la.on.Store(true)
	r.lb.on.Store(true)
	r.a, r.b = NewMux(ca), NewMux(cb)
	go func() { _ = r.a.Serve(accept) }()
	go func() { _ = r.b.Serve(accept) }()
	tb.Cleanup(func() { _ = r.a.Close(); _ = r.b.Close() })
	tb.Cleanup(func() { goroutineProfiles(tb) }) // first: the rig still runs
	return r
}

// goroutineProfiles writes the goroutine profile (debug=2) and, in a
// toolchain that has it (Go 1.27), the goroutine leak
// profile to $LINK_PERF_PROFILE_DIR (scripts/perf.sh profile), one file
// each per benchmark (its last run).
func goroutineProfiles(tb testing.TB) {
	dir := os.Getenv("LINK_PERF_PROFILE_DIR")
	if dir == "" {
		return
	}
	name := strings.NewReplacer("/", "_", "=", "-").Replace(tb.Name())
	for _, p := range []string{"goroutine", "goroutineleak"} {
		prof := pprof.Lookup(p)
		if prof == nil {
			continue
		}
		f, err := os.Create(filepath.Join(dir, name+"."+p+".txt")) // #nosec G304 -- the operator's directory
		if err != nil {
			tb.Error(err)
			return
		}
		_ = prof.WriteTo(f, 2)
		_ = f.Close()
	}
}

// perfSink reads a session to its end with a 32 KiB buffer, then closes it.
func perfSink(_ []byte, s *Session) {
	buf := make([]byte, 32<<10)
	for {
		if _, err := s.Read(buf); err != nil {
			break
		}
	}
	_ = s.Close()
}

// perfEcho accepts the session, then echoes what it reads until the
// session ends.
func perfEcho(_ []byte, s *Session) {
	_ = s.Accept()
	buf := make([]byte, 32<<10)
	for {
		n, err := s.Read(buf)
		if n > 0 {
			if _, err := s.Write(buf[:n]); err != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = s.Close()
}

// send writes n messages of msg to s, ends its write side and waits for
// the sink's close: every byte arrived.
func send(tb testing.TB, s *Session, msg []byte, n int) {
	for range n {
		if _, err := s.Write(msg); err != nil {
			tb.Error(err)
			return
		}
	}
	_ = s.CloseWrite()
	_, _ = io.Copy(io.Discard, s)
	_ = s.Close()
}

// reportPerMsg reports the per-message costs: on the sender's connection
// (writes, frames, data frames) and the receiver's (its writes: window
// updates; its reads).
func reportPerMsg(b *testing.B, tx, rx counts, msgs int, sock *counts) {
	m := float64(msgs)
	b.ReportMetric(float64(tx.writes)/m, "writes/msg")
	b.ReportMetric(float64(tx.frames)/m, "frames/msg")
	b.ReportMetric(float64(rx.reads)/m, "rx-reads/msg")
	b.ReportMetric(float64(rx.writes)/m, "rx-writes/msg")
	if sock != nil {
		b.ReportMetric(float64(sock.writes)/m, "sock-writes/msg")
	}
}

// BenchmarkThroughput: MB/s of messages of each size over one connection,
// with 1 and 8 sessions; oneway from the dialer, or both ways at once
// (each side opens the sessions and writes, so an op is a message each
// way). pipe (net.Pipe) is an extra, at 1 session one way.
func BenchmarkThroughput(b *testing.B) {
	for _, tr := range append(slices.Clone(perfTransports), "pipe") {
		for _, size := range perfSizes {
			for _, sessions := range []int{1, 8} {
				for _, dir := range []string{"oneway", "both"} {
					if tr == "pipe" && (sessions != 1 || dir != "oneway") {
						continue
					}
					b.Run(fmt.Sprintf("tr=%s/size=%d/sessions=%d/dir=%s", tr, size, sessions, dir), func(b *testing.B) {
						benchThroughput(b, tr, size, sessions, dir == "both")
					})
				}
			}
		}
	}
}

func benchThroughput(b *testing.B, tr string, size, sessions int, both bool) {
	r := newPerfRig(b, tr, perfSink)
	msg := make([]byte, size)
	sides := []*Mux{r.a}
	if both {
		sides = append(sides, r.b)
	}
	type job struct {
		s *Session
		n int
	}
	var jobs []job
	for _, m := range sides {
		for i := range sessions {
			s, err := m.Open([]byte("sink"))
			if err != nil {
				b.Fatal(err)
			}
			n := b.N / sessions
			if i < b.N%sessions {
				n++
			}
			jobs = append(jobs, job{s, n})
		}
	}
	b.SetBytes(int64(size * len(sides)))
	b.ReportAllocs()
	tx0, rx0, sock0 := r.la.snap(), r.lb.snap(), r.na.snap()
	b.ResetTimer()
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Go(func() { send(b, j.s, msg, j.n) })
	}
	wg.Wait()
	b.StopTimer()
	if !both {
		var sock *counts
		if tr == "tls" {
			s := r.na.snap().sub(sock0)
			sock = &s
		}
		reportPerMsg(b, r.la.snap().sub(tx0), r.lb.snap().sub(rx0), b.N, sock)
	}
}

// BenchmarkLatency: a 64-byte echo round trip on a held session, and a
// fresh session's open, echo and close (answered: waiting for the far
// end's answer, Answered, before the echo), with p50 and p99.
func BenchmarkLatency(b *testing.B) {
	for _, tr := range perfTransports {
		for _, mode := range []string{"held", "fresh", "fresh-answered"} {
			b.Run(fmt.Sprintf("tr=%s/%s", tr, mode), func(b *testing.B) {
				r := newPerfRig(b, tr, perfEcho)
				msg, got := make([]byte, 64), make([]byte, 64)
				var held *Session
				if mode == "held" {
					var err error
					if held, err = r.a.Open([]byte("echo")); err != nil {
						b.Fatal(err)
					}
					defer held.Close() //nolint:errcheck
				}
				lat := make([]time.Duration, b.N)
				tx0, rx0 := r.la.snap(), r.lb.snap()
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					t0 := time.Now()
					s := held
					if s == nil {
						var err error
						if s, err = r.a.Open([]byte("echo")); err != nil {
							b.Fatal(err)
						}
						if mode == "fresh-answered" {
							if err := s.Answered(context.Background()); err != nil {
								b.Fatal(err)
							}
						}
					}
					if _, err := s.Write(msg); err != nil {
						b.Fatal(err)
					}
					if _, err := io.ReadFull(s, got); err != nil {
						b.Fatal(err)
					}
					if s != held {
						_ = s.Close()
					}
					lat[i] = time.Since(t0)
				}
				b.StopTimer()
				slices.Sort(lat)
				b.ReportMetric(float64(lat[len(lat)/2].Nanoseconds()), "p50-ns")
				b.ReportMetric(float64(lat[len(lat)*99/100].Nanoseconds()), "p99-ns")
				if mode == "held" {
					tx, rx := r.la.snap().sub(tx0), r.lb.snap().sub(rx0)
					b.ReportMetric(float64(tx.writes)/float64(b.N), "writes/msg")
					b.ReportMetric(float64(rx.reads)/float64(b.N), "rx-reads/msg")
				}
			})
		}
	}
}

// BenchmarkIdleSessions: 8 busy sessions moving 4 KiB messages beside 0
// or 1,000 idle ones on the same connection: idle sessions cost nothing on
// the hot path (and hold no goroutine: goroutines/idle).
func BenchmarkIdleSessions(b *testing.B) {
	for _, idle := range []int{0, 1000} {
		b.Run(fmt.Sprintf("idle=%d", idle), func(b *testing.B) {
			r := newPerfRig(b, "tcp", perfEcho)
			runtime.GC()
			g0 := runtime.NumGoroutine()
			x := make([]byte, 1)
			for range idle {
				s, err := r.a.Open([]byte("idle"))
				if err != nil {
					b.Fatal(err)
				}
				defer s.Close() //nolint:errcheck
				// Through and back, then idle: its accepter waits in Read.
				if _, err := s.Write(x); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(s, x); err != nil {
					b.Fatal(err)
				}
			}
			// perfEcho holds a goroutine per session (its Read): that is the
			// caller's; link's own are the rest.
			linkG := float64(runtime.NumGoroutine()-g0-idle) / float64(max(idle, 1))
			const sessions, size = 8, 4 << 10
			msg := make([]byte, size)
			ss := make([]*Session, sessions)
			for i := range ss {
				var err error
				if ss[i], err = r.a.Open([]byte("sink")); err != nil {
					b.Fatal(err)
				}
			}
			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			var wg sync.WaitGroup
			for i, s := range ss {
				n := b.N / sessions
				if i < b.N%sessions {
					n++
				}
				wg.Go(func() {
					buf := make([]byte, size)
					for range n { // an echo: both directions of the session move
						if _, err := s.Write(msg); err != nil {
							b.Error(err)
							return
						}
						if _, err := io.ReadFull(s, buf); err != nil {
							b.Error(err)
							return
						}
					}
				})
			}
			wg.Wait()
			b.StopTimer()
			if idle > 0 {
				b.ReportMetric(linkG, "link-goroutines/idle")
			}
		})
	}
}
