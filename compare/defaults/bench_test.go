// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package defaults

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Every benchmark runs the same code for each library (lib=, innermost, so
// the libraries interleave), over TCP loopback, each library with its
// defaults. Results and how to run: README.md.

const chunk = 64 << 10

// sink accepts streams and reads each to EOF with a 32 KiB buffer (no
// WriterTo shortcut for any library), counting bytes.
func sink(m mux, n *atomic.Int64, wg *sync.WaitGroup) {
	for {
		s, err := m.accept()
		if err != nil {
			return
		}
		go func() {
			defer wg.Done()
			defer s.Close() //nolint:errcheck
			buf := make([]byte, 32<<10)
			for {
				k, err := s.Read(buf)
				n.Add(int64(k))
				if err != nil {
					return
				}
			}
		}()
	}
}

// BenchmarkBulk: sessions streams on one connection write 64 KiB chunks
// one way; the op is one chunk delivered. rtt=40ms runs over delayConn.
func BenchmarkBulk(b *testing.B) {
	for _, c := range []struct {
		sessions int
		rtt      time.Duration
	}{{1, 0}, {64, 0}, {1, 40 * time.Millisecond}} {
		for _, l := range libs() {
			b.Run(fmt.Sprintf("sessions=%d/rtt=%s/lib=%s", c.sessions, c.rtt, l.name), func(b *testing.B) {
				x, y := tcpPair(b, c.rtt)
				m := l.pair(b, x, y)
				var got atomic.Int64
				var wg sync.WaitGroup
				wg.Add(c.sessions)
				go sink(m, &got, &wg)
				ss := make([]net.Conn, c.sessions)
				for i := range ss {
					s, err := m.open()
					if err != nil {
						b.Fatal(err)
					}
					ss[i] = s
				}
				msg := make([]byte, chunk)
				b.SetBytes(chunk)
				b.ReportAllocs()
				b.ResetTimer()
				var ww sync.WaitGroup
				for i, s := range ss {
					ww.Add(1)
					go func() {
						defer ww.Done()
						for range b.N/len(ss) + btoi(i < b.N%len(ss)) {
							if _, err := s.Write(msg); err != nil {
								b.Error(err)
								return
							}
						}
						_ = closeWrite(s)
					}()
				}
				ww.Wait()
				if !waitTimeout(&wg, 30*time.Second) {
					b.Fatalf("sink: %d of %d bytes after 30 s", got.Load(), int64(b.N)*chunk)
				}
				b.StopTimer()
				if got.Load() != int64(b.N)*chunk {
					b.Fatalf("sink got %d bytes, want %d", got.Load(), int64(b.N)*chunk)
				}
			})
		}
	}
}

func btoi(v bool) int {
	if v {
		return 1
	}
	return 0
}

func waitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// echo accepts streams and echoes each until EOF, then closes it.
func echo(m mux) {
	for {
		s, err := m.accept()
		if err != nil {
			return
		}
		go func() {
			defer s.Close() //nolint:errcheck
			buf := make([]byte, 32<<10)
			for {
				k, err := s.Read(buf)
				if k > 0 {
					if _, err := s.Write(buf[:k]); err != nil {
						return
					}
				}
				if err != nil {
					return
				}
			}
		}()
	}
}

// rawLib is no multiplexer: the TCP connection itself is the one stream
// (BenchmarkLatency's raw baseline).
var rawLib = lib{"tcp", func(tb testing.TB, x, y net.Conn) mux {
	acc := make(chan net.Conn, 1)
	acc <- y
	return mux{
		open:   func() (net.Conn, error) { return x, nil },
		accept: chanAccept(acc, tb.Context().Done()),
	}
}}

// BenchmarkLatency: a 64-byte echo on a held stream (held), and a new
// stream's open, 64-byte echo and close (fresh); p50 and p99. raw is the
// floor: the echo on the TCP connection itself.
func BenchmarkLatency(b *testing.B) {
	for _, mode := range []string{"raw", "held", "fresh"} {
		ls := libs()
		if mode == "raw" {
			ls = []lib{rawLib}
		}
		for _, l := range ls {
			b.Run(fmt.Sprintf("%s/lib=%s", mode, l.name), func(b *testing.B) {
				x, y := tcpPair(b, 0)
				m := l.pair(b, x, y)
				go echo(m)
				msg, got := make([]byte, 64), make([]byte, 64)
				var held net.Conn
				if mode != "fresh" {
					s, err := m.open()
					if err != nil {
						b.Fatal(err)
					}
					defer s.Close() //nolint:errcheck
					held = s
				}
				lat := make([]time.Duration, b.N)
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					t0 := time.Now()
					s := held
					if s == nil {
						o, err := m.open()
						if err != nil {
							b.Fatal(err)
						}
						s = o
					}
					if _, err := s.Write(msg); err != nil {
						b.Fatal(err)
					}
					if _, err := io.ReadFull(s, got); err != nil {
						b.Fatal(err)
					}
					if held == nil {
						_ = s.Close()
					}
					lat[i] = time.Since(t0)
				}
				b.StopTimer()
				slices.Sort(lat)
				b.ReportMetric(float64(lat[len(lat)/2].Nanoseconds()), "p50-ns")
				b.ReportMetric(float64(lat[len(lat)*99/100].Nanoseconds()), "p99-ns")
			})
		}
	}
}

// BenchmarkOpen: the open rate. Each op opens a stream, writes one byte
// and closes it without waiting; the listener accepts, reads it to EOF
// and closes. The timer stops when the listener has seen every stream end,
// or once none ended for 5 s: lost/op counts the streams that never
// reached it (refused by the listener, e.g. muxado's full accept backlog,
// after the opener had already closed).
func BenchmarkOpen(b *testing.B) {
	for _, l := range libs() {
		b.Run("lib="+l.name, func(b *testing.B) {
			x, y := tcpPair(b, 0)
			m := l.pair(b, x, y)
			var got, ended atomic.Int64
			var wg sync.WaitGroup
			wg.Add(b.N)
			go sink(m, &got, &wg)
			go func() { wg.Wait(); ended.Store(int64(b.N)) }()
			one := []byte{1}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				s, err := m.open()
				if err != nil {
					b.Fatal(err)
				}
				if _, err := s.Write(one); err != nil {
					b.Fatal(err)
				}
				_ = s.Close()
			}
			last, quiet := got.Load(), time.Now()
			for ended.Load() < int64(b.N) && time.Since(quiet) < 5*time.Second {
				time.Sleep(time.Millisecond)
				if v := got.Load(); v != last {
					last, quiet = v, time.Now()
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(int64(b.N)-got.Load())/float64(b.N), "lost/op")
		})
	}
}

// BenchmarkIdleMemory: N streams opened, one byte each way through each,
// then left idle; the heap (both ends: one process) and goroutines they
// hold, per stream. Run it with -benchtime 1x.
func BenchmarkIdleMemory(b *testing.B) {
	const n = 1000
	for _, l := range libs() {
		b.Run(fmt.Sprintf("n=%d/lib=%s", n, l.name), func(b *testing.B) {
			var heap, gor float64
			for range b.N {
				h, g := idleCost(b, l, n)
				heap += h
				gor += g
			}
			b.ReportMetric(heap/float64(b.N), "heap-B/session")
			b.ReportMetric(gor/float64(b.N), "goroutines/session")
		})
	}
}

func idleCost(b *testing.B, l lib, n int) (heapPer, gorPer float64) {
	x, y := tcpPair(b, 0)
	m := l.pair(b, x, y)
	acc := make(chan net.Conn, n)
	go func() {
		for {
			s, err := m.accept()
			if err != nil {
				return
			}
			acc <- s
		}
	}()
	h0, g0 := heapNow()
	held := make([]net.Conn, 0, 2*n)
	one := make([]byte, 1)
	for range n {
		s, err := m.open()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := s.Write(one); err != nil {
			b.Fatal(err)
		}
		t := <-acc
		if _, err := io.ReadFull(t, one); err != nil {
			b.Fatal(err)
		}
		if _, err := t.Write(one); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(s, one); err != nil {
			b.Fatal(err)
		}
		held = append(held, s, t)
	}
	h1, g1 := heapNow()
	for _, s := range held {
		_ = s.Close()
	}
	return float64(h1-h0) / float64(n), float64(g1-g0) / float64(n)
}

func heapNow() (uint64, int) {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc, runtime.NumGoroutine()
}
