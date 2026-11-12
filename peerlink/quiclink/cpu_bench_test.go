// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package quiclink_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
)

// BenchmarkBulkCPU: one-way bulk transfer, 64 KiB writes, over one
// loopback connection between two Nodes in this process: link over TLS
// 1.3 on TCP (tr=tls) against quiclink (tr=quic), 1 and 8 sessions, as
// fast as the ends go (rate=max) or with the writers held to 20 MB/s in
// all (rate=20MB, an internet path's rate, where sends do not batch).
// The op is one 64 KiB write delivered; cpu-ms/MB is this process's user
// + system CPU (both ends together) per MB moved. How to run and the
// results: README.md "CPU per MB".
func BenchmarkBulkCPU(b *testing.B) {
	for _, rate := range []float64{0, 20e6} {
		for _, sessions := range []int{1, 8} {
			for _, tr := range []string{"tls", "quic"} {
				r := "max"
				if rate > 0 {
					r = fmt.Sprintf("%.0fMB", rate/1e6)
				}
				b.Run(fmt.Sprintf("rate=%s/sessions=%d/tr=%s", r, sessions, tr), func(b *testing.B) {
					benchBulkCPU(b, tr, sessions, rate)
				})
			}
		}
	}
}

func benchBulkCPU(b *testing.B, tr string, sessions int, rate float64) {
	var l pathLink
	if tr == "quic" {
		l = quicBenchPair(b)
	} else {
		l = tlsBenchPair(b)
	}
	var got atomic.Int64
	var done sync.WaitGroup
	done.Add(sessions)
	go func() {
		_ = l.b.Serve(func(_ string, _ []byte, s peerlink.Session) {
			defer done.Done()
			buf := make([]byte, 32<<10)
			for {
				k, err := s.Read(buf)
				got.Add(int64(k))
				if err != nil {
					return
				}
			}
		})
	}()
	go func() { _ = l.a.Serve(func(_ string, _ []byte, s peerlink.Session) { _ = s.Reset() }) }()
	b.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = l.a.Drain(ctx)
		_ = l.b.Drain(ctx)
	})
	ss := make([]peerlink.Session, sessions)
	for i := range ss {
		s, err := l.a.Open(context.Background(), "b", []byte("sink"))
		if err != nil {
			b.Fatal(err)
		}
		ss[i] = s
	}
	const chunk = 64 << 10
	msg := make([]byte, chunk)
	b.SetBytes(chunk)
	b.ReportAllocs()
	every := time.Duration(0) // between one session's writes
	if rate > 0 {
		every = time.Duration(float64(chunk*sessions) / rate * float64(time.Second))
	}
	cpu0 := cpuTime()
	b.ResetTimer()
	start := time.Now()
	var ww sync.WaitGroup
	for i, s := range ss {
		n := b.N / sessions
		if i < b.N%sessions {
			n++
		}
		ww.Go(func() {
			for k := range n {
				if every > 0 {
					time.Sleep(time.Until(start.Add(time.Duration(k) * every)))
				}
				if _, err := s.Write(msg); err != nil {
					b.Error(err)
					return
				}
			}
			_ = s.CloseWrite()
		})
	}
	ww.Wait()
	wait := make(chan struct{})
	go func() { done.Wait(); close(wait) }()
	select {
	case <-wait:
	case <-time.After(30 * time.Second):
		b.Fatalf("sink: %d of %d bytes after 30 s", got.Load(), int64(b.N)*chunk)
	}
	b.StopTimer()
	mb := float64(b.N) * chunk / 1e6
	b.ReportMetric(float64(cpuTime()-cpu0)/1e6/mb, "cpu-ms/MB")
}

// cpuTime is this process's user + system CPU time.
func cpuTime() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func quicBenchPair(tb testing.TB) pathLink {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = pc.Close() })
	bn := mustNew(tb, quiclink.Config{PacketConn: pc, TLS: serverTLS("b")})
	an := mustNew(tb, quiclink.Config{Dial: dialer("a", "b", pc.LocalAddr().String())})
	return pathLink{a: an, b: bn}
}

// tlsBenchPair is link over TLS 1.3 on TCP with the same certificates.
func tlsBenchPair(tb testing.TB) pathLink {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = ln.Close() })
	bn, err := peerlink.New(peerlink.Config{Listener: tls.NewListener(ln, serverTLS("b")), Link: link.Config{Auth: link.NameAuth("b")}})
	if err != nil {
		tb.Fatal(err)
	}
	an, err := peerlink.New(peerlink.Config{Link: link.Config{Auth: link.NameAuth("a")},
		Dial: func(ctx context.Context, _ string) (net.Conn, error) {
			d := tls.Dialer{Config: clientTLS("a", "b", "link")}
			return d.DialContext(ctx, "tcp", ln.Addr().String())
		}})
	if err != nil {
		tb.Fatal(err)
	}
	return pathLink{a: an, b: bn}
}
