// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
)

// Benchmarks of paths of joined sessions over TCP loopback, in process:
// hops is the number of Forward hops (0: the origin talks to the far end
// directly). Results: README.md, "Chaining".

var benchHops = []int{0, 1, 2, 4}

// farSink: the far end accepts and reads to the end, then closes.
func farSink(_ []byte, s *Session) {
	_ = s.Accept()
	s.Pump(io.Discard, func(int64, error) { _ = s.Close() })
}

// BenchmarkChainThroughput: MB/s origin -> far end, 8 MiB per session,
// with 1 and 32 sessions at once.
func BenchmarkChainThroughput(b *testing.B) {
	for _, hops := range benchHops {
		for _, sessions := range []int{1, 32} {
			b.Run(fmt.Sprintf("hops=%d/sessions=%d", hops, sessions), func(b *testing.B) {
				r := newChainRig(b, rigOpts{n: hops + 1, window: DefaultWindow}, farSink)
				const per = 8 << 20
				buf := make([]byte, 256<<10)
				b.SetBytes(int64(per * sessions))
				b.ResetTimer()
				for range b.N {
					var wg sync.WaitGroup
					for range sessions {
						wg.Add(1)
						go func() {
							defer wg.Done()
							s := r.open("sink")
							for sent := 0; sent < per; sent += len(buf) {
								if _, err := s.Write(buf); err != nil {
									b.Error(err)
									return
								}
							}
							_ = s.CloseWrite()
							_, _ = io.Copy(io.Discard, s) // the far end's close: every byte arrived
						}()
					}
					wg.Wait()
				}
			})
		}
	}
}

// BenchmarkChainLatency: a 64-byte echo round trip on an open path.
func BenchmarkChainLatency(b *testing.B) {
	for _, hops := range benchHops {
		b.Run(fmt.Sprintf("hops=%d", hops), func(b *testing.B) {
			r := newChainRig(b, rigOpts{n: hops + 1}, farEcho)
			s := r.open("echo")
			msg, got := make([]byte, 64), make([]byte, 64)
			b.ResetTimer()
			for range b.N {
				if _, err := s.Write(msg); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(s, got); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			_ = s.Close()
		})
	}
}

// BenchmarkChainOpen: OpenChain -> Answered (the far end accepted).
func BenchmarkChainOpen(b *testing.B) {
	for _, hops := range benchHops {
		b.Run(fmt.Sprintf("hops=%d", hops), func(b *testing.B) {
			r := newChainRig(b, rigOpts{n: hops + 1}, func(_ []byte, s *Session) { _ = s.Accept(); _ = s.Close() })
			b.ResetTimer()
			for range b.N {
				s := r.open("open")
				if err := s.Answered(context.Background()); err != nil {
					b.Fatal(err)
				}
				_ = s.Close()
			}
		})
	}
}
