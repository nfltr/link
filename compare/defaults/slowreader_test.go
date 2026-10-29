// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package defaults

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkSlowReader: streams A and B on one connection, both written
// as fast as the library takes it. After warm, the listener stops reading
// A for stall, then reads it again. awriter=stops: A's writer stops when
// A's reader does; awriter=keeps: it keeps writing (and blocks once A's
// window is full). Reported:
//   - A-MB/s-before, B-MB/s-before, B-MB/s-stall: A's and B's rates
//     before A's stall, B's during it;
//   - B-gap-ms: B's longest stretch without a byte during the stall
//     (sampled every 5 ms);
//   - B-stalled: 1 when B moved nothing during the stall;
//   - A-resumed: 1 when A delivered bytes again within 5 s of its reader
//     resuming (awriter=keeps only: with stops nothing is left to send);
//   - stuck: 1 when a writer was still blocked 5 s after teardown began.
//
// Every wait is bounded; nothing here can hang. Run it with -benchtime 1x.
func BenchmarkSlowReader(b *testing.B) {
	for _, stall := range []time.Duration{time.Second, 5 * time.Second} {
		for _, aw := range []string{"stops", "keeps"} {
			for _, l := range libs() {
				b.Run(fmt.Sprintf("stall=%s/awriter=%s/lib=%s", stall, aw, l.name), func(b *testing.B) {
					var r slowResult
					for range b.N {
						r.add(slowReader(b, l, stall, aw == "keeps"))
					}
					r.report(b, float64(b.N))
				})
			}
		}
	}
}

type slowResult struct {
	aBefore, before, during, gapMS, stalled, resumed, stuck float64
}

func (r *slowResult) add(o slowResult) {
	r.aBefore += o.aBefore
	r.before += o.before
	r.during += o.during
	r.gapMS += o.gapMS
	r.stalled += o.stalled
	r.resumed += o.resumed
	r.stuck += o.stuck
}

func (r *slowResult) report(b *testing.B, n float64) {
	b.ReportMetric(0, "ns/op")
	b.ReportMetric(r.aBefore/n, "A-MB/s-before")
	b.ReportMetric(r.before/n, "B-MB/s-before")
	b.ReportMetric(r.during/n, "B-MB/s-stall")
	b.ReportMetric(r.gapMS/n, "B-gap-ms")
	b.ReportMetric(r.stalled/n, "B-stalled")
	b.ReportMetric(r.resumed/n, "A-resumed")
	b.ReportMetric(r.stuck/n, "stuck")
}

const (
	warm   = 500 * time.Millisecond
	tick   = 5 * time.Millisecond
	bounds = 5 * time.Second
)

func slowReader(b *testing.B, l lib, stall time.Duration, keeps bool) slowResult {
	x, y := tcpPair(b, 0)
	m := l.pair(b, x, y)
	var (
		gotA, gotB atomic.Int64
		stalled    atomic.Bool // A's reader stopped
		stop       atomic.Bool // writers: end
		resume     = make(chan struct{})
		writers    sync.WaitGroup
	)

	// Listener: the first byte names the stream.
	go func() {
		for range 2 {
			s, err := m.accept()
			if err != nil {
				return
			}
			go func() {
				buf := make([]byte, 32<<10)
				if _, err := s.Read(buf[:1]); err != nil {
					return
				}
				isA := buf[0] == 'A'
				n := &gotB
				if isA {
					n = &gotA
				}
				for {
					if isA && stalled.Load() {
						<-resume
					}
					k, err := s.Read(buf)
					n.Add(int64(k))
					if err != nil {
						return
					}
				}
			}()
		}
	}()

	open := func(name byte) net.Conn {
		s, err := m.open()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := s.Write([]byte{name}); err != nil {
			b.Fatal(err)
		}
		return s
	}
	sa, sb := open('A'), open('B')
	write := func(s net.Conn, isA bool) {
		defer writers.Done()
		msg := make([]byte, 32<<10)
		for !stop.Load() {
			if isA && !keeps && stalled.Load() {
				return
			}
			if _, err := s.Write(msg); err != nil {
				return
			}
		}
	}
	writers.Add(2)
	go write(sa, true)
	go write(sb, false)

	time.Sleep(warm / 2)
	b0, a0 := gotB.Load(), gotA.Load()
	time.Sleep(warm / 2)
	b1, a1 := gotB.Load(), gotA.Load()
	stalled.Store(true)

	// During the stall: B's bytes, sampled every tick for its longest gap.
	var gap, longest time.Duration
	last := b1
	end := time.Now().Add(stall)
	for time.Now().Before(end) {
		time.Sleep(tick)
		if v := gotB.Load(); v == last {
			gap += tick
			longest = max(longest, gap)
		} else {
			last, gap = v, 0
		}
	}
	b2 := gotB.Load()

	aStall := gotA.Load()
	close(resume)
	var r slowResult
	if keeps {
		deadline := time.Now().Add(bounds)
		for time.Now().Before(deadline) && gotA.Load() == aStall {
			time.Sleep(tick)
		}
		r.resumed = float64(btoi(gotA.Load() > aStall))
	}

	// Teardown: writers end on stop; a writer blocked in Write ends when
	// the connection closes. Bounded either way.
	stop.Store(true)
	done := make(chan struct{})
	go func() { writers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(bounds):
		_ = x.Close()
		_ = y.Close()
		select {
		case <-done:
		case <-time.After(bounds):
			r.stuck = 1
		}
	}

	mb := func(n int64, d time.Duration) float64 { return float64(n) / d.Seconds() / 1e6 }
	r.aBefore = mb(a1-a0, warm/2)
	r.before = mb(b1-b0, warm/2)
	r.during = mb(b2-b1, stall)
	r.gapMS = float64(longest.Milliseconds())
	r.stalled = float64(btoi(b2 == b1))
	return r
}
