// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"container/heap"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
	"github.com/nfltr/quic-go"
)

// TestThroughputReorderedPath: a session keeps its throughput on a path
// that reorders a few datagrams, as internet paths do. quic-go before
// its spurious-loss handling (v0.63.0) took each reordered packet for a
// loss and cut its congestion window for good: on this path a session
// fell to about 1 MB/s and kept falling, where TCP kept its rate.
func TestThroughputReorderedPath(t *testing.T) {
	if testing.Short() {
		t.Skip("moves data for seconds")
	}
	// The floor is the same path without the reordering, moving data in
	// the same window, so a loaded host slows both alike. Measured one
	// after the other, a load burst during the reordered run alone failed
	// 4 of 20 runs under test-under-load (lane QUICLINKLOAD: 0.7-0.9 MB/s
	// against 5-21 MB/s clean seconds before). quic-go v0.63.0 moves
	// 1.2-1.4 MB/s here and falling, v0.63.1-0.20261014084224 23-25 under
	// -race on 2 CPUs, as on the clean path. The path is 20 ms, not the
	// 200 ms of an agent's: a shared CI runner sometimes stalls the relay
	// goroutines long enough for a real loss, and at 200 ms Reno regrows
	// one packet per 200 ms (CI, 2026-10-15). The reordering bug cuts the
	// window at any RTT.
	gate := make(chan struct{})
	clean := startPath(t, gate, 10*time.Millisecond, 0, 0, 0)
	reordered := startPath(t, gate, 10*time.Millisecond, 100, 2*time.Millisecond, 0)
	close(gate)
	r := rates(2*time.Second, 4*time.Second, reordered, clean) // slow start: a few round trips, more on a loaded CI runner
	t.Logf("%.1f MB/s at 20 ms RTT, every 100th datagram 2 ms late; %.1f MB/s without, at the same time", r[0], r[1])
	if r[0] < r[1]/4 {
		t.Fatalf("%.2f MB/s, want at least a quarter of the clean path's %.2f", r[0], r[1])
	}
}

// startPath opens a session that b opens on the connection a dialled and
// sends on once gate closes, as a relay does toward its agent, over a
// reorderingPath (delay each way, every nth datagram late, rate); it
// returns the bytes a has read so far. Sessions whose rates a test
// compares share a gate, so none starts its slow start before another.
// It runs until the test ends.
func startPath(t *testing.T, gate <-chan struct{}, delay time.Duration, nth int, late time.Duration, rate float64) *atomic.Int64 {
	t.Helper()
	pc := listenUDP(t)
	addr := reorderingPath(t, pc.LocalAddr().String(), delay, nth, late, rate)
	const window = 16 << 20
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Window: window})
	a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", addr), Connect: []string{"b"}, Window: window})
	got := new(atomic.Int64)
	go func() {
		_ = a.Serve(func(_ string, _ []byte, s peerlink.Session) {
			buf := make([]byte, 64<<10)
			for {
				n, err := s.Read(buf)
				got.Add(int64(n))
				if err != nil {
					return
				}
			}
		})
	}()
	go func() { _ = b.Serve(func(string, []byte, peerlink.Session) {}) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = a.Drain(ctx)
		_ = b.Drain(ctx)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	s, err := b.Open(ctx, "a", []byte("tput"))
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		select {
		case <-gate:
		case <-stop:
			_ = s.Close()
			return
		}
		buf := make([]byte, 32<<10)
		for {
			select {
			case <-stop:
				_ = s.Close()
				return
			default:
			}
			if _, err := s.Write(buf); err != nil {
				return
			}
		}
	}()
	return got
}

// rates is each counter's rate (MB/s) over the same d, after warm.
func rates(warm, d time.Duration, counters ...*atomic.Int64) []float64 {
	time.Sleep(warm)
	from := make([]int64, len(counters))
	for i, c := range counters {
		from[i] = c.Load()
	}
	time.Sleep(d)
	r := make([]float64, len(counters))
	for i, c := range counters {
		r[i] = float64(c.Load()-from[i]) / d.Seconds() / 1e6
	}
	return r
}

// TestThroughputAfterBurstTheOtherWay: once b has sent a burst through a
// bottleneck, a's sessions toward b still get the path's rate: the burst
// leaves a's sender as a fresh connection's, and its window grows from
// there. a's RTT samples ride behind b's queue while a sends nothing but
// ACKs and flow control; quic-go before its fork's fix (HyStart only
// while the window is in use) took that queue for its own and left slow
// start at its initial window, then grew one packet per round trip:
// ~0.4 MB/s here. BBR that counted those app-limited samples toward full
// bandwidth leaves startup at their rate: an 8 KB window, 0.13 MB/s
// against 19.6 on a fresh path (lane QUICLINKBURST).
//
// The test judges a's congestion window after the burst, not a's rate
// against a fresh path's: on a starved host the rates are the CPU each
// flow got. Two identical fresh paths at the same time fell below half of
// each other in 3 of 10 runs under test-under-load on the Mac and 2 of 20
// on the box, whose loaded rates (0.2-0.9 MB/s) overlap the mutant's
// (lane QUICLINKBURST; measured one after the other, 4 of 20, lane
// QUICLINKLOAD). The window is what the burst's acks did, which a stall
// delays but does not change. The window after sending is logged only:
// on the loaded box it was 104 KB at 0.3 MB/s, the mutant's 48 KB at
// 0.2. The HyStart bug left the window as it was; Nodes use BBR
// (TestNodeUsesBBR), which has no HyStart.
func TestThroughputAfterBurstTheOtherWay(t *testing.T) {
	if testing.Short() {
		t.Skip("moves data for seconds")
	}
	pc := listenUDP(t)
	const rate = 20e6 // bytes per second each way
	addr := reorderingPath(t, pc.LocalAddr().String(), 50*time.Millisecond, 0, 0, rate)
	const window = 16 << 20
	var qc atomic.Pointer[quic.Conn] // the one connection a dials (Conns 0: one)
	dial := dialer("a", "b", addr)
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Window: window})
	a := mustNew(t, quiclink.Config{Dial: func(ctx context.Context, addr string, c *quic.Config) (*quic.Conn, error) {
		q, err := dial(ctx, addr, c)
		if err == nil {
			qc.Store(q)
		}
		return q, err
	}, Connect: []string{"b"}, Window: window})
	var got atomic.Int64
	burst := make(chan error, 1)
	go func() {
		_ = a.Serve(func(_ string, _ []byte, s peerlink.Session) {
			_, err := io.Copy(io.Discard, s)
			burst <- err
		})
	}()
	go func() {
		_ = b.Serve(func(_ string, _ []byte, s peerlink.Session) {
			buf := make([]byte, 64<<10)
			for {
				n, err := s.Read(buf)
				got.Add(int64(n))
				if err != nil {
					return
				}
			}
		})
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = a.Drain(ctx)
		_ = b.Drain(ctx)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := b.Open(ctx, "a", []byte("burst"))
	if err != nil {
		t.Fatal(err)
	}
	initial := qc.Load().ConnectionStats().CongestionWindow // a has sent nothing yet
	const burstBytes = 8 << 20
	start := time.Now()
	if _, err := s.Write(make([]byte, burstBytes)); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	select {
	case err := <-burst:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("burst not received")
	}
	burstRate := burstBytes / time.Since(start).Seconds() / 1e6
	after := qc.Load().ConnectionStats().CongestionWindow

	up, err := a.Open(ctx, "b", []byte("up"))
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	go func() {
		buf := make([]byte, 32<<10)
		for {
			select {
			case <-stop:
				_ = up.Close()
				return
			default:
			}
			if _, err := up.Write(buf); err != nil {
				return
			}
		}
	}()
	defer close(stop)
	r := rates(time.Second, 3*time.Second, &got) // slow start: ten round trips
	grown := qc.Load().ConnectionStats().CongestionWindow
	t.Logf("a's window %d B fresh, %d after b's burst (%.1f MB/s), %d after sending %.1f MB/s a to b; 100 ms RTT, bottleneck %.0f MB/s", initial, after, burstRate, grown, r[0], rate/1e6)
	if after < initial {
		t.Fatalf("a's window %d B after b's burst, want at least a fresh connection's %d", after, initial)
	}
}

// reorderingPath relays datagrams to target and back, each after delay;
// every nth datagram each way (none: 0) waits late longer, so the next ones
// overtake it. A rate (bytes per second each way, none: 0) makes the path a
// bottleneck with an unbounded queue. Its listener closes with the test.
func reorderingPath(t *testing.T, target string, delay time.Duration, nth int, late time.Duration, rate float64) string {
	t.Helper()
	ta, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		t.Fatal(err)
	}
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	up, err := net.DialUDP("udp", nil, ta)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close(); _ = up.Close() })
	for _, c := range []*net.UDPConn{pc, up} {
		_ = c.SetReadBuffer(8 << 20)
		_ = c.SetWriteBuffer(8 << 20)
	}
	var src atomic.Pointer[net.UDPAddr] // the one flow
	toTarget := newDelayLine(delay, nth, late, rate, func(b []byte) { _, _ = up.Write(b) })
	back := newDelayLine(delay, nth, late, rate, func(b []byte) {
		if a := src.Load(); a != nil {
			_, _ = pc.WriteToUDP(b, a)
		}
	})
	go func() {
		for {
			buf := make([]byte, 2048)
			n, a, err := pc.ReadFromUDP(buf)
			if err != nil {
				toTarget.close()
				return
			}
			src.Store(a)
			toTarget.add(buf[:n])
		}
	}()
	go func() {
		for {
			buf := make([]byte, 2048)
			n, err := up.Read(buf)
			if err != nil {
				back.close()
				return
			}
			back.add(buf[:n])
		}
	}()
	return pc.LocalAddr().String()
}

// delayLine sends each datagram at its due time, in due-time order.
type delayLine struct {
	delay, late time.Duration
	nth, n      int
	rate        float64   // bytes per second; 0: no limit
	free        time.Time // when the bottleneck has sent what it holds
	send        func([]byte)

	mu     sync.Mutex
	q      dueQueue
	wake   chan struct{}
	closed bool
}

func newDelayLine(delay time.Duration, nth int, late time.Duration, rate float64, send func([]byte)) *delayLine {
	l := &delayLine{delay: delay, late: late, nth: nth, rate: rate, send: send, wake: make(chan struct{}, 1)}
	go l.run()
	return l
}

func (l *delayLine) add(b []byte) {
	l.mu.Lock()
	l.n++
	now := time.Now()
	if l.rate > 0 {
		if l.free.Before(now) {
			l.free = now
		}
		l.free = l.free.Add(time.Duration(float64(len(b)) / l.rate * float64(time.Second)))
		now = l.free
	}
	due := now.Add(l.delay)
	if l.nth > 0 && l.n%l.nth == 0 {
		due = due.Add(l.late)
	}
	heap.Push(&l.q, dueDatagram{due: due, seq: l.n, b: b})
	l.mu.Unlock()
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (l *delayLine) close() {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (l *delayLine) run() {
	timer := time.NewTimer(time.Hour)
	for {
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return
		}
		var next *dueDatagram
		if len(l.q) > 0 {
			next = &l.q[0]
		}
		if next != nil && !time.Now().Before(next.due) {
			d := heap.Pop(&l.q).(dueDatagram)
			l.mu.Unlock()
			l.send(d.b)
			continue
		}
		wait := time.Hour
		if next != nil {
			wait = time.Until(next.due)
		}
		l.mu.Unlock()
		timer.Reset(wait)
		select {
		case <-timer.C:
		case <-l.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}

type dueDatagram struct {
	due time.Time
	seq int
	b   []byte
}

// dueQueue is a heap by due time, then arrival.
type dueQueue []dueDatagram

func (q dueQueue) Len() int { return len(q) }
func (q dueQueue) Less(i, j int) bool {
	if !q[i].due.Equal(q[j].due) {
		return q[i].due.Before(q[j].due)
	}
	return q[i].seq < q[j].seq
}
func (q dueQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *dueQueue) Push(x any)   { *q = append(*q, x.(dueDatagram)) }
func (q *dueQueue) Pop() any {
	old := *q
	d := old[len(old)-1]
	*q = old[:len(old)-1]
	return d
}
