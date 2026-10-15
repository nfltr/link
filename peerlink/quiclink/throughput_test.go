// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"container/heap"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link/peerlink/quiclink"
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
	pc := listenUDP(t)
	addr := reorderingPath(t, pc.LocalAddr().String(), 10*time.Millisecond, 100, 2*time.Millisecond)
	const window = 16 << 20
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Window: window})
	a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", addr), Connect: []string{"b"}, Window: window})
	var got atomic.Int64
	go func() {
		_ = a.Serve(func(_ string, _ []byte, s net.Conn) {
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
	go func() { _ = b.Serve(func(string, []byte, net.Conn) {}) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = a.Drain(ctx)
		_ = b.Drain(ctx)
	})
	// b opens on the connection a dialled and sends, as a relay does
	// toward its agent.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := b.Open(ctx, "a", []byte("tput"))
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	go func() {
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
	defer close(stop)
	time.Sleep(2 * time.Second) // slow start: a few round trips here, more on a loaded CI runner
	from := got.Load()
	const secs = 4
	time.Sleep(secs * time.Second)
	rate := float64(got.Load()-from) / secs / 1e6
	t.Logf("%.1f MB/s at 20 ms RTT, every 100th datagram 2 ms late", rate)
	// quic-go v0.63.0 moves 1.2-1.4 MB/s here, v0.63.1-0.20261014084224
	// 23-25 under -race on 2 CPUs. The path is 20 ms, not the 200 ms of an
	// agent's: a shared CI runner sometimes stalls the relay goroutines long
	// enough for a real loss, and at 200 ms Reno regrows one packet per
	// 200 ms, which read as 0.2 MB/s over this window (CI, 2026-10-15) with
	// the fix in place. The reordering bug cuts the window at any RTT.
	if rate < 4 {
		t.Fatalf("%.2f MB/s, want at least 4", rate)
	}
}

// reorderingPath relays datagrams to target and back, each after delay;
// every nth datagram each way waits late longer, so the next ones overtake
// it. Its listener closes with the test.
func reorderingPath(t *testing.T, target string, delay time.Duration, nth int, late time.Duration) string {
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
	toTarget := newDelayLine(delay, nth, late, func(b []byte) { _, _ = up.Write(b) })
	back := newDelayLine(delay, nth, late, func(b []byte) {
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
	send        func([]byte)

	mu     sync.Mutex
	q      dueQueue
	wake   chan struct{}
	closed bool
}

func newDelayLine(delay time.Duration, nth int, late time.Duration, send func([]byte)) *delayLine {
	l := &delayLine{delay: delay, late: late, nth: nth, send: send, wake: make(chan struct{}, 1)}
	go l.run()
	return l
}

func (l *delayLine) add(b []byte) {
	l.mu.Lock()
	l.n++
	due := time.Now().Add(l.delay)
	if l.n%l.nth == 0 {
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
