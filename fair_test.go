// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowConn writes at rate bytes per second: a slow, intact path.
type slowConn struct {
	net.Conn
	rate int
}

func (c slowConn) Write(b []byte) (int, error) {
	for p := b; len(p) > 0; {
		n := min(len(p), 16<<10)
		time.Sleep(time.Duration(n) * time.Second / time.Duration(c.rate))
		if _, err := c.Conn.Write(p[:n]); err != nil {
			return len(b) - len(p), err
		}
		p = p[n:]
	}
	return len(b), nil
}

// Sessions sharing a slow connection each keep moving: a busy session does
// not fill buffer after buffer while another waits, and the sender that
// writes a buffer is not kept writing the others' frames for as long as
// they keep coming (fault rig, 50 ms / 2 % loss between two relay
// replicas: one of 8 sessions moved 112 KiB and then nothing for 30 s
// while the others moved megabytes).
func TestSessionsTakeTurnsOnASlowConn(t *testing.T) {
	const n, rate, run = 8, 512 << 10, 6 * time.Second
	a, b := tcpPair(t)
	cfg := Config{Window: 1 << 20, Auth: testToken}
	type res struct {
		c   *Conn
		err error
	}
	got := make(chan res, 1)
	go func() {
		c, err := Client(context.Background(), slowConn{Conn: a, rate: rate}, cfg)
		got <- res{c, err}
	}()
	sc, _, err := Server(context.Background(), b, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := <-got
	if r.err != nil {
		t.Fatal(r.err)
	}
	var moved [n]atomic.Int64
	var last [n]atomic.Int64
	for i := range n {
		last[i].Store(time.Now().UnixNano())
	}
	dm, lm := NewMux(r.c), NewMux(sc)
	t.Cleanup(func() { _ = dm.Close(); _ = lm.Close() })
	go func() {
		_ = lm.Serve(func(h []byte, s *Session) {
			i := int(h[0])
			buf := make([]byte, 64<<10)
			for {
				k, err := s.Read(buf)
				moved[i].Add(int64(k))
				last[i].Store(time.Now().UnixNano())
				if err != nil {
					return
				}
			}
		})
	}()
	go func() { _ = dm.Serve(func([]byte, *Session) {}) }()
	start := time.Now()
	var wg sync.WaitGroup
	for i := range n {
		s, err := dm.Open([]byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.Close() //nolint:errcheck
			chunk := make([]byte, 32<<10)
			for time.Since(start) < run {
				if _, err := s.Write(chunk); err != nil {
					return
				}
			}
		}()
	}
	var worst time.Duration
	for time.Since(start) < run {
		time.Sleep(100 * time.Millisecond)
		for i := range n {
			worst = max(worst, time.Since(time.Unix(0, last[i].Load())))
		}
	}
	_ = r.c.Close()
	wg.Wait()
	var total, least int64 = 0, 1 << 62
	var per []int64
	for i := range n {
		m := moved[i].Load()
		total += m
		least = min(least, m)
		per = append(per, m>>10)
	}
	t.Logf("KiB per session in %v at %d KiB/s: %v; longest stall %v", run, rate>>10, per, worst.Round(time.Millisecond))
	if least < total/int64(4*n) || worst > 5*time.Second {
		t.Fatalf("a session fell behind: KiB per session %v (fair share %d), longest stall %v", per, total/int64(n)>>10, worst.Round(time.Millisecond))
	}
}

// A session waiting for its turn (its data is in the buffer) keeps its
// place in line once a write takes the buffer: a sender that arrives while
// the waiter is woken but not yet running (a busy CPU) goes after it, not
// before. Without the place, senders that never slept filled buffer after
// buffer while one woken session waited to run, and a network with room
// took them all: on a 1 MiB/s path under load, one of 8 sessions moved
// nothing for 2.4 s (peerlinktest Fairness).
func TestTurnsKeepTheirPlace(t *testing.T) {
	nc, _ := net.Pipe()
	c := newTestConn(nc, DefaultWindow, DefaultWindow)
	t.Cleanup(func() { _ = nc.Close() })
	waiting := func(n int) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
			c.wmu.Lock()
			w := len(c.waiters)
			c.wmu.Unlock()
			if w == n {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d senders waiting, want %d", w, n)
			}
		}
	}
	// A write is in flight (writing): the buffer holds session 1's frame
	// and has no room for session 3's, which waits for room.
	c.wmu.Lock()
	c.writing = true
	if err := c.frameLocked(typeData, "1", make([]byte, 200<<10), nil); err != nil {
		t.Fatal(err)
	}
	c.wmu.Unlock()
	var sent sync.WaitGroup
	send := func(sid string, n int) {
		sent.Go(func() {
			if err := c.SendData(sid, make([]byte, n)); err != nil {
				t.Error(err)
			}
		})
	}
	send("3", 100<<10)
	waiting(1)
	send("1", 1<<10) // its data is in the buffer: it waits for its turn
	waiting(2)
	// The write takes the buffer and wakes the waiters; before either
	// runs, session 2 sends.
	c.wmu.Lock()
	c.takeLocked()
	err := c.frameLocked(typeData, "2", make([]byte, 1<<10), nil)
	var order []string
	for b := *c.wbuf; len(b) > 0; {
		n := int(binary.BigEndian.Uint32(b))
		order = append(order, string(b[6:6+int(b[5])]))
		b = b[5+n:]
	}
	c.wbuf, c.writing = nil, false
	c.wmu.Unlock()
	sent.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(order, ","); got != "3,1,2" {
		t.Fatalf("frames in the next buffer from sessions %s, want 3,1,2: session 1 waited for its turn and lost its place", got)
	}
}
