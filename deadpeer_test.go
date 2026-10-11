// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// partition relays between a and the returned conn until cut; then it
// reads and forwards nothing more either way and closes nothing: a dead
// path (no FIN, no RST) on which writers block once the kernel's buffers
// fill.
func partition(t *testing.T, a net.Conn) (net.Conn, func()) {
	t.Helper()
	x, y := tcpPair(t)
	cut, stop := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(stop) })
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if err != nil {
				return
			}
			select {
			case <-cut:
				<-stop
				return
			default:
			}
			if _, err := dst.Write(buf[:n]); err != nil {
				return
			}
		}
	}
	go pipe(a, x)
	go pipe(x, a)
	var once sync.Once
	return y, func() { once.Do(func() { close(cut) }) }
}

// stallConn's writes block once cut, until it is closed: the network
// takes nothing more and the kernel's buffer is full.
type stallConn struct {
	net.Conn
	cut    <-chan struct{}
	closed chan struct{}
	once   sync.Once
}

func (c *stallConn) Write(b []byte) (int, error) {
	select {
	case <-c.cut:
		<-c.closed
		return 0, net.ErrClosed
	default:
		return c.Conn.Write(b)
	}
}

func (c *stallConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// livePair binds a dialer with liveness l to a listener (which takes the
// dialer's) across a partition proxy; once cut, both ends' writes block.
func livePair(t *testing.T, l Liveness, dobs, lobs Observer) (dialer, listener *Conn, cut func()) {
	t.Helper()
	a0, b := tcpPair(t)
	far0, pcut := partition(t, b)
	stalled := make(chan struct{})
	var once sync.Once
	cut = func() { once.Do(func() { close(stalled); pcut() }) }
	a := &stallConn{Conn: a0, cut: stalled, closed: make(chan struct{})}
	far := &stallConn{Conn: far0, cut: stalled, closed: make(chan struct{})}
	sc := make(chan *Conn, 1)
	go func() {
		c, _, err := Server(context.Background(), far, Config{Auth: testToken, Observer: lobs})
		if err != nil {
			t.Error(err)
		}
		sc <- c
	}()
	c, err := Client(context.Background(), a, Config{Liveness: l, Auth: testToken, Observer: dobs})
	if err != nil {
		t.Fatal(err)
	}
	s := <-sc
	if s == nil {
		t.FailNow()
	}
	t.Cleanup(func() { _ = c.Close(); _ = s.Close() })
	return c, s, cut
}

// waitDead waits for c to end with ErrDeadPeer and returns how long after
// start it did.
func waitDead(t *testing.T, who string, c *Conn, start time.Time, bound time.Duration) time.Duration {
	t.Helper()
	select {
	case <-c.Done():
	case <-time.After(time.Until(start.Add(3 * bound))):
		t.Fatalf("%s: dead peer not detected", who)
	}
	if !errors.Is(c.Err(), ErrDeadPeer) {
		t.Fatalf("%s: ended with %v", who, c.Err())
	}
	return time.Since(start)
}

// testPresets is fast and default; LINK_ALL_PRESETS=1 every preset.
func testPresets() []string {
	if os.Getenv("LINK_ALL_PRESETS") != "" {
		return LivenessPresets
	}
	return []string{"fast", "default"}
}

// deadPeerSlack is the scheduling slack over Interval + Timeout.
const deadPeerSlack = 300 * time.Millisecond

// pongCut runs its armed func at the next pong its side receives.
type pongCut struct {
	NopObserver
	arm atomic.Pointer[func()]
}

func (o *pongCut) PingRTT(time.Duration) {
	if f := o.arm.Swap(nil); f != nil {
		(*f)()
	}
}

// TestDeadPeerWorstCaseIdle: an idle connection's path dies right after
// one side's ping was answered (the latest that side hears anything):
// both ends end it within Interval + Timeout.
func TestDeadPeerWorstCaseIdle(t *testing.T) {
	for _, name := range testPresets() {
		for _, side := range []string{"dialer", "listener"} {
			t.Run(name+"/"+side, func(t *testing.T) {
				t.Parallel()
				l := livenessByName[name]
				do, lo := &pongCut{}, &pongCut{}
				c, s, cut := livePair(t, l, do, lo)
				go func() { _, _ = c.Recv() }()
				go func() { _, _ = s.Recv() }()
				started := make(chan time.Time, 1)
				f := func() { cut(); started <- time.Now() }
				if side == "dialer" {
					do.arm.Store(&f)
				} else {
					lo.arm.Store(&f)
				}
				var start time.Time
				select {
				case start = <-started:
				case <-time.After(3 * (l.Interval + l.Timeout)):
					t.Fatal("no pong")
				}
				bound := l.Interval + l.Timeout
				d := waitDead(t, "dialer", c, start, bound)
				ls := waitDead(t, "listener", s, start, bound)
				t.Logf("%s, cut after the %s's pong: dialer %v, listener %v, bound %v", name, side, d.Round(time.Millisecond), ls.Round(time.Millisecond), bound)
				if d > bound+deadPeerSlack || ls > bound+deadPeerSlack {
					t.Fatalf("detected after %v / %v, bound %v", d, ls, bound)
				}
			})
		}
	}
}

// TestDeadPeerBusy: sessions stream one way when the path dies; the
// sender's writes block (the network takes nothing more), and both ends
// still end the connection within Interval + Timeout.
func TestDeadPeerBusy(t *testing.T) {
	for _, name := range testPresets() {
		for _, dir := range []string{"down", "up"} { // down: the listener sends
			t.Run(name+"/"+dir, func(t *testing.T) {
				t.Parallel()
				l := livenessByName[name]
				c, s, cut := livePair(t, l, nil, nil)
				stream := func(s *Session) {
					b := make([]byte, 64<<10)
					for {
						if _, err := s.Write(b); err != nil {
							return
						}
					}
				}
				sink := func(s *Session) { _, _ = io.Copy(io.Discard, s) }
				dm, lm := NewMux(c, nil), NewMux(s, nil)
				go func() { _ = dm.Serve(nil) }()
				go func() {
					_ = lm.Serve(func(_ []byte, s *Session) {
						if dir == "down" {
							stream(s)
						} else {
							sink(s)
						}
					})
				}()
				for range 4 {
					ss, err := openV2(dm, "x")
					if err != nil {
						t.Fatal(err)
					}
					if dir == "down" {
						go sink(ss)
					} else {
						go stream(ss)
					}
				}
				time.Sleep(300 * time.Millisecond)
				cut()
				start := time.Now()
				bound := l.Interval + l.Timeout
				d := waitDead(t, "dialer", c, start, bound)
				ls := waitDead(t, "listener", s, start, bound)
				t.Logf("%s, %s: dialer %v, listener %v, bound %v", name, dir, d.Round(time.Millisecond), ls.Round(time.Millisecond), bound)
				if d > bound+deadPeerSlack || ls > bound+deadPeerSlack {
					t.Fatalf("detected after %v / %v, bound %v", d, ls, bound)
				}
			})
		}
	}
}

// TestConnDownCause: a connection the peer reset or closed reports why to
// ConnDown; one this side closed reports nil.
func TestConnDownCause(t *testing.T) {
	for _, tc := range []struct {
		name  string
		end   func(local *Conn, peer net.Conn)
		cause bool
	}{
		{"peer reset", func(_ *Conn, peer net.Conn) {
			_ = peer.(*net.TCPConn).SetLinger(0)
			_ = peer.Close()
		}, true},
		{"peer closed", func(_ *Conn, peer net.Conn) { _ = peer.Close() }, true},
		{"closed here", func(local *Conn, _ net.Conn) { _ = local.Close() }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obs := &countObserver{}
			a, b := tcpPair(t)
			go func() { _, _, _ = Server(context.Background(), b, Config{Auth: testToken}) }()
			c, err := Client(context.Background(), a, Config{Auth: testToken, Observer: obs})
			if err != nil {
				t.Fatal(err)
			}
			served := make(chan struct{})
			go func() { _ = NewMux(c, nil).Serve(nil); close(served) }()
			tc.end(c, b)
			<-served
			waitUntil(t, func() bool { return obs.down.Load() == 1 })
			err, _ = obs.downErr.Load().(error)
			if (err != nil) != tc.cause || errors.Is(err, ErrDeadPeer) {
				t.Fatalf("ConnDown(%v), want a cause: %v", err, tc.cause)
			}
			t.Logf("ConnDown(%v)", err)
		})
	}
}
