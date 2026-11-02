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

// deadAt returns when c ends (it must end with ErrDeadPeer), observed
// from the moment it is called, so that two ends are timed each on its own.
func deadAt(t *testing.T, who string, c *Conn) <-chan time.Time {
	t.Helper()
	at := make(chan time.Time, 1)
	go func() {
		<-c.Done()
		end := time.Now()
		if !errors.Is(c.Err(), ErrDeadPeer) {
			t.Errorf("%s: ended with %v", who, c.Err())
		}
		at <- end
	}()
	return at
}

// lateProbe measures how late this process's timers fire while a test
// runs: a goroutine sleeps in short steps and keeps the worst lateness. On
// a starved machine (load, -race, background priority) keepAlive's timers
// are as late as its own; that is Late of spec/Liveness.tla.
type lateProbe struct{ worst atomic.Int64 }

func startLateProbe(t *testing.T) *lateProbe {
	p := &lateProbe{}
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		const step = 10 * time.Millisecond
		for {
			t0 := time.Now()
			select {
			case <-stop:
				return
			case <-time.After(step):
			}
			if late := int64(time.Since(t0) - step); late > p.worst.Load() {
				p.worst.Store(late)
			}
		}
	}()
	return p
}

func (p *lateProbe) late() time.Duration { return time.Duration(p.worst.Load()) }

// checkDeadAfter checks when c ended against spec/Liveness.tla, from the
// last frame c received (rxAt), not from when the test cut the path:
// never sooner than Interval + Timeout (NeverEarly), and no later than
// that plus the lateness of the two timer fires that decide it (the
// ping's and the verdict's, DetectBound), a third for the goroutine that
// observes the end, and deadPeerSlack. The lateness is the probe's,
// measured during the run: a starved machine makes the bound longer, a
// wrong mechanism (a ping never sent, a verdict anchored wrong, a timer
// not re-armed) still fails. It returns the silence before the end.
func checkDeadAfter(t *testing.T, who string, c *Conn, l Liveness, at <-chan time.Time, probe *lateProbe) time.Duration {
	t.Helper()
	bound := l.Interval + l.Timeout
	var end time.Time
	select {
	case end = <-at:
	case <-time.After(3*bound + 3*probe.late()):
		t.Fatalf("%s: dead peer not detected", who)
	}
	silent := end.Sub(c.t0.Add(time.Duration(c.rxAt.Load())))
	if silent < bound {
		t.Fatalf("%s: dead %v after its last frame, under Interval + Timeout (%v)", who, silent, bound)
	}
	if most := bound + 3*probe.late() + deadPeerSlack; silent > most {
		t.Fatalf("%s: dead %v after its last frame, past %v (Interval + Timeout %v, timers up to %v late)", who, silent, most, bound, probe.late())
	}
	return silent
}

// testPresets is fast and default; LINK_ALL_PRESETS=1 every preset.
func testPresets() []string {
	if os.Getenv("LINK_ALL_PRESETS") != "" {
		return LivenessPresets
	}
	return []string{"fast", "default"}
}

// deadPeerSlack is what the bound allows beyond Interval + Timeout and
// the timers' measured lateness: the frames still on the way when the path
// died, and wakeups between the probe's 10 ms samples.
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
// both ends end it Interval + Timeout after the last frame each received,
// plus their timers' lateness (checkDeadAfter, spec/Liveness.tla).
func TestDeadPeerWorstCaseIdle(t *testing.T) {
	for _, name := range testPresets() {
		for _, side := range []string{"dialer", "listener"} {
			t.Run(name+"/"+side, func(t *testing.T) {
				t.Parallel()
				l := livenessByName[name]
				do, lo := &pongCut{}, &pongCut{}
				c, s, cut := livePair(t, l, do, lo)
				probe := startLateProbe(t)
				go func() { _, _ = c.Recv() }()
				go func() { _, _ = s.Recv() }()
				started := make(chan time.Time, 1)
				f := func() { cut(); started <- time.Now() }
				armed := c
				if side == "dialer" {
					do.arm.Store(&f)
				} else {
					armed = s
					lo.arm.Store(&f)
				}
				// A side pings only when nothing arrived for Interval, and
				// the peer's ping counts: the peer may be the one that
				// pings round after round (one in 2^9 runs, oftener under
				// load), so after two intervals the armed side pings once
				// itself. Its pong is then the last frame it hears either
				// way.
				select {
				case <-started:
				case <-time.After(2 * l.Interval):
					t.Logf("no pong in two intervals: the %s pings", side)
					armed.ping()
					select {
					case <-started:
					case <-time.After(3 * (l.Interval + l.Timeout)):
						t.Fatal("no pong")
					}
				}
				dat, lat := deadAt(t, "dialer", c), deadAt(t, "listener", s)
				d := checkDeadAfter(t, "dialer", c, l, dat, probe)
				ls := checkDeadAfter(t, "listener", s, l, lat, probe)
				t.Logf("%s, cut after the %s's pong: silent before the end: dialer %v, listener %v; Interval + Timeout %v, timers up to %v late", name, side, d.Round(time.Millisecond), ls.Round(time.Millisecond), l.Interval+l.Timeout, probe.late().Round(time.Millisecond))
			})
		}
	}
}

// TestDeadPeerBusy: sessions stream one way when the path dies; the
// sender's writes block (the network takes nothing more), and both ends
// still end the connection Interval + Timeout after the last frame each
// received, plus their timers' lateness (checkDeadAfter,
// spec/Liveness.tla). It failed in promote with "dialer 15.517s, bound
// 15s" on a loaded Mac under -race: the timers' lateness, which the bound
// now states.
func TestDeadPeerBusy(t *testing.T) {
	for _, name := range testPresets() {
		for _, dir := range []string{"down", "up"} { // down: the listener sends
			t.Run(name+"/"+dir, func(t *testing.T) {
				t.Parallel()
				l := livenessByName[name]
				c, s, cut := livePair(t, l, nil, nil)
				probe := startLateProbe(t)
				stream := func(s *Session) {
					b := make([]byte, 64<<10)
					for {
						if _, err := s.Write(b); err != nil {
							return
						}
					}
				}
				sink := func(s *Session) { _, _ = io.Copy(io.Discard, s) }
				dm, lm := NewMux(c), NewMux(s)
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
				dat, lat := deadAt(t, "dialer", c), deadAt(t, "listener", s)
				cut()
				d := checkDeadAfter(t, "dialer", c, l, dat, probe)
				ls := checkDeadAfter(t, "listener", s, l, lat, probe)
				t.Logf("%s, %s: silent before the end: dialer %v, listener %v; Interval + Timeout %v, timers up to %v late", name, dir, d.Round(time.Millisecond), ls.Round(time.Millisecond), l.Interval+l.Timeout, probe.late().Round(time.Millisecond))
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
			go func() { _ = NewMux(c).Serve(nil); close(served) }()
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
