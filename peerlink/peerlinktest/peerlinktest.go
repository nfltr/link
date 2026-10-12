// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package peerlinktest is the conformance suite of the peerlink contract:
// Run checks an implementation of peerlink.Link against every rule of the
// contract, on pairs of Links the implementation's test makes.
//
//	func TestConformance(t *testing.T) {
//		peerlinktest.Run(t, func(t *testing.T) peerlinktest.Pair {
//			// B listens; A dials B through a Proxy, which injects the faults.
//			px := peerlinktest.NewProxy(t, bListener.Addr().String())
//			...
//			return peerlinktest.Pair{A: a, B: b, AName: "a", BName: "b",
//				DeadAfter: a.DeadAfter(),
//				BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle}
//		})
//	}
//
// Every time bound the suite checks comes from the Pair (DeadAfter) or is a
// generous bound for work that must finish at all (Within); none is
// specific to a transport.
package peerlinktest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link/peerlink"
)

// Pair is two connected Links: A opens sessions to B.
type Pair struct {
	A, B peerlink.Link
	// Reverse: B dials A, and A opens its sessions on the connections B
	// dialled (B behind NAT, say). The suite then calls Serve on A too
	// (to accept B's connections) and runs the cases of that direction.
	// false: A dials B.
	Reverse bool
	// BName is the peer name A opens sessions to B with; AName is the name
	// B's accept is given for A.
	AName, BName string
	// DeadAfter is the implementation's dead-peer bound: once the path
	// between A and B goes silent, every session between them reads an
	// error, on both ends, within it.
	DeadAfter time.Duration
	// Faults on the path between A and B (Proxy has them all).
	// BlackHole: nothing more crosses either way, and nothing is closed.
	BlackHole func()
	// Cut: every connection between A and B is reset.
	Cut func()
	// Throttle: the path carries at most bytesPerSecond each way.
	Throttle func(bytesPerSecond int)
}

// Within bounds work that must finish at all (it would hang if the
// contract were broken): sessions beside a stuck one, a drain.
const Within = time.Minute

// Run runs every case of the suite, each on a Pair of its own from newPair
// (some cases make two). The suite calls Serve on B (and on A in a Reverse
// Pair) and Drain on both (when a case ends: with a done context); newPair
// must not.
func Run(t *testing.T, newPair func(t *testing.T) Pair) {
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.run(t, newPair) })
	}
	t.Run("Unreachable", func(t *testing.T) { testUnreachable(t, newPair) })
}

var cases = []struct {
	name string
	run  func(*testing.T, func(*testing.T) Pair)
}{
	{"Echo", testEcho},
	{"Header", testHeader},
	{"HalfCloseOpenerFirst", testHalfCloseOpenerFirst},
	{"HalfCloseAccepterFirst", testHalfCloseAccepterFirst},
	{"Large", testLarge},
	{"Concurrent", testConcurrent},
	{"StuckReader", testStuckReader},
	{"Fairness", testFairness},
	{"DeadPeer", testDeadPeer},
	{"Cut", testCut},
	{"Reset", testReset},
	{"DrainAccepter", testDrainAccepter},
	{"DrainOpener", testDrainOpener},
	{"DrainDeadline", testDrainDeadline},
	{"Reconnect", testReconnect},
}

// --- helpers ---

func pair(t *testing.T, newPair func(*testing.T) Pair, accept func(peer string, header []byte, s net.Conn)) Pair {
	t.Helper()
	p := newPair(t)
	if p.A == nil || p.B == nil || p.DeadAfter <= 0 || p.BlackHole == nil || p.Cut == nil || p.Throttle == nil {
		t.Fatal("peerlinktest: a Pair needs A, B, DeadAfter, BlackHole, Cut and Throttle")
	}
	served := make(chan error, 2)
	go func() { served <- p.B.Serve(accept) }()
	serving := 1
	if p.Reverse {
		serving++
		go func() {
			served <- p.A.Serve(func(_ string, _ []byte, s net.Conn) {
				t.Error("A accepted a session: in this suite only A opens")
				_ = s.Close()
			})
		}()
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = p.A.Drain(ctx)
		_ = p.B.Drain(ctx)
		for range serving {
			select {
			case err := <-served:
				if err != nil {
					t.Errorf("Serve: %v, want nil after Drain", err)
				}
			case <-time.After(Within):
				t.Error("Serve did not return after Drain")
			}
		}
	})
	return p
}

// open opens a session from A to B; the test's cleanup closes it.
func open(t *testing.T, p Pair, header string) net.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), Within)
	defer cancel()
	s, err := p.A.Open(ctx, p.BName, []byte(header))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func closeWrite(t *testing.T, s net.Conn) {
	t.Helper()
	cw, ok := s.(interface{ CloseWrite() error })
	if !ok {
		t.Fatalf("session %T has no CloseWrite", s)
	}
	if err := cw.CloseWrite(); err != nil {
		t.Errorf("CloseWrite: %v", err)
	}
}

func reset(t *testing.T, s net.Conn) {
	t.Helper()
	r, ok := s.(interface{ Reset() error })
	if !ok {
		t.Fatalf("session %T has no Reset", s)
	}
	_ = r.Reset()
}

// echo copies what the session reads back to it, then closes it.
func echo(_ string, _ []byte, s net.Conn) {
	defer s.Close()
	_, _ = io.Copy(s, s)
}

// readErr reads s until it ends and returns why (nil: io.EOF).
func readErr(s net.Conn) error {
	_, err := io.Copy(io.Discard, s)
	return err
}

// cutErr checks err is how a cut session ends: an error, not io.EOF.
func cutErr(t *testing.T, who string, err error) {
	t.Helper()
	if err == nil || errors.Is(err, io.EOF) {
		t.Errorf("%s: read ended with %v, want an error that is not io.EOF", who, err)
	}
}

// exchange writes n random bytes to s and closes its write side while it
// reads the echo, and checks the echo is what was written.
func exchange(s net.Conn, n int64) error {
	sent := sha256.New()
	werr := make(chan error, 1)
	go func() {
		_, err := io.Copy(s, io.TeeReader(io.LimitReader(rand.Reader, n), sent))
		if err == nil {
			err = s.(interface{ CloseWrite() error }).CloseWrite()
		}
		werr <- err
	}()
	got := sha256.New()
	m, err := io.Copy(got, s)
	if err := <-werr; err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if err != nil {
		return fmt.Errorf("read after %d bytes: %w", m, err)
	}
	if m != n || !bytes.Equal(got.Sum(nil), sent.Sum(nil)) {
		return fmt.Errorf("echo of %d bytes: %d bytes back, checksum match %v", n, m, bytes.Equal(got.Sum(nil), sent.Sum(nil)))
	}
	return nil
}

// recvWithin waits for n values on ch, each within d of start.
func recvWithin[T any](t *testing.T, ch <-chan T, n int, start time.Time, d time.Duration, what string) []T {
	t.Helper()
	var got []T
	timer := time.NewTimer(time.Until(start.Add(d)))
	defer timer.Stop()
	for len(got) < n {
		select {
		case v := <-ch:
			got = append(got, v)
		case <-timer.C:
			t.Fatalf("%s: %d of %d within %v", what, len(got), n, d)
		}
	}
	return got
}

// deadBound is how long the suite waits for a dead peer to surface: the
// implementation's bound and half again, plus scheduling slack.
func deadBound(p Pair) time.Duration { return p.DeadAfter + p.DeadAfter/2 + 2*time.Second }

// --- cases ---

// Echo: open, send, half-close, read the echo, close; the accept sees the
// opener's name and the header.
func testEcho(t *testing.T, newPair func(*testing.T) Pair) {
	type seen struct {
		peer, header string
	}
	got := make(chan seen, 1)
	p := pair(t, newPair, func(peer string, h []byte, s net.Conn) {
		got <- seen{peer, string(h)}
		echo(peer, h, s)
	})
	s := open(t, p, "echo")
	if _, err := io.WriteString(s, "hello"); err != nil {
		t.Fatal(err)
	}
	closeWrite(t, s)
	b, err := io.ReadAll(s)
	if err != nil || string(b) != "hello" {
		t.Fatalf("echo: %q, %v; want \"hello\", nil", b, err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if g := <-got; g != (seen{p.AName, "echo"}) {
		t.Errorf("accept got peer %q header %q, want %q %q", g.peer, g.header, p.AName, "echo")
	}
}

// Header: empty, one zero byte and 4 KiB of random bytes arrive as sent.
func testHeader(t *testing.T, newPair func(*testing.T) Pair) {
	got := make(chan []byte, 1)
	p := pair(t, newPair, func(_ string, h []byte, s net.Conn) {
		got <- bytes.Clone(h)
		_ = s.Close()
	})
	big := make([]byte, 4<<10)
	_, _ = rand.Read(big)
	for _, h := range [][]byte{nil, {0}, big} {
		s := open(t, p, string(h))
		select {
		case g := <-got:
			if !bytes.Equal(g, h) {
				t.Errorf("header of %d bytes arrived as %d bytes (equal: %v)", len(h), len(g), bytes.Equal(g, h))
			}
		case <-time.After(Within):
			t.Fatal("accept not called")
		}
		_ = s.Close()
	}
}

// HalfCloseOpenerFirst: the opener ends its writes, can write no more, and
// still reads what the accepter writes after reading io.EOF.
func testHalfCloseOpenerFirst(t *testing.T, newPair func(*testing.T) Pair) {
	done := make(chan error, 1)
	p := pair(t, newPair, func(_ string, _ []byte, s net.Conn) {
		defer s.Close()
		b, err := io.ReadAll(s)
		if err == nil && string(b) != "ping" {
			err = fmt.Errorf("read %q, want \"ping\"", b)
		}
		if err == nil {
			_, err = io.WriteString(s, "pong")
		}
		done <- err
	})
	s := open(t, p, "x")
	if _, err := io.WriteString(s, "ping"); err != nil {
		t.Fatal(err)
	}
	closeWrite(t, s)
	if _, err := s.Write([]byte("more")); err == nil {
		t.Error("Write after CloseWrite succeeded")
	}
	b, err := io.ReadAll(s)
	if err != nil || string(b) != "pong" {
		t.Errorf("read %q, %v; want \"pong\", nil", b, err)
	}
	if err := <-done; err != nil {
		t.Errorf("accepter: %v", err)
	}
}

// HalfCloseAccepterFirst: the same with the roles swapped.
func testHalfCloseAccepterFirst(t *testing.T, newPair func(*testing.T) Pair) {
	done := make(chan error, 1)
	p := pair(t, newPair, func(_ string, _ []byte, s net.Conn) {
		defer s.Close()
		err := func() error {
			if _, err := io.WriteString(s, "first"); err != nil {
				return err
			}
			if err := s.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
				return err
			}
			if _, err := s.Write([]byte("more")); err == nil {
				return errors.New("Write after CloseWrite succeeded")
			}
			b, err := io.ReadAll(s)
			if err == nil && string(b) != "back" {
				err = fmt.Errorf("read %q, want \"back\"", b)
			}
			return err
		}()
		done <- err
	})
	s := open(t, p, "x")
	b, err := io.ReadAll(s)
	if err != nil || string(b) != "first" {
		t.Fatalf("read %q, %v; want \"first\", nil", b, err)
	}
	if _, err := io.WriteString(s, "back"); err != nil {
		t.Fatal(err)
	}
	closeWrite(t, s)
	if err := <-done; err != nil {
		t.Errorf("accepter: %v", err)
	}
}

// Large: 64 MiB each way at once, intact.
func testLarge(t *testing.T, newPair func(*testing.T) Pair) {
	p := pair(t, newPair, echo)
	if err := exchange(open(t, p, "echo"), 64<<20); err != nil {
		t.Fatal(err)
	}
}

// Concurrent: 32 sessions echo 1 MiB each at once, each intact.
func testConcurrent(t *testing.T, newPair func(*testing.T) Pair) {
	const n = 32
	p := pair(t, newPair, echo)
	var wg sync.WaitGroup
	for i := range n {
		s := open(t, p, "echo")
		wg.Go(func() {
			if err := exchange(s, 1<<20); err != nil {
				t.Errorf("session %d: %v", i, err)
			}
		})
	}
	wg.Wait()
}

// StuckReader: a session whose reader never reads, its writer blocked,
// does not keep 8 others from moving 4 MiB each.
func testStuckReader(t *testing.T, newPair func(*testing.T) Pair) {
	const n = 8
	release := make(chan struct{})
	p := pair(t, newPair, func(peer string, h []byte, s net.Conn) {
		if string(h) == "stall" {
			<-release
			_ = s.Close()
			return
		}
		echo(peer, h, s)
	})
	defer close(release)
	stuck := open(t, p, "stall")
	var wrote atomic.Int64
	go func() {
		buf := make([]byte, 32<<10)
		for {
			k, err := stuck.Write(buf)
			wrote.Add(int64(k))
			if err != nil {
				return
			}
		}
	}()
	// Wait until the stuck session's writer is blocked: nothing moved for
	// 200 ms.
	for last, deadline := int64(-1), time.Now().Add(Within); wrote.Load() != last; {
		if time.Now().After(deadline) {
			t.Fatalf("the stuck session's writer never blocked (%d bytes written)", wrote.Load())
		}
		last = wrote.Load()
		time.Sleep(200 * time.Millisecond)
	}
	start := time.Now()
	var wg sync.WaitGroup
	for i := range n {
		s := open(t, p, "echo")
		wg.Go(func() {
			if err := exchange(s, 4<<20); err != nil {
				t.Errorf("session %d: %v", i, err)
			}
		})
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(Within):
		t.Fatalf("%d sessions beside a stuck one did not finish within %v", n, Within)
	}
	t.Logf("stuck session blocked after %d bytes; %d sessions beside it moved 4 MiB each way in %v", wrote.Load(), n, time.Since(start).Round(time.Millisecond))
}

// Fairness: on a path throttled to 1 MiB/s, 8 sessions the accepter writes
// to as fast as they take each get at least a quarter of a fair share and
// none waits long for its turn.
func testFairness(t *testing.T, newPair func(*testing.T) Pair) {
	const n, rate, warm, run = 8, 1 << 20, 2 * time.Second, 4 * time.Second
	var accepted atomic.Int32
	start := make(chan struct{})
	p := pair(t, newPair, func(_ string, _ []byte, s net.Conn) {
		defer s.Close()
		if accepted.Add(1) == n {
			close(start)
		}
		<-start
		buf := make([]byte, 32<<10)
		for {
			if _, err := s.Write(buf); err != nil {
				return
			}
		}
	})
	p.Throttle(rate)
	var moved, last [n]atomic.Int64
	for i := range n {
		s := open(t, p, "src")
		go func() {
			buf := make([]byte, 32<<10)
			for {
				k, err := s.Read(buf)
				moved[i].Add(int64(k))
				last[i].Store(time.Now().UnixNano())
				if err != nil {
					return
				}
			}
		}()
	}
	select {
	case <-start:
	case <-time.After(Within):
		t.Fatal("accept not called for every session")
	}
	time.Sleep(warm)
	var base [n]int64
	for i := range n {
		base[i] = moved[i].Load()
	}
	var worst time.Duration
	for end := time.Now().Add(run); time.Now().Before(end); {
		time.Sleep(100 * time.Millisecond)
		for i := range n {
			worst = max(worst, time.Since(time.Unix(0, last[i].Load())))
		}
	}
	var total, least int64 = 0, 1 << 62
	var per []int64
	for i := range n {
		m := moved[i].Load() - base[i]
		total += m
		least = min(least, m)
		per = append(per, m>>10)
	}
	t.Logf("KiB per session in %v at %d KiB/s: %v; longest wait for a turn %v", run, rate>>10, per, worst.Round(time.Millisecond))
	if total == 0 || least < total/(4*n) || worst > run/2 {
		t.Fatalf("a session fell behind: KiB per session %v (fair share %d), longest wait %v", per, total/n>>10, worst.Round(time.Millisecond))
	}
}

// DeadPeer: once the path goes silent, every session (two idle, one whose
// opener keeps writing) reads an error on both ends within the bound.
func testDeadPeer(t *testing.T, newPair func(*testing.T) Pair) {
	type end struct {
		who string
		err error
		at  time.Time
	}
	ends := make(chan end, 6)
	var accepted sync.WaitGroup
	accepted.Add(3)
	p := pair(t, newPair, func(_ string, h []byte, s net.Conn) {
		defer s.Close()
		accepted.Done()
		err := readErr(s)
		ends <- end{"accepter " + string(h), err, time.Now()}
	})
	for _, h := range []string{"idle-1", "idle-2", "busy"} {
		s := open(t, p, h)
		if h == "busy" {
			go func() {
				buf := make([]byte, 32<<10)
				for {
					if _, err := s.Write(buf); err != nil {
						return
					}
				}
			}()
		}
		go func() {
			err := readErr(s)
			ends <- end{"opener " + h, err, time.Now()}
		}()
	}
	accepted.Wait()
	time.Sleep(100 * time.Millisecond) // the busy session is moving
	start := time.Now()
	p.BlackHole()
	var slowest time.Duration
	var each []string
	for _, e := range recvWithin(t, ends, 6, start, deadBound(p), "sessions ended after the path went silent") {
		cutErr(t, e.who, e.err)
		slowest = max(slowest, e.at.Sub(start))
		each = append(each, fmt.Sprintf("%s %v", e.who, e.at.Sub(start).Round(time.Millisecond)))
	}
	t.Logf("dead peer surfaced on every session within %v (stated bound %v): %v", slowest.Round(time.Millisecond), p.DeadAfter, each)
}

// Cut: a reset path ends every session, both ends, with an error.
func testCut(t *testing.T, newPair func(*testing.T) Pair) {
	errs := make(chan error, 4)
	var accepted sync.WaitGroup
	accepted.Add(2)
	p := pair(t, newPair, func(_ string, _ []byte, s net.Conn) {
		defer s.Close()
		accepted.Done()
		errs <- readErr(s)
	})
	for _, h := range []string{"idle", "busy"} {
		s := open(t, p, h)
		if h == "busy" {
			go func() { _, _ = io.Copy(s, rand.Reader) }()
		}
		go func() { errs <- readErr(s) }()
	}
	accepted.Wait()
	start := time.Now()
	p.Cut()
	for _, err := range recvWithin(t, errs, 4, start, deadBound(p), "sessions ended after the cut") {
		cutErr(t, "cut session", err)
	}
}

// Reset: a session reset by either side reads an error on the other.
func testReset(t *testing.T, newPair func(*testing.T) Pair) {
	errs := make(chan error, 1)
	p := pair(t, newPair, func(_ string, h []byte, s net.Conn) {
		defer s.Close()
		_, _ = io.WriteString(s, "x") // accepted
		if string(h) == "accepter resets" {
			reset(t, s)
			return
		}
		errs <- readErr(s)
	})
	s := open(t, p, "opener resets")
	if _, err := io.ReadFull(s, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	reset(t, s)
	cutErr(t, "accepter of a reset session", recvWithin(t, errs, 1, time.Now(), Within, "reset seen")[0])

	s = open(t, p, "accepter resets")
	done := make(chan error, 1)
	go func() { done <- readErr(s) }()
	cutErr(t, "opener of a reset session", recvWithin(t, done, 1, time.Now(), Within, "reset seen")[0])
}

// openRefused opens sessions from A to B until one is refused (at Open or
// at its first read), within Within; opens that still go through must
// work. It returns the refusal.
func openRefused(t *testing.T, p Pair) error {
	t.Helper()
	deadline := time.Now().Add(Within)
	for {
		err := func() error {
			ctx, cancel := context.WithTimeout(context.Background(), Within)
			defer cancel()
			s, err := p.A.Open(ctx, p.BName, []byte("echo"))
			if err != nil {
				return err
			}
			defer s.Close()
			return exchange(s, 1)
		}()
		if err != nil {
			return err
		}
		if time.Now().After(deadline) {
			t.Fatalf("Opens still accepted %v after Drain began", Within)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// half sends 1 MiB on s and reads its echo, to show s works.
func half(t *testing.T, s net.Conn) {
	t.Helper()
	buf := make([]byte, 1<<20)
	_, _ = rand.Read(buf)
	go func() { _, _ = s.Write(buf) }()
	got := make([]byte, len(buf))
	if _, err := io.ReadFull(s, got); err != nil || !bytes.Equal(got, buf) {
		t.Fatalf("echo before the drain: %v (intact: %v)", err, bytes.Equal(got, buf))
	}
}

// drain runs l.Drain in the background; the channel gets its result.
func drain(l peerlink.Link, d time.Duration) <-chan error {
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), d)
		defer cancel()
		done <- l.Drain(ctx)
	}()
	return done
}

// DrainAccepter: B drains while A's session runs; A's new opens are
// refused, the session finishes intact, then Drain returns nil.
func testDrainAccepter(t *testing.T, newPair func(*testing.T) Pair) {
	p := pair(t, newPair, echo)
	s := open(t, p, "echo")
	half(t, s)
	drained := drain(p.B, Within)
	t.Logf("open toward a draining peer: %v", openRefused(t, p))
	select {
	case err := <-drained:
		t.Fatalf("Drain returned %v while a session was open", err)
	default:
	}
	if err := exchange(s, 1<<20); err != nil {
		t.Fatalf("the open session across the drain: %v", err)
	}
	_ = s.Close()
	if err := recvWithin(t, drained, 1, time.Now(), Within, "Drain returned")[0]; err != nil {
		t.Errorf("Drain: %v, want nil", err)
	}
}

// DrainOpener: A drains while its session runs; its new opens fail with
// ErrDraining, the session finishes intact, then Drain returns nil.
func testDrainOpener(t *testing.T, newPair func(*testing.T) Pair) {
	p := pair(t, newPair, echo)
	s := open(t, p, "echo")
	half(t, s)
	drained := drain(p.A, Within)
	if err := openRefused(t, p); !errors.Is(err, peerlink.ErrDraining) {
		t.Errorf("Open on a draining Link: %v, want ErrDraining", err)
	}
	if err := exchange(s, 1<<20); err != nil {
		t.Fatalf("the open session across the drain: %v", err)
	}
	_ = s.Close()
	if err := recvWithin(t, drained, 1, time.Now(), Within, "Drain returned")[0]; err != nil {
		t.Errorf("Drain: %v, want nil", err)
	}
	if _, err := p.A.Open(context.Background(), p.BName, nil); !errors.Is(err, peerlink.ErrDraining) {
		t.Errorf("Open after Drain: %v, want ErrDraining", err)
	}
}

// DrainDeadline: a Drain (of either end) whose context ends with a session
// still open cuts it: both ends read an error, Drain returns ctx's error.
func testDrainDeadline(t *testing.T, newPair func(*testing.T) Pair) {
	for _, side := range []string{"opener", "accepter"} {
		t.Run(side, func(t *testing.T) {
			errs := make(chan error, 2)
			accepted := make(chan struct{})
			p := pair(t, newPair, func(_ string, _ []byte, s net.Conn) {
				defer s.Close()
				close(accepted)
				errs <- readErr(s)
			})
			s := open(t, p, "x")
			go func() { errs <- readErr(s) }()
			<-accepted
			l := p.A
			if side == "accepter" {
				l = p.B
			}
			start := time.Now()
			err := recvWithin(t, drain(l, 300*time.Millisecond), 1, start, Within, "Drain returned")[0]
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Drain: %v, want context.DeadlineExceeded", err)
			}
			for _, err := range recvWithin(t, errs, 2, start, Within, "sessions cut by the drain") {
				cutErr(t, "session cut by a drain", err)
			}
		})
	}
}

// Reconnect: after a cut, opens work again within the dead-peer bound
// (the opener retrying the ones that fail; in a Reverse Pair once B dialled
// again).
func testReconnect(t *testing.T, newPair func(*testing.T) Pair) {
	p := pair(t, newPair, echo)
	if err := exchange(open(t, p, "echo"), 1); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	p.Cut()
	var last error
	for time.Since(start) < deadBound(p) {
		last = func() error {
			ctx, cancel := context.WithDeadline(context.Background(), start.Add(deadBound(p)))
			defer cancel()
			s, err := p.A.Open(ctx, p.BName, []byte("echo"))
			if err != nil {
				return err
			}
			defer s.Close()
			return exchange(s, 1<<10)
		}()
		if last == nil {
			t.Logf("opens worked again %v after the cut", time.Since(start).Round(time.Millisecond))
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no open worked within %v of the cut: %v", deadBound(p), last)
}

// Unreachable (Reverse Pairs): Open toward a peer with no connection up
// waits until its ctx ends, then fails with ErrPeerUnreachable and ctx's
// error.
func testUnreachable(t *testing.T, newPair func(*testing.T) Pair) {
	p := pair(t, newPair, echo)
	if !p.Reverse {
		t.Skip("a Reverse Pair's case: A dials the peers it opens to")
	}
	const wait = 300 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	start := time.Now()
	s, err := p.A.Open(ctx, "no such peer", nil)
	if err == nil {
		_ = s.Close()
		t.Fatal("Open toward a peer with no connection succeeded")
	}
	if !errors.Is(err, peerlink.ErrPeerUnreachable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Open: %v, want ErrPeerUnreachable and context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d < wait {
		t.Errorf("Open returned after %v, before its ctx ended (%v)", d, wait)
	}
}
