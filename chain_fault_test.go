// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// echoOnce is the chain soak's echo (soak/chainsoak.EchoOnce) on s: n
// random bytes, CloseWrite, read to the end. intact false with err nil is
// an integrity failure: the session ended cleanly with other bytes.
func echoOnce(s *Session, n int) (intact bool, err error) {
	defer s.Close() //nolint:errcheck
	if err := s.Answered(context.Background()); err != nil {
		return true, err
	}
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(rand.Uint32())
	}
	werr := make(chan error, 1)
	go func() {
		_, err := s.Write(data)
		if err == nil {
			err = s.CloseWrite()
		}
		werr <- err
	}()
	h := sha256.New()
	got, rerr := io.Copy(h, s)
	if err := <-werr; err != nil {
		return true, err
	}
	if rerr != nil {
		return true, rerr
	}
	want := sha256.Sum256(data)
	return got == int64(n) && bytes.Equal(h.Sum(nil), want[:]), nil
}

// chainFaultRun runs workers echoing through a fresh 5-hop rig (6 links)
// until fault, done to the middle hop (3), ended their sessions: the
// echoes that ended cleanly but wrong (bad, logged with each hop's
// HopEnded), that failed, and that came back intact.
func chainFaultRun(t *testing.T, o rigOpts, fault func(r *chainRig)) (bad, failed, ok int64) {
	o.n = 6
	r := newChainRig(t, o, farEcho)
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			for !stop.Load() {
				size := rand.IntN(1 << 10)
				switch p := rand.IntN(10); {
				case p >= 8:
					size = rand.IntN(4 << 20)
				case p >= 4:
					size = rand.IntN(64 << 10)
				}
				s, err := r.links[0].dm.Open([]byte("echo"))
				if err != nil {
					return
				}
				intact, err := echoOnce(s, size)
				switch {
				case err != nil:
					atomic.AddInt64(&failed, 1)
				case !intact:
					atomic.AddInt64(&bad, 1)
					time.Sleep(200 * time.Millisecond)
					for h, o := range r.obs {
						o.mu.Lock()
						for _, e := range o.events {
							if e.Chain.Trace == s.Chain().Trace && e.Kind == HopEnded {
								t.Logf("bad size %d: hop %d ended %+v", size, h+1, e)
							}
						}
						o.mu.Unlock()
					}
				default:
					atomic.AddInt64(&ok, 1)
				}
			}
		})
	}
	time.Sleep(time.Duration(20+rand.IntN(80)) * time.Millisecond)
	fault(r)
	time.Sleep(50 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	return bad, failed, ok
}

// hop3 is the middle hop's two connections: link 2 (it listens) and
// link 3 (it dials).
func hop3(r *chainRig) (in, out *Mux) { return r.links[2].lm, r.links[3].dm }

// TestChainMiddleHopFaultsIntegrity: the chain soak's faults on the
// middle hop of a 5-hop chain with echoes in flight (SOAK.md, "Chains"):
// every echo either fails with an error or comes back intact; none ends
// cleanly with other bytes (a cut connection taken for a clean close).
func TestChainMiddleHopFaultsIntegrity(t *testing.T) {
	runs := 30
	if testing.Short() {
		runs = 8
	}
	kill := func(r *chainRig) {
		in, out := hop3(r)
		_ = in.Close()
		_ = out.Close()
	}
	drain := func(r *chainRig) {
		in, out := hop3(r)
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rand.IntN(40))*time.Millisecond)
		defer cancel()
		go func() { _ = out.Drain(ctx) }()
		_ = in.Drain(ctx)
	}
	// partition: hop 3's links go silent both ways (no close) until the
	// hops beside it see them dead (liveness).
	partition := func() (rigOpts, func(*chainRig)) {
		cut := make(chan struct{})
		return rigOpts{live: LivenessFast, wrap: func(i int, nc net.Conn) net.Conn {
			if i == 2 || i == 3 {
				return &blackHole{Conn: nc, cut: cut, closed: make(chan struct{})}
			}
			return nc
		}}, func(*chainRig) { close(cut) }
	}
	faults := map[string]func() (rigOpts, func(*chainRig)){
		"kill":      func() (rigOpts, func(*chainRig)) { return rigOpts{}, kill },
		"drain":     func() (rigOpts, func(*chainRig)) { return rigOpts{}, drain },
		"partition": partition,
	}
	for name, f := range faults {
		t.Run(name, func(t *testing.T) {
			runs := runs
			if name == "partition" { // a liveness bound each
				if testing.Short() {
					t.Skip("waits a liveness bound per run")
				}
				runs = 3
			}
			var bad, failed, ok int64
			for run := range runs {
				t.Run(fmt.Sprint(run), func(t *testing.T) {
					opts, fault := f()
					b, f, o := chainFaultRun(t, opts, fault)
					bad, failed, ok = bad+b, failed+f, ok+o
				})
			}
			t.Logf("%d echoes intact, %d failed, %d integrity failures", ok, failed, bad)
			if bad != 0 {
				t.Fatalf("%d integrity failures", bad)
			}
		})
	}
}

// TestPumpEndSeesPeerHalfClose: a Pump's done after the peer's CloseWrite
// sees the half close (Join turns it into CloseWrite on the other side;
// seen too late, it closed the other side whole and lost its answer).
func TestPumpEndSeesPeerHalfClose(t *testing.T) {
	late := make(chan bool, 1)
	a := muxPair(t, func(_ []byte, s *Session) {
		s.Pump(io.Discard, func(_ int64, err error) {
			late <- err == nil && !s.peerHalf.Load()
			_ = s.Close()
		})
	})
	for range 2000 {
		s, err := a.Open(nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = s.Write([]byte("x"))
		_ = s.CloseWrite()
		if <-late {
			t.Fatal("the pump ended cleanly before the peer's half close was recorded")
		}
		_ = s.Close()
	}
}

// TestWriteFailsWithPeerAbort: a write waiting for credit when the peer
// aborts fails with the *Abort (errors.Is ErrReset), never a plain
// io.ErrClosedPipe (a clean close to Join): the credit ends as the ABORT
// is read, before the receive loop records it.
func TestWriteFailsWithPeerAbort(t *testing.T) {
	a := muxPair(t, func(_ []byte, s *Session) {
		_ = s.Accept()
		_, _ = io.CopyN(io.Discard, s, 1)
		_ = s.Abort(CodeApp, "no")
	})
	buf := make([]byte, 2*testWindow)
	for range 300 {
		s, err := a.Open(nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.Write(buf)
		if !errors.Is(err, ErrReset) {
			t.Fatalf("write: %v, want the peer's abort", err)
		}
	}
}
