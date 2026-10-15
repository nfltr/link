// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package soak

import (
	"context"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/peerlinktest"
	"github.com/nfltr/link/soak/chainsoak"
)

// The chain-3hop config: origin -> hop 1 -> hop 2 -> echo, peerlink.Nodes
// over TCP through a peerlinktest.Proxy per link, each hop a
// peerlink.Forward (chainsoak, as cmd/linkhop runs it across pods). Per
// epoch: echo workers; at 25 % the middle link is cut (hop 1 redials);
// at 50 % a stalled far reader's bytes in flight are measured against
// 3 windows + 2 x 256 KiB; at 70 % a fault in rotation: hop 2 killed, hop
// 2 drained (its joined sessions finish, new opens are refused
// CodeDraining), or the last link black-holed; then every Node drains.

var chainEpochs atomic.Int64

// chainNode is a Node listening on loopback (and dialling next's proxy).
type chainNode struct {
	n  *peerlink.Node
	px *peerlinktest.Proxy // the proxy in front of its listener
}

func newChainNode(t *testing.T, name string, next *chainNode) *chainNode {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	c := &chainNode{px: peerlinktest.NewProxy(t, ln.Addr().String())}
	cfg := peerlink.Config{Listener: ln, Link: link.Config{Liveness: link.LivenessFast, Auth: nameAuth(name)}}
	if next != nil {
		cfg.Dial = func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", next.px.Addr())
		}
		cfg.Pool = link.PoolConfig{Backoff: func(int) time.Duration { return 100 * time.Millisecond }}
	}
	c.n = mustPeer(t, cfg)
	return c
}

func runChainEpoch(t *testing.T, d time.Duration, st *stats) {
	e := chainEpochs.Add(1)
	echo := newChainNode(t, "echo", nil)
	hop2 := newChainNode(t, "hop2", echo)
	hop1 := newChainNode(t, "hop1", hop2)
	origin := mustPeer(t, peerlink.Config{Link: link.Config{Liveness: link.LivenessFast, Auth: nameAuth("origin")},
		Dial: func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", hop1.px.Addr())
		},
		Pool: link.PoolConfig{Backoff: func(int) time.Duration { return 100 * time.Millisecond }}})
	served := make(chan error, 3)
	go func() { served <- echo.n.Serve(chainsoak.Echo) }()
	go func() { served <- hop2.n.Serve(chainsoak.Hop(hop2.n, "echo", link.ForwardConfig{})) }()
	go func() { served <- hop1.n.Serve(chainsoak.Hop(hop1.n, "hop2", link.ForwardConfig{})) }()
	deadAfter := origin.DeadAfter()
	st.mu.Lock()
	st.DeadAfterBound = deadAfter.Seconds()
	st.InFlightBound = 3*link.DefaultWindow + 2*256<<10
	st.mu.Unlock()

	start := time.Now()
	stop := make(chan struct{})
	faults := &windows{}
	var wg sync.WaitGroup
	once := func(n int) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		intact, err := chainsoak.EchoOnce(ctx, origin, "hop1", n)
		if err == nil {
			atomic.AddInt64(&st.Ops, 1)
			atomic.AddInt64(&st.Bytes, int64(n))
			if !intact {
				atomic.AddInt64(&st.IntegrityFailures, 1)
			}
		}
		return err
	}
	if err := once(1 << 10); err != nil {
		st.unexpected(fmt.Errorf("first echo: %w", err))
	}
	for range envInt("SOAKWORKERS", 6) {
		wg.Go(func() {
			r := mrand.New(mrand.NewPCG(mrand.Uint64(), 0))
			for {
				select {
				case <-stop:
					return
				default:
				}
				began := time.Now()
				if err := once(size(r)); err != nil {
					if faults.covers(began, time.Now()) {
						atomic.AddInt64(&st.ErrorsExpected, 1)
						var a *link.Abort
						if errors.As(err, &a) {
							st.mu.Lock()
							if len(st.Aborts) < 20 {
								st.Aborts = append(st.Aborts, a.Error())
							}
							st.mu.Unlock()
						}
					} else {
						st.unexpected(err)
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		})
	}
	at := func(f float64) { time.Sleep(time.Until(start.Add(time.Duration(f * float64(d))))) }
	bound := deadAfter + slack

	at(0.25) // the middle link resets: hop 1 aborts its joined sessions, then redials
	faults.add(time.Now(), time.Now().Add(bound+10*time.Second))
	hop2.px.Cut()
	st.mu.Lock()
	st.Cuts++
	st.mu.Unlock()

	at(0.5) // a stalled far reader holds at most the path's windows
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	if b, err := chainsoak.InFlight(ctx, origin, "hop1", time.Second); err == nil {
		st.mu.Lock()
		st.InFlightMax = max(st.InFlightMax, b)
		st.mu.Unlock()
	} else {
		st.unexpected(fmt.Errorf("in flight: %w", err))
	}
	cancel()

	at(0.7)
	faults.add(time.Now(), start.Add(d).Add(time.Hour)) // the path does not come back this epoch
	switch e % 3 {
	case 0: // hop 2 dies
		dead, c := context.WithCancel(context.Background())
		c()
		_ = hop2.n.Drain(dead)
	case 1: // hop 2 drains: what it joined finishes, new opens are refused
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = hop2.n.Drain(ctx)
		}()
		st.mu.Lock()
		st.Drains++
		st.mu.Unlock()
	case 2: // the last link goes silent
		echo.px.BlackHole()
		st.mu.Lock()
		st.BlackHoles++
		st.mu.Unlock()
	}
	at(1)
	close(stop)
	wg.Wait()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	for _, n := range []*peerlink.Node{origin, hop1.n, hop2.n, echo.n} {
		_ = n.Drain(ctx)
	}
	cancel()
	for range 3 {
		select {
		case <-served:
		case <-time.After(30 * time.Second):
			st.mu.Lock()
			st.DrainErrors++
			st.mu.Unlock()
		}
	}
}

// TestChainSoakShort: one short chain epoch as a smoke of the soak's
// chain (it runs in check-soak's vet only unless SOAKCHAIN is set).
func TestChainSoakShort(t *testing.T) {
	if testing.Short() {
		t.Skip("a 20 s epoch")
	}
	st := &stats{}
	chainEpochs.Store(0) // epoch 1: hop 2 drains
	runChainEpoch(t, 20*time.Second, st)
	t.Logf("%d echoes, %d MiB, expected errors %d, in flight %d (bound %d), aborts %v",
		st.Ops, st.Bytes>>20, st.ErrorsExpected, st.InFlightMax, st.InFlightBound, st.Aborts)
	if st.ErrorsUnexpected > 0 || st.IntegrityFailures > 0 || st.DrainErrors > 0 || st.InFlightMax > st.InFlightBound || st.Ops == 0 {
		t.Fatalf("%+v", st)
	}
}
