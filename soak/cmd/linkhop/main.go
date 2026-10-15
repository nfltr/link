// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Command linkhop runs one role of a chain soak across machines (pods):
// a path origin -> hop -> ... -> echo of peerlink.Nodes over TCP
// (plaintext, link.NameAuth: a trusted network such as a cluster's pod
// network), each hop a peerlink.Forward.
//
//	linkhop -role echo   -listen :7000
//	linkhop -role hop    -listen :7000 -next echo-0.echo:7000
//	linkhop -role origin -next hop-0.hop:7000 -duration 1h -out /out/chain.json
//
// The origin runs -workers echo loops of mixed sizes, each checked by
// SHA-256 after the far end's answer, and measures what the path holds
// for a stalled far reader every -inflight; it writes a JSON summary
// (errors by kind, *link.Abort code and hop) and exits 1 on an integrity
// failure, on no echo completed, or on an in-flight measure over -bound.
// Faults are the caller's (kill or drain a hop pod, a NetworkPolicy):
// errors during them are counted, never fatal. hop and echo run until
// SIGTERM, then drain (-drain).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	mrand "math/rand/v2" // nosemgrep: math-random-used -- echo sizes for a soak, not a secret
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/soak/chainsoak"
)

type summary struct {
	Start             time.Time        `json:"start"`
	Duration          float64          `json:"duration_s"`
	Echoes            int64            `json:"echoes"`
	Bytes             int64            `json:"bytes_echoed"`
	IntegrityFailures int64            `json:"integrity_failures"`
	Errors            map[string]int64 `json:"errors"` // "abort code=N hops=H refused=B" or "other"
	InFlightMax       int64            `json:"in_flight_max"`
	InFlightBound     int64            `json:"in_flight_bound"`
	Pass              bool             `json:"pass"`
}

func main() {
	role := flag.String("role", "", "origin, hop or echo")
	listen := flag.String("listen", "", "hop, echo: the address to take connections on")
	next := flag.String("next", "", "origin, hop: the next hop's address")
	name := flag.String("name", "", "this node's name (link.NameAuth); default the role")
	workers := flag.Int("workers", 6, "origin: echo loops")
	duration := flag.Duration("duration", time.Hour, "origin: how long to run")
	inflight := flag.Duration("inflight", time.Minute, "origin: how often to measure a stalled far reader's in-flight bytes (0: never)")
	bound := flag.Int64("bound", 0, "origin: the in-flight bound in bytes (0: unchecked), e.g. links x 1 MiB + hops x 256 KiB")
	drain := flag.Duration("drain", 30*time.Second, "hop, echo: the drain's deadline after SIGTERM")
	out := flag.String("out", "", "origin: the JSON summary's path (default stdout)")
	flag.Parse()
	if *name == "" {
		*name = *role
	}
	cfg := link.Config{Liveness: link.LivenessFast, Auth: link.NameAuth(*name)}
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", *next)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	switch *role {
	case "echo", "hop":
		ln, err := net.Listen("tcp", *listen)
		check(err)
		pc := peerlink.Config{Listener: ln, Link: cfg}
		if *role == "hop" {
			pc.Dial = dial
		}
		n, err := peerlink.New(pc)
		check(err)
		accept := chainsoak.Echo
		if *role == "hop" {
			accept = chainsoak.Hop(n, "next", link.ForwardConfig{})
		}
		go func() {
			<-ctx.Done()
			dctx, cancel := context.WithTimeout(context.Background(), *drain)
			defer cancel()
			_ = n.Drain(dctx)
		}()
		check(n.Serve(accept))
	case "origin":
		n, err := peerlink.New(peerlink.Config{Dial: dial, Link: cfg,
			Pool: link.PoolConfig{Backoff: func(int) time.Duration { return 200 * time.Millisecond }}})
		check(err)
		sum := origin(ctx, n, *workers, *duration, *inflight, *bound)
		b, _ := json.MarshalIndent(sum, "", "  ")
		if *out == "" {
			fmt.Println(string(b))
		} else {
			check(os.WriteFile(*out, append(b, '\n'), 0o600))
		}
		if !sum.Pass {
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "linkhop: -role origin, hop or echo")
		os.Exit(2)
	}
}

func origin(ctx context.Context, n *peerlink.Node, workers int, d, every time.Duration, bound int64) *summary {
	sum := &summary{Start: time.Now(), Errors: map[string]int64{}, InFlightBound: bound}
	var mu sync.Mutex
	count := func(err error) {
		key := "other"
		var a *link.Abort
		if errors.As(err, &a) {
			key = fmt.Sprintf("abort code=%d hops=%d refused=%v", a.Code, a.Hops, a.Refused)
		}
		mu.Lock()
		sum.Errors[key]++
		mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			r := mrand.New(mrand.NewPCG(mrand.Uint64(), 0))
			for ctx.Err() == nil {
				size := r.IntN(1 << 10)
				switch p := r.IntN(10); {
				case p >= 8:
					size = r.IntN(4 << 20)
				case p >= 4:
					size = r.IntN(64 << 10)
				}
				octx, ocancel := context.WithTimeout(ctx, 30*time.Second)
				intact, err := chainsoak.EchoOnce(octx, n, "next", size)
				ocancel()
				switch {
				case err != nil && ctx.Err() == nil:
					count(err)
					time.Sleep(100 * time.Millisecond)
				case err != nil:
				case !intact:
					atomic.AddInt64(&sum.IntegrityFailures, 1)
				default:
					atomic.AddInt64(&sum.Echoes, 1)
					atomic.AddInt64(&sum.Bytes, int64(size))
				}
			}
		})
	}
	if every > 0 {
		wg.Go(func() {
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
				if b, err := chainsoak.InFlight(ctx, n, "next", 500*time.Millisecond); err == nil {
					mu.Lock()
					sum.InFlightMax = max(sum.InFlightMax, b)
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	sum.Duration = time.Since(sum.Start).Seconds()
	sum.Pass = sum.IntegrityFailures == 0 && sum.Echoes > 0 && (bound == 0 || sum.InFlightMax <= bound)
	return sum
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "linkhop:", err)
		os.Exit(1)
	}
}
