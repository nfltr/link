// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/exec"
	"slices"
	"sync/atomic"
	"time"
)

// The cases of the internet hop (link vs quic): handshake time,
// head-of-line blocking, an address change.

// hs: cold connect to first byte, n times: a new client (TCP + TLS + link
// hello, or QUIC's handshake), a session, 64 bytes echoed. Microseconds.
func hs(o *opts) result {
	d := make([]time.Duration, 0, o.n)
	for range o.n {
		t := time.Now()
		c := o.newClient()
		s := mustOpen(c, "echo")
		if err := rtt(s, make([]byte, 64)); err != nil {
			log.Fatal(err)
		}
		d = append(d, time.Since(t))
		_ = s.Close()
		_ = c.Close()
	}
	return result{"n": o.n, "hs_p50": pct(d, .5), "hs_p99": pct(d, .99)}
}

// hol: 8 sessions downloading the listener's streams; after 3 s this side
// drops a random 5 % of the listener's packets (iptables statistic) for
// -dur. Per 10 ms tick it notes which sessions got bytes, in the burst and
// in as long a stretch before it: ticks where every session stalled,
// where some but not all did, and each session's longest gap.
func hol(o *opts) result {
	const n, tick = 8, 10 * time.Millisecond
	c := o.newClient()
	defer c.Close()
	var counts [n]atomic.Int64
	for i := range n {
		s := mustOpen(c, "src 0")
		go func() { _, _ = readAll(s, 0, false, &counts[i]) }()
	}
	measure := func(d time.Duration) result {
		var last [n]int64
		var gap, longest [n]time.Duration
		all, some, ticks := 0, 0, 0
		var moved int64
		for end := time.Now().Add(d); time.Now().Before(end); ticks++ {
			time.Sleep(tick)
			stalled := 0
			for i := range n {
				v := counts[i].Load()
				if v == last[i] {
					stalled++
					gap[i] += tick
					longest[i] = max(longest[i], gap[i])
				} else {
					moved += v - last[i]
					gap[i] = 0
				}
				last[i] = v
			}
			if ticks == 0 {
				continue // last[] was zero
			}
			switch {
			case stalled == n:
				all++
			case stalled > 0:
				some++
			}
		}
		ms := make([]float64, n)
		for i := range n {
			ms[i] = float64(longest[i].Milliseconds())
		}
		slices.Sort(ms)
		return result{"ticks": ticks, "all_stalled": all, "some_stalled": some,
			"gap_ms_median": ms[n/2], "gap_ms_max": ms[n-1], "MBps": float64(moved) / 1e6 / d.Seconds()}
	}
	time.Sleep(time.Second)
	before := measure(o.dur)
	burst(o, "-A")
	during := measure(o.dur)
	burst(o, "-D")
	return result{"sessions": n, "burst_s": o.dur.Seconds(), "loss_pct": 5, "before": before, "during": during,
		"all_stalled_share": float64(during["all_stalled"].(int)) / float64(max(1, during["all_stalled"].(int)+during["some_stalled"].(int)))}
}

func burst(o *opts, op string) {
	args := []string{op, "INPUT", "-s", host(o.addr), "-m", "statistic", "--mode", "random", "--probability", "0.05", "-j", "DROP"}
	if out, err := exec.Command("iptables", args...).CombinedOutput(); err != nil { // #nosec G204 -- the rig's own addresses
		log.Fatalf("iptables %v: %v %s", args, err, out)
	}
}

// netchange: one session downloading the listener's stream (checked);
// after 3 s this side's address changes from -oldip to -newip (the old
// one removed, as when a laptop changes networks; the /24 kept), then 30 s
// more. Reports whether the session lived on intact (first_session
// "alive"), the longest stretch without bytes after the change (on it, or
// on a new session opened once it ended), and how many sessions it took.
func netchange(o *opts) result {
	c := o.newClient()
	defer c.Close()
	var count atomic.Int64
	var opened atomic.Int32
	firstEnd := make(chan string, 1)
	go func() {
		for {
			s := mustOpenRetry(c, "src 0")
			k := opened.Add(1)
			_, err := readAll(s, 0, true, &count)
			_ = s.Close()
			if k == 1 {
				firstEnd <- endKind(err)
			}
		}
	}()
	time.Sleep(3 * time.Second)
	sub := o.oldIP[:len(o.oldIP)-len(lastOctet(o.oldIP))] + "0/24"
	change := time.Now()
	ipcmd(fmt.Sprintf("ip addr add %s/32 dev eth0 && ip addr del %s/24 dev eth0 && ip route add %s dev eth0 src %s", o.newIP, o.oldIP, sub, o.newIP))
	defer ipcmd(fmt.Sprintf("ip route del %s dev eth0 src %s; ip addr add %s/24 dev eth0; ip addr del %s/32 dev eth0; ip route add default via %s1 || true",
		sub, o.newIP, o.oldIP, o.newIP, sub[:len(sub)-4]))
	last, lastAt := count.Load(), change
	var longest time.Duration
	for time.Since(change) < 30*time.Second {
		time.Sleep(10 * time.Millisecond)
		if v := count.Load(); v != last {
			longest = max(longest, time.Since(lastAt))
			last, lastAt = v, time.Now()
		}
	}
	longest = max(longest, time.Since(lastAt))
	first := "alive"
	select {
	case first = <-firstEnd:
	default:
	}
	return result{"live": o.live, "first_session": first, "sessions_opened": opened.Load(),
		"longest_gap_s": longest.Seconds(), "MB_total": float64(last) / 1e6}
}

func lastOctet(ip string) string {
	for i := len(ip) - 1; i >= 0; i-- {
		if ip[i] == '.' {
			return ip[i+1:]
		}
	}
	return ip
}

func ipcmd(script string) {
	if out, err := exec.Command("sh", "-c", script).CombinedOutput(); err != nil { // #nosec G204 -- the rig's own addresses
		log.Printf("%s: %v %s", script, err, out)
	}
}

// mustOpenRetry opens a session, retrying for a minute.
func mustOpenRetry(c client, hdr string) net.Conn {
	for start := time.Now(); ; {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		s, err := c.Open(ctx, []byte(hdr))
		cancel()
		if err == nil {
			return s
		}
		if time.Since(start) > time.Minute {
			log.Fatalf("open %q: %v", hdr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
