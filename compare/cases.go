// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type result = map[string]any

// run is the dialer side ("A"): one case against a running listener ("B").
func run(o *opts) result {
	switch o.test {
	case "tput":
		return tput(o)
	case "lat":
		return lat(o)
	case "mem":
		return mem(o)
	case "busy":
		return busy(o)
	case "iso":
		return iso(o)
	case "fair":
		return fair(o)
	case "part":
		return part(o)
	case "kill":
		return kill(o)
	case "drain":
		return drain(o)
	case "falsedead":
		return falsedead(o)
	case "hs":
		return hs(o)
	case "hol":
		return hol(o)
	case "netchange":
		return netchange(o)
	case "ping":
		return result{"ok": o.peerStats(false, false).T > 0}
	}
	log.Fatalf("test %q", o.test)
	return nil
}

func (o *opts) newClient() client {
	if u, ok := o.L.(udpLib); ok {
		return u.dialUDP(o.addr)
	}
	return o.L.client(o.dialer(o.addr), o.pool)
}

// tput: sessions moving bytes for dur (after 1 s of warm-up): MB/s,
// per-session spread, CPU and allocations per MB on both sides, peak heap.
func tput(o *opts) result {
	c := o.newClient()
	defer c.Close()
	counts := make([]atomic.Int64, o.sessions)
	var ss []net.Conn
	for i := range o.sessions {
		if o.dir == "down" {
			s := mustOpen(c, "src 0")
			go func() { _, _ = readAll(s, 0, false, &counts[i]) }()
			ss = append(ss, s)
		} else {
			s := mustOpen(c, "sink")
			go func() {
				for off := int64(0); ; off += 64 << 10 {
					if _, err := s.Write(chunk(off, 64<<10)); err != nil {
						return
					}
					counts[i].Add(64 << 10)
				}
			}()
			ss = append(ss, s)
		}
	}
	time.Sleep(time.Second)
	sum := func() (t int64, per []int64) {
		for i := range counts {
			per = append(per, counts[i].Load())
			t += per[i]
		}
		return
	}
	a0, b0 := stats(o, false, true), o.peerStats(false, true)
	t0, p0 := sum()
	start := time.Now()
	time.Sleep(o.dur)
	t1, p1 := sum()
	el := time.Since(start).Seconds()
	a1, b1 := stats(o, false, false), o.peerStats(false, false)
	for _, s := range ss {
		_ = s.Close()
	}
	mb := float64(t1-t0) / 1e6
	var per []float64
	for i := range p1 {
		per = append(per, float64(p1[i]-p0[i])/1e6)
	}
	return result{
		"sessions": o.sessions, "pool": o.pool, "dir": o.dir, "secs": el,
		"MBps": mb / el, "per_session_MB_min": slices.Min(per), "per_session_MB_max": slices.Max(per),
		"cores_a": (a1.CPU - a0.CPU) / el, "cores_b": (b1.CPU - b0.CPU) / el,
		"cpu_us_per_MB_a": (a1.CPU - a0.CPU) * 1e6 / mb, "cpu_us_per_MB_b": (b1.CPU - b0.CPU) * 1e6 / mb,
		"allocs_per_MB_a": float64(a1.Mallocs-a0.Mallocs) / mb, "allocs_per_MB_b": float64(b1.Mallocs-b0.Mallocs) / mb,
		"alloc_B_per_MB_a": float64(a1.AllocBytes-a0.AllocBytes) / mb, "alloc_B_per_MB_b": float64(b1.AllocBytes-b0.AllocBytes) / mb,
		"peak_heap_a": a1.Peak, "peak_heap_b": b1.Peak, "goroutines_a": a1.Goroutines, "goroutines_b": b1.Goroutines,
		"dead_peer_a": a1.DeadPeer, "dead_peer_b": b1.DeadPeer,
	}
}

func pct(d []time.Duration, p float64) float64 {
	slices.Sort(d)
	return float64(d[int(float64(len(d)-1)*p)].Microseconds())
}

func rtt(s net.Conn, b []byte) error {
	if _, err := s.Write(b); err != nil {
		return err
	}
	_, err := io.ReadFull(s, b)
	return err
}

// lat: 64-byte echo, n ops: on a held raw TCP/TLS connection (the
// baseline the hop adds to), on a held session, and on a fresh session
// per request (open + echo + close). Microseconds.
func lat(o *opts) result {
	b := make([]byte, 64)
	measure := func(n int, op func() error) []time.Duration {
		d := make([]time.Duration, 0, n)
		for range n {
			t := time.Now()
			if err := op(); err != nil {
				log.Fatal(err)
			}
			d = append(d, time.Since(t))
		}
		return d
	}
	raw, err := o.dialer(o.raw)(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	measure(200, func() error { return rtt(raw, b) })
	rawD := measure(o.n, func() error { return rtt(raw, b) })
	_ = raw.Close()
	c := o.newClient()
	defer c.Close()
	s := mustOpen(c, "echo")
	measure(200, func() error { return rtt(s, b) })
	held := measure(o.n, func() error { return rtt(s, b) })
	fresh := measure(o.n, func() error {
		f := mustOpen(c, "echo")
		defer f.Close()
		return rtt(f, b)
	})
	_ = s.Close()
	return result{
		"n": o.n, "raw_p50": pct(rawD, .5), "raw_p99": pct(rawD, .99),
		"held_p50": pct(held, .5), "held_p99": pct(held, .99),
		"held_added_p50": pct(held, .5) - pct(rawD, .5), "held_added_p99": pct(held, .99) - pct(rawD, .99),
		"fresh_p50": pct(fresh, .5), "fresh_p99": pct(fresh, .99),
	}
}

// mem: n idle sessions (each echoed 64 bytes, or preload bytes, then
// left open): heap (after GC) and goroutines per session on both sides.
func mem(o *opts) result {
	c := o.newClient()
	defer c.Close()
	_ = rtt(mustOpen(c, "echo"), make([]byte, 64)) // the connection(s) up
	a0, b0 := stats(o, true, false), o.peerStats(true, false)
	var ss []net.Conn
	for range o.n {
		s := mustOpen(c, "echo")
		if o.preload == 0 {
			if err := rtt(s, make([]byte, 64)); err != nil {
				log.Fatal(err)
			}
		} else {
			go func() {
				for off := 0; off < o.preload; off += 64 << 10 {
					_, _ = s.Write(chunk(int64(off), min(64<<10, o.preload-off)))
				}
			}()
			if _, err := readAll(s, int64(o.preload), true, nil); err != nil {
				log.Fatal(err)
			}
		}
		ss = append(ss, s)
	}
	time.Sleep(time.Second)
	a1, b1 := stats(o, true, false), o.peerStats(true, false)
	n := float64(o.n)
	runtime.KeepAlive(ss)
	return result{
		"n": o.n, "preload": o.preload,
		"heap_per_session_a": float64(int64(a1.Heap)-int64(a0.Heap)) / n, "heap_per_session_b": float64(int64(b1.Heap)-int64(b0.Heap)) / n,
		"goroutines_per_session_a": float64(a1.Goroutines-a0.Goroutines) / n, "goroutines_per_session_b": float64(b1.Goroutines-b0.Goroutines) / n,
	}
}

// busy: n sessions each streaming at full speed (listener sends): heap
// and goroutines per session sampled mid-transfer, against idle.
func busy(o *opts) result {
	c := o.newClient()
	defer c.Close()
	_ = rtt(mustOpen(c, "echo"), make([]byte, 64))
	a0, b0 := stats(o, true, false), o.peerStats(true, false)
	for range o.n {
		s := mustOpen(c, "src 0")
		go func() { _, _ = readAll(s, 0, false, nil) }()
	}
	time.Sleep(2 * time.Second)
	a1, b1 := stats(o, false, false), o.peerStats(false, false)
	n := float64(o.n)
	return result{
		"n":                  o.n,
		"heap_per_session_a": float64(int64(a1.Heap)-int64(a0.Heap)) / n, "heap_per_session_b": float64(int64(b1.Heap)-int64(b0.Heap)) / n,
		"goroutines_per_session_a": float64(a1.Goroutines-a0.Goroutines) / n, "goroutines_per_session_b": float64(b1.Goroutines-b0.Goroutines) / n,
	}
}

// iso (the F1 shape): one session whose reader stops for good (mode a:
// this side never reads the listener's endless stream; b: the listener
// never reads this side's writes), then 8 sessions must each move 16 MiB
// intact. Bounded at 60 s.
func iso(o *opts) result {
	c := o.newClient()
	defer c.Close()
	if o.mode == "a" {
		_ = mustOpen(c, "src 0")
	} else {
		s := mustOpen(c, "stall")
		go func() {
			for off := int64(0); ; off += 64 << 10 {
				if _, err := s.Write(chunk(off, 64<<10)); err != nil {
					return
				}
			}
		}()
	}
	time.Sleep(time.Second) // the stuck session's window is full
	const n = 16 << 20
	start := time.Now()
	var wg sync.WaitGroup
	ends := make([]string, 8)
	took := make([]float64, 8)
	for i := range 8 {
		wg.Go(func() {
			var err error
			if o.mode == "a" {
				s := mustOpen(c, "src "+itoa(n))
				_ = s.SetReadDeadline(time.Now().Add(60 * time.Second))
				var got int64
				got, err = readAll(s, n, true, nil)
				if err == nil && got != n {
					err = io.ErrUnexpectedEOF
				}
				_ = s.Close()
			} else {
				s := mustOpen(c, "sink")
				err = sinkN(s, n)
			}
			ends[i], took[i] = endKind(err), time.Since(start).Seconds()
		})
	}
	wg.Wait()
	done := 0
	for _, e := range ends {
		if e == "done" {
			done++
		}
	}
	return result{"mode": o.mode, "completed": done, "of": 8, "ends": ends, "slowest_s": slices.Max(took)}
}

// sinkN writes n bytes of the stream, half-closes, and checks the
// listener's count (60 s bound).
func sinkN(s net.Conn, n int64) error {
	defer s.Close()
	_ = s.SetReadDeadline(time.Now().Add(60 * time.Second))
	for off := int64(0); off < n; off += 64 << 10 {
		if _, err := s.Write(chunk(off, int(min(64<<10, n-off)))); err != nil {
			return err
		}
	}
	if cw, ok := s.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
	var b [8]byte
	if _, err := io.ReadFull(s, b[:]); err != nil {
		return err
	}
	if got := int64(binary.BigEndian.Uint64(b[:])); got != n {
		return io.ErrShortWrite
	}
	return nil
}

func itoa(n int) string { return strconv.Itoa(n) }

// fair: 8 sessions read the listener's endless streams for dur: bytes
// per session and each session's longest gap between reads.
func fair(o *opts) result {
	c := o.newClient()
	defer c.Close()
	const k = 8
	counts := make([]atomic.Int64, k)
	gaps := make([]atomic.Int64, k)
	ends := make([]string, k)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	start := time.Now()
	for i := range k {
		s := mustOpen(c, "src 0")
		wg.Go(func() {
			buf := make([]byte, 64<<10)
			last := time.Now()
			for {
				select {
				case <-stop:
					if g := time.Since(last); g.Nanoseconds() > gaps[i].Load() { // a session starved to the end
						gaps[i].Store(g.Nanoseconds())
					}
					_ = s.Close()
					return
				default:
				}
				_ = s.SetReadDeadline(time.Now().Add(time.Second))
				n, err := s.Read(buf)
				counts[i].Add(int64(n))
				if n > 0 {
					if g := time.Since(last); g.Nanoseconds() > gaps[i].Load() {
						gaps[i].Store(g.Nanoseconds())
					}
					last = time.Now()
				}
				if err != nil && endKind(err) != "hang" {
					ends[i] = fmt.Sprintf("%s at %.1fs", endKind(err), time.Since(start).Seconds())
					return
				}
			}
		})
	}
	time.Sleep(o.dur)
	close(stop)
	wg.Wait()
	var per, gap []float64
	for i := range k {
		per = append(per, float64(counts[i].Load())/(1<<20))
		gap = append(gap, float64(gaps[i].Load())/1e9)
	}
	return result{"secs": o.dur.Seconds(), "pool": o.pool, "MiB": per, "min_MiB": slices.Min(per), "max_MiB": slices.Max(per),
		"max_gap_s": slices.Max(gap), "ends": ends, "dead_peer_a": o.ev.deadPeerCount()}
}

func (e *events) deadPeerCount() int { _, d := e.snapshot(); return d }

// sessionEnds runs a reader per session and reports when (seconds after
// t0) and how each ended; "hang" = still open at the bound.
type ending struct {
	S    float64 `json:"s"`
	Kind string  `json:"kind"`
}

func watch(ss []net.Conn, t0 *atomic.Int64, bound time.Duration) func() []ending {
	out := make([]ending, len(ss))
	var wg sync.WaitGroup
	for i, s := range ss {
		wg.Go(func() {
			_ = s.SetReadDeadline(time.Now().Add(bound))
			_, err := readAll(s, 0, false, nil)
			out[i] = ending{S: float64(time.Now().UnixNano()-t0.Load()) / 1e9, Kind: endKind(err)}
		})
	}
	return func() []ending { wg.Wait(); return out }
}

func iptables(op, peer string) {
	for _, args := range [][]string{{op, "INPUT", "-s", peer, "-j", "DROP"}, {op, "OUTPUT", "-d", peer, "-j", "DROP"}} {
		if out, err := exec.Command("iptables", args...).CombinedOutput(); err != nil { // #nosec G204 -- the rig's own addresses
			log.Fatalf("iptables %v: %v %s", args, err, out)
		}
	}
}

func leakBase(o *opts) pstats { return stats(o, true, false) }

func leak(o *opts, base pstats) result {
	time.Sleep(3 * time.Second)
	now := stats(o, true, false)
	return result{"goroutines_before": base.Goroutines, "goroutines_after": now.Goroutines, "fds_before": base.FDs, "fds_after": now.FDs}
}

// openEcho opens a session and echoes 64 bytes (3 s bound): the probe for "back".
func openEcho(c client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := c.Open(ctx, []byte("echo"))
	if err != nil {
		return err
	}
	defer s.Close()
	_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
	return rtt(s, make([]byte, 64))
}

func waitBack(c client, from time.Time, bound time.Duration) float64 {
	for time.Since(from) < bound {
		if openEcho(c) == nil {
			return time.Since(from).Seconds()
		}
		time.Sleep(100 * time.Millisecond)
	}
	return -1
}

func host(addr string) string { h, _, _ := net.SplitHostPort(addr); return h }

func connEvents(evs []event, t0 int64) []ending {
	var out []ending
	for _, e := range evs {
		if e.T >= t0 {
			out = append(out, ending{S: float64(e.T-t0) / 1e9, Kind: e.Kind + ": " + e.Err})
		}
	}
	return out
}

// part: 4 idle and -sessions busy ones (the listener sends without end), then this side drops every packet to
// and from the listener (iptables, nothing closed) for dur, then heals.
// Reports when each end declared the connection dead, how the sessions
// ended, when a new session worked after the heal, and leaks.
func part(o *opts) result {
	base := leakBase(o)
	bBase := o.peerStats(true, false)
	c := o.newClient()
	var ss []net.Conn
	for range 4 {
		s := mustOpen(c, "echo")
		_ = rtt(s, make([]byte, 64))
		ss = append(ss, s)
	}
	for range o.sessions {
		ss = append(ss, mustOpen(c, "src 0"))
	}
	time.Sleep(2 * time.Second)
	var t0 atomic.Int64
	t0.Store(time.Now().UnixNano())
	wait := watch(ss, &t0, o.dur+30*time.Second)
	iptables("-A", host(o.addr))
	t0.Store(time.Now().UnixNano())
	time.Sleep(o.dur)
	iptables("-D", host(o.addr))
	healed := time.Now()
	back := waitBack(c, healed, 30*time.Second)
	ends := wait()
	evs, _ := o.ev.snapshot()
	b := o.peerStats(false, false)
	_ = c.Close()
	r := leak(o, base)
	bAfter := o.peerStats(true, false)
	r["ends"], r["conn_down_a"], r["conn_down_b"] = ends, connEvents(evs, t0.Load()), connEvents(b.Events, t0.Load())
	r["back_after_heal_s"], r["cut_s"], r["sessions"] = back, o.dur.Seconds(), o.sessions
	r["b_goroutines_before"], r["b_goroutines_after"], r["b_fds_before"], r["b_fds_after"] = bBase.Goroutines, bAfter.Goroutines, bBase.FDs, bAfter.FDs
	return r
}

// kill: 8 sessions downloading, then the listener's process SIGKILLs
// itself (/die); its supervisor restarts it after 1 s. Reports how and
// when each session ended, when a new session worked, and leaks here.
func kill(o *opts) result {
	base := leakBase(o)
	c := o.newClient()
	var ss []net.Conn
	for range 8 {
		ss = append(ss, mustOpen(c, "src 0"))
	}
	time.Sleep(2 * time.Second)
	var t0 atomic.Int64
	t0.Store(time.Now().UnixNano())
	wait := watch(ss, &t0, 30*time.Second)
	t0.Store(time.Now().UnixNano())
	o.peerCall("/die")
	ends := wait()
	back := waitBack(c, time.Unix(0, t0.Load()), 30*time.Second)
	evs, _ := o.ev.snapshot()
	_ = c.Close()
	r := leak(o, base)
	r["ends"], r["conn_down_a"], r["back_s"] = ends, connEvents(evs, t0.Load()), back
	return r
}

// drain: -sessions sessions each reading 16 MiB; after 1 MiB each the listener
// sends GOAWAY on its connections (/goaway, 30 s deadline). The old
// sessions must finish intact, 8 new sessions must open (on a new
// connection), and the old connections must close.
func drain(o *opts) result {
	c := o.newClient()
	defer c.Close()
	const n = 16 << 20
	var ss []net.Conn
	old := map[string]bool{}
	for range o.sessions {
		s := mustOpen(c, "src "+itoa(n))
		old[s.LocalAddr().String()] = true
		ss = append(ss, s)
	}
	for _, s := range ss {
		if _, err := readAll(s, 1<<20, true, nil); err != nil {
			log.Fatal(err)
		}
	}
	o.peerCall("/goaway")
	t0 := time.Now()
	var wg sync.WaitGroup
	ends := make([]string, len(ss))
	for i, s := range ss {
		wg.Go(func() {
			_ = s.SetReadDeadline(time.Now().Add(60 * time.Second))
			buf := make([]byte, 64<<10)
			off := int64(1 << 20)
			var err error
			for off < n {
				var k int
				k, err = s.Read(buf)
				if k > 0 && string(buf[:k]) != string(chunk(off, k)) {
					err = io.ErrUnexpectedEOF
				}
				off += int64(k)
				if err != nil {
					break
				}
			}
			if off == n {
				err = nil
			}
			ends[i] = endKind(err)
			_ = s.Close()
		})
	}
	newOK, newOther, newErr := 0, 0, []string{}
	for range 8 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		s, err := c.Open(ctx, []byte("echo"))
		cancel()
		if err == nil {
			_ = s.SetReadDeadline(time.Now().Add(5 * time.Second))
			err = rtt(s, make([]byte, 64))
			if err == nil && !old[s.LocalAddr().String()] {
				newOther++
			}
			_ = s.Close()
		}
		if err == nil {
			newOK++
		} else {
			newErr = append(newErr, err.Error())
		}
	}
	wg.Wait()
	finished := time.Since(t0).Seconds()
	time.Sleep(2 * time.Second)
	done := 0
	for _, e := range ends {
		if e == "done" {
			done++
		}
	}
	b := o.peerStats(false, false)
	return result{"sessions": len(ss), "pool": o.pool, "old_intact": done, "old_ends": ends, "old_finished_s": finished, "new_ok": newOK, "new_on_new_conn": newOther,
		"new_errors": newErr, "b_conns_after": b.Served}
}

// falsedead: 8 echo sessions busy (writing and reading) for dur, then
// idle (pings only) for dur; run under netem loss and a CPU limit. Counts
// dead-peer declarations on both sides and checks every session still
// echoes at the end.
func falsedead(o *opts) result {
	c := o.newClient()
	defer c.Close()
	var ss []net.Conn
	var moved atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		s := mustOpen(c, "echo")
		ss = append(ss, s)
		wg.Go(func() { // writer: 16 KiB at a time until stop
			b := make([]byte, 16<<10)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := s.Write(b); err != nil {
					return
				}
			}
		})
		wg.Go(func() { // reader: until stop and the echo drained
			buf := make([]byte, 64<<10)
			for {
				_ = s.SetReadDeadline(time.Now().Add(2 * time.Second))
				n, err := s.Read(buf)
				moved.Add(int64(n))
				if err != nil {
					select {
					case <-stop:
						if endKind(err) == "hang" {
							return
						}
					default:
						if endKind(err) == "hang" {
							continue
						}
					}
					return
				}
			}
		})
	}
	time.Sleep(o.dur)
	close(stop)
	wg.Wait()
	busyMB := float64(moved.Load()) / 1e6
	busyDead := o.ev.deadPeerCount()
	time.Sleep(o.dur)
	alive := 0
	for _, s := range ss {
		_ = s.SetReadDeadline(time.Now().Add(10 * time.Second))
		if rtt(s, make([]byte, 64)) == nil {
			alive++
		}
	}
	b := o.peerStats(false, false)
	evs, _ := o.ev.snapshot()
	return result{"busy_MB_echoed": busyMB, "dead_peer_a_busy": busyDead, "dead_peer_a": o.ev.deadPeerCount(), "dead_peer_b": b.DeadPeer,
		"sessions_alive_after_idle": alive, "conn_down_a": len(evs), "conn_down_b": len(b.Events)}
}
