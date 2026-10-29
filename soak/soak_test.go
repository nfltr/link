// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package soak runs link for hours under faults (make soak, SOAK.md):
// peerlink.Node over TCP and quiclink over UDP, each in both directions,
// echo sessions of mixed sizes checked by SHA-256, stuck readers, cuts,
// throttling, black holes (the dead-peer bound measured), address
// rebinds (QUIC) and a fresh pair with a drain every epoch; between
// epochs it samples goroutines, file descriptors and heap. It writes a
// JSON summary and fails on any unexpected error, integrity failure,
// dead-peer bound missed or leak.
package soak

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	mrand "math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/peerlinktest"
	"github.com/nfltr/link/peerlink/quiclink"
	quic "github.com/nfltr/quic-go"
)

// Knobs (environment): SOAKTIME the whole run (unset: the test skips),
// SOAKEPOCH one epoch (5m), SOAKOUT the summary's path, SOAKWORKERS echo
// workers per pair (6), SOAKRATE each pair's path rate in bytes/s (8 MiB).
func env(name string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(name)); err == nil {
		return d
	}
	return def
}

func envInt(name string, def int) int {
	var n int
	if _, err := fmt.Sscan(os.Getenv(name), &n); err == nil && n > 0 {
		return n
	}
	return def
}

const slack = 2 * time.Second // on top of a dead-peer bound: scheduling, a loaded host

type pair struct {
	a, b      peerlink.Link
	reverse   bool
	deadAfter time.Duration
	blackHole func()
	cut       func()
	throttle  func(int)
	rebind    func() // nil: none
}

type config struct {
	name    string
	newPair func(t *testing.T) pair
	// epoch, when set, runs the config's epoch instead of runEpoch (the
	// chain: chain_test.go).
	epoch func(t *testing.T, d time.Duration, st *stats)
}

// stats is one config's tally over the run.
type stats struct {
	Ops               int64    `json:"ops"`
	Bytes             int64    `json:"bytes_echoed"`
	ErrorsExpected    int64    `json:"errors_in_fault_windows"`
	ErrorsUnexpected  int64    `json:"errors_unexpected"`
	IntegrityFailures int64    `json:"integrity_failures"`
	Cuts              int      `json:"cuts"`
	Throttles         int      `json:"throttles"`
	BlackHoles        int      `json:"black_holes"`
	Rebinds           int      `json:"rebinds"`
	Drains            int      `json:"drains"`
	DrainErrors       int      `json:"drain_errors"`
	DeadAfterBound    float64  `json:"dead_after_bound_s"`
	DeadDetectMax     float64  `json:"dead_detect_max_s"`
	DeadDetectMisses  int      `json:"dead_detect_misses"`
	StalledMinutes    int      `json:"stalled_minutes"`
	InFlightMax       int64    `json:"in_flight_max,omitempty"`   // chain: a stalled far reader's bytes in flight
	InFlightBound     int64    `json:"in_flight_bound,omitempty"` // and its bound
	Aborts            []string `json:"first_aborts,omitempty"`    // chain: the first typed aborts the origin read in fault windows
	FirstErrors       []string `json:"first_unexpected_errors,omitempty"`
	mu                sync.Mutex
}

func (s *stats) unexpected(err error) {
	atomic.AddInt64(&s.ErrorsUnexpected, 1)
	s.mu.Lock()
	if len(s.FirstErrors) < 10 {
		s.FirstErrors = append(s.FirstErrors, fmt.Sprintf("%s: %v", time.Now().Format(time.TimeOnly), err))
	}
	s.mu.Unlock()
}

type sample struct {
	Epoch      int    `json:"epoch"`
	Goroutines int    `json:"goroutines"`
	FDs        int    `json:"fds"`
	HeapInuse  uint64 `json:"heap_inuse"`
}

type summary struct {
	Start     time.Time         `json:"start"`
	Duration  float64           `json:"duration_s"`
	Epoch     float64           `json:"epoch_s"`
	Epochs    int               `json:"epochs"`
	GoVersion string            `json:"go_version"`
	OS        string            `json:"os"`
	CPUs      int               `json:"cpus"`
	Configs   map[string]*stats `json:"configs"`
	Samples   []sample          `json:"samples"`
	Failures  []string          `json:"failures"`
	Pass      bool              `json:"pass"`
}

func TestSoak(t *testing.T) {
	total := env("SOAKTIME", 0)
	if total <= 0 {
		t.Skip("SOAKTIME unset (make soak SOAKTIME=1h)")
	}
	epoch := env("SOAKEPOCH", 5*time.Minute)
	configs := []config{
		{"link-forward", func(t *testing.T) pair { return linkPair(t, false) }, nil},
		{"link-reverse", func(t *testing.T) pair { return linkPair(t, true) }, nil},
		{"quic-forward", func(t *testing.T) pair { return quicPair(t, false) }, nil},
		{"quic-reverse", func(t *testing.T) pair { return quicPair(t, true) }, nil},
		{"chain-3hop", nil, runChainEpoch},
	}
	sum := &summary{Start: time.Now(), Epoch: epoch.Seconds(), GoVersion: runtime.Version(),
		OS: runtime.GOOS + "/" + runtime.GOARCH, CPUs: runtime.NumCPU(), Configs: map[string]*stats{}}
	for _, c := range configs {
		sum.Configs[c.name] = &stats{}
	}
	sum.Samples = append(sum.Samples, sampleNow(0))
	for e := 1; time.Since(sum.Start) < total; e++ {
		t.Run(fmt.Sprintf("epoch%d", e), func(t *testing.T) {
			var wg sync.WaitGroup
			for _, c := range configs {
				wg.Go(func() {
					if c.epoch != nil {
						c.epoch(t, epoch, sum.Configs[c.name])
						return
					}
					runEpoch(t, c, epoch, sum.Configs[c.name])
				})
			}
			wg.Wait()
		})
		// Everything of the epoch is drained and its proxies closed: let
		// the last goroutines (timers, closing sockets) end, then sample.
		var s sample
		for range 20 {
			if s = sampleNow(e); len(sum.Samples) < 2 || s.Goroutines <= sum.Samples[1].Goroutines+10 {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		sum.Samples = append(sum.Samples, s)
		sum.Epochs = e
		t.Logf("epoch %d: %+v", e, s)
		write(t, sum, total)
	}
	sum.Duration = time.Since(sum.Start).Seconds()
	judge(sum)
	write(t, sum, total)
	for _, f := range sum.Failures {
		t.Error(f)
	}
}

// judge fills Failures and Pass.
func judge(sum *summary) {
	for name, s := range sum.Configs {
		if s.ErrorsUnexpected > 0 {
			sum.Failures = append(sum.Failures, fmt.Sprintf("%s: %d unexpected errors (first: %v)", name, s.ErrorsUnexpected, s.FirstErrors))
		}
		if s.IntegrityFailures > 0 {
			sum.Failures = append(sum.Failures, fmt.Sprintf("%s: %d echoes came back different", name, s.IntegrityFailures))
		}
		if s.DeadDetectMisses > 0 {
			sum.Failures = append(sum.Failures, fmt.Sprintf("%s: %d sessions outlived the dead-peer bound %.1fs (+%v)", name, s.DeadDetectMisses, s.DeadAfterBound, slack))
		}
		if s.StalledMinutes > 0 {
			sum.Failures = append(sum.Failures, fmt.Sprintf("%s: %d minutes outside faults with no echo completed", name, s.StalledMinutes))
		}
		if s.InFlightMax > s.InFlightBound && s.InFlightBound > 0 {
			sum.Failures = append(sum.Failures, fmt.Sprintf("%s: %d bytes in flight for a stalled far reader, bound %d", name, s.InFlightMax, s.InFlightBound))
		}
		if s.DrainErrors > 0 {
			sum.Failures = append(sum.Failures, fmt.Sprintf("%s: %d drains did not finish", name, s.DrainErrors))
		}
	}
	// Leaks: the first epoch is the warm baseline; the last must be back
	// to it (goroutines, FDs exactly or nearly; heap within 2x + 16 MiB).
	if n := len(sum.Samples); n >= 3 {
		base, last := sum.Samples[1], sum.Samples[n-1]
		if last.Goroutines > base.Goroutines+10 {
			sum.Failures = append(sum.Failures, fmt.Sprintf("goroutines %d after epoch 1, %d at the end", base.Goroutines, last.Goroutines))
		}
		if last.FDs > base.FDs+8 {
			sum.Failures = append(sum.Failures, fmt.Sprintf("file descriptors %d after epoch 1, %d at the end", base.FDs, last.FDs))
		}
		if last.HeapInuse > 2*base.HeapInuse+16<<20 {
			sum.Failures = append(sum.Failures, fmt.Sprintf("heap in use %d after epoch 1, %d at the end", base.HeapInuse, last.HeapInuse))
		}
	}
	sum.Pass = len(sum.Failures) == 0
}

func write(t *testing.T, sum *summary, total time.Duration) {
	out := os.Getenv("SOAKOUT")
	if out == "" {
		out = filepath.Join("out", fmt.Sprintf("soak-%s.json", sum.Start.Format("20060102-150405")))
	}
	if sum.Duration == 0 {
		sum.Duration = time.Since(sum.Start).Seconds()
	}
	b, err := json.MarshalIndent(sum, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	_ = os.MkdirAll(filepath.Dir(out), 0o750)
	if err := os.WriteFile(out, append(b, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func sampleNow(epoch int) sample {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fds, _ := os.ReadDir("/dev/fd")
	return sample{Epoch: epoch, Goroutines: runtime.NumGoroutine(), FDs: len(fds), HeapInuse: ms.HeapInuse}
}

// windows are the spans in which errors are expected (a fault and its bound).
type windows struct {
	mu    sync.Mutex
	spans [][2]time.Time
}

func (w *windows) add(from, to time.Time) {
	w.mu.Lock()
	w.spans = append(w.spans, [2]time.Time{from, to})
	w.mu.Unlock()
}

// covers: the op that ran from start to end overlapped a fault window.
func (w *windows) covers(start, end time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, s := range w.spans {
		if start.Before(s[1]) && end.After(s[0]) {
			return true
		}
	}
	return false
}

// runEpoch runs one pair of config c for d: echo workers, stuck readers,
// a cut, a throttle, a rebind (QUIC), a black hole with the dead-peer
// bound measured, then a drain of both Links.
func runEpoch(t *testing.T, c config, d time.Duration, st *stats) {
	p := c.newPair(t)
	st.mu.Lock()
	st.DeadAfterBound = p.deadAfter.Seconds()
	st.mu.Unlock()
	rate := envInt("SOAKRATE", 8<<20)
	p.throttle(rate)
	probes := &probeQueue{}
	served := make(chan error, 2)
	go func() { served <- p.b.Serve(func(_ string, h []byte, s peerlink.Session) { accept(probes, h, s) }) }()
	if p.reverse {
		go func() { served <- p.a.Serve(func(_ string, _ []byte, s peerlink.Session) { _ = s.Close() }) }()
	}
	start := time.Now()
	stop := make(chan struct{})
	faults := &windows{}
	var done atomic.Int64 // echoes completed, for the stall check
	var wg sync.WaitGroup

	// The pair is up once an echo goes through (a reverse Open waits for
	// the dialler's connection).
	if err := echo(p, 1<<10, st); err != nil {
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
				if err := echo(p, size(r), st); err != nil {
					if faults.covers(began, time.Now()) {
						atomic.AddInt64(&st.ErrorsExpected, 1)
					} else {
						st.unexpected(err)
					}
					time.Sleep(100 * time.Millisecond) // a cut path: do not spin
					continue
				}
				done.Add(1)
			}
		})
	}
	// Stuck readers: two sessions the accepter writes to and the opener
	// never reads; the echoes beside them must keep moving.
	var stuck []net.Conn
	for range 2 {
		if s, err := openTo(p, "stuck"); err == nil {
			stuck = append(stuck, s)
		}
	}
	// Stalls: a minute outside the fault windows with no echo completed.
	wg.Go(func() {
		tk := time.NewTicker(time.Minute)
		defer tk.Stop()
		last, from := done.Load(), time.Now()
		for {
			select {
			case <-stop:
				return
			case now := <-tk.C:
				if n := done.Load(); n == last && !faults.covers(from, now) {
					st.mu.Lock()
					st.StalledMinutes++
					st.mu.Unlock()
				} else {
					last = n
				}
				from = now
			}
		}
	})

	at := func(f float64) <-chan time.Time {
		return time.After(time.Until(start.Add(time.Duration(f * float64(d)))))
	}
	bound := p.deadAfter + slack
	<-at(0.25)
	faults.add(time.Now(), time.Now().Add(bound+10*time.Second))
	p.cut()
	st.mu.Lock()
	st.Cuts++
	st.mu.Unlock()
	<-at(0.45)
	p.throttle(1 << 20)
	<-at(0.55)
	p.throttle(rate)
	st.mu.Lock()
	st.Throttles++
	st.mu.Unlock()
	if p.rebind != nil {
		<-at(0.65)
		p.rebind()
		st.mu.Lock()
		st.Rebinds++
		st.mu.Unlock()
	}
	// Black hole: idle probe sessions, one reading on each end, must read
	// an error within the bound.
	<-at(0.8)
	ends := probe(p, probes, 3)
	<-at(0.85)
	holed := time.Now()
	faults.add(holed, start.Add(d).Add(time.Hour))
	p.blackHole()
	st.mu.Lock()
	st.BlackHoles++
	st.mu.Unlock()
	for _, ch := range ends {
		select {
		case at := <-ch:
			st.mu.Lock()
			st.DeadDetectMax = max(st.DeadDetectMax, at.Sub(holed).Seconds())
			st.mu.Unlock()
		case <-time.After(time.Until(holed.Add(bound))):
			st.mu.Lock()
			st.DeadDetectMisses++
			st.mu.Unlock()
		}
	}
	<-at(1)
	close(stop)
	for _, s := range stuck {
		_ = s.Close()
	}
	// Drain: the black-holed sessions are cut at its deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = p.a.Drain(ctx)
	_ = p.b.Drain(ctx)
	cancel()
	wg.Wait()
	for range 1 + btoi(p.reverse) {
		select {
		case <-served:
		case <-time.After(30 * time.Second):
			st.mu.Lock()
			st.DrainErrors++
			st.mu.Unlock()
		}
	}
	st.mu.Lock()
	st.Drains++
	st.mu.Unlock()
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// size draws an echo's size: small (under 1 KiB), medium (up to 64 KiB)
// or large (up to 4 MiB).
func size(r *mrand.Rand) int {
	switch x := r.IntN(10); {
	case x < 4:
		return r.IntN(1 << 10)
	case x < 8:
		return 1<<10 + r.IntN(63<<10)
	default:
		return 64<<10 + r.IntN(4<<20-64<<10)
	}
}

func openTo(p pair, header string) (peerlink.Session, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return p.a.Open(ctx, "b", []byte(header))
}

// echo sends n random bytes on a new session and checks that the same
// bytes came back.
func echo(p pair, n int, st *stats) error {
	s, err := openTo(p, "echo")
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer s.Close()
	payload := make([]byte, n)
	_, _ = rand.Read(payload)
	want := sha256.Sum256(payload)
	werr := make(chan error, 1)
	go func() {
		_, err := s.Write(payload)
		if err == nil {
			err = s.CloseWrite()
		}
		werr <- err
	}()
	h := sha256.New()
	got, rerr := io.Copy(h, s)
	if err := <-werr; err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if rerr != nil {
		return fmt.Errorf("read after %d of %d bytes: %w", got, n, rerr)
	}
	atomic.AddInt64(&st.Ops, 1)
	atomic.AddInt64(&st.Bytes, got)
	if got != int64(n) || !bytes.Equal(h.Sum(nil), want[:]) {
		atomic.AddInt64(&st.IntegrityFailures, 1)
	}
	return nil
}

// accept serves B's sessions by header: echo copies back, stuck writes
// until the session ends, probe reads until it ends and reports when.
func accept(probes *probeQueue, header []byte, s net.Conn) {
	defer s.Close()
	switch string(header) {
	case "echo":
		_, _ = io.Copy(s, s)
	case "stuck":
		buf := make([]byte, 32<<10)
		for {
			if _, err := s.Write(buf); err != nil {
				return
			}
		}
	case "probe":
		_, _ = io.Copy(io.Discard, s)
		if ch := probes.get(); ch != nil {
			ch <- time.Now()
		}
	}
}

// probeQueue hands the accepter's end of a probe a channel to report on
// (any of them: they all report a time).
type probeQueue struct {
	mu sync.Mutex
	ch []chan time.Time
}

func (q *probeQueue) put(ch chan time.Time) {
	q.mu.Lock()
	q.ch = append(q.ch, ch)
	q.mu.Unlock()
}

func (q *probeQueue) get() chan time.Time {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.ch) == 0 {
		return nil
	}
	ch := q.ch[0]
	q.ch = q.ch[1:]
	return ch
}

// probe opens n idle sessions; each returned channel gets the time its
// opener's read ended, and one the time an accepter's read ended.
func probe(p pair, q *probeQueue, n int) []chan time.Time {
	var out []chan time.Time
	for range n {
		acc := make(chan time.Time, 1)
		q.put(acc)
		s, err := openTo(p, "probe")
		if err != nil {
			continue
		}
		mine := make(chan time.Time, 1)
		go func() {
			_, _ = io.Copy(io.Discard, s)
			mine <- time.Now()
			_ = s.Close()
		}()
		out = append(out, mine, acc)
	}
	return out
}

// --- pairs ---

// nameAuth sends a name and takes the one it is sent (it proves nothing:
// the soak's peers are on loopback).
type nameAuth string

func (a nameAuth) Hello(context.Context, tls.ConnectionState) (byte, []byte, error) {
	return 128, []byte(a), nil
}

func (nameAuth) Verify(_ context.Context, _ tls.ConnectionState, method byte, payload []byte) (link.Identity, error) {
	if method != 128 {
		return link.Identity{}, fmt.Errorf("method %d", method)
	}
	return link.Identity{Method: "name", Subject: string(payload)}, nil
}

// linkPair: peerlink.Node over TCP through a peerlinktest.Proxy; A opens
// to "b". Reverse: A listens without Dial and B keeps a connection to it.
func linkPair(t *testing.T, reverse bool) pair {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	px := peerlinktest.NewProxy(t, ln.Addr().String())
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", px.Addr())
	}
	l := link.LivenessFast
	var a, b *peerlink.Node
	if reverse {
		a = mustPeer(t, peerlink.Config{Listener: ln, Link: link.Config{Liveness: l, Auth: nameAuth("a")}})
		b = mustPeer(t, peerlink.Config{Link: link.Config{Liveness: l, Auth: nameAuth("b")}, Connect: []string{"a"}, Dial: dial,
			Pool: link.PoolConfig{Backoff: func(int) time.Duration { return 100 * time.Millisecond }}})
	} else {
		b = mustPeer(t, peerlink.Config{Listener: ln, Link: link.Config{Liveness: l, Auth: nameAuth("b")}})
		a = mustPeer(t, peerlink.Config{Link: link.Config{Liveness: l, Auth: nameAuth("a")}, Dial: dial,
			Pool: link.PoolConfig{Backoff: func(int) time.Duration { return 100 * time.Millisecond }}})
	}
	return pair{a: a, b: b, reverse: reverse, deadAfter: a.DeadAfter(), blackHole: px.BlackHole, cut: px.Cut, throttle: px.Throttle}
}

// quicPair: quiclink over UDP through a peerlinktest.UDPProxy, mutual TLS
// naming the peers.
func quicPair(t *testing.T, reverse bool) pair {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	px := peerlinktest.NewUDPProxy(t, pc.LocalAddr().String())
	l := link.LivenessFast
	dial := func(name, peer string) func(context.Context, string, *quic.Config) (*quic.Conn, error) {
		return func(ctx context.Context, _ string, qc *quic.Config) (*quic.Conn, error) {
			return quic.DialAddr(ctx, px.Addr(), clientTLS(name, peer), qc)
		}
	}
	var a, b *quiclink.Node
	if reverse {
		a = mustQuic(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("a"), Liveness: l})
		b = mustQuic(t, quiclink.Config{Dial: dial("b", "a"), Connect: []string{"a"}, Liveness: l})
	} else {
		b = mustQuic(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: l})
		a = mustQuic(t, quiclink.Config{Dial: dial("a", "b"), Liveness: l})
	}
	return pair{a: a, b: b, reverse: reverse, deadAfter: a.DeadAfter(), blackHole: px.BlackHole, cut: px.Cut, throttle: px.Throttle, rebind: px.Rebind}
}

// pki: a throwaway CA and a certificate for each of "a" and "b".
var pki = sync.OnceValue(func() struct {
	pool  *x509.CertPool
	certs map[string]tls.Certificate
} {
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "soak-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(30 * 24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	ca, _ := x509.ParseCertificate(caDER)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	certs := map[string]tls.Certificate{}
	for i, name := range []string{"a", "b"} {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: name},
			DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(30 * 24 * time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der, _ := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		certs[name] = tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}
	return struct {
		pool  *x509.CertPool
		certs map[string]tls.Certificate
	}{pool, certs}
})

func serverTLS(name string) *tls.Config {
	p := pki()
	return &tls.Config{Certificates: []tls.Certificate{p.certs[name]}, ClientCAs: p.pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}
}

func clientTLS(name, peer string) *tls.Config {
	p := pki()
	return &tls.Config{Certificates: []tls.Certificate{p.certs[name]}, RootCAs: p.pool, ServerName: peer,
		NextProtos: []string{quiclink.ALPN}, MinVersion: tls.VersionTLS13}
}

func mustPeer(t testing.TB, cfg peerlink.Config) *peerlink.Node {
	t.Helper()
	n, err := peerlink.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func mustQuic(t testing.TB, cfg quiclink.Config) *quiclink.Node {
	t.Helper()
	n, err := quiclink.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
