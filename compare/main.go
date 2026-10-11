// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// Usage:
//
//	lc gencert -dir D
//	lc serve -lib link|yamux -tls -certs D -live default|fast -addr :7000 -raw :7001 -ctl :7002 [-gcpressure]
//	lc run   -lib ... -tls -certs D -live ... -addr host:7000 -raw host:7001 -ctl host:7002 -test NAME [...]
//
// run prints one JSON object (the case's result) on stdout.
func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: lc gencert|serve|run ...")
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	o := &opts{}
	fs.StringVar(&o.lib, "lib", "link", "link or yamux")
	fs.BoolVar(&o.tls, "tls", false, "mutual TLS 1.3 (else plaintext TCP)")
	fs.StringVar(&o.certs, "certs", "certs", "certificate directory (gencert)")
	fs.StringVar(&o.live, "live", "default", "liveness preset: default or fast")
	fs.StringVar(&o.addr, "addr", "127.0.0.1:7000", "listener's mux address")
	fs.StringVar(&o.raw, "raw", "127.0.0.1:7001", "listener's raw echo address (latency baseline)")
	fs.StringVar(&o.ctl, "ctl", "127.0.0.1:7002", "listener's control HTTP address")
	fs.BoolVar(&o.gcp, "gcpressure", false, "allocate garbage in the background (32 MiB live, 12.8 MB/s)")
	fs.StringVar(&o.test, "test", "tput", "run: tput lat mem busy iso fair part kill drain falsedead")
	fs.IntVar(&o.sessions, "sessions", 1, "run: sessions")
	fs.IntVar(&o.pool, "pool", 1, "run: connections at most")
	fs.DurationVar(&o.dur, "dur", 5*time.Second, "run: measured duration")
	fs.StringVar(&o.dir, "dir", "down", "tput: down (listener sends) or up")
	fs.IntVar(&o.n, "n", 1000, "lat: ops; mem: sessions")
	fs.IntVar(&o.preload, "preload", 0, "mem: bytes echoed per session before idling")
	fs.StringVar(&o.mode, "mode", "a", "iso: a (dialer stops reading) or b (listener stops reading)")
	_ = fs.Parse(os.Args[2:])
	l, ok := presets[o.live]
	if !ok {
		log.Fatalf("liveness %q", o.live)
	}
	ev := &events{}
	switch o.lib {
	case "link":
		o.L = &linkLib{live: l, ev: ev}
	case "yamux":
		o.L = &yamuxLib{live: l, ev: ev}
	default:
		log.Fatalf("lib %q", o.lib)
	}
	o.ev = ev
	go samplePeak()
	if o.gcp {
		go gcPressure()
	}
	switch os.Args[1] {
	case "gencert":
		genCert(o.certs)
	case "serve":
		serve(o)
	case "run":
		res := run(o)
		res["lib"], res["test"], res["tls"], res["live"] = o.lib, o.test, o.tls, o.live
		_ = json.NewEncoder(os.Stdout).Encode(res)
	default:
		log.Fatalf("command %q", os.Args[1])
	}
}

type opts struct {
	lib, certs, live, addr, raw, ctl, test, dir, mode string
	tls, gcp                                          bool
	sessions, pool, n, preload                        int
	dur                                               time.Duration
	L                                                 lib
	ev                                                *events
}

// ---- TLS: one self-signed P-256 certificate, both ends present and trust it.

func genCert(dir string) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), DNSNames: []string{"lc"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(30 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:        true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		log.Fatal(err)
	}
	kb, _ := x509.MarshalECPrivateKey(key)
	_ = os.MkdirAll(dir, 0o700) // nosemgrep: incorrect-default-permission -- a directory needs its execute bit; owner only
	_ = os.WriteFile(filepath.Join(dir, "cert.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "key.pem"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o644) // #nosec G306 -- throwaway test key
}

func tlsConfig(dir string) *tls.Config {
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"))
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert.Leaf)
	return &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: pool, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, ServerName: "lc", MinVersion: tls.VersionTLS13,
	}
}

func (o *opts) dialer(addr string) func(context.Context) (net.Conn, error) {
	var tc *tls.Config
	if o.tls {
		tc = tlsConfig(o.certs)
	}
	return func(ctx context.Context) (net.Conn, error) {
		d := &net.Dialer{Timeout: 3 * time.Second}
		if tc == nil {
			return d.DialContext(ctx, "tcp", addr)
		}
		return (&tls.Dialer{NetDialer: d, Config: tc}).DialContext(ctx, "tcp", addr)
	}
}

func (o *opts) listen(addr string) net.Listener {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	if o.tls {
		return tls.NewListener(ln, tlsConfig(o.certs))
	}
	return ln
}

// ---- deterministic byte stream: byte at offset i is i mod 251.

var pat = func() []byte {
	b := make([]byte, 64<<10+251)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}()

func chunk(off int64, n int) []byte { k := int(off % 251); return pat[k : k+n] }

// ---- listener side

func serve(o *opts) {
	ln := o.listen(o.addr)
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				log.Fatal(err)
			}
			go o.L.serve(nc, handle)
		}
	}()
	raw := o.listen(o.raw)
	go func() {
		for {
			nc, err := raw.Accept()
			if err != nil {
				log.Fatal(err)
			}
			go func() { echo(nc); _ = nc.Close() }()
		}
	}()
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(stats(o, r.URL.Query().Get("gc") != "", r.URL.Query().Get("reset") != ""))
	})
	http.HandleFunc("/goaway", func(w http.ResponseWriter, _ *http.Request) { o.L.goAwayAll(time.Now().Add(30 * time.Second)) })
	http.HandleFunc("/die", func(http.ResponseWriter, *http.Request) { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL) })
	srv := &http.Server{Addr: o.ctl, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

// handle serves one session by its header: "echo", "src N" (N bytes of
// the stream, 0 = without end), "sink" (reads to EOF, checks the stream,
// answers the count, high bit set if a byte was wrong), "stall" (never reads).
func handle(hdr []byte, s net.Conn) {
	defer s.Close()
	f := strings.Fields(string(hdr))
	switch f[0] {
	case "echo":
		echo(s)
	case "src":
		n, _ := strconv.ParseInt(f[1], 10, 64)
		for off := int64(0); n == 0 || off < n; {
			k := 64 << 10
			if n > 0 {
				k = int(min(int64(k), n-off))
			}
			if _, err := s.Write(chunk(off, k)); err != nil {
				log.Printf("src: write after %d bytes: %v", off, err)
				return
			}
			off += int64(k)
		}
	case "sink":
		buf := make([]byte, 64<<10)
		var off int64
		bad := false
		for {
			k, err := s.Read(buf)
			if k > 0 {
				bad = bad || string(buf[:k]) != string(chunk(off, k))
				off += int64(k)
			}
			if err != nil {
				break
			}
		}
		if bad {
			off |= 1 << 62
		}
		_, _ = s.Write(binary.BigEndian.AppendUint64(nil, uint64(off)))
	case "stall":
		time.Sleep(10 * time.Minute)
	}
}

func echo(s net.Conn) {
	buf := make([]byte, 64<<10)
	for {
		k, err := s.Read(buf)
		if k > 0 {
			if _, werr := s.Write(buf[:k]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// ---- process stats: CPU, heap, allocations, goroutines, FDs.

var peak atomic.Uint64

func heapObjects() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

func samplePeak() {
	for range time.Tick(10 * time.Millisecond) {
		if h := heapObjects(); h > peak.Load() {
			peak.Store(h)
		}
	}
}

func gcPressure() {
	ring := make([][]byte, 512)
	for i := 0; ; i++ {
		ring[i%len(ring)] = make([]byte, 64<<10)
		time.Sleep(5 * time.Millisecond)
	}
}

type pstats struct {
	T          int64   `json:"t"`
	CPU        float64 `json:"cpu_s"`
	Heap       uint64  `json:"heap"` // live heap objects (after GC when asked)
	Peak       uint64  `json:"peak_heap"`
	Mallocs    uint64  `json:"mallocs"`
	AllocBytes uint64  `json:"alloc_bytes"`
	Goroutines int     `json:"goroutines"`
	FDs        int     `json:"fds"`
	Served     int     `json:"served_conns"`
	DeadPeer   int     `json:"dead_peer"`
	Events     []event `json:"events,omitempty"`
}

func stats(o *opts, gc, reset bool) pstats {
	if gc {
		runtime.GC()
		runtime.GC()
	}
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	ms := []metrics.Sample{{Name: "/gc/heap/allocs:objects"}, {Name: "/gc/heap/allocs:bytes"}}
	metrics.Read(ms)
	fds, _ := os.ReadDir("/dev/fd")
	evs, dead := o.ev.snapshot()
	st := pstats{
		T:   time.Now().UnixNano(),
		CPU: float64(ru.Utime.Nano()+ru.Stime.Nano()) / 1e9, Heap: heapObjects(), Peak: peak.Load(),
		Mallocs: ms[0].Value.Uint64(), AllocBytes: ms[1].Value.Uint64(),
		Goroutines: runtime.NumGoroutine(), FDs: len(fds), Served: o.L.served(), DeadPeer: dead, Events: evs,
	}
	if reset {
		peak.Store(0)
	}
	return st
}

func (o *opts) peerStats(gc, reset bool) pstats {
	var st pstats
	c := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	q := "?"
	if gc {
		q += "gc=1&"
	}
	if reset {
		q += "reset=1"
	}
	resp, err := c.Get("http://" + o.ctl + "/stats" + q)
	if err != nil {
		st.T = -1
		return st
	}
	defer resp.Body.Close()
	_ = json.NewDecoder(resp.Body).Decode(&st)
	return st
}

func (o *opts) peerCall(path string) {
	c := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	if resp, err := c.Get("http://" + o.ctl + path); err == nil {
		resp.Body.Close()
	}
}

// endKind names how a read loop ended.
func endKind(err error) string {
	switch {
	case err == nil:
		return "done"
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, os.ErrDeadlineExceeded):
		return "hang"
	}
	var ne net.Error // yamux's deadline error is a net.Error, not os.ErrDeadlineExceeded
	if errors.As(err, &ne) && ne.Timeout() {
		return "hang"
	}
	return "err: " + err.Error()
}

var errHang = fmt.Errorf("no end within the bound: %w", os.ErrDeadlineExceeded)

// readAll reads s until it ends or until n bytes (n > 0); verify checks
// the stream. It returns bytes read and how it ended (nil = n bytes, all intact).
func readAll(s net.Conn, n int64, verify bool, count *atomic.Int64) (int64, error) {
	buf := make([]byte, 64<<10)
	var off int64
	for n == 0 || off < n {
		k, err := s.Read(buf)
		if k > 0 {
			if verify && string(buf[:k]) != string(chunk(off, k)) {
				return off, errors.New("corrupt")
			}
			off += int64(k)
			if count != nil {
				count.Add(int64(k))
			}
		}
		if err != nil {
			return off, err
		}
	}
	return off, nil
}

func mustOpen(c client, hdr string) net.Conn {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := c.Open(ctx, []byte(hdr))
	if err != nil {
		log.Fatalf("open %q: %v", hdr, err)
	}
	return s
}
