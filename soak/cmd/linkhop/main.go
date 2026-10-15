// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Command linkhop runs one role of a chain soak across machines (pods):
// a path origin -> hop -> ... -> echo of peerlink.Links, each hop a
// peerlink.Forward. By default over plaintext TCP with link.NameAuth (a
// trusted network such as a cluster's pod network); -tls-dir makes it
// mutual TLS (link.CertAuth) from ca.crt, tls.crt and tls.key in that
// directory (a mounted Kubernetes Secret, say), and -transport quic runs
// it over quiclink (UDP; -tls-dir required).
//
//	linkhop -role echo   -listen :7000
//	linkhop -role hop    -listen :7000 -next echo-0.echo:7000
//	linkhop -role origin -next hop-0.hop:7000 -duration 1h -out /out/chain.json
//
// The origin runs -workers echo loops of mixed sizes, each checked by
// SHA-256 after the far end's answer, and measures what the path holds
// for a stalled far reader every -inflight; it writes a JSON summary
// (errors by kind, *link.Abort code and hop; throughput, echo latency)
// and exits 1 on an integrity failure, on no echo completed, or on an
// in-flight measure over -bound. Faults are the caller's (kill or drain a
// hop pod, a NetworkPolicy): errors during them are counted, never
// fatal. hop and echo run until SIGTERM, then drain (-drain). -metrics
// serves each role's Prometheus text on /metrics.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	mrand "math/rand/v2" // nosemgrep: math-random-used -- echo sizes for a soak, not a secret
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
	"github.com/nfltr/link/soak/chainsoak"
	"github.com/quic-go/quic-go"
)

type summary struct {
	Start             time.Time        `json:"start"`
	Duration          float64          `json:"duration_s"`
	Hops              int              `json:"hops"`
	Transport         string           `json:"transport"`
	Echoes            int64            `json:"echoes"`
	Bytes             int64            `json:"bytes_echoed"`
	Throughput        float64          `json:"throughput_bytes_per_s"`
	EchoP50           float64          `json:"echo_p50_ms"`
	EchoP99           float64          `json:"echo_p99_ms"`
	IntegrityFailures int64            `json:"integrity_failures"`
	Errors            map[string]int64 `json:"errors"` // "abort code=N hops=H refused=B" or "other"
	InFlightMax       int64            `json:"in_flight_max"`
	InFlightBound     int64            `json:"in_flight_bound"`
	Pass              bool             `json:"pass"`
}

type opts struct {
	role, listen, next, name string
	transport, tlsDir, tls   string // tls: the server name
	workers, hops            int
	duration, inflight       time.Duration
	drain                    time.Duration
	bound                    int64
}

func main() {
	var o opts
	flag.StringVar(&o.role, "role", "", "origin, hop or echo")
	flag.StringVar(&o.listen, "listen", "", "hop, echo: the address to take connections on")
	flag.StringVar(&o.next, "next", "", "origin, hop: the next hop's address")
	flag.StringVar(&o.name, "name", "", "this node's name (link.NameAuth, plaintext TCP only); default the role")
	flag.StringVar(&o.transport, "transport", "tcp", "tcp, or quic (needs -tls-dir)")
	flag.StringVar(&o.tlsDir, "tls-dir", "", "mutual TLS from ca.crt, tls.crt and tls.key in this directory (empty: plaintext TCP)")
	flag.StringVar(&o.tls, "tls-name", "", "the server name the next hop's certificate is verified for (default: -next's host)")
	flag.IntVar(&o.workers, "workers", 6, "origin: echo loops")
	flag.IntVar(&o.hops, "hops", 0, "origin: the path's hops between it and the echo, for the summary and metrics")
	hopIndex := flag.Int("hop-index", 0, "this role's place on the path for its metrics: origin 0, hops 1..N from the origin, echo N+1")
	metricsAddr := flag.String("metrics", "", "serve Prometheus text on this address's /metrics (empty: none)")
	flag.DurationVar(&o.duration, "duration", time.Hour, "origin: how long to run")
	flag.DurationVar(&o.inflight, "inflight", time.Minute, "origin: how often to measure a stalled far reader's in-flight bytes (0: never)")
	flag.Int64Var(&o.bound, "bound", 0, "origin: the in-flight bound in bytes (0: unchecked), e.g. links x 1 MiB + hops x 256 KiB")
	flag.DurationVar(&o.drain, "drain", 30*time.Second, "hop, echo: the drain's deadline after SIGTERM")
	out := flag.String("out", "", "origin: the JSON summary's path (default stdout)")
	flag.Parse()
	if o.role != "origin" && o.role != "hop" && o.role != "echo" {
		fmt.Fprintln(os.Stderr, "linkhop: -role origin, hop or echo")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	m := newMetrics(o.role, *hopIndex)
	if *metricsAddr != "" {
		srv := &http.Server{Addr: *metricsAddr, Handler: m, ReadHeaderTimeout: 10 * time.Second}
		go func() { check(srv.ListenAndServe()) }()
	}
	n, _, err := newNode(o, m)
	check(err)
	if o.role != "origin" {
		check(serve(ctx, n, o, m))
		return
	}
	sum := origin(ctx, n, o, m)
	b, _ := json.MarshalIndent(sum, "", "  ")
	if *out == "" {
		fmt.Println(string(b))
	} else {
		check(os.WriteFile(*out, append(b, '\n'), 0o600))
	}
	if !sum.Pass {
		os.Exit(1)
	}
}

// newNode is o's Link, and the address it listens on (hop, echo).
func newNode(o opts, m *metrics) (peerlink.Link, string, error) {
	var server, client *tls.Config
	if o.tlsDir != "" {
		var err error
		if server, client, err = loadTLS(o.tlsDir, o.tls, o.next); err != nil {
			return nil, "", err
		}
	}
	listen := o.role != "origin"
	if o.transport == "quic" {
		if server == nil {
			return nil, "", errors.New("-transport quic needs -tls-dir")
		}
		return quicNode(o, m, server, client, listen)
	}
	if o.transport != "tcp" {
		return nil, "", fmt.Errorf("-transport %q: tcp or quic", o.transport)
	}
	name := o.name
	if name == "" {
		name = o.role
	}
	pc := peerlink.Config{
		Link:       link.Config{Liveness: link.LivenessFast, Auth: link.NameAuth(name)},
		Observer:   func(peer string) link.Observer { return observer{m: m, peer: peer} },
		ServedPeer: func(net.Conn, link.Identity) string { return "prev" },
	}
	if server != nil {
		pc.Link.Auth = link.CertAuth
	}
	if o.role != "echo" {
		pc.Dial = func(ctx context.Context, _ string) (net.Conn, error) {
			if client != nil {
				d := tls.Dialer{Config: client}
				return d.DialContext(ctx, "tcp", o.next)
			}
			var d net.Dialer
			return d.DialContext(ctx, "tcp", o.next)
		}
	}
	if o.role == "origin" {
		pc.Pool = link.PoolConfig{Backoff: func(int) time.Duration { return 200 * time.Millisecond }}
	}
	addr := ""
	if listen {
		ln, err := net.Listen("tcp", o.listen)
		if err != nil {
			return nil, "", err
		}
		addr = ln.Addr().String()
		if server != nil {
			ln = tls.NewListener(ln, server)
		}
		pc.Listener = ln
	}
	n, err := peerlink.New(pc)
	return n, addr, err
}

// quicNode is newNode over quiclink. quiclink has no Observer: its
// connections' ups and downs are counted from Dial and ServedPeer.
func quicNode(o opts, m *metrics, server, client *tls.Config, listen bool) (peerlink.Link, string, error) {
	watch := func(q *quic.Conn, peer string) {
		m.inc("linkhop_conn_up_total", "peer=\""+peer+"\"")
		go func() {
			<-q.Context().Done()
			m.inc("linkhop_conn_down_total", fmt.Sprintf("peer=%q,reason=%q", peer, reason(context.Cause(q.Context()))))
		}()
	}
	client = client.Clone()
	client.NextProtos = []string{quiclink.ALPNChain, quiclink.ALPN}
	qc := quiclink.Config{Liveness: link.LivenessFast, TLS: server,
		ServedPeer: func(q *quic.Conn) (string, error) { watch(q, "prev"); return "prev", nil }}
	if o.role != "echo" {
		qc.Dial = func(ctx context.Context, _ string, c *quic.Config) (*quic.Conn, error) {
			q, err := quic.DialAddr(ctx, o.next, client, c)
			if err == nil {
				watch(q, "next")
			}
			return q, err
		}
	}
	addr := ""
	if listen {
		pc, err := net.ListenPacket("udp", o.listen)
		if err != nil {
			return nil, "", err
		}
		addr = pc.LocalAddr().String()
		qc.PacketConn = pc
	}
	n, err := quiclink.New(qc)
	return n, addr, err
}

// loadTLS is mutual TLS from dir's ca.crt, tls.crt and tls.key: server
// requires and verifies a client certificate; client verifies the next
// hop's for name (empty: next's host).
func loadTLS(dir, name, next string) (server, client *tls.Config, err error) {
	ca, err := os.ReadFile(filepath.Join(dir, "ca.crt")) // #nosec G304 -- the operator's -tls-dir
	if err != nil {
		return nil, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, nil, fmt.Errorf("%s: no PEM certificate", filepath.Join(dir, "ca.crt"))
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"))
	if err != nil {
		return nil, nil, err
	}
	if name == "" {
		if name, _, err = net.SplitHostPort(next); err != nil && next != "" {
			return nil, nil, err
		}
	}
	server = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	client = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert},
		RootCAs: pool, ServerName: name}
	return server, client, nil
}

// serve runs a hop or the echo until ctx ends, then drains.
func serve(ctx context.Context, n peerlink.Link, o opts, m *metrics) error {
	accept := chainsoak.Echo
	if o.role == "hop" {
		accept = chainsoak.Hop(n, "next", link.ForwardConfig{Observer: observer{m: m, peer: "next"}})
	}
	go func() {
		<-ctx.Done()
		dctx, cancel := context.WithTimeout(context.Background(), o.drain)
		defer cancel()
		_ = n.Drain(dctx)
	}()
	return n.Serve(accept)
}

func origin(ctx context.Context, n peerlink.Link, o opts, m *metrics) *summary {
	sum := &summary{Start: time.Now(), Errors: map[string]int64{}, InFlightBound: o.bound, Hops: o.hops, Transport: o.transport}
	m.set("linkhop_chain_hops", "", float64(o.hops))
	var mu sync.Mutex
	count := func(err error) {
		key := "other"
		var a *link.Abort
		if errors.As(err, &a) {
			key = fmt.Sprintf("abort code=%d hops=%d refused=%v", a.Code, a.Hops, a.Refused)
			m.inc("linkhop_echo_errors_total", fmt.Sprintf("code=\"%d\",hops=\"%d\",refused=\"%v\"", a.Code, a.Hops, a.Refused))
		} else {
			m.inc("linkhop_echo_errors_total", `code="other",hops="",refused=""`)
		}
		mu.Lock()
		sum.Errors[key]++
		mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(ctx, o.duration)
	defer cancel()
	var wg sync.WaitGroup
	for range o.workers {
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
				t0 := time.Now()
				intact, err := chainsoak.EchoOnce(octx, n, "next", size)
				ocancel()
				switch {
				case err != nil && ctx.Err() == nil:
					count(err)
					time.Sleep(100 * time.Millisecond)
				case err != nil:
				case !intact:
					atomic.AddInt64(&sum.IntegrityFailures, 1)
					m.inc("linkhop_integrity_failures_total", "")
				default:
					atomic.AddInt64(&sum.Echoes, 1)
					atomic.AddInt64(&sum.Bytes, int64(size))
					m.inc("linkhop_echoes_total", "")
					m.add("counter", "linkhop_echo_bytes_total", "", float64(size))
					m.observe("linkhop_echo_seconds", time.Since(t0))
				}
			}
		})
	}
	if o.inflight > 0 {
		wg.Go(func() {
			t := time.NewTicker(o.inflight)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
				if b, err := chainsoak.InFlight(ctx, n, "next", 500*time.Millisecond); err == nil {
					m.set("linkhop_inflight_bytes", "", float64(b))
					mu.Lock()
					sum.InFlightMax = max(sum.InFlightMax, b)
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	sum.Duration = time.Since(sum.Start).Seconds()
	sum.Throughput = float64(sum.Bytes) / sum.Duration
	sum.EchoP50 = m.quantile("linkhop_echo_seconds", .5) * 1000
	sum.EchoP99 = m.quantile("linkhop_echo_seconds", .99) * 1000
	sum.Pass = sum.IntegrityFailures == 0 && sum.Echoes > 0 && (o.bound == 0 || sum.InFlightMax <= o.bound)
	return sum
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "linkhop:", err)
		os.Exit(1)
	}
}
