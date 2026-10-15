// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/peerlinktest"
	"github.com/nfltr/link/peerlink/quiclink"
	"github.com/quic-go/quic-go"
)

// pki is a CA and a certificate per peer name, each trusting the CA.
type pki struct {
	pool  *x509.CertPool
	certs map[string]tls.Certificate
}

var testPKI = sync.OnceValue(func() *pki {
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		panic(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	p := &pki{pool: x509.NewCertPool(), certs: map[string]tls.Certificate{}}
	p.pool.AddCert(ca)
	for i, name := range []string{"a", "b"} {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: name},
			DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			panic(err)
		}
		p.certs[name] = tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}
	return p
})

// serverTLS is the listener's TLS as name: peers name themselves by
// their certificates.
func serverTLS(name string) *tls.Config {
	p := testPKI()
	return &tls.Config{Certificates: []tls.Certificate{p.certs[name]}, ClientCAs: p.pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}
}

// clientTLS is name's TLS dialling peer, offering chaining (ALPNChain)
// unless protos names the protocols.
func clientTLS(name, peer string, protos ...string) *tls.Config {
	p := testPKI()
	if protos == nil {
		protos = []string{quiclink.ALPNChain, quiclink.ALPN}
	}
	return &tls.Config{Certificates: []tls.Certificate{p.certs[name]}, RootCAs: p.pool, ServerName: peer,
		NextProtos: protos, MinVersion: tls.VersionTLS13}
}

func listenUDP(t *testing.T) net.PacketConn {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

// dialer dials peer (the only one it knows) as name at addr (offering
// protos: clientTLS).
func dialer(name, peer, addr string, protos ...string) func(context.Context, string, *quic.Config) (*quic.Conn, error) {
	return func(ctx context.Context, p string, qc *quic.Config) (*quic.Conn, error) {
		if p != peer {
			return nil, fmt.Errorf("no peer %q", p)
		}
		return quic.DialAddr(ctx, addr, clientTLS(name, peer, protos...), qc)
	}
}

// TestConformanceWithoutChain: a dialler offering only ALPN (quiclink
// before v0.4.0) gets plain sessions, which keep the contract.
func TestConformanceWithoutChain(t *testing.T) {
	peerlinktest.Run(t, func(t *testing.T) peerlinktest.Pair {
		pc := listenUDP(t)
		px := peerlinktest.NewUDPProxy(t, pc.LocalAddr().String())
		b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: link.LivenessFast})
		a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", px.Addr(), quiclink.ALPN), Liveness: link.LivenessFast})
		return peerlinktest.Pair{
			A: a, B: b, AName: "a", BName: "b",
			DeadAfter: a.DeadAfter(),
			BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle,
		}
	})
}

func TestConformance(t *testing.T) {
	peerlinktest.Run(t, func(t *testing.T) peerlinktest.Pair {
		pc := listenUDP(t)
		px := peerlinktest.NewUDPProxy(t, pc.LocalAddr().String())
		b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: link.LivenessFast})
		a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", px.Addr()), Liveness: link.LivenessFast})
		return peerlinktest.Pair{
			A: a, B: b, AName: "a", BName: "b",
			DeadAfter: a.DeadAfter(),
			BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle,
		}
	})
}

// TestConformanceReverse: A listens and opens its sessions on the
// connection B dialled (Config.Connect).
func TestConformanceReverse(t *testing.T) {
	peerlinktest.Run(t, func(t *testing.T) peerlinktest.Pair {
		pc := listenUDP(t)
		px := peerlinktest.NewUDPProxy(t, pc.LocalAddr().String())
		a := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("a"), Liveness: link.LivenessFast})
		b := mustNew(t, quiclink.Config{Dial: dialer("b", "a", px.Addr()), Connect: []string{"a"}, Liveness: link.LivenessFast})
		return peerlinktest.Pair{
			A: a, B: b, AName: "a", BName: "b", Reverse: true,
			DeadAfter: a.DeadAfter(),
			BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle,
		}
	})
}

// TestMigration: the dialer's address changes (the proxy rebinds) three
// times while a session moves 32 MiB each way: the session goes on intact
// and the listener sees the new addresses.
func TestMigration(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%v", reverse), func(t *testing.T) {
			pc := listenUDP(t)
			px := peerlinktest.NewUDPProxy(t, pc.LocalAddr().String())
			var (
				mu    sync.Mutex
				addrs = map[string]bool{}
			)
			echo := func(_ string, _ []byte, s peerlink.Session) {
				defer s.Close()
				buf := make([]byte, 32<<10)
				for {
					k, err := s.Read(buf)
					mu.Lock()
					addrs[s.RemoteAddr().String()] = true
					mu.Unlock()
					if k > 0 {
						if _, err := s.Write(buf[:k]); err != nil {
							return
						}
					}
					if err != nil {
						_ = s.CloseWrite()
						return
					}
				}
			}
			listener := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: link.LivenessFast})
			dcfg := quiclink.Config{Dial: dialer("a", "b", px.Addr()), Liveness: link.LivenessFast}
			opener, peer := peerlink.Link(nil), "b"
			if reverse {
				dcfg.Connect = []string{"b"}
			}
			dialling := mustNew(t, dcfg)
			if reverse {
				opener, peer = listener, "a"
				go func() { _ = listener.Serve(func(_ string, _ []byte, s peerlink.Session) { _ = s.Close() }) }()
				go func() { _ = dialling.Serve(echo) }()
			} else {
				opener = dialling
				go func() { _ = listener.Serve(echo) }()
			}
			px.Throttle(16 << 20) // 2 s each way: time for the rebinds
			t.Cleanup(func() {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_ = listener.Drain(ctx)
				_ = dialling.Drain(ctx)
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			s, err := opener.Open(ctx, peer, []byte("echo"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			const n = 32 << 20
			sent, got := sha256.New(), sha256.New()
			go func() {
				for i := range 3 {
					time.Sleep(time.Duration(200*(i+1)) * time.Millisecond)
					px.Rebind()
				}
			}()
			go func() {
				_, _ = io.Copy(s, io.TeeReader(io.LimitReader(rand.Reader, n), sent))
				_ = s.CloseWrite()
			}()
			start := time.Now()
			m, err := io.Copy(got, s)
			if err != nil || m != n || string(got.Sum(nil)) != string(sent.Sum(nil)) {
				t.Fatalf("echo across the rebinds: %d of %d bytes, %v (intact %v)", m, n, err, string(got.Sum(nil)) == string(sent.Sum(nil)))
			}
			mu.Lock()
			defer mu.Unlock()
			t.Logf("32 MiB each way in %v across 3 rebinds; the listener saw %d addresses", time.Since(start).Round(time.Millisecond), len(addrs))
			if !reverse && len(addrs) < 2 {
				t.Errorf("the listener saw the dialer at %d address(es), want the new ones too", len(addrs))
			}
		})
	}
}

// TestListenerSettings: connections Serve accepts keep the Node's settings
// (here MaxSessions 2: a third session waits for one to end).
func TestListenerSettings(t *testing.T) {
	pc := listenUDP(t)
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), MaxSessions: 2})
	a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", pc.LocalAddr().String())})
	go func() {
		_ = b.Serve(func(_ string, _ []byte, s peerlink.Session) { _, _ = io.Copy(io.Discard, s); _ = s.Close() })
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = a.Drain(ctx)
		_ = b.Drain(ctx)
	})
	open := func(d time.Duration) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(context.Background(), d)
		defer cancel()
		return a.Open(ctx, "b", nil)
	}
	for range 2 {
		s, err := open(time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
	}
	if s, err := open(300 * time.Millisecond); err == nil {
		_ = s.Close()
		t.Fatal("a third session opened toward a listener taking 2")
	}
}

func mustNew(t testing.TB, cfg quiclink.Config) *quiclink.Node {
	t.Helper()
	n, err := quiclink.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestOpenTowardDrainingPeer: once the peer that dialled in sent GOAWAY
// on its only connection (it drains, a session still open), Open toward
// it fails at once with link.ErrGoAway instead of waiting for another
// connection until its ctx ends.
func TestOpenTowardDrainingPeer(t *testing.T) {
	pc := listenUDP(t)
	a := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("a"), Liveness: link.LivenessFast})
	b := mustNew(t, quiclink.Config{Dial: dialer("b", "a", pc.LocalAddr().String()), Connect: []string{"a"}, Liveness: link.LivenessFast})
	go func() { _ = a.Serve(func(_ string, _ []byte, s peerlink.Session) { _ = s.Close() }) }()
	go func() {
		_ = b.Serve(func(_ string, _ []byte, s peerlink.Session) { defer s.Close(); _, _ = io.Copy(s, s) })
	}()
	t.Cleanup(func() { _ = a.Drain(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := a.Open(ctx, "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	drained := make(chan error, 1)
	go func() { drained <- b.Drain(ctx) }()
	for !quiclink.PeerGone(a, "b") {
		time.Sleep(time.Millisecond)
	}
	if _, err := a.Open(ctx, "b", nil); !errors.Is(err, link.ErrGoAway) || ctx.Err() != nil {
		t.Errorf("Open toward a draining peer: %v (ctx: %v), want link.ErrGoAway at once", err, ctx.Err())
	}
	_ = s.Close()
	if err := <-drained; err != nil {
		t.Errorf("Drain: %v", err)
	}
}
