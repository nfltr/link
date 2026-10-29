// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestChainPlain(t *testing.T) { chain(t, opts{transport: "tcp"}) }
func TestChainTLS(t *testing.T)   { chain(t, opts{transport: "tcp", tlsDir: pki(t), tls: "localhost"}) }
func TestChainQUIC(t *testing.T)  { chain(t, opts{transport: "quic", tlsDir: pki(t), tls: "localhost"}) }

// chain runs origin -> hop -> echo in process for 3 s over base's
// transport and checks the summary and each role's /metrics.
func chain(t *testing.T, base opts) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	start := func(role, next string, hop int) (string, *metrics) {
		o := base
		o.role, o.listen, o.next, o.drain = role, "127.0.0.1:0", next, 5*time.Second
		m := newMetrics(role, hop)
		n, addr, err := newNode(o, m)
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			if err := serve(ctx, n, o, m); err != nil {
				t.Errorf("%s: %v", role, err)
			}
		})
		return addr, m
	}
	echoAddr, echoM := start("echo", "", 2)
	hopAddr, hopM := start("hop", echoAddr, 1)

	o := base
	o.role, o.next, o.workers, o.hops = "origin", hopAddr, 4, 1
	o.duration, o.inflight, o.bound = 3*time.Second, time.Second, 3<<20
	m := newMetrics("origin", 0)
	n, _, err := newNode(o, m)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *summary)
	go func() { done <- origin(ctx, n, o, m) }()
	time.Sleep(1500 * time.Millisecond)
	mid := scrape(t, m)
	sum := <-done
	end := scrape(t, m)
	t.Logf("%s: %d echoes, %.0f B/s, p50 %.2f ms, p99 %.2f ms, in flight max %d, errors %v",
		base.transport, sum.Echoes, sum.Throughput, sum.EchoP50, sum.EchoP99, sum.InFlightMax, sum.Errors)
	if !sum.Pass || sum.Hops != 1 || sum.Throughput <= 0 || sum.EchoP50 <= 0 || sum.EchoP99 < sum.EchoP50 {
		t.Fatalf("summary %+v", sum)
	}
	key := `linkhop_echoes_total{role="origin",hop="0"}`
	if !(mid[key] > 0 && end[key] > mid[key]) {
		t.Errorf("%s: %v mid, %v end", key, mid[key], end[key])
	}
	for _, k := range []string{`linkhop_echo_seconds_count{role="origin",hop="0"}`, `linkhop_chain_hops{role="origin",hop="0"}`,
		`linkhop_conn_up_total{role="origin",hop="0",peer="next"}`, `linkhop_goroutines{role="origin",hop="0"}`} {
		if end[k] <= 0 {
			t.Errorf("%s = %v", k, end[k])
		}
	}
	h, e := scrape(t, hopM), scrape(t, echoM)
	if h[`linkhop_conn_up_total{role="hop",hop="1",peer="prev"}`] <= 0 || h[`linkhop_conn_up_total{role="hop",hop="1",peer="next"}`] <= 0 ||
		e[`linkhop_conn_up_total{role="echo",hop="2",peer="prev"}`] <= 0 {
		t.Errorf("connections: hop %v, echo %v", h, e)
	}
	if h[`linkhop_forward_events_total{role="hop",hop="1",event="opened",code=""}`] < end[key] {
		t.Errorf("hop forwarded fewer than the origin echoed: %v", h)
	}
	if base.transport == "tcp" && h[`linkhop_sessions_opened_total{role="hop",hop="1"}`] <= 0 {
		t.Errorf("hop sessions: %v", h)
	}
}

var line = regexp.MustCompile(`^([a-z_]+\{[^}]*\}) (\S+)$`)

// scrape is m's /metrics, each line checked against the text format.
func scrape(t *testing.T, m *metrics) map[string]float64 {
	t.Helper()
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	vals := map[string]float64{}
	for l := range strings.Lines(w.Body.String()) {
		l = strings.TrimSuffix(l, "\n")
		if strings.HasPrefix(l, "# TYPE ") {
			continue
		}
		g := line.FindStringSubmatch(l)
		if g == nil {
			t.Fatalf("not Prometheus text: %q", l)
		}
		v, err := strconv.ParseFloat(g[2], 64)
		if err != nil {
			t.Fatalf("%q: %v", l, err)
		}
		vals[g[1]] = v
	}
	return vals
}

// pki writes ca.crt, tls.crt and tls.key (one leaf for every role, for
// localhost and 127.0.0.1) to a temporary directory.
func pki(t *testing.T) string {
	dir := t.TempDir()
	key := func() *ecdsa.PrivateKey {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	write := func(name, typ string, der []byte) {
		if err := os.WriteFile(filepath.Join(dir, name), pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	caKey, leafKey := key(), key()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "linkhop ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "linkhop"},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	write("ca.crt", "CERTIFICATE", caDER)
	write("tls.crt", "CERTIFICATE", leafDER)
	write("tls.key", "PRIVATE KEY", keyDER)
	return dir
}

