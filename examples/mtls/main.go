// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Command mtls runs two peers, a and b, in one process over mutual TLS: a
// throwaway CA signs a certificate for each, b's listener verifies a's,
// a verifies b's, and link.CertAuth names each peer by its certificate.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

func main() {
	ca, certs := pki("a", "b")

	// b listens: TLS verifies the dialer's certificate, CertAuth names the
	// dialer by it (its CommonName, here "a").
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{certs["b"]},
		ClientCAs:    ca,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	})
	if err != nil {
		log.Fatal(err)
	}
	b, err := peerlink.New(peerlink.Config{
		Listener: ln,
		Link:     link.Config{Auth: link.CertAuth},
	})
	if err != nil {
		log.Fatal(err)
	}
	go b.Serve(reply("b"))

	// a dials b with its own certificate and checks b's: ServerName is the
	// peer's name, which b's certificate must carry.
	a, err := peerlink.New(peerlink.Config{
		Link:    link.Config{Auth: link.CertAuth},
		Connect: []string{"b"},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			d := tls.Dialer{Config: &tls.Config{
				Certificates: []tls.Certificate{certs["a"]},
				RootCAs:      ca,
				ServerName:   peer,
				MinVersion:   tls.VersionTLS13,
			}}
			return d.DialContext(ctx, "tcp", ln.Addr().String())
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	go a.Serve(reply("a"))

	fmt.Println(ask(a, "b")) // a opens a session to b
	fmt.Println(ask(b, "a")) // b opens one to a, on the connection a dialled

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = a.Drain(ctx)
	_ = b.Drain(ctx)
}

// pki makes a throwaway CA and a certificate it signs for each name (its
// CommonName and DNS name), valid for an hour, for both TLS roles. In
// production the certificates come from your CA.
func pki(names ...string) (*x509.CertPool, map[string]tls.Certificate) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "throwaway CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		log.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	certs := map[string]tls.Certificate{}
	for i, name := range names {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			log.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name},
			NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			log.Fatal(err)
		}
		certs[name] = tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}
	return pool, certs
}

// reply serves each session a peer opens: it reads the request and
// answers it.
func reply(me string) func(peer string, header []byte, s peerlink.Session) {
	return func(peer string, header []byte, s peerlink.Session) {
		defer s.Close()
		req, _ := io.ReadAll(s) // until the opener's CloseWrite
		fmt.Fprintf(s, "%s got %q from %s", me, req, peer)
	}
}

// ask opens a session to peer, sends a request and reads the answer.
func ask(l peerlink.Link, peer string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := l.Open(ctx, peer, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	if _, err := io.WriteString(s, "hello"); err != nil {
		log.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
		log.Fatal(err)
	}
	answer, err := io.ReadAll(s)
	if err != nil {
		log.Fatal(err)
	}
	return string(answer)
}
