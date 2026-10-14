// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

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
	"math/big"
	"net"
	"time"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
	"github.com/quic-go/quic-go"
)

// Example: node "b" listens on a UDP socket with a certificate and
// requires the dialer's (mutual TLS: peers are named by their
// certificates' CommonName); node "a" dials it with its own and opens a
// session.
func Example() {
	pool, certs := examplePKI("a", "b")
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	b, err := quiclink.New(quiclink.Config{PacketConn: pc, TLS: &tls.Config{
		Certificates: []tls.Certificate{certs["b"]},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}})
	if err != nil {
		panic(err)
	}
	go b.Serve(func(peer string, header []byte, s net.Conn) {
		defer s.Close()
		msg, _ := io.ReadAll(s)
		fmt.Fprintf(s, "%s %s %s", peer, header, msg)
	})

	a, err := quiclink.New(quiclink.Config{
		Dial: func(ctx context.Context, peer string, qc *quic.Config) (*quic.Conn, error) {
			return quic.DialAddr(ctx, pc.LocalAddr().String(), &tls.Config{
				Certificates: []tls.Certificate{certs["a"]},
				RootCAs:      pool,
				ServerName:   peer,
				NextProtos:   []string{quiclink.ALPN},
				MinVersion:   tls.VersionTLS13,
			}, qc)
		},
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := a.Open(ctx, "b", []byte("echo"))
	if err != nil {
		panic(err)
	}
	_, _ = io.WriteString(s, "hello")
	_ = s.(peerlink.Session).CloseWrite()
	reply, _ := io.ReadAll(s)
	_ = s.Close()
	fmt.Println(string(reply))

	_ = a.Drain(ctx)
	_ = b.Drain(ctx)
	// Output: a echo hello
}

// examplePKI is a throwaway CA and a certificate for each name (its
// CommonName and DNS name), valid for an hour.
func examplePKI(names ...string) (*x509.CertPool, map[string]tls.Certificate) {
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "example CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	ca, _ := x509.ParseCertificate(caDER)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	certs := map[string]tls.Certificate{}
	for i, name := range names {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: name},
			DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der, _ := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		certs[name] = tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}
	return pool, certs
}
