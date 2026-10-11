// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"time"

	"github.com/nfltr/link"
)

// tokenAuth is a shared-secret Authenticator: the dialer sends the token,
// the listener compares it. Method 128 and up are the caller's to define.
type tokenAuth []byte

func (t tokenAuth) Hello(context.Context, tls.ConnectionState) (byte, []byte, error) {
	return 128, t, nil
}

func (t tokenAuth) Verify(_ context.Context, _ tls.ConnectionState, method byte, payload []byte) (link.Identity, error) {
	if method != 128 || subtle.ConstantTimeCompare(payload, t) != 1 {
		return link.Identity{}, errors.New("bad token")
	}
	return link.Identity{Method: "token", Subject: "example-client"}, nil
}

// Example is a listener and a dialer end to end over TLS on loopback: the
// listener authenticates the dialer and serves each session it opens (here,
// upper-casing what it reads); the dialer keeps a Pool of connections to it
// and opens a session per request.
func Example() {
	ctx := context.Background()
	auth := tokenAuth("s3cret")
	serverTLS, clientTLS := exampleTLS()

	// Listener: one link per accepted connection, one goroutine per session.
	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	who := make(chan link.Identity, 1)
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				c, id, err := link.Server(ctx, nc, link.Config{Auth: auth})
				if err != nil {
					nc.Close()
					return
				}
				who <- id
				_ = link.NewMux(c).Serve(func(header []byte, s *link.Session) {
					defer s.Close()
					body, _ := io.ReadAll(s) // until the dialer's CloseWrite
					fmt.Fprintf(s, "%s: %s", header, bytes.ToUpper(body))
				})
			}()
		}
	}()

	// Dialer: a Pool dials (TLS is the caller's), authenticates, and opens
	// sessions on its connections; the listener adopts the dialer's
	// liveness, so both ends notice a dead path within 1.5 s + 3 s.
	p := link.NewPool(link.PoolConfig{
		Link: link.Config{Auth: auth, Liveness: link.LivenessFast},
		Dial: func(ctx context.Context) (net.Conn, error) {
			var d tls.Dialer
			d.Config = clientTLS
			return d.DialContext(ctx, "tcp", ln.Addr().String())
		},
		Max:     2,
		Backoff: func(attempt int) time.Duration { return time.Duration(attempt) * 100 * time.Millisecond },
	})
	defer p.Close()

	for _, msg := range []string{"hello", "world"} {
		s, err := p.Open(ctx, []byte("upper"))
		if err != nil {
			panic(err)
		}
		_, _ = io.WriteString(s, msg)
		_ = s.CloseWrite() // the listener reads io.EOF after msg
		reply, err := io.ReadAll(s)
		s.Close()
		if err != nil {
			panic(err)
		}
		fmt.Println(string(reply))
	}
	fmt.Println("listener saw:", (<-who).Subject)
	// Output:
	// upper: HELLO
	// upper: WORLD
	// listener saw: example-client
}

// exampleTLS is a throwaway self-signed certificate for 127.0.0.1 and the
// client config that trusts it.
func exampleTLS() (server, client *tls.Config) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	cert, _ := x509.ParseCertificate(der)
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS13},
		&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}
}
