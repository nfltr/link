// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

// echo serves a session: it writes back what it reads, prefixed with who
// opened it and the header.
func echo(peer string, header []byte, s peerlink.Session) {
	defer s.Close()
	b, _ := io.ReadAll(s)
	fmt.Fprintf(s, "%s %s %s", peer, header, b)
}

// call opens a session to peer, writes msg and returns the reply.
func call(l peerlink.Link, peer, msg string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := l.Open(ctx, peer, []byte("echo"))
	if err != nil {
		return err.Error()
	}
	defer s.Close()
	_, _ = io.WriteString(s, msg)
	_ = s.CloseWrite()
	b, _ := io.ReadAll(s)
	return string(b)
}

func drain(l peerlink.Link) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = l.Drain(ctx)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// dialTCP dials addr, whatever the peer's name.
func dialTCP(addr string) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
}

// ExampleNode: a Node that listens serves the sessions a Node that dials
// opens to it. Peers are named by what Link.Auth verified: here
// link.NameAuth, a name each sends, for plaintext on a trusted network.
func ExampleNode() {
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	server := must(peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("server")}}))
	go server.Serve(echo)
	defer drain(server)

	client := must(peerlink.New(peerlink.Config{
		Link: link.Config{Auth: link.NameAuth("client")},
		Dial: dialTCP(ln.Addr().String()),
	}))
	defer drain(client)

	fmt.Println(call(client, "server", "hello"))
	// Output: client echo hello
}

// ExampleNode_reverse: the server opens sessions to a client it cannot
// dial (behind NAT, say). The client dials it (Config.Connect) and serves
// the sessions the server opens on that connection; the server, with no
// Config.Dial, opens on the connections its peers dialled in.
func ExampleNode_reverse() {
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	server := must(peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("server")}}))
	go server.Serve(echo)
	defer drain(server)

	client := must(peerlink.New(peerlink.Config{
		Link:    link.Config{Auth: link.NameAuth("client")},
		Connect: []string{"server"},
		Dial:    dialTCP(ln.Addr().String()),
	}))
	go client.Serve(echo)
	defer drain(client)

	// Open waits (up to its ctx) for the client's connection to come up.
	fmt.Println(call(server, "client", "hello"))
	// Output: server echo hello
}

// Example_mutualTLS: both ends present certificates of one CA; the
// listener's TLS verifies the dialer's, and link.CertAuth names the peer
// by its certificate (here "client").
func Example_mutualTLS() {
	pool, certs := examplePKI("server", "client")
	ln := must(tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{certs["server"]},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}))
	server := must(peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.CertAuth}}))
	go server.Serve(echo)
	defer drain(server)

	client := must(peerlink.New(peerlink.Config{
		Link: link.Config{Auth: link.CertAuth},
		Dial: func(ctx context.Context, peer string) (net.Conn, error) {
			d := tls.Dialer{Config: &tls.Config{
				Certificates: []tls.Certificate{certs["client"]},
				RootCAs:      pool,
				ServerName:   peer, // the server's certificate names it
				MinVersion:   tls.VersionTLS13,
			}}
			return d.DialContext(ctx, "tcp", ln.Addr().String())
		},
	}))
	defer drain(client)

	fmt.Println(call(client, "server", "hello"))
	// Output: client echo hello
}

// Example_http: an http.Server serves the sessions peers open (through a
// Listener), and an http.Client reaches it with Open as its dialer, the
// URL's host naming the peer.
func Example_http() {
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	server := must(peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("server")}}))
	sessions := peerlink.NewListener()
	defer sessions.Close()
	go server.Serve(sessions.Handle)
	go http.Serve(sessions, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s over a link", r.URL.Path)
	}))
	defer drain(server)

	client := must(peerlink.New(peerlink.Config{
		Link: link.Config{Auth: link.NameAuth("client")},
		Dial: dialTCP(ln.Addr().String()),
	}))
	defer drain(client)
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			peer, _, _ := net.SplitHostPort(addr) // "server:80"
			return client.Open(ctx, peer, []byte("http"))
		},
	}
	defer transport.CloseIdleConnections() // they are sessions: Drain waits for them
	resp := must((&http.Client{Transport: transport}).Get("http://server/hello"))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	fmt.Println(string(body))
	// Output: /hello over a link
}

// ExampleNode_Drain: on shutdown, Drain stops taking new sessions, lets
// the open ones finish (here a request already in flight), then releases
// everything; at its ctx's end it cuts what is left.
func ExampleNode_Drain() {
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	server := must(peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("server")}}))
	go server.Serve(echo)
	client := must(peerlink.New(peerlink.Config{Link: link.Config{Auth: link.NameAuth("client")}, Dial: dialTCP(ln.Addr().String())}))
	defer drain(client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := must(client.Open(ctx, "server", []byte("echo")))
	defer s.Close()

	// SIGTERM, say: drain for up to 10 s.
	drained := make(chan error, 1)
	go func() { drained <- server.Drain(ctx) }()

	_, _ = io.WriteString(s, "last request")
	_ = s.CloseWrite()
	reply, _ := io.ReadAll(s)
	fmt.Println(string(reply))
	fmt.Println("drained:", <-drained)
	_, err := server.Open(ctx, "client", nil)
	fmt.Println(errors.Is(err, peerlink.ErrDraining))
	// Output:
	// client echo last request
	// drained: <nil>
	// true
}

// Example_reconnect: with Pool.Backoff, Open redials a peer that is not
// up yet (here the first two dials fail) until its ctx ends; connections
// that end later are redialled the same way. Without Backoff, Open fails
// with the dial's error at once: retries are then the caller's.
func Example_reconnect() {
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	server := must(peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("server")}}))
	go server.Serve(echo)
	defer drain(server)

	var dials atomic.Int32
	flaky := func(ctx context.Context, peer string) (net.Conn, error) {
		if dials.Add(1) <= 2 {
			return nil, errors.New("connection refused")
		}
		return dialTCP(ln.Addr().String())(ctx, peer)
	}
	client := must(peerlink.New(peerlink.Config{
		Link: link.Config{Auth: link.NameAuth("client")},
		Dial: flaky,
		Pool: link.PoolConfig{Backoff: func(attempt int) time.Duration {
			return min(time.Duration(attempt)*50*time.Millisecond, 2*time.Second)
		}},
	}))
	defer drain(client)
	fmt.Println(call(client, "server", "hello"), "after", dials.Load(), "dials")

	dials.Store(0)
	failFast := must(peerlink.New(peerlink.Config{Link: link.Config{Auth: link.NameAuth("client")}, Dial: flaky}))
	defer drain(failFast)
	fmt.Println(call(failFast, "server", "hello"))
	// Output:
	// client echo hello after 3 dials
	// peerlink: open to "server": connection refused
}

// counter counts a peer's connections and the sessions it opened.
type counter struct {
	link.NopObserver // the events it does not count
	conns, sessions  atomic.Int32
}

func (c *counter) ConnUp(link.Identity) { c.conns.Add(1) }
func (c *counter) SessionOpen()         { c.sessions.Add(1) }

// Example_observer: Config.Observer gives each peer an Observer, whose
// events (connections up and down, sessions, credit waits, ping round
// trips, GOAWAY) map onto the caller's metrics.
func Example_observer() {
	perPeer := map[string]*counter{"client": {}}
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	server := must(peerlink.New(peerlink.Config{
		Listener: ln,
		Link:     link.Config{Auth: link.NameAuth("server")},
		Observer: func(peer string) link.Observer {
			if c := perPeer[peer]; c != nil {
				return c
			}
			return nil // peers it does not count
		},
	}))
	go server.Serve(echo)
	defer drain(server)
	client := must(peerlink.New(peerlink.Config{Link: link.Config{Auth: link.NameAuth("client")}, Dial: dialTCP(ln.Addr().String())}))
	defer drain(client)

	call(client, "server", "one")
	call(client, "server", "two")
	c := perPeer["client"]
	fmt.Println("client:", c.conns.Load(), "connection,", c.sessions.Load(), "sessions")
	// Output: client: 1 connection, 2 sessions
}

// examplePKI is a throwaway CA and a certificate for each name (its
// CommonName and DNS name), valid for an hour.
func examplePKI(names ...string) (*x509.CertPool, map[string]tls.Certificate) {
	caKey := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "example CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	ca := must(x509.ParseCertificate(must(x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey))))
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	certs := map[string]tls.Certificate{}
	for i, name := range names {
		key := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: name},
			DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der := must(x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey))
		certs[name] = tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}
	return pool, certs
}
