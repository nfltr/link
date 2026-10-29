// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

func selfSigned(t testing.TB) (server, client *tls.Config) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS13},
		&tls.Config{RootCAs: roots, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS13}
}

// TestHandshakeErrorsSayWhatToChange: a TLS mismatch either way fails the
// handshake with ErrHandshake on both ends, saying to check TLS.
func TestHandshakeErrorsSayWhatToChange(t *testing.T) {
	stls, ctls := selfSigned(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, c := range []struct {
		name               string
		serverTLS, dialTLS bool
	}{
		{"plaintext dialer, TLS listener", true, false},
		{"TLS dialer, plaintext listener", false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			a, b := tcpPair(t)
			if c.serverTLS {
				b = tls.Server(b, stls)
			}
			if c.dialTLS {
				a = tls.Client(a, ctls)
			}
			errc := make(chan error, 1)
			go func() {
				_, _, err := Server(ctx, b, Config{Auth: NoAuth})
				_ = b.Close()
				errc <- err
			}()
			_, err := Client(ctx, a, Config{})
			_ = a.Close()
			serr := <-errc
			for side, err := range map[string]error{"dialer": err, "listener": serr} {
				if !errors.Is(err, ErrHandshake) || !strings.Contains(err.Error(), "TLS") || strings.Contains(err.Error(), "refused a certificate") {
					t.Errorf("%s: %v, want ErrHandshake saying to check TLS on both ends", side, err)
				}
			}
		})
	}
}

// TestHandshakeCertErrorsSayCheckCertificates: when TLS refuses a
// certificate (here the dialer does not trust the listener's CA), both
// ends fail with ErrHandshake pointing at the certificates, not at TLS on
// one end only.
func TestHandshakeCertErrorsSayCheckCertificates(t *testing.T) {
	stls, _ := selfSigned(t)
	_, ctls := selfSigned(t) // trusts another CA
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := tcpPair(t)
	errc := make(chan error, 1)
	go func() {
		_, _, err := Server(ctx, tls.Server(b, stls), Config{Auth: NoAuth})
		_ = b.Close()
		errc <- err
	}()
	_, err := Client(ctx, tls.Client(a, ctls), Config{})
	_ = a.Close()
	for side, err := range map[string]error{"dialer": err, "listener": <-errc} {
		if !errors.Is(err, ErrHandshake) || !strings.Contains(err.Error(), "trust the other's CA") || strings.Contains(err.Error(), "both or neither") {
			t.Errorf("%s: %v, want ErrHandshake saying to check the certificates", side, err)
		}
	}
}

// TestValidateNamesTheField: Config and PoolConfig errors name the field.
func TestValidateNamesTheField(t *testing.T) {
	dial := func(context.Context) (net.Conn, error) { return nil, errors.New("no") }
	for _, c := range []struct {
		err  error
		want string
	}{
		{Config{Window: 5}.Validate(), "Config.Window"},
		{Config{MaxSessions: -1}.Validate(), "Config.MaxSessions"},
		{Config{Liveness: Liveness{Interval: time.Millisecond, Timeout: time.Second}}.Validate(), "Config.Liveness"},
		{PoolConfig{}.Validate(), "PoolConfig.Dial"},
		{PoolConfig{Dial: dial, Min: 2, Max: 1}.Validate(), "PoolConfig.Min"},
		{PoolConfig{Dial: dial, Link: Config{Window: 5}}.Validate(), "PoolConfig.Link: link: Config.Window"},
	} {
		if c.err == nil || !strings.Contains(c.err.Error(), c.want) {
			t.Errorf("%v: want an error naming %s", c.err, c.want)
		}
	}
	if err := (Config{}).Validate(); err != nil {
		t.Errorf("zero Config: %v", err)
	}
	if _, err := NewPool(PoolConfig{}); err == nil {
		t.Error("NewPool without Dial")
	}
	if _, _, err := Server(context.Background(), nil, Config{}); err == nil || !strings.Contains(err.Error(), "Config.Auth") {
		t.Errorf("Server without Auth: %v", err)
	}
}

// TestNameAuthAndCertAuth: NameAuth names a peer by what it sends;
// CertAuth by its verified client certificate, and refuses a dialer
// without one.
func TestNameAuthAndCertAuth(t *testing.T) {
	id, err := NameAuth("x").Verify(context.Background(), tls.ConnectionState{}, MethodName, []byte("peer-1"))
	if err != nil || id.Subject != "peer-1" {
		t.Fatalf("NameAuth: %v %v", id, err)
	}
	if _, _, err := NameAuth("").Hello(context.Background(), tls.ConnectionState{}); err == nil {
		t.Fatal("NameAuth of an empty name")
	}
	if _, err := CertAuth.Verify(context.Background(), tls.ConnectionState{}, MethodNone, nil); err == nil || !strings.Contains(err.Error(), "RequireAndVerifyClientCert") {
		t.Fatalf("CertAuth without a certificate: %v", err)
	}
	cert := &x509.Certificate{DNSNames: []string{"node-2"}}
	id, err = CertAuth.Verify(context.Background(), tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}}, MethodNone, nil)
	if err != nil || id.Subject != "node-2" {
		t.Fatalf("CertAuth: %v %v", id, err)
	}
}

// TestDeadPeerIsATimeout: ErrDeadPeer is a net.Error whose Timeout() is
// true, also wrapped as a session reads it, and errors.Is still matches it.
func TestDeadPeerIsATimeout(t *testing.T) {
	wrapped := fmt.Errorf("%w: %w; open a new session", ErrConnClosed, ErrDeadPeer)
	var ne net.Error
	if !errors.As(wrapped, &ne) || !ne.Timeout() {
		t.Fatalf("%v: not a net.Error with Timeout() true", wrapped)
	}
	if !errors.Is(wrapped, ErrDeadPeer) || !errors.Is(wrapped, ErrConnClosed) {
		t.Fatalf("%v: errors.Is lost ErrDeadPeer or ErrConnClosed", wrapped)
	}
}
