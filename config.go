// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"
)

// Config is one side's settings for a connection (Client, Server).
type Config struct {
	// Window is how many bytes of one session this side buffers: 0 is
	// DefaultWindow.
	Window int
	// MaxSessions is how many sessions the peer may have open toward this
	// side at once: 0 is MaxSessions.
	MaxSessions int
	// Liveness: this side declares the connection dead when it
	// received nothing for Interval, sent a ping, and received nothing for
	// Timeout more (ErrDeadPeer). A dialer's zero is LivenessDefault; a
	// listener's zero is the dialer's, so both ends detect a dead path
	// within the same bound.
	// A listener that must bound how long a vanished dialer holds its
	// connection sets its own: the dialer's Timeout has no upper bound.
	Liveness Liveness
	// Auth builds the dialer's credential (Hello) and verifies it on the
	// listener (Verify). A dialer's nil is NoAuth; a listener needs one:
	// NameAuth on a trusted network, CertAuth over mutual TLS, NoAuth
	// where peers need no names, or the caller's own.
	Auth Authenticator
	// Observer receives the connection's events; nil is none.
	Observer Observer
}

// MaxVersion is the highest wire version this package speaks.
const MaxVersion = 2

// DefaultWindow is a Config's window when it names none.
const DefaultWindow = 1 << 20

// Auth methods with a meaning in this package; the others are the
// Authenticator's to define (128 and up are never assigned here).
const (
	MethodNone  byte = 0 // no credential (NoAuth, CertAuth)
	MethodToken byte = 1 // a bearer token the listener issued
	MethodName  byte = 2 // a name the dialer claims (NameAuth)
)

// MaxAuth bounds a hello's auth payload.
const MaxAuth = 4096

// Authenticator proves a dialer to a listener. The connection's TLS state
// (zero on a plaintext one) is there for channel binding.
//
// Verify runs before the listener has bound anything, on bytes from an
// unauthenticated peer (SECURITY.md): it compares secrets in constant time
// (crypto/subtle), bounds its own work (ctx carries the handshake's
// deadline), and returns an Identity whose Subject names the peer
// uniquely where callers route by it (peerlink names a peer by Subject).
// Its error is not sent to the dialer, which reads only a refusal.
type Authenticator interface {
	// Hello returns the dialer's auth method and payload (at most MaxAuth
	// bytes).
	Hello(ctx context.Context, cs tls.ConnectionState) (method byte, payload []byte, err error)
	// Verify checks a dialer's method and payload and returns who it is.
	// An error refuses the connection.
	Verify(ctx context.Context, cs tls.ConnectionState, method byte, payload []byte) (Identity, error)
}

// Identity is who the peer is, as the listener's Authenticator says.
type Identity struct{ Method, Subject string }

// NoAuth sends no credential and accepts only peers that send none: for
// connections whose security is elsewhere (an mTLS peer port, a pipe).
// Every peer it accepts has the zero Identity: a peerlink.Node names them
// all "" (see NameAuth and CertAuth for peers with names).
var NoAuth Authenticator = noAuth{}

// NameAuth is an Authenticator for a trusted network (plaintext inside a
// cluster, a pipe): the dialer sends name, and the listener accepts any
// name of 1 to MaxSID bytes as the peer's Identity.Subject. It proves
// nothing: anyone who reaches the listener can claim any name. Over an
// untrusted network use CertAuth with mutual TLS, or an Authenticator that
// checks a secret.
func NameAuth(name string) Authenticator { return nameAuth(name) }

type nameAuth string

func (a nameAuth) Hello(context.Context, tls.ConnectionState) (byte, []byte, error) {
	if len(a) == 0 || len(a) > MaxSID {
		return 0, nil, fmt.Errorf("link: NameAuth(%q): a name is 1 to %d bytes", string(a), MaxSID)
	}
	return MethodName, []byte(a), nil
}

func (nameAuth) Verify(_ context.Context, _ tls.ConnectionState, method byte, payload []byte) (Identity, error) {
	if method != MethodName || len(payload) == 0 || len(payload) > MaxSID {
		return Identity{}, fmt.Errorf("link: NameAuth expects a name (method %d, 1 to %d bytes), got method %d with %d bytes", MethodName, MaxSID, method, len(payload))
	}
	return Identity{Method: "name", Subject: string(payload)}, nil
}

// CertAuth is an Authenticator for mutual TLS: the dialer sends no
// credential, and the listener accepts a dialer whose client certificate
// its tls.Config verified (ClientAuth: tls.RequireAndVerifyClientCert),
// named by the certificate's Subject CommonName, else its first DNS name.
// Give Server (or peerlink.Config.Listener) a *tls.Conn.
var CertAuth Authenticator = certAuth{}

type certAuth struct{}

func (certAuth) Hello(context.Context, tls.ConnectionState) (byte, []byte, error) {
	return MethodNone, nil, nil
}

func (certAuth) Verify(_ context.Context, cs tls.ConnectionState, method byte, payload []byte) (Identity, error) {
	if method != MethodNone || len(payload) > 0 {
		return Identity{}, errors.New("link: CertAuth expects no credential in the hello")
	}
	if len(cs.VerifiedChains) == 0 || len(cs.VerifiedChains[0]) == 0 {
		return Identity{}, errors.New("link: CertAuth: no verified client certificate (a TLS listener with ClientAuth tls.RequireAndVerifyClientCert, and a dialer with a certificate)")
	}
	c := cs.VerifiedChains[0][0]
	name := c.Subject.CommonName
	if name == "" && len(c.DNSNames) > 0 {
		name = c.DNSNames[0]
	}
	if name == "" {
		return Identity{}, errors.New("link: CertAuth: the client certificate has no CommonName or DNS name")
	}
	return Identity{Method: "tls", Subject: name}, nil
}

type noAuth struct{}

func (noAuth) Hello(context.Context, tls.ConnectionState) (byte, []byte, error) {
	return MethodNone, nil, nil
}

func (noAuth) Verify(_ context.Context, _ tls.ConnectionState, method byte, payload []byte) (Identity, error) {
	if method != MethodNone || len(payload) > 0 {
		return Identity{}, errors.New("link: a credential where none is expected")
	}
	return Identity{}, nil
}

// Observer receives a connection's events, to map onto the caller's
// metrics. Calls come from the connection's goroutines and must not block.
// Embed NopObserver to take only some.
type Observer interface {
	ConnUp(Identity)          // the handshake bound the connection
	ConnDown(Identity, error) // it ended: ErrDeadPeer, the peer's or network's error, nil when this side closed it
	SessionOpen()             // a session opened by an OPEN frame
	SessionClose(error)       // one of those ended
	CreditWait(time.Duration) // a sender waited this long for credit
	PingRTT(time.Duration)    // a ping's round trip
	GoAway(Identity)          // the peer sent GOAWAY
}

// NopObserver ignores every event.
type NopObserver struct{}

// ConnUp ignores the event.
func (NopObserver) ConnUp(Identity) {}

// ConnDown ignores the event.
func (NopObserver) ConnDown(Identity, error) {}

// SessionOpen ignores the event.
func (NopObserver) SessionOpen() {}

// SessionClose ignores the event.
func (NopObserver) SessionClose(error) {}

// CreditWait ignores the event.
func (NopObserver) CreditWait(time.Duration) {}

// PingRTT ignores the event.
func (NopObserver) PingRTT(time.Duration) {}

// GoAway ignores the event.
func (NopObserver) GoAway(Identity) {}

var (
	// ErrDeadPeer ends a connection on which nothing arrived for its
	// liveness's interval plus timeout, a ping included.
	ErrDeadPeer = errors.New("link: peer silent past the liveness timeout")
	// ErrGoAway is Open's refusal on a connection that is going away
	// (either side sent GOAWAY).
	ErrGoAway = errors.New("link: connection going away")
	// ErrHandshake is a hello or its reply that did not arrive whole:
	// the other end closed the connection, timed out, or is not a link
	// end speaking the same TLS-or-not (the error says which, and wraps
	// the network's).
	ErrHandshake = errors.New("link: handshake failed")
	// ErrAuth is a listener's refusal of a dialer its Authenticator did
	// not verify.
	ErrAuth = errors.New("link: peer not authenticated")
	// ErrV1 is a listener's refusal of a version-1 hello: a dialer of
	// link v0.4 or older (version 1 was removed in v0.5.0).
	ErrV1 = fmt.Errorf("link: a version-1 hello: the peer is older than link v0.5.0, which speaks only version 2: %w", ErrVersion)
	// ErrVersion is a peer that does not speak the version asked for
	// (VersionError says which it speaks).
	ErrVersion = errors.New("link: wire version not supported")
)

// VersionError is a listener's refusal of a hello's version: it speaks
// versions up to Max.
type VersionError struct{ Max int }

// Error says which versions the peer speaks.
func (e *VersionError) Error() string {
	return fmt.Sprintf("link: peer speaks wire versions up to %d", e.Max)
}

// Is makes a VersionError an ErrVersion.
func (e *VersionError) Is(target error) bool { return target == ErrVersion }

// withDefaults fills a Config's zero values that have defaults.
func (cfg Config) withDefaults() Config {
	if cfg.Window == 0 {
		cfg.Window = DefaultWindow
	}
	if cfg.MaxSessions == 0 {
		cfg.MaxSessions = MaxSessions
	}
	return cfg
}

// Validate reports the first field of cfg out of its range, naming it and
// what to set; the zero Config is valid. Client and Server (and NewPool,
// for PoolConfig.Link) refuse a Config it refuses.
func (cfg Config) Validate() error {
	cfg = cfg.withDefaults()
	if cfg.Window < MinWindow || cfg.Window > MaxWindow {
		return fmt.Errorf("link: Config.Window %d: set 0 (DefaultWindow) or a value in [MinWindow %d, MaxWindow %d]", cfg.Window, MinWindow, MaxWindow)
	}
	if cfg.MaxSessions < 1 || cfg.MaxSessions > maxSessionsWire {
		return fmt.Errorf("link: Config.MaxSessions %d: set 0 (MaxSessions) or a value in [1, %d]", cfg.MaxSessions, maxSessionsWire)
	}
	if cfg.Liveness != (Liveness{}) {
		if err := cfg.Liveness.Validate(); err != nil {
			return fmt.Errorf("link: Config.Liveness: %w (or leave it zero, or use a preset such as LivenessDefault)", err)
		}
	}
	return nil
}
