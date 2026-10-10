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
	// Version is the highest wire version to speak: 0 is MaxVersion. A
	// dialer speaks it; give it what the listener advertised (out of
	// band): a version-1 listener refuses a version-2 hello only after it
	// read 37 bytes of it.
	Version int
	// Window is how many bytes of one session this side buffers: 0 is
	// DefaultWindow.
	Window int
	// MaxSessions is how many sessions the peer may have open toward this
	// side at once (version 2): 0 is MaxSessions.
	MaxSessions int
	// Liveness (version 2): this side declares the connection dead when it
	// received nothing for Interval, sent a ping, and received nothing for
	// Timeout more (ErrDeadPeer). A dialer's zero is LivenessDefault; a
	// listener's zero is the dialer's, so both ends detect a dead path
	// within the same bound.
	Liveness Liveness
	// Auth builds the dialer's credential (Hello) and verifies it on the
	// listener (Verify). A dialer's nil is NoAuth; a listener needs one.
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
	MethodNone  byte = 0 // no credential (NoAuth)
	MethodToken byte = 1 // a bearer token the listener issued; version 1 carries only this, TokenLen bytes
)

// MaxAuth bounds a hello's auth payload.
const MaxAuth = 4096

// Authenticator proves a dialer to a listener. The connection's TLS state
// (zero on a plaintext one) is there for channel binding.
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
var NoAuth Authenticator = noAuth{}

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
	ConnUp(Identity)
	ConnDown(Identity, error)
	SessionOpen()             // a session opened by an OPEN frame (version 2)
	SessionClose(error)       // one of those ended
	CreditWait(time.Duration) // a sender waited this long for credit
	PingRTT(time.Duration)    // a ping's round trip (version 2)
	GoAway(Identity)          // the peer sent GOAWAY (version 2)
}

// NopObserver ignores every event.
type NopObserver struct{}

func (NopObserver) ConnUp(Identity)          {}
func (NopObserver) ConnDown(Identity, error) {}
func (NopObserver) SessionOpen()             {}
func (NopObserver) SessionClose(error)       {}
func (NopObserver) CreditWait(time.Duration) {}
func (NopObserver) PingRTT(time.Duration)    {}
func (NopObserver) GoAway(Identity)          {}

var (
	// ErrDeadPeer ends a connection on which nothing arrived for its
	// liveness's interval plus timeout, a ping included.
	ErrDeadPeer = errors.New("link: peer silent past the liveness timeout")
	// ErrGoAway is Open's refusal on a connection that is going away
	// (either side sent GOAWAY).
	ErrGoAway = errors.New("link: connection going away")
	// ErrAuth is a listener's refusal of a dialer its Authenticator did
	// not verify.
	ErrAuth = errors.New("link: peer not authenticated")
	// ErrVersion is a peer that does not speak the version asked for
	// (VersionError says which it speaks).
	ErrVersion = errors.New("link: wire version not supported")
)

// VersionError is a listener's refusal of a hello's version: it speaks
// versions up to Max.
type VersionError struct{ Max int }

func (e *VersionError) Error() string {
	return fmt.Sprintf("link: peer speaks wire versions up to %d", e.Max)
}

// Is makes a VersionError an ErrVersion. allow:wrapper
func (e *VersionError) Is(target error) bool { return target == ErrVersion }

// withDefaults fills a Config's zero values that have defaults.
func (cfg Config) withDefaults() Config {
	if cfg.Version == 0 {
		cfg.Version = MaxVersion
	}
	if cfg.Window == 0 {
		cfg.Window = DefaultWindow
	}
	if cfg.MaxSessions == 0 {
		cfg.MaxSessions = MaxSessions
	}
	return cfg
}

func (cfg Config) check() error {
	if cfg.Version < 1 || cfg.Version > MaxVersion {
		return fmt.Errorf("link: version %d, this package speaks 1 to %d", cfg.Version, MaxVersion)
	}
	if cfg.Window < MinWindow || cfg.Window > MaxWindow {
		return fmt.Errorf("link: window %d out of range", cfg.Window)
	}
	if cfg.MaxSessions < 1 || cfg.MaxSessions > maxSessionsWire {
		return fmt.Errorf("link: max sessions %d out of range", cfg.MaxSessions)
	}
	if cfg.Liveness != (Liveness{}) {
		return cfg.Liveness.Validate()
	}
	return nil
}
