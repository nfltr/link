// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// StatusVersion is a listener's reply to a hello whose version
// it does not speak; its highest version follows.
const statusVersion = 2

// maxSessionsWire bounds a hello's max sessions.
const maxSessionsWire = 1 << 20

// helloV2 is a version-2 hello after its Magic and version byte (doc.go).
type helloV2 struct {
	maxVersion  byte
	features    uint32
	window      uint32
	maxSessions uint32
	liveness    Liveness // whole milliseconds on the wire
	method      byte
	auth        []byte
}

// helloV2Fixed is a version-2 hello's length after Magic and version,
// without the auth payload.
const helloV2Fixed = 1 + 4 + 4 + 4 + 4 + 4 + 1 + 2

func appendHelloV2(b []byte, h helloV2) []byte {
	b = append(append(b, Magic...), 2, h.maxVersion)
	b = binary.BigEndian.AppendUint32(b, h.features)
	b = binary.BigEndian.AppendUint32(b, h.window)
	b = binary.BigEndian.AppendUint32(b, h.maxSessions)
	b = binary.BigEndian.AppendUint32(b, uint32(h.liveness.Interval.Milliseconds())) // #nosec G115 -- validated: at most 15s
	b = binary.BigEndian.AppendUint32(b, uint32(h.liveness.Timeout.Milliseconds()))  // #nosec G115 -- validated
	b = append(b, h.method)
	b = binary.BigEndian.AppendUint16(b, uint16(len(h.auth))) // #nosec G115 -- len <= MaxAuth
	return append(b, h.auth...)
}

// readHelloV2 reads a version-2 hello after its Magic and version byte.
func readHelloV2(r io.Reader) (helloV2, error) {
	var b [helloV2Fixed]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return helloV2{}, err
	}
	h := helloV2{
		maxVersion:  b[0],
		features:    binary.BigEndian.Uint32(b[1:]),
		window:      binary.BigEndian.Uint32(b[5:]),
		maxSessions: binary.BigEndian.Uint32(b[9:]),
		liveness: Liveness{
			Interval: time.Duration(binary.BigEndian.Uint32(b[13:])) * time.Millisecond,
			Timeout:  time.Duration(binary.BigEndian.Uint32(b[17:])) * time.Millisecond,
		},
		method: b[21],
	}
	n := int(binary.BigEndian.Uint16(b[22:]))
	if n > MaxAuth {
		return helloV2{}, fmt.Errorf("%w: hello auth payload of %d bytes", ErrFrame, n)
	}
	h.auth = make([]byte, n)
	if _, err := io.ReadFull(r, h.auth); err != nil {
		return helloV2{}, err
	}
	if _, err := CheckWindow(h.window); err != nil {
		return helloV2{}, fmt.Errorf("%w: hello: %w", ErrFrame, err)
	}
	if h.maxSessions < 1 || h.maxSessions > maxSessionsWire {
		return helloV2{}, fmt.Errorf("%w: hello max sessions %d out of range", ErrFrame, h.maxSessions)
	}
	if err := h.liveness.Validate(); err != nil {
		return helloV2{}, fmt.Errorf("%w: hello: %w", ErrFrame, err)
	}
	return h, nil
}

// replyV2 is a version-2 listener's reply to a hello it bound.
type replyV2 struct {
	features    uint32
	window      uint32
	maxSessions uint32
}

func appendReplyV2(b []byte, r replyV2) []byte {
	b = append(b, statusBound, 2)
	b = binary.BigEndian.AppendUint32(b, r.features)
	b = binary.BigEndian.AppendUint32(b, r.window)
	return binary.BigEndian.AppendUint32(b, r.maxSessions)
}

// readReplyV2 reads a listener's reply to a version-2 hello.
func readReplyV2(r io.Reader) (replyV2, error) {
	var b [14]byte
	if _, err := io.ReadFull(r, b[:1]); err != nil {
		return replyV2{}, err
	}
	switch b[0] {
	case statusBound:
	case statusRefused:
		return replyV2{}, ErrRefused
	case statusVersion:
		if _, err := io.ReadFull(r, b[1:2]); err != nil {
			return replyV2{}, err
		}
		return replyV2{}, &VersionError{Max: int(b[1])}
	default:
		return replyV2{}, ErrFrame
	}
	if _, err := io.ReadFull(r, b[1:]); err != nil {
		return replyV2{}, err
	}
	if b[1] != 2 {
		return replyV2{}, fmt.Errorf("%w: reply of version %d to a version-2 hello", ErrFrame, b[1])
	}
	rep := replyV2{
		features:    binary.BigEndian.Uint32(b[2:]),
		window:      binary.BigEndian.Uint32(b[6:]),
		maxSessions: binary.BigEndian.Uint32(b[10:]),
	}
	if _, err := CheckWindow(rep.window); err != nil {
		return replyV2{}, fmt.Errorf("%w: reply: %w", ErrFrame, err)
	}
	if rep.maxSessions < 1 || rep.maxSessions > maxSessionsWire {
		return replyV2{}, fmt.Errorf("%w: reply max sessions %d out of range", ErrFrame, rep.maxSessions)
	}
	return rep, nil
}

// IsHello reports whether a connection's first bytes are a link hello's:
// a listener that shares its port (with TLS or HTTP/2) tells them apart
// by its first len(Magic) bytes.
func IsHello(prefix []byte) bool {
	return len(prefix) >= len(Magic) && string(prefix[:len(Magic)]) == Magic
}

// connState is nc's TLS state, zero on a plaintext connection.
func connState(nc net.Conn) tls.ConnectionState {
	if t, ok := nc.(interface{ ConnectionState() tls.ConnectionState }); ok {
		return t.ConnectionState()
	}
	return tls.ConnectionState{}
}

// handshake bounds a handshake on nc by ctx: its deadline, and its
// cancellation. The returned func ends the bound.
func handshake(ctx context.Context, nc net.Conn) func() {
	if d, ok := ctx.Deadline(); ok {
		_ = nc.SetDeadline(d)
	}
	stop := context.AfterFunc(ctx, func() { _ = nc.SetDeadline(time.Unix(1, 0)) })
	return func() {
		stop()
		_ = nc.SetDeadline(time.Time{})
	}
}

// Client binds nc, whose security (TLS) is the caller's, as the dialer:
// it sends the hello with cfg.Auth's credential and reads
// the listener's reply. A listener that does not speak the version
// answers with a VersionError (errors.Is ErrVersion) naming the versions
// it speaks; a listener of link v0.4 or older that speaks only version 1
// closes the connection after it read 37 bytes. A refusal is ErrRefused.
// On error the caller closes nc.
func Client(ctx context.Context, nc net.Conn, cfg Config) (*Conn, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()
	if cfg.Auth == nil {
		cfg.Auth = NoAuth
	}
	if cfg.Liveness == (Liveness{}) {
		cfg.Liveness = LivenessDefault
	}
	defer handshake(ctx, nc)()
	cs := connState(nc)
	method, auth, err := cfg.Auth.Hello(ctx, cs)
	if err != nil {
		return nil, err
	}
	if len(auth) > MaxAuth {
		return nil, fmt.Errorf("link: auth payload of %d bytes", len(auth))
	}
	h := appendHelloV2(nil, helloV2{
		maxVersion: MaxVersion, features: features, window: uint32(cfg.Window), maxSessions: uint32(cfg.MaxSessions), // #nosec G115 -- checked
		liveness: cfg.Liveness, method: method, auth: auth,
	})
	if _, err := nc.Write(h); err != nil {
		return nil, fmt.Errorf("%w: sending the hello: %w (%s)", ErrHandshake, err, hint(err, dialerHint))
	}
	rep, err := readReplyV2(nc)
	if err != nil {
		if errors.Is(err, ErrRefused) || errors.Is(err, ErrVersion) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: reading the listener's reply: %w (%s)", ErrHandshake, err, hint(err, dialerHint))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c := newConnV2(nc, cfg, int(rep.window), int(rep.maxSessions), true)
	c.peerFeatures = rep.features
	c.up(cfg.Observer, Identity{})
	return c, nil
}

// Server binds nc, whose security (TLS) is the caller's, as the
// listener: it reads the dialer's hello (Magic first: a caller that
// sniffed it replays it), verifies its credential with cfg.Auth, replies,
// and returns the connection and who the dialer is. It speaks version 2
// (MaxVersion). A version-1 hello (a dialer of link v0.4 or
// older) gets the version reply (StatusVersion, MaxVersion), which such a
// dialer reads as a refusal, and Server returns ErrV1. A refused dialer
// gets the refusal and Server ErrAuth. On error the caller closes nc.
//
// ctx bounds the handshake: give it a deadline. Without one, a dialer
// that sends its hello slowly, or not at all, holds nc and this goroutine
// for as long as it likes.
func Server(ctx context.Context, nc net.Conn, cfg Config) (*Conn, Identity, error) {
	if cfg.Auth == nil {
		return nil, Identity{}, errors.New("link: Config.Auth is nil: a listener needs an Authenticator (NameAuth on a trusted network, CertAuth over mutual TLS, NoAuth where peers need no names)")
	}
	if err := cfg.Validate(); err != nil {
		return nil, Identity{}, err
	}
	cfg = cfg.withDefaults()
	defer handshake(ctx, nc)()
	var head [len(Magic) + 1]byte
	if _, err := io.ReadFull(nc, head[:]); err != nil {
		return nil, Identity{}, fmt.Errorf("%w: reading the dialer's hello: %w (%s)", ErrHandshake, err, hint(err, "a dialer speaking TLS to a plaintext listener, or the other way round, or one that is not link's?"))
	}
	if !IsHello(head[:]) {
		return nil, Identity{}, fmt.Errorf("%w: %w: the dialer's first bytes %q are not a link hello (a TLS dialer on a plaintext listener, or not a link dialer?)", ErrHandshake, ErrFrame, head[:])
	}
	cs := connState(nc)
	switch v := int(head[len(Magic)]); {
	case v == 1:
		_, _ = nc.Write([]byte{statusVersion, MaxVersion})
		return nil, Identity{}, ErrV1
	case v == MaxVersion:
		h, err := readHelloV2(nc)
		if err != nil {
			return nil, Identity{}, err
		}
		id, err := cfg.Auth.Verify(ctx, cs, h.method, h.auth)
		if err != nil {
			_, _ = nc.Write([]byte{statusRefused})
			return nil, Identity{}, fmt.Errorf("%w: %w", ErrAuth, err)
		}
		rep := appendReplyV2(nil, replyV2{features: features, window: uint32(cfg.Window), maxSessions: uint32(cfg.MaxSessions)}) // #nosec G115 -- checked
		if _, err := nc.Write(rep); err != nil {
			return nil, Identity{}, err
		}
		if err := ctx.Err(); err != nil {
			return nil, Identity{}, err
		}
		if cfg.Liveness == (Liveness{}) {
			cfg.Liveness = h.liveness
		}
		c := newConnV2(nc, cfg, int(h.window), int(h.maxSessions), false)
		c.peerFeatures = h.features
		c.up(cfg.Observer, id)
		return c, id, nil
	case v > 1:
		_, _ = nc.Write([]byte{statusVersion, MaxVersion})
		return nil, Identity{}, &VersionError{Max: MaxVersion}
	default:
		return nil, Identity{}, ErrFrame
	}
}

// dialerHint is what to check when a dialer's hello fails for no TLS
// reason: the address, or TLS on one end only.
const dialerHint = "is it a link listener, and do both ends agree on TLS: both or neither?"

// hint is what to check after a handshake failed with err: the
// certificates when TLS refused them (this end's verification, or the
// peer's alert), else plain.
func hint(err error, plain string) string {
	var verify *tls.CertificateVerificationError
	var authority x509.UnknownAuthorityError
	var invalid x509.CertificateInvalidError
	var host x509.HostnameError
	var alert tls.AlertError
	var op *net.OpError
	if errors.As(err, &verify) || errors.As(err, &authority) || errors.As(err, &invalid) ||
		errors.As(err, &host) || errors.As(err, &alert) || (errors.As(err, &op) && op.Op == "remote error") {
		return "TLS refused a certificate: does each end trust the other's CA, does the certificate name the peer (ServerName, its DNS names), has it expired?"
	}
	return plain
}
