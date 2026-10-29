// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink

import (
	"net"
	"sync"
)

// Session is a session of a Link (the contract): a net.Conn that also
// ends its writes alone (CloseWrite) or ends as failed (Reset).
type Session interface {
	net.Conn
	// CloseWrite ends this side's writes: the peer reads io.EOF after
	// them and can still write back.
	CloseWrite() error
	// Reset ends the session as failed: the peer reads an error that is
	// not io.EOF.
	Reset() error
}

// Listener is a net.Listener of sessions, for a server that takes one (an
// http.Server, a grpc.Server): Handle is an accept for a Link's Serve,
// or for the sessions an accept routes to it by header, and Accept returns
// them. The caller of Open dials such a server with Open as its dialer:
// http.Transport.DialContext, grpc.WithContextDialer.
type Listener struct {
	conns     chan net.Conn
	done      chan struct{}
	closeOnce sync.Once
}

// NewListener returns a Listener.
func NewListener() *Listener {
	return &Listener{conns: make(chan net.Conn), done: make(chan struct{})}
}

// Handle hands s to Accept, waiting for it; once the Listener is closed
// it closes s.
func (l *Listener) Handle(_ string, _ []byte, s Session) {
	select {
	case l.conns <- s:
	case <-l.done:
		_ = s.Close()
	}
}

// Accept returns the next session Handle got; net.ErrClosed once the
// Listener is closed.
func (l *Listener) Accept() (net.Conn, error) {
	select {
	case s := <-l.conns:
		return s, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// Close makes Accept return net.ErrClosed and Handle close what it gets.
// It closes no session Accept returned.
func (l *Listener) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return nil
}

// Addr is a placeholder: sessions have no listening address.
func (l *Listener) Addr() net.Addr { return addr{} }

type addr struct{}

func (addr) Network() string { return "peerlink" }
func (addr) String() string  { return "peerlink" }
