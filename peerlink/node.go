// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink

import (
	"context"
	"errors"
	"maps"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/link"
)

// Config is a Node's settings. Every policy in it is the caller's.
type Config struct {
	// Dial connects to peer with its security done (TLS, mutual TLS, or
	// none on a trusted path). It is also where a caller fails fast toward
	// a peer it takes for unreachable: its error returns from Open at once
	// (with Pool.Backoff nil).
	Dial func(ctx context.Context, peer string) (net.Conn, error)
	// Listener is where Serve accepts connections, its security done (a
	// TLS listener, say). nil: the Node only dials.
	Listener net.Listener
	// Link configures every connection, dialled and accepted: Auth (a
	// listener needs one; the peer name Serve passes is the
	// Identity.Subject its Verify returns), Liveness, Window, MaxSessions.
	// Its Observer is not used: see Observer.
	Link link.Config
	// Pool configures the connections to each peer: Min, Max, Grow,
	// GrowWait, Backoff. Its Link and Dial are not used.
	Pool link.PoolConfig
	// Observer, when set, returns the observer for the connections to and
	// from peer (nil: none): for a dialled connection the name Open was
	// given, for an accepted one the authenticated Subject, asked once the
	// handshake verified it.
	Observer func(peer string) link.Observer
	// ServedPeer, when set, names the peer of a connection Serve accepted,
	// for accept and Observer, from the connection Listener returned and
	// the Identity Link.Auth verified (its remote address, say, where
	// peers share one credential). nil: the Identity's Subject.
	ServedPeer func(nc net.Conn, id link.Identity) string
}

// Node is a Link over github.com/nfltr/link: a link.Pool per peer it opens
// sessions to (made at the first Open, kept until Drain), and a link.Mux
// per connection Serve accepts. Sessions are *link.Session (dialled ones
// wrapped, with every method): read deadlines, no write deadlines.
type Node struct {
	cfg Config

	mu       sync.Mutex
	pools    map[string]*link.Pool
	served   map[*link.Mux]struct{}
	draining bool
	opened   sync.WaitGroup // dialled sessions not yet closed
}

var _ Link = (*Node)(nil)

// New returns a Node; it dials nothing until Open.
func New(cfg Config) *Node {
	return &Node{cfg: cfg, pools: map[string]*link.Pool{}, served: map[*link.Mux]struct{}{}}
}

// DeadAfter is the Node's dead-peer bound: Interval + Timeout of
// Config.Link.Liveness (zero: link.LivenessDefault), given every Node uses
// the same liveness (a listener's zero liveness takes the dialer's).
func (n *Node) DeadAfter() time.Duration {
	l := n.cfg.Link.Liveness
	if l == (link.Liveness{}) {
		l = link.LivenessDefault
	}
	return l.Interval + l.Timeout
}

// Open opens a session to peer on one of its pool's connections.
func (n *Node) Open(ctx context.Context, peer string, header []byte) (net.Conn, error) {
	if n.cfg.Dial == nil {
		return nil, errors.New("peerlink: Open needs Config.Dial")
	}
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		return nil, ErrDraining
	}
	p := n.pools[peer]
	if p == nil {
		p = n.newPool(peer)
		n.pools[peer] = p
	}
	n.opened.Add(1) // before Drain's Wait: both under mu
	n.mu.Unlock()
	s, err := p.Open(ctx, header)
	if err != nil {
		n.opened.Done()
		return nil, err
	}
	return &session{Session: s, done: n.opened.Done}, nil
}

func (n *Node) newPool(peer string) *link.Pool {
	pc := n.cfg.Pool
	pc.Link = n.cfg.Link
	pc.Link.Observer = nil
	if n.cfg.Observer != nil {
		if o := n.cfg.Observer(peer); o != nil {
			pc.Link.Observer = o
		}
	}
	pc.Dial = func(ctx context.Context) (net.Conn, error) { return n.cfg.Dial(ctx, peer) }
	return link.NewPool(pc)
}

// session is a dialled session: Drain waits for its Close or Reset.
type session struct {
	*link.Session
	once sync.Once
	done func()
}

func (s *session) Close() error {
	err := s.Session.Close()
	s.once.Do(s.done)
	return err
}

func (s *session) Reset() error {
	err := s.Session.Reset()
	s.once.Do(s.done)
	return err
}

// Serve accepts connections on Config.Listener and serves their sessions
// until Drain. A failed handshake closes that connection only; an Accept
// error other than Drain's ends Serve.
func (n *Node) Serve(accept func(peer string, header []byte, s net.Conn)) error {
	if n.cfg.Listener == nil {
		return errors.New("peerlink: Serve needs Config.Listener")
	}
	for {
		nc, err := n.cfg.Listener.Accept()
		if err != nil {
			n.mu.Lock()
			draining := n.draining
			n.mu.Unlock()
			if draining {
				return nil
			}
			return err
		}
		go n.serveConn(nc, accept)
	}
}

func (n *Node) serveConn(nc net.Conn, accept func(string, []byte, net.Conn)) {
	cfg := n.cfg.Link
	cfg.Observer = nil
	if n.cfg.Observer != nil {
		cfg.Observer = &servedObserver{of: func(id link.Identity) link.Observer { return n.cfg.Observer(n.servedPeer(nc, id)) }}
	}
	// A dialer silent for the dead-peer bound in its handshake is dead too.
	ctx, cancel := context.WithTimeout(context.Background(), n.DeadAfter())
	c, id, err := link.Server(ctx, nc, cfg)
	cancel()
	if err != nil {
		_ = nc.Close()
		return
	}
	m := link.NewMux(c)
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		_ = m.Close()
		return
	}
	n.served[m] = struct{}{}
	n.mu.Unlock()
	peer := n.servedPeer(nc, id)
	_ = m.Serve(func(h []byte, s *link.Session) { accept(peer, h, s) })
	n.mu.Lock()
	delete(n.served, m)
	n.mu.Unlock()
}

func (n *Node) servedPeer(nc net.Conn, id link.Identity) string {
	if n.cfg.ServedPeer != nil {
		return n.cfg.ServedPeer(nc, id)
	}
	return id.Subject
}

// Drain closes the listener, sends GOAWAY on every accepted connection
// (each closes once its sessions ended), waits for the sessions this Node
// opened to be closed, then closes every pool. At ctx's end it closes
// everything at once and returns ctx's error.
func (n *Node) Drain(ctx context.Context) error {
	n.mu.Lock()
	first := !n.draining
	n.draining = true
	served := slices.Collect(maps.Keys(n.served))
	n.mu.Unlock()
	if first && n.cfg.Listener != nil {
		_ = n.cfg.Listener.Close()
	}
	for _, m := range served {
		_ = m.GoAway(time.Time{}) // ctx's end is Drain's to act on
	}
	idle := make(chan struct{})
	go func() {
		n.opened.Wait()
		for _, m := range served {
			<-m.Conn().Done()
		}
		close(idle)
	}()
	var err error
	select {
	case <-idle:
	case <-ctx.Done():
		err = ctx.Err()
	}
	n.mu.Lock()
	pools := slices.Collect(maps.Values(n.pools))
	n.mu.Unlock()
	for _, m := range served {
		_ = m.Close()
	}
	for _, p := range pools {
		_ = p.Close()
	}
	return err
}

// servedObserver hands an accepted connection's events to the observer
// Config.Observer returns for its peer, once ConnUp names the peer.
type servedObserver struct {
	of func(link.Identity) link.Observer
	o  atomic.Pointer[link.Observer]
}

func (s *servedObserver) get() link.Observer {
	if o := s.o.Load(); o != nil {
		return *o
	}
	return link.NopObserver{}
}

func (s *servedObserver) ConnUp(id link.Identity) {
	o := s.of(id)
	if o == nil {
		o = link.NopObserver{}
	}
	s.o.Store(&o)
	o.ConnUp(id)
}

func (s *servedObserver) ConnDown(id link.Identity, err error) { s.get().ConnDown(id, err) }
func (s *servedObserver) SessionOpen()                         { s.get().SessionOpen() }
func (s *servedObserver) SessionClose(err error)               { s.get().SessionClose(err) }
func (s *servedObserver) CreditWait(d time.Duration)           { s.get().CreditWait(d) }
func (s *servedObserver) PingRTT(d time.Duration)              { s.get().PingRTT(d) }
func (s *servedObserver) GoAway(id link.Identity)              { s.get().GoAway(id) }
