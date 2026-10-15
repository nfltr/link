// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink

import (
	"context"
	"errors"
	"fmt"
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
	// (with Pool.Backoff nil). nil: the Node dials nothing, and Open opens
	// on the connections the peer dialled in (Serve accepted them): see
	// Open.
	Dial func(ctx context.Context, peer string) (net.Conn, error)
	// Connect is the peers Serve dials when it starts and keeps Pool.Min
	// connections to (at least one) until Drain, so that they open
	// sessions toward this Node on connections it dialled: a Node behind
	// NAT, say, that its peers cannot dial. Serve takes those sessions as
	// it takes the ones on accepted connections.
	Connect []string
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
	// HandshakeError, when set, gets each connection Serve accepted whose
	// handshake failed (a wrong credential, a TLS mismatch, a dialer that
	// is not link's) and why; the connection is closed. nil: they are
	// closed silently.
	HandshakeError func(nc net.Conn, err error)
}

// Node is a Link over github.com/nfltr/link: a link.Pool per peer it opens
// sessions to (made at the first Open, or by Serve for Config.Connect,
// kept until Drain), and a link.Mux per connection Serve accepts. Sessions
// go both ways on every connection: a peer opens sessions on the
// connections this Node dialled (Serve takes them), and a Node without
// Config.Dial opens sessions on the connections its peers dialled in.
// Sessions are *link.Session (some wrapped, with every method): read
// deadlines, no write deadlines.
type Node struct {
	cfg Config

	mu       sync.Mutex
	pools    map[string]*link.Pool
	served   map[*link.Mux]string // accepted connections and their peer
	changed  chan struct{}        // closed when served changes
	accept   func(string, []byte, net.Conn)
	draining bool
	drained  chan struct{}  // closed when Drain starts
	opened   sync.WaitGroup // dialled sessions not yet closed
	accepted sync.WaitGroup // sessions taken on dialled connections, not yet closed
}

var (
	_ Link    = (*Node)(nil)
	_ Session = (*session)(nil)
	_ Session = (*link.Session)(nil)
)

// New returns a Node; it dials nothing until Open or Serve. It refuses a
// Config that cannot work, naming the field: neither Dial nor Listener;
// a Listener without Link.Auth; Connect without Dial; an invalid Link
// (link.Config.Validate) or Pool.
func New(cfg Config) (*Node, error) {
	switch {
	case cfg.Dial == nil && cfg.Listener == nil:
		return nil, errors.New("peerlink: Config.Dial and Config.Listener are both nil: set Dial to open sessions to peers, Listener to take theirs, or both")
	case cfg.Listener != nil && cfg.Link.Auth == nil:
		return nil, errors.New("peerlink: Config.Listener is set but Config.Link.Auth is nil: set link.NameAuth(name) on a trusted network, link.CertAuth over mutual TLS, or an Authenticator of your own")
	case len(cfg.Connect) > 0 && cfg.Dial == nil:
		return nil, errors.New("peerlink: Config.Connect needs Config.Dial: set Dial to reach the peers it names")
	}
	if err := cfg.Link.Validate(); err != nil {
		return nil, fmt.Errorf("peerlink: Config.Link: %w", err)
	}
	if cfg.Dial != nil {
		pc := cfg.Pool
		pc.Link, pc.Dial = cfg.Link, func(ctx context.Context) (net.Conn, error) { return cfg.Dial(ctx, "") } // as newPool sets them
		if err := pc.Validate(); err != nil {
			return nil, fmt.Errorf("peerlink: Config.Pool: %w", err)
		}
	}
	return &Node{cfg: cfg, pools: map[string]*link.Pool{}, served: map[*link.Mux]string{},
		changed: make(chan struct{}), drained: make(chan struct{})}, nil
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

// Open opens a session to peer on one of its pool's connections. Without
// Config.Dial it opens on one of the connections peer dialled in (the one
// with the fewest sessions, not going away); while peer has none, Open
// waits for one until ctx ends, then fails with ErrPeerUnreachable (and
// ctx's error).
func (n *Node) Open(ctx context.Context, peer string, header []byte) (net.Conn, error) {
	if n.cfg.Dial == nil {
		return n.openServed(ctx, peer, header)
	}
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		return nil, ErrDraining
	}
	p := n.poolLocked(peer)
	n.opened.Add(1) // before Drain's Wait: both under mu
	n.mu.Unlock()
	s, err := p.Open(ctx, header)
	if err != nil {
		n.opened.Done()
		return nil, fmt.Errorf("peerlink: open to %q: %w", peer, err)
	}
	return &session{Session: s, done: n.opened.Done}, nil
}

// openServed opens a session on a connection peer dialled in.
func (n *Node) openServed(ctx context.Context, peer string, header []byte) (net.Conn, error) {
	refused := map[*link.Mux]bool{}
	for {
		n.mu.Lock()
		if n.draining {
			n.mu.Unlock()
			return nil, ErrDraining
		}
		var best *link.Mux
		names := map[string]bool{}
		for m, p := range n.served {
			names[p] = true
			if p == peer && !refused[m] && !m.Conn().GoingAway() && (best == nil || m.Sessions() < best.Sessions()) {
				best = m
			}
		}
		changed := n.changed
		n.mu.Unlock()
		if best == nil {
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return nil, fmt.Errorf("%w %q (no connection from it; connected: %q): %w", ErrPeerUnreachable, peer, slices.Sorted(maps.Keys(names)), ctx.Err())
			}
		}
		s, err := best.Open(header)
		if err == nil {
			return s, nil
		}
		if !errors.Is(err, link.ErrGoAway) && !errors.Is(err, link.ErrFull) && !errors.Is(err, net.ErrClosed) {
			return nil, err
		}
		refused[best] = true // it stopped taking sessions meanwhile
	}
}

// poolLocked returns peer's pool, made at the first call.
func (n *Node) poolLocked(peer string) *link.Pool {
	p := n.pools[peer]
	if p == nil {
		p = n.newPool(peer)
		n.pools[peer] = p
	}
	return p
}

func (n *Node) newPool(peer string) *link.Pool {
	pc := n.cfg.Pool
	if slices.Contains(n.cfg.Connect, peer) {
		pc.Min = max(pc.Min, 1)
	}
	pc.Accept = func(h []byte, s *link.Session) { n.acceptDialled(peer, h, s) }
	pc.Link = n.cfg.Link
	pc.Link.Observer = nil
	if n.cfg.Observer != nil {
		if o := n.cfg.Observer(peer); o != nil {
			pc.Link.Observer = o
		}
	}
	pc.Dial = func(ctx context.Context) (net.Conn, error) { return n.cfg.Dial(ctx, peer) }
	p, err := link.NewPool(pc)
	if err != nil {
		panic(err) // New validated pc's fields
	}
	return p
}

// acceptDialled hands a session peer opened on a connection this Node
// dialled to Serve's accept; before Serve or once draining, it is reset.
func (n *Node) acceptDialled(peer string, h []byte, s *link.Session) {
	n.mu.Lock()
	accept := n.accept
	ok := accept != nil && !n.draining
	if ok {
		n.accepted.Add(1) // before Drain's Wait: both under mu
	}
	n.mu.Unlock()
	if !ok {
		_ = s.Reset()
		return
	}
	accept(peer, h, &session{Session: s, done: n.accepted.Done})
}

// session is a session on a dialled connection: Drain waits for its Close
// or Reset.
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

// Serve takes the sessions peers open until Drain: on the connections it
// accepts on Config.Listener (if any), and on the connections this Node
// dialled, Config.Connect's among them, which it dials now. A failed
// handshake closes that connection only (Config.HandshakeError hears of
// it); an Accept error other than Drain's ends Serve.
func (n *Node) Serve(accept func(peer string, header []byte, s net.Conn)) error {
	n.mu.Lock()
	n.accept = accept
	if !n.draining {
		for _, peer := range n.cfg.Connect {
			n.poolLocked(peer)
		}
	}
	n.mu.Unlock()
	if n.cfg.Listener == nil {
		<-n.drained
		return nil
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
		if n.cfg.HandshakeError != nil {
			n.cfg.HandshakeError(nc, err)
		}
		return
	}
	m := link.NewMux(c)
	peer := n.servedPeer(nc, id)
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		_ = m.Close()
		return
	}
	n.served[m] = peer
	n.changedLocked()
	n.mu.Unlock()
	_ = m.Serve(func(h []byte, s *link.Session) { accept(peer, h, s) })
	n.mu.Lock()
	delete(n.served, m)
	n.changedLocked()
	n.mu.Unlock()
}

func (n *Node) changedLocked() {
	close(n.changed)
	n.changed = make(chan struct{})
}

func (n *Node) servedPeer(nc net.Conn, id link.Identity) string {
	if n.cfg.ServedPeer != nil {
		return n.cfg.ServedPeer(nc, id)
	}
	return id.Subject
}

// Drain closes the listener, sends GOAWAY on every accepted connection
// (each closes once its sessions ended), resets the sessions peers open on
// dialled connections from then on, waits for the sessions on dialled
// connections to be closed, then closes every pool. At ctx's end it
// closes everything at once and returns ctx's error.
func (n *Node) Drain(ctx context.Context) error {
	n.mu.Lock()
	first := !n.draining
	n.draining = true
	served := slices.Collect(maps.Keys(n.served))
	if first {
		close(n.drained)
	}
	n.mu.Unlock()
	if first && n.cfg.Listener != nil {
		_ = n.cfg.Listener.Close()
	}
	for _, m := range served {
		go func() { _ = m.Drain(context.WithoutCancel(ctx)) }() // ctx's end is this Drain's to act on (below)
	}
	idle := make(chan struct{})
	go func() {
		n.opened.Wait()
		n.accepted.Wait()
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
