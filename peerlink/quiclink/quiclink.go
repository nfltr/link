// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package quiclink is a peerlink.Link over QUIC (github.com/quic-go/quic-go):
// one QUIC connection per peer, each session a bidirectional QUIC stream.
// QUIC streams are independent: a packet lost on one session delays that
// session only, where a TCP connection holds back every session behind it.
// QUIC also keeps a connection across an address change of its dialler
// (connection migration: Wi-Fi to cellular, a NAT rebinding its port).
//
// On the wire a session's stream starts with a preamble: the header's
// length (two bytes, big endian) and the header. CloseWrite ends the
// stream's send side (FIN); Close ends it and stops reading
// (STOP_SENDING); Reset cancels both sides with an error code, so the peer
// reads an error, never io.EOF. A Node's Drain opens a unidirectional
// control stream on each of its connections and sends GOAWAY ('G') on it,
// then DONE ('D') once its own sessions on that connection ended; a peer
// opens nothing more on a connection it got GOAWAY on, and the connection
// closes (CONNECTION_CLOSE, code 0) once one side has the other's DONE and
// no session of its own left on it: nothing either side still reads is in
// flight then. A draining Node refuses new connections (CONNECTION_REFUSED)
// and resets the streams peers open (code 1).
//
// TLS 1.3 is QUIC's own and the caller's: Config.TLS (the listener's
// certificate, its ClientAuth) and the tls.Config the caller dials with.
// Handshakes are 1-RTT: 0-RTT is off (its data can be replayed).
//
// Liveness comes from a link.Liveness: QUIC keep-alive (a PING) Interval
// after the last packet received, and QUIC's idle timeout of Timeout,
// which runs from the first packet sent after the last one received (at
// most Interval later): a silent peer is dead within Interval + Timeout of
// its last packet. QUIC's idle timeout is never under three probe timeouts
// of the path (on a 150 ms path about 1 s, below every preset); its
// keep-alive never more often than 1.5 of them. DeadAfter states the
// bound.
//
// The reverse direction (peerlink's contract): a Node without Config.Dial
// opens its sessions on the connections its peers dialled in, and a Node
// listing a peer in Config.Connect dials it from Serve and keeps a
// connection up, redialling as it ends.
package quiclink

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/quic-go/quic-go"
)

// ALPN is the application protocol a Node sets on its TLS configs that
// name none.
const ALPN = "peerlink"

// Stream error codes: the peer reads a *quic.StreamError with one.
const (
	CodeRefused quic.StreamErrorCode = 1 // draining, not serving, or a bad preamble
	CodeReset   quic.StreamErrorCode = 2 // the peer's Reset
	CodeClosed  quic.StreamErrorCode = 3 // the peer's Close: it reads no more
)

// Connection error codes: the peer reads a *quic.ApplicationError with one.
const (
	CodeDone          quic.ApplicationErrorCode = 0 // drained: no session was left
	CodeDrainDeadline quic.ApplicationErrorCode = 1 // a Drain's ctx ended with sessions open
	CodeUnnamed       quic.ApplicationErrorCode = 2 // ServedPeer refused the connection
	CodeDeadPeer      quic.ApplicationErrorCode = 3 // nothing arrived for DeadAfter
)

// MaxHeader is the longest session header.
const MaxHeader = 1<<16 - 1

// Config is a Node's settings. Every policy in it is the caller's.
type Config struct {
	// Dial connects to peer with qc (the Node's QUIC settings: liveness,
	// windows, stream limit) and the caller's TLS: quic.DialAddr(ctx,
	// addr, tlsConf, qc), say, or Transport.Dial on a socket of the
	// caller's. A Node's tls.Configs need NextProtos (ALPN, say). Its
	// error returns from Open at once. nil: the Node dials nothing, and
	// Open opens on the connections the peer dialled in (see Open).
	Dial func(ctx context.Context, peer string, qc *quic.Config) (*quic.Conn, error)
	// Connect is the peers Serve dials when it starts and keeps one
	// connection up to until Drain (redialling after 100 ms, doubling to
	// 5 s while dials fail), so that they open sessions toward this Node
	// on it: a Node behind NAT, say, that its peers cannot dial.
	Connect []string
	// PacketConn is the UDP socket Serve accepts connections on, with
	// TLS (its certificate, ClientAuth and ClientCAs to name peers by
	// their certificates; NextProtos ALPN when empty). nil: the Node only
	// dials.
	PacketConn net.PacketConn
	TLS        *tls.Config
	// ServedPeer names the peer of a connection Serve accepted (for
	// accept); an error closes the connection. nil: the Subject
	// CommonName of the certificate the peer presented, which TLS
	// verified (none: the connection is closed).
	ServedPeer func(c *quic.Conn) (string, error)
	// Liveness: keep-alive after Interval, the connection dead after
	// Interval + Timeout without a packet. Zero: link.LivenessDefault.
	Liveness link.Liveness
	// Window is how many bytes of one session this side buffers (0:
	// link.DefaultWindow, 1 MiB); a connection buffers 16 windows.
	Window int
	// MaxSessions is how many sessions the peer may have open toward this
	// side on one connection (0: link.MaxSessions, 1024).
	MaxSessions int
}

// Node is a peerlink.Link over QUIC. Sessions are *Session: read and write
// deadlines.
type Node struct {
	cfg Config
	qc  *quic.Config

	mu       sync.Mutex
	dialled  map[string]*peerConns // the connections this Node dialled, by peer
	served   map[*conn]bool        // the connections Serve accepted
	changed  chan struct{}         // closed when served changes
	accept   func(string, []byte, peerlink.Session)
	draining bool
	drained  chan struct{} // closed when Drain starts
	sessions sync.WaitGroup
	tr       *quic.Transport
}

var (
	_ peerlink.Link    = (*Node)(nil)
	_ peerlink.Session = (*Session)(nil)
)

// New returns a Node; it dials nothing until Open or Serve. It refuses a
// Config that cannot work, naming the field: neither Dial nor PacketConn;
// a PacketConn without TLS; Connect without Dial; an invalid Liveness,
// Window or MaxSessions (as link.Config.Validate).
func New(cfg Config) (*Node, error) {
	switch {
	case cfg.Dial == nil && cfg.PacketConn == nil:
		return nil, errors.New("quiclink: Config.Dial and Config.PacketConn are both nil: set Dial to open sessions to peers, PacketConn (and TLS) to take theirs, or both")
	case cfg.PacketConn != nil && cfg.TLS == nil:
		return nil, errors.New("quiclink: Config.PacketConn is set but Config.TLS is nil: set the listener's certificate (and ClientCAs with ClientAuth tls.RequireAndVerifyClientCert, to name peers by their certificates)")
	case len(cfg.Connect) > 0 && cfg.Dial == nil:
		return nil, errors.New("quiclink: Config.Connect needs Config.Dial: set Dial to reach the peers it names")
	}
	if err := (link.Config{Liveness: cfg.Liveness, Window: cfg.Window, MaxSessions: cfg.MaxSessions}).Validate(); err != nil {
		return nil, fmt.Errorf("quiclink: %w", err)
	}
	n := &Node{cfg: cfg, dialled: map[string]*peerConns{}, served: map[*conn]bool{},
		changed: make(chan struct{}), drained: make(chan struct{})}
	l := n.liveness()
	window := uint64(max(cmpOr(cfg.Window, link.DefaultWindow), 1)) // #nosec G115 -- at least 1
	n.qc = &quic.Config{
		HandshakeIdleTimeout:           l.Interval + l.Timeout,
		MaxIdleTimeout:                 l.Timeout, // from the first packet sent after the last received: within Interval of it
		KeepAlivePeriod:                l.Interval,
		InitialStreamReceiveWindow:     window,
		MaxStreamReceiveWindow:         window,
		InitialConnectionReceiveWindow: 16 * window,
		MaxConnectionReceiveWindow:     16 * window,
		MaxIncomingStreams:             int64(cmpOr(cfg.MaxSessions, link.MaxSessions)),
		MaxIncomingUniStreams:          1, // the control stream
	}
	return n, nil
}

func cmpOr(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

func (n *Node) liveness() link.Liveness {
	if n.cfg.Liveness == (link.Liveness{}) {
		return link.LivenessDefault
	}
	return n.cfg.Liveness
}

// DeadAfter is the Node's dead-peer bound: Interval + Timeout of
// Config.Liveness (zero: link.LivenessDefault), given both Nodes use the
// same liveness (QUIC takes the lower idle timeout of the two).
func (n *Node) DeadAfter() time.Duration {
	l := n.liveness()
	return l.Interval + l.Timeout
}

// QUICConfig is the Node's QUIC settings, for Config.Dial.
func (n *Node) QUICConfig() *quic.Config { return n.qc.Clone() }

// conn is one QUIC connection and the sessions this Node has on it.
type conn struct {
	q     *quic.Conn
	peer  string
	n     *Node
	gone  atomic.Bool   // the peer sent GOAWAY: open nothing more here
	done  chan struct{} // the peer sent DONE
	dOnce sync.Once

	// under n.mu
	active int
	idle   chan struct{} // closed when active drops to 0
	ctl    *quic.SendStream
}

func (n *Node) newConn(q *quic.Conn, peer string) *conn {
	return &conn{q: q, peer: peer, n: n, done: make(chan struct{}), idle: make(chan struct{})}
}

// addLocked counts a session on c; false once draining.
func (c *conn) addLocked() bool {
	if c.n.draining {
		return false
	}
	c.active++
	c.n.sessions.Add(1)
	return true
}

func (c *conn) end() {
	c.n.mu.Lock()
	c.active--
	if c.active == 0 {
		close(c.idle)
		c.idle = make(chan struct{})
	}
	c.n.mu.Unlock()
	c.n.sessions.Done()
}

// watch closes c once no packet arrived for DeadAfter. QUIC's idle timer
// runs from the first packet sent after the last one received, and a
// connection that cannot send then (its writes stalled, its congestion
// window full) sends it late: by as long as it could not send.
// Sampling every Timeout/4 and closing after Interval + Timeout/2 without
// a packet keeps the bound (Interval + Timeout) while a live peer's
// keep-alives (every Interval) arrive within a round trip under Timeout/2.
func (c *conn) watch() {
	l := c.n.liveness()
	g, quiet := l.Timeout/4, l.Interval+l.Timeout/2
	tk := time.NewTicker(g)
	defer tk.Stop()
	last, seen := c.q.ConnectionStats().PacketsReceived, time.Now()
	for {
		select {
		case <-c.q.Context().Done():
			return
		case now := <-tk.C:
			if n := c.q.ConnectionStats().PacketsReceived; n != last {
				last, seen = n, now
			} else if now.Sub(seen) >= quiet {
				_ = c.q.CloseWithError(CodeDeadPeer, "dead peer")
				return
			}
		}
	}
}

// waitIdle waits until c has no session of this Node's, or ctx ends.
func (c *conn) waitIdle(ctx context.Context) bool {
	for {
		c.n.mu.Lock()
		active, idle := c.active, c.idle
		c.n.mu.Unlock()
		if active == 0 {
			return true
		}
		select {
		case <-idle:
		case <-ctx.Done():
			return false
		case <-c.q.Context().Done():
			return false
		}
	}
}

// run serves c until it closes: the streams the peer opens, its control
// stream, and closing c once the peer is DONE and c is idle.
func (c *conn) run() {
	go c.control()
	go c.watch()
	go func() {
		select {
		case <-c.done:
		case <-c.q.Context().Done():
			return
		}
		if c.waitIdle(c.q.Context()) {
			_ = c.q.CloseWithError(CodeDone, "done")
		}
	}()
	for {
		st, err := c.q.AcceptStream(context.Background())
		if err != nil {
			return
		}
		go c.n.acceptStream(c, st)
	}
}

// control reads the peer's control stream: GOAWAY, then DONE.
func (c *conn) control() {
	us, err := c.q.AcceptUniStream(context.Background())
	if err != nil {
		return
	}
	b := make([]byte, 1)
	for {
		if _, err := us.Read(b); err != nil {
			return
		}
		switch b[0] {
		case 'G':
			c.gone.Store(true)
			c.n.changedNow()
		case 'D':
			c.dOnce.Do(func() { close(c.done) })
		}
	}
}

// send writes a control byte to the peer (opening the control stream).
func (c *conn) send(b byte) {
	c.n.mu.Lock()
	if c.ctl == nil {
		s, err := c.q.OpenUniStream()
		if err != nil {
			c.n.mu.Unlock()
			return
		}
		c.ctl = s
	}
	s := c.ctl
	c.n.mu.Unlock()
	_, _ = s.Write([]byte{b})
}

func (c *conn) usable() bool { return c.q.Context().Err() == nil && !c.gone.Load() }

func (n *Node) changedNow() {
	n.mu.Lock()
	close(n.changed)
	n.changed = make(chan struct{})
	n.mu.Unlock()
}

// peerConns is the connection this Node dialled to one peer.
type peerConns struct {
	sem chan struct{} // held while dialling
	c   *conn
}

// Open opens a session to peer: a stream on the connection this Node
// dialled to it (dialled at the first Open, again once it closed or the
// peer sent GOAWAY). Without Config.Dial it opens on one of the
// connections peer dialled in (the one with the fewest sessions, not going
// away); while peer has none, Open waits for one until ctx ends, then
// fails with peerlink.ErrPeerUnreachable (and ctx's error).
func (n *Node) Open(ctx context.Context, peer string, header []byte) (peerlink.Session, error) {
	if len(header) > MaxHeader {
		return nil, fmt.Errorf("quiclink: header of %d bytes, at most %d", len(header), MaxHeader)
	}
	for {
		c, err := n.pick(ctx, peer)
		if err != nil {
			return nil, err
		}
		n.mu.Lock()
		ok := c.addLocked()
		n.mu.Unlock()
		if !ok {
			return nil, peerlink.ErrDraining
		}
		st, err := c.q.OpenStreamSync(ctx)
		if err == nil {
			_, err = st.Write(appendPreamble(make([]byte, 0, 2+len(header)), header))
			if err == nil {
				return &Session{Stream: st, c: c}, nil
			}
			st.CancelWrite(CodeReset)
			st.CancelRead(CodeReset)
		}
		c.end()
		if ctx.Err() != nil || c.q.Context().Err() == nil {
			return nil, err
		}
		// the connection closed meanwhile: pick (or dial) another
	}
}

// pick returns a usable connection to peer.
func (n *Node) pick(ctx context.Context, peer string) (*conn, error) {
	if n.cfg.Dial == nil {
		return n.pickServed(ctx, peer)
	}
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		return nil, peerlink.ErrDraining
	}
	p := n.dialled[peer]
	if p == nil {
		p = &peerConns{sem: make(chan struct{}, 1)}
		n.dialled[peer] = p
	}
	n.mu.Unlock()
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-p.sem }()
	n.mu.Lock()
	c := p.c
	n.mu.Unlock()
	if c != nil && c.usable() {
		return c, nil
	}
	return n.dial(ctx, peer, p)
}

// dial dials peer for p and starts serving the connection.
func (n *Node) dial(ctx context.Context, peer string, p *peerConns) (*conn, error) {
	q, err := n.cfg.Dial(ctx, peer, n.qc.Clone())
	if err != nil {
		return nil, err
	}
	c := n.newConn(q, peer)
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		_ = q.CloseWithError(CodeDone, "draining")
		return nil, peerlink.ErrDraining
	}
	p.c = c
	n.mu.Unlock()
	go c.run() // #nosec G118 -- the connection outlives the Open that dialled it
	return c, nil
}

// pickServed returns the least-loaded usable connection peer dialled in,
// waiting for one until ctx ends.
func (n *Node) pickServed(ctx context.Context, peer string) (*conn, error) {
	for {
		n.mu.Lock()
		if n.draining {
			n.mu.Unlock()
			return nil, peerlink.ErrDraining
		}
		var best *conn
		for c := range n.served {
			if c.peer == peer && c.usable() && (best == nil || c.active < best.active) {
				best = c
			}
		}
		changed := n.changed
		n.mu.Unlock()
		if best != nil {
			return best, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, fmt.Errorf("%w %q: %w", peerlink.ErrPeerUnreachable, peer, ctx.Err())
		}
	}
}

// appendPreamble appends a session stream's preamble for header (at most
// MaxHeader bytes) to b: its length (2 bytes, big endian), then header.
func appendPreamble(b, header []byte) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(header))) // #nosec G115 -- at most MaxHeader, checked by Open
	return append(b, header...)
}

// readPreamble reads a session stream's preamble and returns its header.
func readPreamble(r io.Reader) ([]byte, error) {
	var l [2]byte
	if _, err := io.ReadFull(r, l[:]); err != nil {
		return nil, err
	}
	h := make([]byte, binary.BigEndian.Uint16(l[:]))
	if _, err := io.ReadFull(r, h); err != nil {
		return nil, err
	}
	return h, nil
}

// acceptStream reads a stream's preamble and hands it to Serve's accept;
// before Serve, while draining or on a bad preamble, it is reset.
func (n *Node) acceptStream(c *conn, st *quic.Stream) {
	refuse := func() {
		st.CancelRead(CodeRefused)
		st.CancelWrite(CodeRefused)
	}
	_ = st.SetReadDeadline(time.Now().Add(n.DeadAfter()))
	h, err := readPreamble(st)
	if err != nil {
		refuse()
		return
	}
	_ = st.SetReadDeadline(time.Time{})
	n.mu.Lock()
	accept := n.accept
	ok := accept != nil && c.addLocked()
	n.mu.Unlock()
	if !ok {
		refuse()
		return
	}
	accept(c.peer, h, &Session{Stream: st, c: c})
}

// Serve takes the sessions peers open until Drain: on the connections it
// accepts on Config.PacketConn (if any) and on the connections this Node
// dialled, Config.Connect's among them, which it dials now. An error of
// the listener other than Drain's ends Serve.
func (n *Node) Serve(accept func(peer string, header []byte, s peerlink.Session)) error {
	n.mu.Lock()
	n.accept = accept
	draining := n.draining
	n.mu.Unlock()
	if !draining {
		for _, peer := range n.cfg.Connect {
			go n.keep(peer)
		}
	}
	if n.cfg.PacketConn == nil {
		<-n.drained
		return nil
	}
	qc := n.qc.Clone()
	qc.GetConfigForClient = func(*quic.ClientInfo) (*quic.Config, error) {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.draining {
			return nil, peerlink.ErrDraining // CONNECTION_REFUSED
		}
		return n.qc.Clone(), nil // nil would be quic-go's defaults
	}
	tr := &quic.Transport{Conn: n.cfg.PacketConn}
	ln, err := tr.Listen(withALPN(n.cfg.TLS), qc)
	if err != nil {
		return err
	}
	n.mu.Lock()
	if n.draining { // Drain began before the listener was up
		n.mu.Unlock()
		_ = tr.Close()
		return nil
	}
	n.tr = tr
	n.mu.Unlock()
	for {
		q, err := ln.Accept(context.Background())
		if err != nil {
			n.mu.Lock()
			draining := n.draining
			n.mu.Unlock()
			if draining {
				return nil
			}
			return err
		}
		go n.serveConn(q)
	}
}

// withALPN returns tc with NextProtos ALPN when it names none.
func withALPN(tc *tls.Config) *tls.Config {
	if tc == nil {
		tc = &tls.Config{MinVersion: tls.VersionTLS13}
	}
	if len(tc.NextProtos) > 0 {
		return tc
	}
	tc = tc.Clone()
	tc.NextProtos = []string{ALPN}
	return tc
}

func (n *Node) serveConn(q *quic.Conn) {
	peer, err := n.servedPeer(q)
	if err != nil {
		_ = q.CloseWithError(CodeUnnamed, err.Error())
		return
	}
	c := n.newConn(q, peer)
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		_ = q.CloseWithError(CodeDone, "draining")
		return
	}
	n.served[c] = true
	close(n.changed)
	n.changed = make(chan struct{})
	n.mu.Unlock()
	c.run()
	n.mu.Lock()
	delete(n.served, c)
	close(n.changed)
	n.changed = make(chan struct{})
	n.mu.Unlock()
}

func (n *Node) servedPeer(q *quic.Conn) (string, error) {
	if n.cfg.ServedPeer != nil {
		return n.cfg.ServedPeer(q)
	}
	certs := q.ConnectionState().TLS.PeerCertificates
	if len(certs) == 0 || certs[0].Subject.CommonName == "" {
		return "", errors.New("quiclink: the peer presented no certificate with a CommonName")
	}
	return certs[0].Subject.CommonName, nil
}

// keep keeps a connection to peer up until Drain.
func (n *Node) keep(peer string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { <-n.drained; cancel() }()
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		c, err := n.pick(ctx, peer)
		if err != nil {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
			}
			backoff = min(2*backoff, 5*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		for {
			n.mu.Lock()
			changed := n.changed
			n.mu.Unlock()
			if !c.usable() {
				break // closed, or GOAWAY: dial the next one
			}
			select {
			case <-c.q.Context().Done():
			case <-changed:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Drain stops the Node: it refuses new connections and the streams peers
// open from now on, sends GOAWAY on every connection, and once its
// sessions on a connection ended sends DONE there; it returns once every
// connection closed (see the package doc). At ctx's end it closes every
// connection (code CodeDrainDeadline) and returns ctx's error.
func (n *Node) Drain(ctx context.Context) error {
	n.mu.Lock()
	first := !n.draining
	n.draining = true
	if first {
		close(n.drained)
	}
	conns := slices.Collect(maps.Keys(n.served))
	for _, p := range n.dialled {
		if p.c != nil {
			conns = append(conns, p.c)
		}
	}
	n.mu.Unlock()
	for _, c := range conns {
		go func() {
			c.send('G')
			if c.waitIdle(ctx) {
				c.send('D')
			}
		}()
	}
	idle := make(chan struct{})
	go func() {
		n.sessions.Wait()
		for _, c := range conns {
			<-c.q.Context().Done()
		}
		close(idle)
	}()
	var err error
	select {
	case <-idle:
	case <-ctx.Done():
		err = ctx.Err()
		for _, c := range conns {
			_ = c.q.CloseWithError(CodeDrainDeadline, "drain deadline")
		}
	}
	n.mu.Lock()
	tr := n.tr
	n.mu.Unlock()
	if tr != nil {
		_ = tr.Close()
	}
	return err
}

// Session is a session: a QUIC stream with the net.Conn methods and
// CloseWrite and Reset.
type Session struct {
	*quic.Stream
	c     *conn
	once  sync.Once
	reset atomic.Bool
}

var _ net.Conn = (*Session)(nil)

func (s *Session) ended() { s.once.Do(s.c.end) }

// CloseWrite ends this side's writes: the peer reads io.EOF after them.
func (s *Session) CloseWrite() error {
	if s.reset.Load() {
		return net.ErrClosed
	}
	return s.Stream.Close()
}

// Close ends the session: the peer reads what was written, then io.EOF;
// its writes fail.
func (s *Session) Close() error {
	var err error
	if !s.reset.Load() {
		err = s.Stream.Close()
	}
	s.Stream.CancelRead(CodeClosed)
	s.ended()
	return err
}

// Reset ends the session as failed: the peer's reads and writes fail.
func (s *Session) Reset() error {
	s.reset.Store(true)
	s.Stream.CancelWrite(CodeReset)
	s.Stream.CancelRead(CodeReset)
	s.ended()
	return nil
}

// LocalAddr is the QUIC connection's local address.
func (s *Session) LocalAddr() net.Addr { return s.c.q.LocalAddr() }

// RemoteAddr is the QUIC connection's peer address (it changes as the
// peer migrates).
func (s *Session) RemoteAddr() net.Addr { return s.c.q.RemoteAddr() }
