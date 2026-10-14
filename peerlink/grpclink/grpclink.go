// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package grpclink is a peerlink.Link over gRPC: each session is one
// bidirectional stream, so HTTP/2 multiplexes sessions over a connection
// per peer. It needs no generated code: the service is registered with a
// hand-written grpc.ServiceDesc and messages are raw bytes.
//
// It carries peerlink sessions over gRPC, for a network that passes only
// gRPC. To serve a gRPC service over a link instead, give a grpc.Server a
// peerlink.Listener and dial it with Open (Example_grpcOverLink).
//
// On the wire a session is the stream /peerlink.v1.Link/Session; the
// opener's header travels in the metadata key peerlink-header-bin. Every
// message is one kind byte and its bytes: 0 data, 1 the sender writes no
// more (CloseWrite), 2 the sender neither writes nor reads more (Close).
// A Reset cancels the stream (opener) or ends it with codes.Aborted
// (accepter), so the peer reads an error, never io.EOF.
//
// Sessions go one way on a connection: the dialer opens them (a gRPC
// stream starts at the client). A Node opens nothing on the connections
// its peers dialled, so it cannot open sessions toward a peer it cannot
// dial (one behind NAT): Open needs Config.Dial. That would take the
// dialer keeping streams open for the listener to take as sessions, a
// protocol of its own on top of gRPC; peerlink.Node does it.
//
// Liveness is gRPC keepalive, set from a link.Liveness: the dialer pings
// after max(Interval, 10 s) of silence (gRPC's floor for clients), the
// listener after Interval, each waiting Timeout for an answer; a dialer
// whose writes are stuck then waits up to 5 s more trying to send GOAWAY
// before it closes the connection (grpc-go's transport). DeadAfter states
// the bound that results.
package grpclink

import (
	"context"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Config is a Node's settings. Every policy in it is the caller's.
type Config struct {
	// Dial connects to peer. Its security is the caller's: done here
	// (with insecure transport credentials in DialOptions), or by
	// transport credentials in DialOptions over a plain connection.
	Dial func(ctx context.Context, peer string) (net.Conn, error)
	// Listener is where Serve accepts connections. nil: the Node only
	// dials.
	Listener net.Listener
	// Liveness sets keepalive on both sides; zero is
	// link.LivenessDefault.
	Liveness link.Liveness
	// DialOptions apply to every peer's grpc.ClientConn and must carry
	// transport credentials. Keepalive, the dialer and the codec are the
	// Node's.
	DialOptions []grpc.DialOption
	// ServerOptions apply to Serve's grpc.Server (credentials,
	// interceptors). Keepalive and the codec are the Node's.
	ServerOptions []grpc.ServerOption
	// Identify names the opener of an accepted stream from its context
	// (peer.FromContext, metadata, the credentials' AuthInfo); an error
	// refuses the session (codes.Unauthenticated). nil: the peer's
	// address.
	Identify func(ctx context.Context) (string, error)
}

// Node is a peerlink.Link over gRPC: a grpc.ClientConn per peer it opens
// sessions to (made at the first Open, kept until Drain) and a grpc.Server
// for Serve. Sessions have no deadlines (SetDeadline and its kind return
// errors.ErrUnsupported).
type Node struct {
	cfg Config

	mu       sync.Mutex
	clients  map[string]*grpc.ClientConn
	srv      *grpc.Server
	draining bool
	opened   sync.WaitGroup // dialled streams not yet ended
}

var (
	_ peerlink.Link    = (*Node)(nil)
	_ peerlink.Session = (*session)(nil)
)

// New returns a Node; it dials nothing until Open. It refuses a Config
// that cannot work, naming the field: neither Dial nor Listener, or an
// invalid Liveness.
func New(cfg Config) (*Node, error) {
	if cfg.Dial == nil && cfg.Listener == nil {
		return nil, errors.New("grpclink: Config.Dial and Config.Listener are both nil: set Dial to open sessions to peers, Listener to take theirs, or both")
	}
	if err := (link.Config{Liveness: cfg.Liveness}).Validate(); err != nil {
		return nil, fmt.Errorf("grpclink: %w", err)
	}
	return &Node{cfg: cfg, clients: map[string]*grpc.ClientConn{}}, nil
}

const (
	method     = "/peerlink.v1.Link/Session"
	headerKey  = "peerlink-header-bin"
	maxChunk   = 32 << 10
	clientPing = 10 * time.Second // gRPC's least keepalive time for a client
	goAwayWait = 5 * time.Second  // grpc-go's wait to write GOAWAY when closing a transport
)

// Message kinds, the first byte of every message.
const (
	kindData  byte = 0
	kindEOF   byte = 1 // the sender writes no more
	kindClose byte = 2 // the sender neither writes nor reads more
)

var (
	errCut      = errors.New("grpclink: stream ended without the peer's close")
	errProtocol = errors.New("grpclink: malformed message")
)

var streamDesc = grpc.StreamDesc{StreamName: "Session", ServerStreams: true, ClientStreams: true}

func (n *Node) liveness() link.Liveness {
	if n.cfg.Liveness == (link.Liveness{}) {
		return link.LivenessDefault
	}
	return n.cfg.Liveness
}

// DeadAfter is the Node's dead-peer bound: max(Interval, 10 s) + Timeout
// + 5 s of its Liveness, the dialer's (the listener notices within
// Interval + Timeout).
func (n *Node) DeadAfter() time.Duration {
	l := n.liveness()
	return max(l.Interval, clientPing) + l.Timeout + goAwayWait
}

// Open opens a stream to peer, on its grpc.ClientConn. It does not wait
// for the peer: a refusal surfaces as the session's first Read error.
func (n *Node) Open(ctx context.Context, peer string, header []byte) (net.Conn, error) {
	if n.cfg.Dial == nil {
		return nil, errors.New("grpclink: Open needs Config.Dial")
	}
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		return nil, peerlink.ErrDraining
	}
	cc := n.clients[peer]
	if cc == nil {
		var err error
		if cc, err = n.dial(peer); err != nil {
			n.mu.Unlock()
			return nil, err
		}
		n.clients[peer] = cc
	}
	n.opened.Add(1) // before Drain's Wait: both under mu
	n.mu.Unlock()

	sctx, cancel := context.WithCancel(metadata.AppendToOutgoingContext(context.Background(), headerKey, string(header)))
	stop := context.AfterFunc(ctx, cancel) // ctx bounds the open only
	cs, err := cc.NewStream(sctx, &streamDesc, method)
	if !stop() && err == nil {
		err = ctx.Err()
	}
	if err != nil {
		cancel()
		n.opened.Done()
		return nil, err
	}
	s := newSession(cs.SendMsg, cs.CloseSend, cs.Context())
	s.reset = cancel
	go func() {
		s.recvLoop(cs.RecvMsg)
		cancel() // the stream ended: release it
		n.opened.Done()
	}()
	return s, nil
}

func (n *Node) dial(peer string) (*grpc.ClientConn, error) {
	l := n.liveness()
	opts := append(slices.Clone(n.cfg.DialOptions),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return n.cfg.Dial(ctx, peer) }),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: max(l.Interval, clientPing), Timeout: l.Timeout, PermitWithoutStream: true}),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(rawCodec{})),
	)
	return grpc.NewClient("passthrough:///"+peer, opts...)
}

// Serve serves streams on Config.Listener until Drain.
func (n *Node) Serve(accept func(peer string, header []byte, s net.Conn)) error {
	if n.cfg.Listener == nil {
		return errors.New("grpclink: Serve needs Config.Listener")
	}
	l := n.liveness()
	srv := grpc.NewServer(append(slices.Clone(n.cfg.ServerOptions),
		grpc.ForceServerCodecV2(rawCodec{}),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: l.Interval, Timeout: l.Timeout}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: l.Interval, PermitWithoutStream: true}),
	)...)
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "peerlink.v1.Link",
		Streams: []grpc.StreamDesc{{
			StreamName: "Session", ServerStreams: true, ClientStreams: true,
			Handler: func(_ any, ss grpc.ServerStream) error { return n.handle(ss, accept) },
		}},
	}, nil)
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		return nil
	}
	n.srv = srv
	n.mu.Unlock()
	err := srv.Serve(n.cfg.Listener)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.draining {
		return nil
	}
	return err
}

// handle serves one stream: it runs until the session is closed here, or
// the stream ends (the opener's reset, a dead or closed connection).
func (n *Node) handle(ss grpc.ServerStream, accept func(string, []byte, net.Conn)) error {
	ctx := ss.Context()
	n.mu.Lock()
	draining := n.draining
	n.mu.Unlock()
	if draining {
		return status.Error(codes.Unavailable, "grpclink: draining")
	}
	who, err := n.identify(ctx)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	var header []byte
	if v := metadata.ValueFromIncomingContext(ctx, headerKey); len(v) > 0 {
		header = []byte(v[0])
	}
	s := newSession(ss.SendMsg, nil, ctx)
	go s.recvLoop(ss.RecvMsg)
	go accept(who, header, s)
	defer s.gone.Store(true) // no SendMsg after the handler returns
	select {
	case <-s.closed:
		if s.wasReset.Load() {
			return status.Error(codes.Aborted, "grpclink: session reset")
		}
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

func (n *Node) identify(ctx context.Context) (string, error) {
	if n.cfg.Identify != nil {
		return n.cfg.Identify(ctx)
	}
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String(), nil
	}
	return "", errors.New("no peer address")
}

// Drain stops the server gracefully (no new streams; open ones run to
// their end), waits for the streams this Node opened to end, then
// closes every grpc.ClientConn. At ctx's end it stops the server and
// closes the connections at once and returns ctx's error.
func (n *Node) Drain(ctx context.Context) error {
	n.mu.Lock()
	n.draining = true
	srv := n.srv
	n.mu.Unlock()
	if srv == nil && n.cfg.Listener != nil {
		_ = n.cfg.Listener.Close() // Serve is not running
	}
	idle := make(chan struct{})
	go func() {
		if srv != nil {
			srv.GracefulStop()
		}
		n.opened.Wait()
		close(idle)
	}()
	var err error
	select {
	case <-idle:
	case <-ctx.Done():
		err = ctx.Err()
		if srv != nil {
			srv.Stop()
		}
	}
	n.mu.Lock()
	clients := slices.Collect(maps.Values(n.clients))
	n.mu.Unlock()
	for _, cc := range clients {
		_ = cc.Close()
	}
	return err
}

// session is one stream as a net.Conn.
type session struct {
	send      func(any) error
	closeSend func() error // the opener's CloseSend; nil on the accepter
	reset     func()       // the opener's: cancel the stream
	local     net.Addr
	remote    net.Addr

	in         chan recvd    // from recvLoop
	closed     chan struct{} // Close or Reset
	once       sync.Once
	wasReset   atomic.Bool
	peerClosed atomic.Bool // the peer sent kindClose: it reads no more
	gone       atomic.Bool // the accepter's handler returned

	rmu  sync.Mutex
	rest []byte
	rerr error // what Read returns once rest is empty

	wmu     sync.Mutex
	wclosed bool // CloseWrite or Close sent
}

type recvd struct {
	b   []byte
	err error
}

func newSession(send func(any) error, closeSend func() error, ctx context.Context) *session {
	s := &session{send: send, closeSend: closeSend, in: make(chan recvd), closed: make(chan struct{})}
	if p, ok := peer.FromContext(ctx); ok {
		s.local, s.remote = p.LocalAddr, p.Addr
	}
	return s
}

// recvLoop hands the stream's messages to Read, up to the peer's EOF or
// close (or an error), then reads on until the stream ends, so the opener
// can release it.
func (s *session) recvLoop(recv func(any) error) {
	ended := false
	for {
		var m []byte
		err := recv(&m)
		if err == nil && len(m) == 0 {
			err = errProtocol
		}
		if err == nil && m[0] == kindClose {
			s.peerClosed.Store(true)
		}
		if !ended {
			if err == io.EOF {
				err = errCut // the stream ended before the peer's EOF or close
			}
			select {
			case s.in <- recvd{m, err}:
			case <-s.closed:
			}
			ended = err != nil || m[0] != kindData
		}
		if err != nil {
			return
		}
	}
}

// Read reads the peer's bytes, then io.EOF after its CloseWrite or Close,
// or the error that cut the stream.
func (s *session) Read(b []byte) (int, error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	for len(s.rest) == 0 {
		if s.rerr != nil {
			return 0, s.rerr
		}
		select {
		case r := <-s.in:
			switch {
			case r.err != nil:
				s.rerr = r.err
			case r.b[0] == kindData:
				s.rest = r.b[1:]
			case r.b[0] == kindEOF || r.b[0] == kindClose:
				s.rerr = io.EOF
			default:
				s.rerr = errProtocol
			}
		case <-s.closed:
			return 0, net.ErrClosed
		}
	}
	k := copy(b, s.rest)
	s.rest = s.rest[k:]
	return k, nil
}

// Write sends b in messages of at most 32 KiB, each waiting for the
// stream's flow control.
func (s *session) Write(b []byte) (int, error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	written := 0
	for len(b) > 0 {
		if s.wclosed || s.peerClosed.Load() || s.gone.Load() {
			return written, io.ErrClosedPipe
		}
		k := min(len(b), maxChunk)
		m := make([]byte, 1+k) // gRPC may hold a message after SendMsg returns
		copy(m[1:], b[:k])
		if err := s.send(m); err != nil {
			return written, sendErr(err)
		}
		written += k
		b = b[k:]
	}
	return written, nil
}

// sendErr: the opener's SendMsg says io.EOF once the accepter ended the
// stream (the reason is RecvMsg's).
func sendErr(err error) error {
	if err == io.EOF {
		return io.ErrClosedPipe
	}
	return err
}

// CloseWrite sends the EOF message: the peer reads io.EOF after what was
// written, and can still write back.
func (s *session) CloseWrite() error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.wclosed || s.gone.Load() {
		return nil
	}
	s.wclosed = true
	err := s.send([]byte{kindEOF})
	if s.closeSend != nil {
		_ = s.closeSend()
	}
	if err != nil {
		return sendErr(err)
	}
	return nil
}

// Close sends the close message (unless CloseWrite did its part) and
// releases the session; a pending Read returns net.ErrClosed. Close while
// a Write is in progress resets the session instead.
func (s *session) Close() error {
	s.once.Do(func() {
		if !s.wmu.TryLock() {
			s.end(true)
			return
		}
		if !s.wclosed && !s.gone.Load() {
			s.wclosed = true
			_ = s.send([]byte{kindClose})
			if s.closeSend != nil {
				_ = s.closeSend()
			}
		}
		s.wmu.Unlock()
		s.end(false)
	})
	return nil
}

// Reset ends the session as failed: the peer reads an error.
func (s *session) Reset() error {
	s.once.Do(func() { s.end(true) })
	return nil
}

func (s *session) end(reset bool) {
	s.wasReset.Store(reset)
	close(s.closed) // the accepter's handler returns
	if reset && s.reset != nil {
		s.reset()
	}
}

func (s *session) LocalAddr() net.Addr  { return s.local }
func (s *session) RemoteAddr() net.Addr { return s.remote }

// SetDeadline is not supported.
func (s *session) SetDeadline(time.Time) error { return errors.ErrUnsupported }

// SetReadDeadline is not supported.
func (s *session) SetReadDeadline(time.Time) error { return errors.ErrUnsupported }

// SetWriteDeadline is not supported.
func (s *session) SetWriteDeadline(time.Time) error { return errors.ErrUnsupported }

// rawCodec carries []byte messages as they are.
type rawCodec struct{}

func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("grpclink: cannot marshal %T", v)
	}
	return mem.BufferSlice{mem.SliceBuffer(b)}, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	p, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("grpclink: cannot unmarshal into %T", v)
	}
	*p = data.Materialize()
	return nil
}

func (rawCodec) Name() string { return "peerlink-raw" }
