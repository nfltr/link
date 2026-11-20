// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package link multiplexes many byte-stream sessions, each a net.Conn with
// its own flow control, over one connection that the caller dials and
// secures (plain TCP, TLS, a pipe). A session whose reader is slow stalls
// only itself, dead peers are noticed within a bound the caller picks, and
// the package imports only the Go standard library.
//
// # Where to start
//
// Most programs want [github.com/nfltr/link/peerlink]: a Node opens
// sessions to named peers and serves the sessions they open, keeping the
// connections (dialling, redialling, draining) for it. The README's quick
// start and docs/recipes.md (plaintext and mutual TLS, liveness presets,
// gRPC and HTTP over a link, graceful drain, reconnects, metrics) build on
// it.
//
// This package is the layer below, for callers that manage connections
// themselves:
//
//   - [Client] and [Server] run the handshake on a connection (the hello,
//     an [Authenticator]'s credential) and return a [Conn].
//   - [Mux] carries sessions over a Conn as [Session]s (net.Conns).
//   - [Pool] keeps several connections to one peer, with growth, dial
//     backoff and make-before-break [Pool.Drain].
//   - [Credit] is per-session flow control, for a caller that carries
//     sessions over a transport of its own (window frames: [AppendWindow]).
//
// Authenticators: [NameAuth] on a trusted network, [CertAuth] over mutual
// TLS, [NoAuth] where peers need no names, or the caller's own (SECURITY.md).
// [Config.Validate] and [PoolConfig.Validate] name the field to fix.
//
// # Liveness
//
// A connection is dead once nothing arrived for Interval + Timeout ([ErrDeadPeer]):
// each side pings once it received nothing for Interval. Pick a preset by
// how fast a dead peer must be noticed against how many idle frames you can
// afford (one small frame each way per Interval per connection):
//
//   - [LivenessFast] (4.5 s): failover and interactive use on a good network.
//   - [LivenessDefault] (15 s): most services; what a zero Config gets.
//   - [LivenessNAT] (30 s): paths through NATs, conntrack or load balancers
//     that drop mappings idle for 30 s or more.
//   - [LivenessDatacenter] (45 s): stable networks where fewer wakeups matter
//     more than fast failover.
//   - [LivenessMobile] (60 s): laptops and phones whose radio sleeps or hands
//     over: the fewest wakeups, and a short silence is not taken for death.
//
// [ParseLiveness] reads a preset by name, with overrides ("nat,timeout=40s"),
// for a flag or a config file. The listener takes the dialer's liveness
// unless its Config sets one.
//
// # Errors
//
// Every error to test with errors.Is, by where it comes from (the README
// has the same as a table, with what to change):
//
//   - Client: [ErrRefused] (the listener refused the credential),
//     [ErrVersion] (errors.As a *[VersionError]), [ErrHandshake] (the reply
//     did not arrive: not a link listener, or a TLS mismatch; it wraps the
//     network's error), [ErrFrame], ctx's error.
//   - Server: [ErrAuth] (the Authenticator refused; it wraps Verify's
//     error), [ErrVersion] (a version-1 hello among them), [ErrHandshake] (with [ErrFrame] when
//     the first bytes are not a hello), ctx's error.
//   - Mux.Serve: [ErrFrame], [ErrCredit], [ErrFull] (the peer
//     broke the protocol), [ErrDeadPeer] (liveness ended the connection),
//     the network's (io.EOF once the peer closed).
//   - Mux.Open, Pool.Open: [ErrFull] (the peer's max
//     sessions), [ErrGoAway] (the connection is going away); Mux.Open after
//     Serve returned: net.ErrClosed.
//   - [Session.Answered]: nil (accepted), an [*Abort] ([ErrOpenRefused]: a
//     refusal), [ErrNoAnswers] (the peer predates chaining), ctx's error.
//   - [Session.CloseWriteSync]: [ErrReset] (the peer closed the session
//     before the end reached it), [ErrEndAckIdle] (the far end acknowledged
//     nothing for 30 s), [ErrConnClosed], CloseWrite's.
//   - Session reads: io.EOF (the peer ended its writes on purpose),
//     [ErrReset] (the peer's Reset), an [*Abort] (the peer's, with chaining),
//     [ErrConnClosed] (the connection ended under the session; it wraps why,
//     [ErrDeadPeer] among them), [ErrDrainDeadline] (this side's
//     [Mux.Drain] deadline cut it), os.ErrDeadlineExceeded. Writes past such an end: io.ErrClosedPipe,
//     wrapping the same error.
//   - Config.Validate, PoolConfig.Validate, NewPool, Client, Server: a
//     message naming the field and what to set.
//
// # Stability
//
// The wire (version 2, WIRE.md) is held byte for byte by its test vectors;
// a change to it is a new version negotiated in the hello. Before v1.0.0
// the Go API may change in a minor release, with the migration in the
// changelog; from v1.0.0 it follows the Go 1 compatibility rules, and a
// breaking change is a /v2 module. [Observer] may gain methods in minor
// releases (implementations embed [NopObserver]); interfaces that callers
// call ([Authenticator], peerlink.Link, peerlink.Session) never change,
// and new capabilities arrive as optional interfaces.
//
// # Wire format
//
// WIRE.md in the module is the specification of wire version 2 (the only
// version since v0.5.0) and of how a version above 2 is negotiated, with
// test vectors for every message (testdata/wire_vectors.json).
package link
