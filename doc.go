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
//   - [Conn] and [Credit] are frames and per-session flow control, for a
//     caller with its own receive loop or its own transport.
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
//     error), [ErrV1] and [ErrVersion], [ErrHandshake] (with [ErrFrame] when
//     the first bytes are not a hello), ctx's error.
//   - Conn.Recv, Mux.Serve: [ErrFrame], [ErrCredit], [ErrFull] (the peer
//     broke the protocol), [ErrDeadPeer] (liveness ended the connection),
//     the network's (io.EOF once the peer closed).
//   - Conn.SendOpen, Mux.Open, Pool.Open: [ErrFull] (the peer's max
//     sessions), [ErrGoAway] (the connection is going away); Mux.Open after
//     Serve returned: net.ErrClosed.
//   - Session reads: io.EOF (the peer ended its writes on purpose),
//     [ErrReset] (the peer's Reset), [ErrConnClosed] (the connection ended
//     under the session; it wraps why, [ErrDeadPeer] among them),
//     os.ErrDeadlineExceeded. Writes past such an end: io.ErrClosedPipe,
//     wrapping the same error.
//   - Config.Validate, PoolConfig.Validate, NewPool, Client, Server: a
//     message naming the field and what to set.
//
// # Wire format, version 2
//
// WIRE.md in the module is the full specification, with test vectors for
// every message (testdata/wire_vectors.json); in short:
//
// Version 2 is the only version since v0.5.0 (version 1, the hello of a
// bare token and opens and closes in the caller's messages, was removed;
// a version-1 hello gets the version reply, below, and Server ErrV1).
//
//	hello  "NFDP" version(1)=2 max_version(1) features(4) window(4)
//	       max_sessions(4) interval_ms(4) timeout_ms(4)
//	       auth_method(1) auth_len(2) auth(auth_len, at most 4096)
//	reply  status(1):
//	         0 bound:   version(1)=2 features(4) window(4) max_sessions(4)
//	         1 refused (the credential)
//	         2 version: max_version(1), the highest the listener speaks
//	frame  both ways: length(4, of the body) type(1) body
//	  type 0, data:    sid_len(1) sid payload
//	  type 1, message: opaque bytes of the caller's (never empty; Conn.SendMsg
//	                   and Conn.Recv carry them, a Mux drops them)
//	  type 2, window:  sid_len(1) sid increment(4)
//	  type 3, open:    sid_len(1) sid header (the opener's bytes, at most 64 KiB)
//	  type 4, close:   sid_len(1) sid code(1): 0 the sender neither sends
//	                   nor takes more; 1 it sends no more (half-close);
//	                   2 as 0, and the session failed (reset; only to a
//	                   peer that announced FeatureReset)
//	  type 5, ping:    8 opaque bytes
//	  type 6, pong:    the ping's 8 bytes
//	  type 7, goaway:  drain_ms(4, 0 = none) reason (UTF-8, at most 1024)
//
// Integers are big endian. A session id (sid) is at most 255 bytes. A
// window is how many bytes of one session the side that announces it
// buffers (see Credit.Acquire). Session ids that begin with '_' name
// control frames, which Mux leaves to the caller.
//
// A listener answers a version it does not speak with status 2. A
// listener of link v0.4 or older that speaks only version 1 reads 37
// bytes of a version-2 hello before it closes the connection.
//
// max_sessions is how many sessions the peer may have open toward the
// side that announces it at once; an open past it ends the connection.
// Feature bits: a side announces the ones it reads and ignores the bits
// it does not know. Bit 0, reset (since v0.3.0): the side reads
// CLOSE code 2; v0.2 sides announce none and end the connection on code 2,
// so a reset to them is sent as code 0. Liveness is the dialer's (interval 1s to 15s, timeout at
// least one interval); a listener takes it unless it has its own: each
// side pings once it received nothing for the interval and ends the
// connection when nothing arrived for the timeout after that (dead within
// interval + timeout of the peer's last frame, idle or busy); every ping
// is answered (the latest, when several wait). An open starts the
// session's credit on both sides with the peer's window; a close of code 0
// ends it. A Mux picks session ids as uvarints, odd on the dialer and even
// on the listener. After a goaway the receiver opens no more sessions on
// the connection. The sender follows its goaway with a ping: opens that
// arrive before that ping's pong crossed the goaway and run like any other
// session (since v0.4.2; before, they were refused with a close), opens
// after it are refused; the sender closes the connection once its last
// session ended and the pong arrived, after writing every frame.
//
// testdata/v2_session.golden holds a recorded version-2 session.
package link
