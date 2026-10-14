// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package link is the nfltr endpoint-to-endpoint link layer: many
// sessions, each a byte stream with its own flow control, multiplexed over
// one connection the caller dials and secures (TLS, a proxy, a pipe).
// A session whose reader is slow stalls only itself; one write to the
// network carries every frame queued at the time.
//
// It imports only the Go standard library.
//
// Client and Server bind a connection (the hello, an Authenticator's
// credential, version negotiation); Mux carries sessions over it as
// net.Conns; Pool keeps several connections to one peer. Credit is the
// per-session flow control, also for a caller that carries its sessions
// on a transport of its own.
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
// it does not know. Bit 0, FeatureReset (since v0.3.0): the side reads
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
