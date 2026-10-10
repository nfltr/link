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
// # Wire format, version 1
//
//	hello  dialer -> listener: "NFDP" version(1) token(32 bytes) window(4)
//	reply  listener -> dialer: status(1), 0 = bound; window(4) when bound
//	frame  both ways:          length(4, of the body) type(1) body
//	  type 0, data:    sid_len(1) sid payload
//	  type 1, message: opaque bytes of the caller's (never empty)
//	  type 2, window:  sid_len(1) sid increment(4)
//
// Integers are big endian. A session id (sid) is at most 255 bytes, chosen
// by the side that opens the session. A window is how many bytes of one
// session the side that announces it buffers (see Conn.Acquire). In
// version 1 the frames that open and close a session are message frames in
// the caller's encoding (Codec); session ids that begin with '_' name
// control frames, which Mux leaves to the caller.
//
// testdata/v1_session.golden holds a recorded version-1 session; the tests
// hold every release to those bytes.
package link
