// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package pathswitch moves a session's byte stream between paths, a
// relayed one and a direct one (and back), with no byte lost, duplicated
// or reordered: the path layer of nfltr's direct paths
// (docs/decisions/direct-paths-2026-11-11.md in github.com/nfltr/nfltr),
// checked as spec/DirectPath.tla (spec/README.md "Direct paths"). It sits
// under the session's end-to-end TLS: the bytes it moves are TLS records,
// so a byte the relay rewrites on the relayed path is a TLS error above
// it (a reset), never a silent change. Standard library only; it runs
// over any Stream: a link session through the relay, a QUIC stream on a
// direct connection.
//
// Each attach opens a fresh stream, an epoch. The client (the side that
// dials) is the only side that opens them (Move): OPEN carries the epoch,
// the client's count of the server's bytes and a token the server's
// caller checks before it calls Attach (the session's proof: an exporter
// of the session's TLS bound to the epoch, the relay client's). The
// server answers RESUME with its count of the client's bytes; each end
// reads only its newest stream and sends from the count it got. A sender
// keeps every byte its peer has not acknowledged (ACK, RESUME), at most
// Config.Window of them, and resends from the RESUME's count on the new
// stream; the client resets the streams it left once both counts are in.
// A direction's end is END after its last byte, kept and resent like a
// byte; ABORT is the session's reset, which the aborter sends again on
// every stream the session opens until the peer answered it. A clean end
// (Close at both ends) ends the current stream with FIN at each end: the
// server's once it has everything, the client's only after the server's,
// since the client alone can open another stream for it (a server that
// left answers one with its count and FIN).
//
// Wire: one type byte, then uvarints.
//
//	D len payload                 DATA, 1..16384 bytes
//	K n                           ACK: the peer holds every position below n
//	O epoch rcvd flags len token  OPEN (flags 1: the client reset the session)
//	R n                           RESUME: the server holds every position below n
//	E n                           END after byte n-1 (position n)
//	X                             ABORT
//
// Positions count bytes from the session's start and the END as one more:
// an ACK of n+1 after END(n) frees the END.
//
// Trace mode (go test -tags linktrace, LINKTRACE=<dir>) records each
// session's steps; make spec-traces checks them against the model.
package pathswitch
