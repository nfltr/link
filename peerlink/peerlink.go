// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package peerlink is a small, transport-neutral API for byte-stream
// sessions between peers (Link), an implementation of it over
// github.com/nfltr/link (Node), and, in peerlinktest, a conformance suite
// any implementation runs to show it keeps the contract below.
//
// # Contract
//
// A Link opens sessions to peers (Open) and takes the sessions peers open
// to it (Serve). Every implementation keeps these rules; peerlinktest
// checks each one.
//
//   - A session is a net.Conn: each direction a byte stream, in order and
//     intact, independent of the other direction. Every session also has
//     CloseWrite() error and Reset() error, reached by a type assertion
//     (as with *net.TCPConn's CloseWrite).
//   - Close ends the session both ways. CloseWrite ends this side's
//     writes only: the peer reads every byte written, then io.EOF, and can
//     still write back. Reset ends the session as failed.
//   - io.EOF means the peer ended its writes on purpose (CloseWrite or
//     Close). A session cut any other way (the peer's Reset, its
//     connection closed or broken, a dead peer, a Drain's deadline) reads
//     an error that is not io.EOF.
//   - A dead peer (nothing closed and nothing arriving: it lost power, the
//     path is silently partitioned) surfaces as an error on every session
//     with it, on both ends, within a bound the implementation states
//     (Node.DeadAfter).
//   - A session whose reader is slow or stuck stalls only itself; on a
//     slow path every session keeps getting its turn.
//   - Drain stops taking new sessions while the open ones run to their
//     end: Open on the draining Link fails with ErrDraining, and a peer's
//     Open toward it fails (at Open, or as an error on the session's first
//     Read). Once its sessions ended, or at ctx's end, when it cuts the
//     rest, Drain releases everything the Link holds and Serve returns.
//   - A header of up to 4 KiB reaches the peer's accept as it was sent.
//   - An implementation may open sessions on the connections a peer
//     dialled in (toward a peer behind NAT, which it cannot dial), and
//     says whether it does. Its Open toward such a peer with no
//     connection up waits for one until ctx ends, then fails with an
//     error that is ErrPeerUnreachable and ctx's error; once the peer
//     dials again, sessions open on the new connection. Every other rule
//     holds in that direction too.
//   - Policy is the caller's: which peer to use, retries, who may connect
//     (the implementation's authenticator or credentials), and timeouts.
//
// Deadlines are not part of the contract; each implementation documents
// the ones its sessions keep.
package peerlink

import (
	"context"
	"errors"
	"net"
)

// Link carries byte-stream sessions between peers.
type Link interface {
	// Open opens a session to peer, a name the implementation's dialer
	// resolves, with header: the bytes the peer's accept receives. ctx
	// bounds the open, not the session. A peer's refusal surfaces as
	// Open's error or as the session's first Read error (never io.EOF).
	Open(ctx context.Context, peer string, header []byte) (net.Conn, error)
	// Serve takes the sessions peers open until Drain. accept runs in a
	// goroutine of its own for each, with the opener's name as the
	// implementation authenticated it and the header; it owns the session
	// and closes it (it may return first). Serve returns nil once Drain
	// stopped it, else the error that did.
	Serve(accept func(peer string, header []byte, s net.Conn)) error
	// Drain stops the Link: no new sessions either way, open ones run to
	// their end, then everything is released; at ctx's end the sessions
	// still open are cut and Drain returns ctx's error. It is the only way
	// to release a Link (a done ctx closes at once), and the caller
	// chooses when: until then Serve and Open run as usual.
	Drain(ctx context.Context) error
}

// ErrDraining is Open's refusal on a Link that is draining.
var ErrDraining = errors.New("peerlink: draining")

// ErrPeerUnreachable is Open's failure toward a peer it opens sessions to
// only on connections the peer dialled, when none came up before Open's
// ctx ended; the error is ctx's error too.
var ErrPeerUnreachable = errors.New("peerlink: peer unreachable")
