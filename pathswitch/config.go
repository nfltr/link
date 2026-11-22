// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"errors"
	"time"
)

// Config is a Conn's configuration. The zero value is usable.
type Config struct {
	// Window is W: the bytes a sender keeps until its peer acknowledged
	// them, per direction (spec BufferBound). 0: 1 MiB, link's session
	// window. With MaxWindow it is where the window starts, and what a
	// receiver lets its application hold unread before it stops
	// acknowledging (half of it: spec AckGate).
	Window int
	// MaxWindow is the most the window grows to (0: Window, fixed). A
	// sender doubles its window, up to MaxWindow, while its writes wait on
	// it and a window's bytes come back acknowledged within a few round
	// trips (spec DirectPath.tla TakeAck, as quic-go's stream windows):
	// the receiver acknowledges only while its reader keeps up, so a slow
	// reader's session keeps the small window, a fast one on a long path
	// grows to what the path holds. The window never falls.
	MaxWindow int
	// Detach bounds how long an end waits for a new stream once its
	// stream ended before it resets the session (ErrDetached); an
	// aborter (Abort) waits as long for the peer, and a Close (and
	// WaitAcked) as long with nothing acknowledged. 0: 30 s.
	Detach time.Duration
	// Linger bounds how long a Close (and WaitAcked) waits with nothing
	// heard from the peer (no frame: an ACK, a byte, its END): a peer
	// silent that long is taken as gone and a Close resets the session,
	// as TCP's user timeout (RFC 5482; spec DirectPath.tla LingerOut,
	// CloseBounded; guard TestCloseGonePeerWithinLinger). A peer that
	// keeps answering is waited for while it acknowledges, Detach with
	// nothing acknowledged. 0: 5 s.
	Linger time.Duration
	// OnStreamEnd, a client's only: the current stream ended (its path
	// died); the caller opens one on a path that works and calls Move
	// (spec Fallback). Called on its own goroutine, no lock held.
	OnStreamEnd func(epoch uint64, err error)
}

var (
	// ErrReset: the session was reset (Abort, the peer's ABORT, a Close
	// of the peer's direction before its end).
	ErrReset = errors.New("pathswitch: session reset")
	// ErrDetached: no stream came back within Config.Detach.
	ErrDetached = errors.New("pathswitch: no stream within the detach bound")
	// ErrProtocol: a frame the protocol does not allow (a relay
	// rewriting the relayed stream, a bug): the session is reset.
	ErrProtocol = errors.New("pathswitch: protocol error")
	// ErrStaleEpoch: Attach of an epoch not newer than the session's (a
	// replayed OPEN): refused, the session untouched.
	ErrStaleEpoch = errors.New("pathswitch: stale epoch")
	// ErrNotClient: Move on a server's Conn, or Attach on a client's.
	ErrNotClient = errors.New("pathswitch: wrong side")
)
