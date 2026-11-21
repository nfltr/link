// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package linkhook carries the link methods peerlink calls that are not
// link's API (docs/v1-api-review.md items 4, 6, 7): package link sets
// these in its init, peerlink calls them. Their arguments are typed any
// because this package cannot import link (link imports it); each is a
// *link.Session or *link.Conn as named, and any other type panics.
package linkhook

import (
	"io"
	"net"
	"sync/atomic"
)

var (
	// Join is link.join(a, b *link.Session): peerlink.Join of two link
	// sessions.
	Join func(a, b any) error
	// Pump is (*link.Session).startPump.
	Pump func(s any, w io.Writer, done func(n int64, err error))
	// WriteBuffers is (*link.Session).writeBuffers.
	WriteBuffers func(s any, bufs [][]byte) (int64, error)
	// GoingAway is (*link.Conn).isGoingAway.
	GoingAway func(c any) bool
	// Flush is (*link.Conn).flush.
	Flush func(c any) error

	// TraceHopHold and TraceHopRead are peerlink's two-joint trace
	// recorder (spec/TraceHopHold.tla) for the tests of a module that
	// cannot import peerlink's own (quiclink's): peerlink sets them under
	// -tags linktrace, nil otherwise. TraceHopHold is peerlink's
	// traceHopHold (g, r: the marked joints' app conns; far: R's far end
	// apps); TraceHopRead logs AppRead when c is one of those far ends,
	// called before c sends the ENDACK of the end it read.
	TraceHopHold func(test string, g, r net.Conn, far ...net.Conn) func()
	TraceHopRead func(c net.Conn)

	// Joint, set by a test, sees a peerlink joint's steps
	// (spec/HopHold.tla sync-no-hold's guards): "sync" (src: the conn
	// whose end it passes on) in joint.sync's goroutine before its
	// CloseWrite, which waits while it blocks; "held" (src nil) when the
	// joint holds an end while its other direction syncs, under the
	// joint's lock: it must not block then.
	Joint atomic.Pointer[func(ev string, src net.Conn)]
)
