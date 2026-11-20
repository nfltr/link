// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package linkhook carries the link methods peerlink calls that are not
// link's API (docs/v1-api-review.md items 4, 6, 7): package link sets
// these in its init, peerlink calls them. Their arguments are typed any
// because this package cannot import link (link imports it); each is a
// *link.Session or *link.Conn as named, and any other type panics.
package linkhook

import "io"

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
)
