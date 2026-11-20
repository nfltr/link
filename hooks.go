// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"io"

	"github.com/nfltr/link/internal/linkhook"
)

// The methods peerlink calls that are not link's API (internal/linkhook).
func init() {
	linkhook.Join = func(a, b any) error { return join(a.(*Session), b.(*Session)) }
	linkhook.Pump = func(s any, w io.Writer, done func(int64, error)) { s.(*Session).startPump(w, done) }
	linkhook.WriteBuffers = func(s any, bufs [][]byte) (int64, error) { return s.(*Session).writeBuffers(bufs) }
	linkhook.CloseWriteSync = func(s any) error { return s.(*Session).closeWriteSync() }
	linkhook.GoingAway = func(c any) bool { return c.(*Conn).isGoingAway() }
	linkhook.Flush = func(c any) error { return c.(*Conn).flush() }
}
