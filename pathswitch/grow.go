// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import "time"

// RoundTripper is a Stream that knows its path's least round trip (a
// direct QUIC stream: quiclink Session.MinRTT). A session grows its window
// (Config.MaxWindow) only on such a stream: the round trip must be the
// transport's, since this layer's ACKs wait on the reader (at most
// Config.Window / 2 unread) and a round trip timed from them is the
// reader's pace.
type RoundTripper interface {
	MinRTT() time.Duration
}

// growth is a sender's window auto-tuning (Config.MaxWindow; spec
// DirectPath.tla TakeAck, lane LINKLEFT): quic-go's receive-window rule
// (flow_controller_base.go maybeAdjustWindowSize) on the sender's side,
// the receiver's ACKs being its reader's pace. An epoch runs until a whole
// window was acknowledged (quic-go: half of it; a whole one here so that
// the gate's slack, Window/2 acknowledged before the reader is behind,
// never grows a slow reader's window); it took less than 4 x that fraction
// of the path's round trip while a Write waited on the window: the window
// doubles, up to MaxWindow. It never falls; a new path starts an epoch.
type growth struct {
	waiting int       // Writes waiting on the window
	t0      time.Time // the epoch's start (zero: none yet)
	a0      uint64    // acked at its start
}

// path starts the epoch anew: a new path's (Resume).
func (g *growth) path() { g.t0 = time.Time{} }

// growOnAck runs after an ACK freed what it acknowledged (under c.mu);
// limited: a Write waited on the window when it came.
func (c *Conn) growOnAck(now time.Time, limited bool) {
	rt, ok := c.cur.s.(RoundTripper)
	if !ok || c.w >= uint64(c.cfg.MaxWindow) { // #nosec G115 -- >= Window > 0
		return
	}
	g := &c.grow
	if g.t0.IsZero() {
		g.t0, g.a0 = now, c.acked
		return
	}
	n := c.acked - g.a0
	rtt := rt.MinRTT()
	if n < c.w || rtt <= 0 {
		return
	}
	if limited && now.Sub(g.t0) < time.Duration(4*float64(n)/float64(c.w)*float64(rtt)) {
		c.w = min(2*c.w, uint64(c.cfg.MaxWindow)) // #nosec G115 -- as above
	}
	g.t0, g.a0 = now, c.acked
}
