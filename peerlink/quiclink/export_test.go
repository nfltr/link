// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

import "time"

// PeerGone reports whether peer sent GOAWAY on a connection it dialled
// in to n.
func PeerGone(n *Node, peer string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	for c := range n.served {
		if c.peer == peer && c.gone.Load() {
			return true
		}
	}
	return false
}

// Draining reports whether n's Drain began.
func Draining(n *Node) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.draining
}

// Peers is how many peers n keeps dialled state for.
func Peers(n *Node) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.dialled)
}

// SetWatchTickHook makes f run in each connection's liveness check as a
// tick came, with the connection's peer; nil removes it.
func SetWatchTickHook(f func(peer string)) {
	if f == nil {
		testHookWatchTick.Store(nil)
		return
	}
	testHookWatchTick.Store(&f)
}

// ErrNoDatagramsForTest, ProbeForTest: errNoDatagrams, probe.
var (
	ErrNoDatagramsForTest = errNoDatagrams
	ProbeForTest          = probe
)

// DatagramsDroppedForTest is n's count of dropped DATAGRAM frames.
func DatagramsDroppedForTest(n *Node) uint64 { return n.dgDropped.Load() }

// SetEndAckIdleForTest sets n's CloseWriteSync idle bound.
func SetEndAckIdleForTest(n *Node, d time.Duration) { n.endAckIdle = d }

// TraceMuteForTest records that s's side reads nothing from now on
// (spec/EndAckLeg.tla RMute; -tags linktrace).
func TraceMuteForTest(s *Session) {
	if traceOn {
		traceEA(s, "RMute")
	}
}
