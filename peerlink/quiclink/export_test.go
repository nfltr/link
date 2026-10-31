// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

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
