// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpclink

// Peers is how many peers n keeps a grpc.ClientConn for.
func Peers(n *Node) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.clients)
}
