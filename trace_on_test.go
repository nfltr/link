// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package link

import (
	"net"
	"testing"
)

// TestTraceFileNamesTheConnection: both sides of one connection write one
// file, and two connections from one local port (Linux reuses an ephemeral
// port towards another destination) write two: one file holding two
// connections' steps is a trace no single connection took (the nightly
// spec-traces rejection of 2026-11-17, a keepAlive Fire with another
// connection's state).
func TestTraceFileNamesTheConnection(t *testing.T) {
	a := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 57564}
	l1 := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40001}
	l2 := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40002}
	if d, l := tracePair(a, l1, true), tracePair(l1, a, false); d != l {
		t.Fatalf("one connection's sides name %q and %q", d, l)
	}
	if one, two := tracePair(a, l1, true), tracePair(a, l2, true); one == two {
		t.Fatalf("two connections from %v both name %q", a, one)
	}
}
