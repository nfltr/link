// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linkdebug

package link

import (
	"testing"
	"unsafe"
)

// TestDebugOffAddsNothing: without the linkdebug tag a Session carries no
// debug byte (the field is zero size and not last, so no padding either);
// the hooks are behind the constant debugOn, which the compiler drops.
// TestSteadyStateAllocs holds the allocations, and BenchmarkLatency and
// BenchmarkThroughput the time (README "Debug mode": against v0.10.11).
func TestDebugOffAddsNothing(t *testing.T) {
	var s Session
	if unsafe.Sizeof(s.dbg) != 0 || unsafe.Offsetof(s.dbg) != unsafe.Offsetof(s.id) {
		t.Fatalf("debugState takes %d bytes in a default build", unsafe.Offsetof(s.id)-unsafe.Offsetof(s.dbg))
	}
}
