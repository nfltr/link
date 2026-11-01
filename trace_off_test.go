// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package link

import (
	"testing"
	"unsafe"
)

// TestTraceOffCostsNothing: without -tags linktrace the recorder is a
// constant false (every call compiled away) and its state takes no room.
func TestTraceOffCostsNothing(t *testing.T) {
	if traceOn || unsafe.Sizeof(traceState{}) != 0 {
		t.Fatal("the trace recorder is on, or takes room, without -tags linktrace")
	}
}
