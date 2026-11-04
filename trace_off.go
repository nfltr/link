// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package link

import "time"

// traceOn is false without the linktrace tag: every trace call sits behind
// `if traceOn`, so the compiler drops it with its arguments, and a Credit's
// traceState has no size (trace_on.go has the recorder; spec/README.md
// "Trace validation").
const traceOn = false

type traceState struct{}

// traceEvent is one line of a trace (trace_on.go).
type traceEvent struct {
	ev, sid, t      string
	n, credit, held int
	grant           int
}

func traceUp(*Conn)                                                              {}
func traceLive(*Conn, string, time.Duration, time.Duration, time.Duration, bool) {}
func traceEv(traceState, traceEvent)                                             {}
