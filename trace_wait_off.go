// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package link

// waitTrace is a session credit's wait trace (trace_wait_on.go); no size here.
type waitTrace struct{}

func traceWaitUp(*sessionCredit, int)                   {}
func traceWaiter() int                                  { return 0 }
func (*sessionCredit) traceWait(string, int, int, bool) {}
