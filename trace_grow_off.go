// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package link

// growTrace and growSess are the growth trace's (trace_grow_on.go); no size here.
type (
	growTrace struct{}
	growSess  struct{}
)

func traceFixedWindows() bool { return false }

func traceGrowUp(*Conn)                                        {}
func (*Credit) traceGrowOpen(string, *sessionCredit)           {}
func (*Credit) traceGrowDrop(string)                           {}
func (*Credit) traceGrowSend(string, *sessionCredit, int)      {}
func (*sessionCredit) traceGrowSent(int)                       {}
func (*Credit) traceGrowBlock(string, *sessionCredit)          {}
func (*Credit) traceGrowData(string, *sessionCredit, int)      {}
func (*Credit) traceGrowBlocked(string, *sessionCredit, int)   {}
func (*Credit) traceGrowRead(string, *sessionCredit, int, int) {}
func (*Credit) traceGrowWin(string, *sessionCredit, int)       {}
