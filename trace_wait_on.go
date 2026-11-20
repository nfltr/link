// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package link

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// Wait trace (go test -tags linktrace, LINKTRACE_WAIT=<dir>): each
// session's send credit (each Credit.open) writes one JSON line per step
// of spec/CreditWaiters.tla, to <dir>/wait-<n>.ndjson (n counts the
// process's opens); spec/TraceCreditWaiters.tla checks it (make
// spec-traces). Every line is written under the Credit's lock. Each call
// of acquire is a waiter of its own (w: the process's count of them).
//
//	Open   n: the peer's window (the credit's start)
//	Check  w, n: bytes wanted, cr: the credit after, took: 1 (taken) or 0 (it waits)
//	Grant  n: bytes, cr: after (Grant, and refund's credit given back)
type waitTrace struct{ path string }

var waitSeq, waiterSeq atomic.Int64

// traceWaitUp starts s's wait trace when LINKTRACE_WAIT names a directory.
func traceWaitUp(s *sessionCredit, window int) {
	dir := os.Getenv("LINKTRACE_WAIT")
	if dir == "" {
		return
	}
	s.wt.path = filepath.Join(dir, fmt.Sprintf("wait-%d.ndjson", waitSeq.Add(1)))
	traceWrite(s.wt.path, map[string]any{"ev": "Open", "n": window})
}

// traceWaiter names an acquire's waiter.
func traceWaiter() int { return int(waiterSeq.Add(1)) }

func (s *sessionCredit) traceWait(ev string, w, n int, took bool) {
	if s.wt.path == "" {
		return
	}
	line := map[string]any{"ev": ev, "n": n, "cr": s.n}
	if ev == "Check" {
		line["w"], line["took"] = w, 0
		if took {
			line["took"] = 1
		}
	}
	traceWrite(s.wt.path, line)
}
