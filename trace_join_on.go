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

// Joint trace (go test -tags linktrace, LINKTRACE_JOIN=<dir>): each joint
// (Join, Forward) writes one JSON line per step of spec/JoinClose.tla it
// takes, to <dir>/joinclose-<n>.ndjson (n counts the process's joints);
// spec/TraceJoinClose.tla checks it (make spec-traces). Its side A is
// s[0] (the origin), B s[1] (the onward side). The peers' and the
// network's steps are not logged: the check finds them.
//
//	D1Copy  a write of B's bytes onto A (one model byte), ok: 1, or 0
//	        (it failed: A was closed)
//	D1End   B's bytes ended cleanly (under the joint's lock, where its
//	        end is decided), kind: fin (A's half close) or close
//	D0Fail  a write of A's bytes onto B failed (B's peer closed it whole;
//	        under the joint's lock), now: 1 (A closed now: direction 1
//	        ended) or 0 (whole: direction 1's end closes A)
type joinTrace struct{ path string }

// joinWTrace is a joinWriter's: its joint, for direction 1's writes.
type joinWTrace struct{ j *joint }

var joinSeq atomic.Int64

// traceJoinUp starts j's trace when LINKTRACE_JOIN names a directory.
func traceJoinUp(j *joint) {
	if dir := os.Getenv("LINKTRACE_JOIN"); dir != "" {
		j.jtr.path = filepath.Join(dir, fmt.Sprintf("joinclose-%d.ndjson", joinSeq.Add(1)))
	}
}

// traceJoinW names w's joint when w writes direction 1 (onto A).
func (w *joinWriter) traceJoinW(j *joint, i int) {
	if i == 1 && j.jtr.path != "" {
		w.jt.j = j
	}
}

// traceCopy logs a write of direction 1.
func (w *joinWriter) traceCopy(err error) {
	if j := w.jt.j; j != nil {
		ok := 1
		if err != nil {
			ok = 0
		}
		traceWrite(j.jtr.path, map[string]any{"ev": "D1Copy", "ok": ok})
	}
}

// traceOver logs direction i's end where the joint decides it (j.mu
// held): direction 1's clean end, and how it closes A.
func (j *joint) traceOver(i int, err, werr error) {
	if j.jtr.path == "" || i != 1 || err != nil || werr != nil || j.err != nil {
		return
	}
	kind := "close"
	if j.s[1].peerHalf.Load() && !j.whole {
		kind = "fin"
	}
	traceWrite(j.jtr.path, map[string]any{"ev": "D1End", "kind": kind})
}

// traceFail logs direction 0's failed write onto B (j.mu held).
func (j *joint) traceFail(i int, now bool) {
	if j.jtr.path == "" || i != 0 {
		return
	}
	n := 0
	if now {
		n = 1
	}
	traceWrite(j.jtr.path, map[string]any{"ev": "D0Fail", "now": n})
}
