// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package link

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Write trace (go test -tags linktrace, LINKTRACE_WRITE=<dir>): each
// Session writes one JSON line per step of spec/SessionWrite.tla its
// writes waiting for credit take, to <dir>/write-<n>.ndjson (n counts the
// process's sessions); spec/TraceSessionWrite.tla checks it (make
// spec-traces). Each line is written under the lock of its step: wdMu
// (Set, Fire, Take, Past) or Credit.mu (Open, Check, Grant); Wake is
// Credit.acquire's select. w names the writer: each call of
// Session.acquire (the process's count of them).
//
//	Open   cr: the session's credit at its first step
//	Set    kind: past, future (its timer armed) or zero (no timer)
//	Fire   the deadline's timer, its generation current
//	Take   w: writeWait, ch: the channel it holds (the session's count of
//	       those made, from 1; 0: nil)
//	Past   w, res: timeout (wd.past set) or go
//	Check  w, n: bytes wanted, took: 1 (taken) or 0 (it waits), cr: after
//	Wake   w: the deadline's channel closed (stopped)
//	Grant  n: bytes (a window update, or a refund), cr: after
type writeTrace struct {
	path string
	once sync.Once // the Open line, from the session's first step
	ch   int       // the deadline channels made (wdMu): the model's numbers
}

// writeStep is a call of Session.acquire: its writer in s's trace.
type writeStep struct {
	path string
	w    int
}

// creditTrace is a session credit's write trace: the Session's path,
// set by its first step.
type creditTrace struct{ path string }

var writeSeq, writerSeq atomic.Int64

// traceWriteUp starts s's write trace when LINKTRACE_WRITE names a
// directory.
func traceWriteUp(s *Session) {
	if dir := os.Getenv("LINKTRACE_WRITE"); dir != "" {
		s.wtr.path = filepath.Join(dir, fmt.Sprintf("write-%d.ndjson", writeSeq.Add(1)))
	}
}

// writeOpen writes the Open line once, before s's first step, and gives
// s's credit the path for its Check and Grant lines. Callers may hold
// wdMu (wdMu before Credit.mu).
func (s *Session) writeOpen() bool {
	if s.wtr.path == "" {
		return false
	}
	s.wtr.once.Do(func() {
		c := s.m.c.credit
		c.mu.Lock()
		cr := 0
		if sc := c.sessions[s.id]; sc != nil {
			sc.wtr.path, cr = s.wtr.path, sc.n
		}
		traceWrite(s.wtr.path, map[string]any{"ev": "Open", "cr": cr})
		c.mu.Unlock()
	})
	return true
}

// traceSW writes a Session-side line (under wdMu).
func (s *Session) traceSW(line map[string]any) {
	if s.writeOpen() {
		traceWrite(s.wtr.path, line)
	}
}

// writeStepUp names a new writer: a call of acquire.
func (s *Session) writeStepUp() writeStep {
	if !s.writeOpen() {
		return writeStep{}
	}
	return writeStep{path: s.wtr.path, w: int(writerSeq.Add(1))}
}

// writeSet logs SetWriteDeadline's kind (under wdMu).
func (s *Session) writeSet(past, armed bool) {
	kind := "zero"
	switch {
	case past:
		kind = "past"
	case armed:
		kind = "future"
	}
	s.traceSW(map[string]any{"ev": "Set", "kind": kind})
}

// writeFire logs the deadline's timer (under wdMu).
func (s *Session) writeFire() { s.traceSW(map[string]any{"ev": "Fire"}) }

// writeNewCh counts a deadline channel made (under wdMu).
func (s *Session) writeNewCh() { s.wtr.ch++ }

// traceTake logs writeWait (under wdMu).
func (s *Session) traceTake(ws writeStep) {
	if ws.path == "" {
		return
	}
	ch := 0
	if s.wdCh != nil {
		ch = s.wtr.ch
	}
	traceWrite(ws.path, map[string]any{"ev": "Take", "w": ws.w, "ch": ch})
}

// writePast is acquire's look at wd.past: under wdMu when traced, so the
// trace orders it with Set and Fire.
func (s *Session) writePast(ws writeStep) bool {
	if ws.path == "" {
		return s.wd.past.Load()
	}
	s.wdMu.Lock()
	defer s.wdMu.Unlock()
	past := s.wd.past.Load()
	res := "go"
	if past {
		res = "timeout"
	}
	traceWrite(ws.path, map[string]any{"ev": "Past", "w": ws.w, "res": res})
	return past
}

// traceCheck logs Credit.acquire's look at the credit (under Credit.mu).
func (sc *sessionCredit) traceCheck(ws writeStep, n int, took bool) {
	if ws.path == "" {
		return
	}
	t := 0
	if took {
		t = 1
	}
	traceWrite(ws.path, map[string]any{"ev": "Check", "w": ws.w, "n": n, "took": t, "cr": sc.n})
}

// traceWake logs Credit.acquire's stop.
func (ws writeStep) traceWake() {
	if ws.path != "" {
		traceWrite(ws.path, map[string]any{"ev": "Wake", "w": ws.w})
	}
}

// traceGrant logs credit given back (under Credit.mu).
func (sc *sessionCredit) traceGrant(n int) {
	if sc.wtr.path != "" {
		traceWrite(sc.wtr.path, map[string]any{"ev": "Grant", "n": n, "cr": sc.n})
	}
}
