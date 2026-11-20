// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package link

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Relayed close trace (go test -tags linktrace, LINKTRACE_RELAYED=<dir>):
// a test marks the four sessions of one relayed path (traceRelayed: A the
// writer, J's sessions in (from A) and out (to C), C the far end); each
// step of spec/RelayedClose.tla one of them takes is one JSON line in
// <dir>/relayedclose-<test>.ndjson, written under trace_on.go's
// process-wide lock: a frame's send is logged before its write, its
// receipt after its read, so the file's order is causal (all three
// parties run in the test's process). spec/TraceRelayedClose.tla checks
// it (make spec-traces); the peers' bytes are not logged (the check
// finds them). Fields:
//
//	ev   the action; t: a frame received (fin, abort, eack); ret: AClose's
//	     wait or err; r: CloseWriteSync's result (ARet: clean or err);
//	     eof: D1End's clean end ("1") or failure ("0")
type rcKey struct {
	m   *Mux
	sid string
}

var rc struct {
	mu    sync.Mutex
	path  string
	roles map[rcKey]string // A, Jin, Jout, C
	rst   map[rcKey]bool   // A's reset was logged (AReset, AIdle)
}

// traceRelayed marks a relayed path's sessions for test, when
// LINKTRACE_RELAYED names a directory; traceRelayedOff ends it.
func traceRelayed(test string, a, jin, jout, c *Session) {
	dir := os.Getenv("LINKTRACE_RELAYED")
	if dir == "" {
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.path = filepath.Join(dir, "relayedclose-"+strings.ReplaceAll(test, "/", "-")+".ndjson")
	rc.roles = map[rcKey]string{{a.m, a.id}: "A", {jin.m, jin.id}: "Jin", {jout.m, jout.id}: "Jout", {c.m, c.id}: "C"}
	rc.rst = map[rcKey]bool{}
}

func traceRelayedOff() {
	rc.mu.Lock()
	rc.path, rc.roles = "", nil
	rc.mu.Unlock()
}

// traceRC logs ev when sid on m is the marked session of role, with kv's
// field pairs.
func traceRC(m *Mux, sid, role, ev string, kv ...string) {
	rc.mu.Lock()
	k := rcKey{m, sid}
	path, e := rc.path, ev
	if rc.roles[k] != role {
		e = ""
	} else if e == "AReset" || e == "AIdle" {
		if rc.rst[k] { // AIdle's own Reset, or a second one
			e = ""
		}
		rc.rst[k] = true
	}
	rc.mu.Unlock()
	if e == "" {
		return
	}
	traceRCWrite(path, e, kv)
}

// traceRCEnv logs a step of the environment the test takes (CCut, CMute).
func traceRCEnv(ev string) {
	rc.mu.Lock()
	path := rc.path
	rc.mu.Unlock()
	if path != "" {
		traceRCWrite(path, ev, nil)
	}
}

func traceRCWrite(path, ev string, kv []string) {
	line := map[string]any{"ev": ev}
	for i := 0; i+1 < len(kv); i += 2 {
		line[kv[i]] = kv[i+1]
	}
	traceWrite(path, line)
}

// traceRCFrame logs a non-data frame a marked session received: an ENDACK
// (A's ARecv, J's JAck) or a CLOSE (fin: a half close; abort: a reset or
// an ABORT; close: a clean whole close, which no step of the model is).
// ENDACKs for the other direction's ends (J's in, C) are no step of it.
func traceRCFrame(m *Mux, f rxFrame) {
	t := "fin"
	switch {
	case f.EndAck:
		traceRC(m, f.SID, "A", "ARecv", "t", "eack")
		traceRC(m, f.SID, "Jout", "JAck")
		return
	case !f.Close:
		return
	case f.Half:
	case f.Abort != nil || f.Reset:
		t = "abort"
	default:
		t = "close"
	}
	traceRC(m, f.SID, "A", "ARecv", "t", t)
	traceRC(m, f.SID, "Jin", "JRecv", "t", t)
	traceRC(m, f.SID, "C", "CRecv", "t", t)
}

// traceRCClose logs A's half close (AClose): its FIN goes now (wait), or
// the session failed already (err).
func traceRCClose(s *Session) {
	r := "wait"
	if s.err.Load() != nil {
		r = "err"
	}
	traceRC(s.m, s.id, "A", "AClose", "ret", r)
}

// traceRCRet logs what A's wait for its ENDACK returned (ARet).
func traceRCRet(s *Session, err error) {
	r := "clean"
	if err != nil {
		r = "err"
	}
	traceRC(s.m, s.id, "A", "ARet", "r", r)
}
