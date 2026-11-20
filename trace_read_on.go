// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package link

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
)

// Read trace (go test -tags linktrace, LINKTRACE_READ=<dir>): each Session
// writes one JSON line per step of spec/SessionRead.tla its readers take,
// to <dir>/read-<n>.ndjson (n counts the process's sessions);
// spec/TraceSessionRead.tla checks it (make spec-traces). Each line is
// written under the lock of its step: the queue's (Push, Append, Close,
// Pop and the deadline's Fire and Clear, stored there; Pump, and Begin
// or Refuse, looked at there too) or rmu (Copy, End, WTake); WWrite and
// WRel are WriteTo's and Pump's drain's own. r names the reader: its
// goroutine for Read, minus it for WriteTo (one goroutine may do both,
// one after the other: two readers of the model), -1 for Pump (its
// drain's runs are goroutines of their own; goroutine 1 runs no test).
//
//	Push, Append  n: bytes (a new slice; onto the newest queued one), s: the
//	              slices pushed so far, b: the bytes (TLC reads a byte's slice
//	              off them, no recursion over the trace)
//	Close         the queue's end
//	Fire, Clear   the read deadline's flag set, cleared
//	Begin         r, n: len(s.rest)
//	Refuse        r: a Read once Pump was called (errPumped)
//	Pump          r: -1, Pump called
//	Pop           r, res (data, timeout, eof), k: slices, n: bytes
//	Copy          r, n: bytes copied, rel: 1 when s.cur went back to the pool
//	End           r, res: what Read returned (data, timeout, eof)
//	WTake         r, n: len of the tail takeRest took
//	WWrite        r, n: bytes written
//	WRel          r: what it wrote released (or kept by an OwnedWriter)
type readTrace struct {
	path  string
	drain atomic.Int64 // the goroutine of Pump's drain's current run
}

// pumpR is Pump's reader in the trace.
const pumpR = -1

var readSeq atomic.Int64

// traceReadUp starts s's read trace when LINKTRACE_READ names a directory.
func traceReadUp(s *Session) {
	dir := os.Getenv("LINKTRACE_READ")
	if dir == "" {
		return
	}
	s.rt.path = filepath.Join(dir, fmt.Sprintf("read-%d.ndjson", readSeq.Add(1)))
	var slices, bytes int // under the queue's lock, as the hook
	s.q.SetTrace(func(ev string, unless bool, k, n int) {
		r := s.traceR(!unless)
		switch ev {
		case "Pop":
			s.readEv(map[string]any{"ev": "Pop", "r": r, "res": "data", "k": k, "n": n})
		case "Stopped":
			s.readEv(map[string]any{"ev": "Pop", "r": r, "res": "timeout", "k": 0, "n": 0})
		case "Ended":
			s.readEv(map[string]any{"ev": "Pop", "r": r, "res": "eof", "k": 0, "n": 0})
		case "Push", "Append":
			if ev == "Push" {
				slices++
			}
			bytes += n
			s.readEv(map[string]any{"ev": ev, "n": n, "s": slices, "b": bytes})
		default:
			s.readEv(map[string]any{"ev": ev, "n": n})
		}
	})
}

func (s *Session) readEv(line map[string]any) {
	if s.rt.path != "" {
		traceWrite(s.rt.path, line)
	}
}

// traceRead logs a step of a Read (r: this goroutine) or of WriteTo (-it).
func (s *Session) traceRead(ev string, writeTo bool, n int, rel bool) {
	if s.rt.path == "" {
		return
	}
	line := map[string]any{"ev": ev, "r": s.traceR(writeTo), "n": n}
	if ev == "Copy" {
		line["rel"] = 0
		if rel {
			line["rel"] = 1
		}
	}
	s.readEv(line)
}

// traceR is the trace's reader: this goroutine for a Read, minus it for
// WriteTo, pumpR for Pump's drain.
func (s *Session) traceR(writeTo bool) int {
	g := goid()
	switch {
	case !writeTo:
		return g
	case int64(g) == s.rt.drain.Load():
		return pumpR
	default:
		return -g
	}
}

// traceDrain notes the goroutine of a run of Pump's drain (runs one at a
// time: pumping).
func (s *Session) traceDrain() { s.rt.drain.Store(int64(goid())) }

// pumpedRead is Read's look at s.pump, under rmu: Refuse, or Begin.
func (s *Session) pumpedRead() (pumped bool) {
	if s.rt.path == "" {
		return s.pump.Load() != nil
	}
	s.q.Locked(func() {
		if pumped = s.pump.Load() != nil; pumped {
			s.readEv(map[string]any{"ev": "Refuse", "r": goid()})
		} else {
			s.traceRead("Begin", false, len(s.rest), false)
		}
	})
	return pumped
}

// setPump is Pump's: s.pump set under the queue's lock, so the trace
// orders it with the Reads that look at it.
func (s *Session) setPump(p *pump) {
	if s.rt.path == "" {
		s.pump.Store(p)
		return
	}
	s.q.Locked(func() {
		s.pump.Store(p)
		s.readEv(map[string]any{"ev": "Pump", "r": pumpR})
	})
}

// traceReadEnd logs Read's return.
func (s *Session) traceReadEnd(err error) {
	res := "data"
	switch {
	case err == nil:
	case errors.Is(err, os.ErrDeadlineExceeded):
		res = "timeout"
	default:
		res = "eof"
	}
	s.readEv(map[string]any{"ev": "End", "r": goid(), "res": res})
}

// setDLPast stores the read deadline's flag under the queue's lock, so the
// trace orders it with the pops that look at it; a change is Fire or Clear.
func (s *Session) setDLPast(v bool) {
	if s.rt.path == "" {
		s.dlPast.Store(v)
		return
	}
	s.q.Locked(func() {
		if s.dlPast.Swap(v) != v {
			ev := "Clear"
			if v {
				ev = "Fire"
			}
			s.readEv(map[string]any{"ev": ev})
		}
	})
}

// goid is the calling goroutine's id (runtime.Stack's first line): the
// trace's reader.
func goid() int {
	var b [64]byte
	f := bytes.Fields(b[:runtime.Stack(b[:], false)])
	id, err := strconv.Atoi(string(f[1]))
	if err != nil {
		panic(err)
	}
	return id
}
