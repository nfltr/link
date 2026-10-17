// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linkdebug

package link

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Debug mode (go build -tags linkdebug; README "Debug mode") reports
// misuse of a Session on stderr, each report with the stacks involved:
//   - a session garbage-collected while this side never closed it
//     (Close, CloseWrite, Reset, Abort, Refuse), with where it was opened;
//   - two Reads (Read, WriteTo) or two Writes (Write, WriteBuffers) at
//     once on one session, with both stacks;
//   - with LINKDEBUG_IDLE set (a time.Duration), a session open that long
//     with no Read or Write, once, with where it was opened.
const debugOn = true

// debugReportTo, when set (tests), takes the reports instead of stderr.
var debugReportTo atomic.Pointer[func(string)]

func debugReport(msg string) {
	if f := debugReportTo.Load(); f != nil {
		(*f)(msg)
		return
	}
	fmt.Fprint(os.Stderr, msg)
}

// debugIdle is LINKDEBUG_IDLE, read once.
var debugIdle = sync.OnceValue(func() time.Duration {
	d, _ := time.ParseDuration(os.Getenv("LINKDEBUG_IDLE"))
	if d > 0 {
		go debugWatchIdle(d)
	}
	return d
})

// debugOpen is every session the idle watch looks at.
var debugOpen sync.Map // *debugInfo -> struct{}

type debugState struct{ d *debugInfo }

type debugOp uint8

const (
	debugRead debugOp = iota
	debugWrite
)

// debugInfo is a session's debug state; it does not point at the
// session, so the session can be collected (runtime.AddCleanup).
type debugInfo struct {
	what     string // "session 3 (header "x") opened" / "accepted"
	opened   []uintptr
	released atomic.Bool
	lastIO   atomic.Int64 // unix nanos of the last Read or Write
	idle     atomic.Bool  // reported idle
	ops      [2]debugBusy
}

// debugBusy is one direction's calls in flight, and the first's stack.
type debugBusy struct {
	mu  sync.Mutex
	n   int
	pcs []uintptr
}

func debugOpened(s *Session) {
	how := "opened"
	if s.accepter {
		how = "accepted"
	}
	d := &debugInfo{what: fmt.Sprintf("session %s (header %q) %s", s.id, s.header, how), opened: debugCallers()}
	d.lastIO.Store(time.Now().UnixNano())
	s.dbg.d = d
	runtime.AddCleanup(s, func(d *debugInfo) {
		debugOpen.Delete(d)
		if !d.released.Load() {
			debugReport(fmt.Sprintf("link debug: %s and garbage-collected, never closed (Close, CloseWrite, Reset or Abort); opened at:\n%s", d.what, debugStack(d.opened)))
		}
	}, d)
	if debugIdle() > 0 {
		debugOpen.Store(d, struct{}{})
	}
}

func debugReleased(s *Session) {
	if d := s.dbg.d; d != nil {
		d.released.Store(true)
		debugOpen.Delete(d)
	}
}

// debugEnter records a Read or Write starting; the func it returns
// records its end.
func debugEnter(s *Session, op debugOp) func() {
	d := s.dbg.d
	if d == nil {
		return func() {}
	}
	d.lastIO.Store(time.Now().UnixNano())
	b := &d.ops[op]
	pcs := debugCallers()
	b.mu.Lock()
	b.n++
	if b.n == 1 {
		b.pcs = pcs
	} else {
		name := [...]string{"Read", "Write"}[op]
		debugReport(fmt.Sprintf("link debug: concurrent %ss on %s; the first at:\n%s\nthe second at:\n%s", name, d.what, debugStack(b.pcs), debugStack(pcs)))
	}
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		b.n--
		b.mu.Unlock()
	}
}

func debugWatchIdle(after time.Duration) {
	for range time.Tick(after / 2) {
		now := time.Now().UnixNano()
		debugOpen.Range(func(k, _ any) bool {
			d := k.(*debugInfo)
			if now-d.lastIO.Load() >= int64(after) && !d.idle.Swap(true) {
				debugReport(fmt.Sprintf("link debug: %s and open %v with no Read or Write; opened at:\n%s", d.what, after, debugStack(d.opened)))
			}
			return true
		})
	}
}

func debugCallers() []uintptr {
	pcs := make([]uintptr, 32)
	return pcs[:runtime.Callers(3, pcs)]
}

func debugStack(pcs []uintptr) string {
	var b strings.Builder
	frames := runtime.CallersFrames(pcs)
	for {
		f, more := frames.Next()
		fmt.Fprintf(&b, "\t%s\n\t\t%s:%d\n", f.Function, f.File, f.Line)
		if !more {
			return b.String()
		}
	}
}
