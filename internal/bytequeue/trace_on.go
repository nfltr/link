// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package bytequeue

// traceOn: go test -tags linktrace. A queue given a hook (SetTrace) calls
// it under its lock at each change a reader of it sees: link's
// Session.Read recorder (spec/TraceSessionRead.tla) logs them in the
// order the lock took them.
const traceOn = true

// traceHook gets ev (Push, Append: n bytes; Close; Pop: k slices of n
// bytes in all; Stopped: PopUnless gave up for its stop flag; Ended: the
// queue closed and drained) and unless: the pop was PopUnless's with a
// stop flag.
type traceHook func(ev string, unless bool, k, n int)

// SetTrace sets the hook; before the first push.
func (q *Queue) SetTrace(fn func(ev string, unless bool, k, n int)) { q.tr = fn }

// Locked runs f under the queue's lock: a stop flag stored there is
// ordered with the pops that look at it.
func (q *Queue) Locked(f func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	f()
}

func (q *Queue) trace(ev string, unless bool, k, n int) {
	if q.tr != nil {
		q.tr(ev, unless, k, n)
	}
}
