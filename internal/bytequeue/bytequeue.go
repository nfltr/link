// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package bytequeue is a FIFO of byte slices bounded by total bytes. A TCP
// tunnel session puts one between the shared stream's receive loop (Push)
// and the goroutine writing to the session's conn (Pop), so a slow conn
// stalls only its own session until it holds limit bytes.
package bytequeue

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Queue is safe for one or more pushers and poppers.
type Queue struct {
	mu     sync.Mutex
	cond   sync.Cond
	bufs   [][]byte // queued from head on; a drained queue keeps its array
	head   int
	size   int
	limit  int
	closed bool
	onPop  func(n int)
	pops   uint64 // pops so far: PushWithin's progress
}

// SetLimit changes the limit; set it before the first push.
func (q *Queue) SetLimit(limit int) { q.limit = limit }

// OnPop makes every pop report how many bytes it took, after the queue is
// unlocked (a data connection returns credit with it). Set it before the
// first push.
func (q *Queue) OnPop(fn func(n int)) { q.onPop = fn }

func (q *Queue) popped(n int) {
	if q.onPop != nil && n > 0 {
		q.onPop(n)
	}
}

// New returns a queue that accepts pushes while it holds fewer than limit
// bytes (so it holds at most limit-1 plus one slice). It panics on a limit
// under 1, a queue that would take nothing.
func New(limit int) *Queue {
	if limit < 1 {
		panic(fmt.Sprintf("bytequeue: New(%d): the limit is at least 1 byte", limit))
	}
	q := &Queue{limit: limit}
	q.cond.L = &q.mu
	return q
}

// Push appends b, waiting while the queue is full. It reports false, and
// drops b, once the queue is closed.
func (q *Queue) Push(b []byte) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.size >= q.limit && !q.closed {
		q.cond.Wait()
	}
	if q.closed {
		return false
	}
	q.pushLocked(b)
	return true
}

// pushLocked appends b, reusing the array's head that pops freed before
// it grows a new one.
func (q *Queue) pushLocked(b []byte) {
	if len(q.bufs) == cap(q.bufs) && q.head > 0 {
		n := copy(q.bufs, q.bufs[q.head:])
		clear(q.bufs[n:])
		q.bufs, q.head = q.bufs[:n], 0
	}
	q.bufs = append(q.bufs, b)
	q.size += len(b)
	q.cond.Broadcast()
}

// queued is how many slices are queued.
func (q *Queue) queued() int { return len(q.bufs) - q.head }

// ErrStalled is PushWithin's report of a queue that took nothing for the
// stall bound.
var ErrStalled = errors.New("bytequeue: reader took nothing for the stall bound")

// ErrClosed is PushWithin's report of a closed queue.
var ErrClosed = errors.New("bytequeue: closed")

// PushWithin is Push for a pusher that serves other queues too (a shared
// receive loop): it waits while the queue is full only as long as its
// reader keeps taking, and gives up (ErrStalled, b dropped) once the
// reader took nothing for stall; each pop starts the wait again. It
// reports ErrClosed, b dropped, once the queue is closed.
func (q *Queue) PushWithin(b []byte, stall time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size >= q.limit && !q.closed {
		// expired is set by the timer of the current wait only (gen): a
		// stale timer that fires as a pop restarts the wait sets nothing.
		var expired bool
		var gen int
		var t *time.Timer
		arm := func() {
			gen++
			g := gen
			t = time.AfterFunc(stall, func() {
				q.mu.Lock()
				if gen == g {
					expired = true
				}
				q.mu.Unlock()
				q.cond.Broadcast()
			})
		}
		arm()
		defer func() { t.Stop() }()
		pops := q.pops
		for q.size >= q.limit && !q.closed {
			if q.pops != pops { // the reader took some: wait again
				pops, expired = q.pops, false
				t.Stop()
				arm()
			} else if expired {
				return ErrStalled
			}
			q.cond.Wait()
		}
	}
	if q.closed {
		return ErrClosed
	}
	q.pushLocked(b)
	return nil
}

// Pop removes and returns the oldest slice, waiting for one. After Close it
// still returns every slice pushed before, then false.
func (q *Queue) Pop() ([]byte, bool) { return q.PopUnless(nil) }

// Wake makes waiting poppers recheck their stop flag (PopUnless).
func (q *Queue) Wake() {
	q.mu.Lock()
	q.cond.Broadcast()
	q.mu.Unlock()
}

// PopUnless is Pop that also gives up, reporting false, once stop is set
// and Wake is called: a conn's read deadline.
func (q *Queue) PopUnless(stop *atomic.Bool) ([]byte, bool) {
	q.mu.Lock()
	for q.queued() == 0 && !q.closed && (stop == nil || !stop.Load()) {
		q.cond.Wait()
	}
	if q.queued() == 0 || stop != nil && stop.Load() {
		q.mu.Unlock()
		return nil, false
	}
	b := q.bufs[q.head]
	q.bufs[q.head] = nil
	q.trim(1)
	q.size -= len(b)
	q.pops++
	q.cond.Broadcast()
	q.mu.Unlock()
	q.popped(len(b))
	return b, true
}

// PopBatch waits like Pop, then appends to dst the queued slices up to max
// bytes in all, and always at least one. A writer that has fallen behind
// sends its backlog with one write (net.Buffers) instead of one per slice.
func (q *Queue) PopBatch(dst [][]byte, max int) ([][]byte, bool) {
	q.mu.Lock()
	for q.queued() == 0 && !q.closed {
		q.cond.Wait()
	}
	if q.queued() == 0 {
		q.mu.Unlock()
		return dst, false
	}
	return q.popBatchLocked(dst, max), true
}

// TryPopBatch is PopBatch that never waits: ok is false when nothing is
// queued, ended too once the queue is closed and drained.
func (q *Queue) TryPopBatch(dst [][]byte, max int) (out [][]byte, ok, ended bool) {
	q.mu.Lock()
	if q.queued() == 0 {
		ended = q.closed
		q.mu.Unlock()
		return dst, false, ended
	}
	return q.popBatchLocked(dst, max), true, false
}

// Ready reports whether a pop would not wait: slices are queued, or the
// queue is closed.
func (q *Queue) Ready() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queued() > 0 || q.closed
}

// popBatchLocked pops at least one slice (one is queued); it unlocks mu.
func (q *Queue) popBatchLocked(dst [][]byte, max int) [][]byte {
	n, k := 0, 0
	for i, b := range q.bufs[q.head:] {
		if k > 0 && n+len(b) > max {
			break
		}
		n += len(b)
		dst = append(dst, b)
		q.bufs[q.head+i] = nil
		k++
	}
	q.trim(k)
	q.size -= n
	q.pops++
	q.cond.Broadcast()
	q.mu.Unlock()
	q.popped(n)
	return dst
}

// trim drops the first k queued slices (already cleared); a drained
// queue starts its array over.
func (q *Queue) trim(k int) {
	if q.head += k; q.head == len(q.bufs) {
		q.bufs, q.head = q.bufs[:0], 0
	}
}

// Close stops further pushes and wakes every waiter. Already queued slices
// stay poppable, so a writer can deliver them before it closes its conn.
func (q *Queue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}
