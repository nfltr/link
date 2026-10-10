// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package bytequeue is a FIFO of byte slices bounded by total bytes. A TCP
// tunnel session puts one between the shared stream's receive loop (Push)
// and the goroutine writing to the session's conn (Pop), so a slow conn
// stalls only its own session until it holds limit bytes.
package bytequeue

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Queue is safe for one or more pushers and poppers.
type Queue struct {
	mu     sync.Mutex
	cond   sync.Cond
	bufs   [][]byte
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
// bytes (so it holds at most limit-1 plus one slice).
func New(limit int) *Queue {
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
	q.bufs = append(q.bufs, b)
	q.size += len(b)
	q.cond.Broadcast()
	return true
}

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
	q.bufs = append(q.bufs, b)
	q.size += len(b)
	q.cond.Broadcast()
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
	for len(q.bufs) == 0 && !q.closed && (stop == nil || !stop.Load()) {
		q.cond.Wait()
	}
	if len(q.bufs) == 0 || stop != nil && stop.Load() {
		q.mu.Unlock()
		return nil, false
	}
	b := q.bufs[0]
	q.bufs[0] = nil
	q.bufs = q.bufs[1:]
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
	for len(q.bufs) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.bufs) == 0 {
		q.mu.Unlock()
		return dst, false
	}
	n, k := 0, 0
	for _, b := range q.bufs {
		if k > 0 && n+len(b) > max {
			break
		}
		n += len(b)
		dst = append(dst, b)
		q.bufs[k] = nil
		k++
	}
	q.bufs = q.bufs[k:]
	q.size -= n
	q.pops++
	q.cond.Broadcast()
	q.mu.Unlock()
	q.popped(n)
	return dst, true
}

// Close stops further pushes and wakes every waiter. Already queued slices
// stay poppable, so a writer can deliver them before it closes its conn.
func (q *Queue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}
