// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

// ring is a byte queue over one array used circularly: a push copies its
// bytes in once, a drop moves the head, and nothing slides, so a byte
// costs one copy whatever the window. The fifo before it moved every kept
// byte to the front on an append past capacity: O(W) per acknowledgement,
// 34 % of a 1.8 Gbit/s sender's CPU at W = 16 MiB (lane SPEED-IO; guard
// BenchmarkWindow16M within 1.5x of BenchmarkWindow1M, TestRingMatchesASlice).
// It grows by doubling, to at most max while what it holds fits in max.
type ring struct {
	b       []byte
	head, n int
	max     int
}

func (q *ring) len() int { return q.n }

func (q *ring) push(p []byte) {
	if len(p) == 0 {
		return
	}
	if q.n+len(p) > len(q.b) {
		q.grow(q.n + len(p))
	}
	k := copy(q.b[(q.head+q.n)%len(q.b):], p)
	copy(q.b, p[k:])
	q.n += len(p)
}

func (q *ring) grow(need int) {
	c := max(need, 2*len(q.b))
	if need <= q.max {
		c = min(c, q.max)
	}
	b := make([]byte, c)
	a, t := q.peek(0, q.n)
	copy(b[copy(b, a):], t)
	q.b, q.head = b, 0
}

func (q *ring) drop(n int) {
	q.n -= n
	if q.n == 0 {
		q.head = 0
	} else {
		q.head = (q.head + n) % len(q.b)
	}
}

// peek returns the k bytes from position off as at most two slices of
// the array.
func (q *ring) peek(off, k int) (a, b []byte) {
	if k == 0 {
		return nil, nil
	}
	s := (q.head + off) % len(q.b)
	if s+k <= len(q.b) {
		return q.b[s : s+k], nil
	}
	return q.b[s:], q.b[:k-(len(q.b)-s)]
}
