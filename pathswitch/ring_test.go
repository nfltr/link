// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

// TestRingMatchesASlice: random pushes and drops keep the bytes and
// positions of a plain slice, and the ring's array stays within its
// bound (max) whenever what it holds does.
func TestRingMatchesASlice(t *testing.T) {
	for _, w := range []int{1, 7, 1 << 10, 64 << 10} {
		r := rand.New(rand.NewPCG(uint64(w), 1))
		q := ring{max: w}
		var model []byte
		next := byte(0)
		for i := 0; i < 20000; i++ {
			if r.IntN(2) == 0 {
				p := make([]byte, r.IntN(min(w, maxFrame)+1))
				for j := range p {
					p[j] = next
					next++
				}
				if len(model)+len(p) > w {
					continue // a sender never holds more than W
				}
				q.push(p)
				model = append(model, p...)
			} else {
				n := r.IntN(len(model) + 1)
				q.drop(n)
				model = model[n:]
			}
			if q.len() != len(model) {
				t.Fatalf("w=%d step %d: len %d, want %d", w, i, q.len(), len(model))
			}
			if len(q.b) > w {
				t.Fatalf("w=%d step %d: capacity %d over W", w, i, len(q.b))
			}
			off := r.IntN(len(model) + 1)
			k := r.IntN(len(model) - off + 1)
			a, b := q.peek(off, k)
			if got := append(append([]byte(nil), a...), b...); !bytes.Equal(got, model[off:off+k]) {
				t.Fatalf("w=%d step %d: peek(%d, %d) differs", w, i, off, k)
			}
		}
	}
}

// TestRingGrowsPastMax: a receiver may hold more than its bound (bytes
// a RESUME acknowledged, unread); the ring still grows by doubling.
func TestRingGrowsPastMax(t *testing.T) {
	q := ring{max: 4}
	for i := 0; i < 100; i++ {
		q.push([]byte{byte(i)})
	}
	if q.len() != 100 || len(q.b) > 256 {
		t.Fatalf("len %d cap %d", q.len(), len(q.b))
	}
	a, b := q.peek(0, 100)
	got := append(append([]byte(nil), a...), b...)
	for i := range got {
		if got[i] != byte(i) {
			t.Fatalf("byte %d is %d", i, got[i])
		}
	}
}
