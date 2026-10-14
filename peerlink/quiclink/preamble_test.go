// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

import (
	"bytes"
	"testing"
)

// FuzzPreamble: a stream's preamble reads back as written, and arbitrary
// bytes never read past the length they state.
func FuzzPreamble(f *testing.F) {
	f.Add([]byte{0, 0})
	f.Add([]byte{0, 3, 'a', 'b', 'c', 'd'})
	f.Add([]byte{0xff, 0xff, 'x'})
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, wire []byte) {
		r := bytes.NewReader(wire)
		h, err := readPreamble(r)
		if err != nil {
			return
		}
		if read := len(wire) - r.Len(); read != 2+len(h) || !bytes.Equal(h, wire[2:read]) {
			t.Fatalf("read %d bytes, header %x of %x", read, h, wire)
		}
		if got, err := readPreamble(bytes.NewReader(appendPreamble(nil, h))); err != nil || !bytes.Equal(got, h) {
			t.Fatalf("round trip of %x: %x, %v", h, got, err)
		}
	})
}
