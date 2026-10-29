// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

import (
	"bytes"
	"testing"
	"unicode/utf8"

	"github.com/nfltr/link"
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

// FuzzAnswer: an accepter's answer (ALPNChain) parses only when whole and
// well formed, never past what it states, and a refusal reads back as
// written.
func FuzzAnswer(f *testing.F) {
	f.Add([]byte{0, 3})
	f.Add([]byte{0})
	f.Add(appendAnswer(nil, &link.Abort{Refused: true, Code: link.CodeHopLimit, Hops: 2, Msg: "loop"}))
	f.Add([]byte{1, 0, 0, 0, 0, 1, 0x04, 0x01})
	f.Add([]byte{1, 0, 0, 0, 0, 1, 0, 1, 0xff})
	f.Add([]byte{2, 0})
	f.Fuzz(func(t *testing.T, wire []byte) {
		a, hops, need, err := parseAnswer(wire)
		switch {
		case err != nil || need > 0:
			return
		case a == nil:
			if wire[0] != 0 || hops != wire[1] {
				t.Fatalf("accept from %x", wire)
			}
		default:
			if len(a.Msg) > link.MaxAbortMsg || !utf8.ValidString(a.Msg) {
				t.Fatalf("refusal %+v out of bounds", a)
			}
			if b := appendAnswer(nil, a); !bytes.Equal(b, wire[:len(b)]) {
				t.Fatalf("round trip of %x: %x", wire, b)
			}
		}
	})
}
