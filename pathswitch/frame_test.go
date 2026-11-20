// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"bufio"
	"bytes"
	"testing"
)

// encodeFrame writes f as the side that sends it does (conn.go).
func encodeFrame(f frame) []byte {
	switch f.t {
	case fData:
		return appendData(nil, f.data)
	case fOpen:
		return appendOpen(nil, f.epoch, f.n, f.abort, f.token)
	case fAbort:
		return []byte{fAbort}
	default:
		return appendCount(nil, f.t, f.n)
	}
}

// FuzzReadFrame: a peer's bytes never panic readFrame, a frame it accepts
// is within the DATA and token bounds, and encoding it again reads back the
// same frame.
func FuzzReadFrame(f *testing.F) {
	for _, fr := range []frame{{t: fData, data: []byte("x")}, {t: fData, data: make([]byte, maxFrame)},
		{t: fAck, n: 1 << 40}, {t: fResume, n: 7}, {t: fEnd}, {t: fAbort},
		{t: fOpen, epoch: 3, n: 9, abort: true, token: make([]byte, maxToken)}, {t: fOpen, epoch: 1}} {
		f.Add(encodeFrame(fr))
	}
	f.Add([]byte{fData, 0x81, 0x80, 0x01})                                                // DATA over maxFrame
	f.Add([]byte{fOpen, 1, 0, 2, 0})                                                      // an unknown flag
	f.Add([]byte{fOpen, 1, 0, 0, 0x81, 0x02})                                             // a token over maxToken
	f.Add([]byte{fAck, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}) // uvarint overflow
	f.Fuzz(func(t *testing.T, b []byte) {
		r := bufio.NewReader(bytes.NewReader(b))
		var buf []byte
		for {
			fr, err := readFrame(r, &buf)
			if err != nil {
				return
			}
			if (fr.t == fData && (len(fr.data) == 0 || len(fr.data) > maxFrame)) || len(fr.token) > maxToken {
				t.Fatalf("accepted %c of %d data bytes, %d token bytes", fr.t, len(fr.data), len(fr.token))
			}
			var buf2 []byte
			again, err := readFrame(bufio.NewReader(bytes.NewReader(encodeFrame(fr))), &buf2)
			if err != nil || again.t != fr.t || again.n != fr.n || again.epoch != fr.epoch || again.abort != fr.abort ||
				!bytes.Equal(again.token, fr.token) || !bytes.Equal(again.data, fr.data) {
				t.Fatalf("round trip of %+v: %+v %v", fr, again, err)
			}
		}
	})
}
