// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"fmt"
	"io"
	"testing"
)

// BenchmarkCleanEnd: one session's whole clean end, Open, 64 bytes, the
// opener's CloseWriteSync (its ENDACK: lane ENDACK moved it from the far
// end's receipt to its reader's EOF) and Close, the far end reading to EOF
// and closing; hops 0 direct, more relayed (a hop, Config.Hop as
// relay-core's, holds the ENDACK until the far end's).
func BenchmarkCleanEnd(b *testing.B) {
	for _, hops := range []int{0, 1} {
		b.Run(fmt.Sprintf("hops=%d", hops), func(b *testing.B) {
			r := newChainRig(b, rigOpts{n: hops + 1, hop: true}, func(_ []byte, s *Session) {
				_ = s.Accept()
				_, _ = io.Copy(io.Discard, s)
				_ = s.Close()
			})
			msg := make([]byte, 64)
			b.ResetTimer()
			for range b.N {
				s := r.open("end")
				if _, err := s.Write(msg); err != nil {
					b.Fatal(err)
				}
				if err := s.CloseWriteSync(); err != nil {
					b.Fatal(err)
				}
				_ = s.Close()
			}
		})
	}
}
